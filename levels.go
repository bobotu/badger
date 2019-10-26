/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/coocood/badger/options"
	"github.com/coocood/badger/protos"
	"github.com/coocood/badger/table"
	"github.com/coocood/badger/y"
	"github.com/ncw/directio"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"golang.org/x/time/rate"
)

type levelsController struct {
	nextFileID uint64 // Atomic

	// The following are initialized once and const.
	levels []*levelHandler
	kv     *DB

	cstatus compactStatus

	opt options.TableBuilderOptions
}

var (
	// This is for getting timings between stalls.
	lastUnstalled time.Time
)

// revertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest.  idMap is a set of table file id's that were read from the directory
// listing.
func revertToManifest(kv *DB, mf *Manifest, idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	// for id := range mf.Tables {
	// 	if _, ok := idMap[id]; !ok {
	// 		return fmt.Errorf("file does not exist for table %d", id)
	// 	}
	// }

	// 2. Delete files that shouldn't exist.
	// for id := range idMap {
	// 	if _, ok := mf.Tables[id]; !ok {
	// 		log.Infof("Table file %d not referenced in MANIFEST\n", id)
	// 		filename := table.NewFilename(id, kv.opt.Dir)
	// 		if err := os.Remove(filename); err != nil {
	// 			return y.Wrapf(err, "While removing table %d", id)
	// 		}
	// 	}
	// }

	return nil
}

func newLevelsController(kv *DB, mf *Manifest, opt options.TableBuilderOptions) (*levelsController, error) {
	y.Assert(kv.opt.NumLevelZeroTablesStall > kv.opt.NumLevelZeroTables)
	s := &levelsController{
		kv:     kv,
		levels: make([]*levelHandler, kv.opt.TableBuilderOptions.MaxLevels),
		opt:    opt,
	}
	s.cstatus.levels = make([]*levelCompactStatus, kv.opt.TableBuilderOptions.MaxLevels)

	for i := 0; i < kv.opt.TableBuilderOptions.MaxLevels; i++ {
		s.levels[i] = newLevelHandler(kv, i)
		if i == 0 {
			// Do nothing.
		} else if i == 1 {
			// Level 1 probably shouldn't be too much bigger than level 0.
			s.levels[i].maxTotalSize = kv.opt.LevelOneSize
		} else {
			s.levels[i].maxTotalSize = s.levels[i-1].maxTotalSize * int64(kv.opt.TableBuilderOptions.LevelSizeMultiplier)
		}
		s.cstatus.levels[i] = new(levelCompactStatus)
	}

	// Compare manifest against directory, check for existent/non-existent files, and remove.
	if err := revertToManifest(kv, mf, getIDMap(kv.opt.Dir)); err != nil {
		return nil, err
	}

	// Some files may be deleted. Let's reload.
	tables := make([][]*table.Table, kv.opt.TableBuilderOptions.MaxLevels)
	var maxFileID uint64
	for fileID, tableManifest := range mf.Tables {
		level := tableManifest.Level
		dir := kv.opt.Dir
		if int(level) >= kv.opt.RemoteLevelStart {
			dir = kv.opt.RemoteDir
		}
		fname := table.NewFilename(fileID, dir)
		var flags uint32 = y.Sync
		if kv.opt.ReadOnly {
			flags |= y.ReadOnly
		}

		isRemote := int(level) >= kv.opt.RemoteLevelStart
		t, err := table.OpenTable(fname, isRemote, kv.opt.TableLoadingMode, kv.cacheManger)
		if err != nil {
			closeAllTables(tables)
			return nil, errors.Wrapf(err, "Opening table: %q", fname)
		}
		if isRemote {
			ex, _ := fileExist(fname)
			kv.cacheManger.Add(fname, t, true, ex)
		}

		tables[level] = append(tables[level], t)

		if fileID > maxFileID {
			maxFileID = fileID
		}
	}
	s.nextFileID = maxFileID + 1
	for i, tbls := range tables {
		s.levels[i].initTables(tbls)
	}

	// Make sure key ranges do not overlap etc.
	if err := s.validate(); err != nil {
		_ = s.cleanupLevels()
		return nil, errors.Wrap(err, "Level validation")
	}

	// Sync directory (because we have at least removed some files, or previously created the
	// manifest file).
	if err := syncDir(kv.opt.Dir); err != nil {
		_ = s.close()
		return nil, err
	}

	return s, nil
}

func fileExist(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Closes the tables, for cleanup in newLevelsController.  (We Close() instead of using DecrRef()
// because that would delete the underlying files.)  We ignore errors, which is OK because tables
// are read-only.
func closeAllTables(tables [][]*table.Table) {
	for _, tableSlice := range tables {
		for _, table := range tableSlice {
			_ = table.Close()
		}
	}
}

func (lc *levelsController) cleanupLevels() error {
	var firstErr error
	for _, l := range lc.levels {
		if err := l.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (lc *levelsController) startCompact(c *y.Closer) {
	n := lc.kv.opt.NumCompactors
	c.AddRunning(n - 1)
	for i := 0; i < n; i++ {
		go lc.runWorker(c)
	}
}

func (lc *levelsController) runWorker(c *y.Closer) {
	defer c.Done()
	if lc.kv.opt.DoNotCompact {
		return
	}

	time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C:
			prios := lc.pickCompactLevels()
			for _, p := range prios {
				// TODO: Handle error.
				didCompact, _ := lc.doCompact(p)
				if didCompact {
					break
				}
			}
		case <-c.HasBeenClosed():
			return
		}
	}
}

// Returns true if level zero may be compacted, without accounting for compactions that already
// might be happening.
func (lc *levelsController) isL0Compactable() bool {
	return lc.levels[0].numTables() >= lc.kv.opt.NumLevelZeroTables
}

// Returns true if the non-zero level may be compacted.  deltaSize provides the size of the tables
// which are currently being compacted so that we treat them as already having started being
// compacted (because they have been, yet their size is already counted in getTotalSize).
func (l *levelHandler) isCompactable(deltaSize int64) bool {
	return l.getTotalSize() >= l.maxTotalSize+deltaSize
}

type compactionPriority struct {
	level int
	score float64
}

// pickCompactLevel determines which level to compact.
// Based on: https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
func (lc *levelsController) pickCompactLevels() (prios []compactionPriority) {
	// This function must use identical criteria for guaranteeing compaction's progress that
	// addLevel0Table uses.

	// cstatus is checked to see if level 0's tables are already being compacted
	if !lc.cstatus.overlapsWith(0, infRange) && lc.isL0Compactable() {
		pri := compactionPriority{
			level: 0,
			score: float64(lc.levels[0].numTables()) / float64(lc.kv.opt.NumLevelZeroTables),
		}
		prios = append(prios, pri)
	}

	// now calcalute scores from level 1
	for levelNum := 1; levelNum < len(lc.levels); levelNum++ {
		// Don't consider those tables that are already being compacted right now.
		deltaSize := lc.cstatus.deltaSize(levelNum)

		l := lc.levels[levelNum]
		if l.isCompactable(deltaSize) {
			pri := compactionPriority{
				level: levelNum,
				score: float64(l.getTotalSize()-deltaSize) / float64(l.maxTotalSize),
			}
			prios = append(prios, pri)
		}
	}
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].score > prios[j].score
	})
	return prios
}

func (lc *levelsController) hasOverlapTable(cd compactDef) bool {
	kr := getKeyRange(cd.top)
	for i := cd.nextLevel.level + 1; i < len(lc.levels); i++ {
		lh := lc.levels[i]
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

type DiscardStats struct {
	numSkips     int64
	skippedBytes int64
	ptrs         []blobPointer
}

func (ds *DiscardStats) collect(vs y.ValueStruct) {
	if vs.Meta&bitValuePointer > 0 {
		var bp blobPointer
		bp.decode(vs.Value)
		ds.ptrs = append(ds.ptrs, bp)
		ds.skippedBytes += int64(bp.length)
	}
	ds.numSkips++
}

func (ds *DiscardStats) String() string {
	return fmt.Sprintf("numSkips:%d, skippedBytes:%d", ds.numSkips, ds.skippedBytes)
}

func shouldFinishFile(key, lastKey []byte, guard *Guard, currentSize, maxSize int64) bool {
	if len(lastKey) == 0 {
		return false
	}
	if guard != nil {
		if !bytes.HasPrefix(key, guard.Prefix) {
			return true
		}
		if !matchGuard(key, lastKey, guard) {
			if maxSize > guard.MinSize {
				maxSize = guard.MinSize
			}
		}
	}
	return currentSize > maxSize
}

func matchGuard(key, lastKey []byte, guard *Guard) bool {
	if len(lastKey) < guard.MatchLen {
		return false
	}
	return bytes.HasPrefix(key, lastKey[:guard.MatchLen])
}

func searchGuard(key []byte, guards []Guard) *Guard {
	var maxMatchGuard *Guard
	for i := range guards {
		guard := &guards[i]
		if bytes.HasPrefix(key, guard.Prefix) {
			if maxMatchGuard == nil || len(guard.Prefix) > len(maxMatchGuard.Prefix) {
				maxMatchGuard = guard
			}
		}
	}
	return maxMatchGuard
}

func overSkipTables(key []byte, skippedTables []*table.Table) (newSkippedTables []*table.Table, over bool) {
	var i int
	for i < len(skippedTables) {
		t := skippedTables[i]
		if y.CompareKeysWithVer(key, t.Biggest()) > 0 {
			i++
		} else {
			break
		}
	}
	return skippedTables[i:], i > 0
}

// compactBuildTables merge topTables and botTables to form a list of new tables.
func (lc *levelsController) compactBuildTables(level int, cd compactDef,
	limiter *rate.Limiter, splitHints [][]byte) (newTables []*table.Table, err error) {
	topTables := cd.top
	botTables := cd.bot

	hasOverlap := lc.hasOverlapTable(cd)
	log.Infof("Key range overlaps with lower levels: %v", hasOverlap)

	// Try to collect stats so that we can inform value log about GC. That would help us find which
	// value log file should be GCed.
	discardStats := &DiscardStats{}

	// Create iterators across all the tables involved first.
	var iters []y.Iterator
	if level == 0 {
		iters = appendIteratorsReversed(iters, topTables, false)
	} else {
		y.Assert(len(topTables) == 1)
		iters = []y.Iterator{topTables[0].NewIterator(false)}
	}

	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	iters = append(iters, table.NewConcatIterator(botTables, false))
	it := table.NewMergeIterator(iters, false)
	defer it.Close() // Important to close the iterator to do ref counting.

	it.Rewind()

	// Pick up the currently pending transactions' min readTs, so we can discard versions below this
	// readTs. We should never discard any versions starting from above this timestamp, because that
	// would affect the snapshot view guarantee provided by transactions.
	minReadTs := lc.kv.orc.readMark.MinReadTS()

	var filter CompactionFilter
	var guards []Guard
	if lc.kv.opt.CompactionFilterFactory != nil {
		filter = lc.kv.opt.CompactionFilterFactory(level+1, cd.smallest(), cd.biggest())
		guards = filter.Guards()
	}
	skippedTbls := cd.skippedTbls

	var lastKey, skipKey []byte
	var builder *table.Builder
	var bytesRead, bytesWrite, numRead, numWrite int
	isRemote := cd.nextLevel.level >= lc.kv.opt.RemoteLevelStart

	for it.Valid() {
		timeStart := time.Now()
		fileID := lc.reserveFileID()
		dir := lc.kv.opt.Dir
		if isRemote {
			dir = lc.kv.opt.RemoteDir
		}
		fileName := table.NewFilename(fileID, dir)
		var fd *os.File
		fd, err = directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return
		}

		if builder == nil {
			builder = table.NewTableBuilder(fd, limiter, cd.nextLevel.level, cd.nextLevel.level >= lc.kv.opt.RemoteLevelStart, lc.opt)
		} else {
			builder.Reset(fd)
		}
		lastKey = lastKey[:0]
		guard := searchGuard(it.Key(), guards)
		for ; it.Valid(); it.Next() {
			numRead++
			vs := it.Value()
			key := it.Key()
			kvSize := int(vs.EncodedSize()) + len(key)
			bytesRead += kvSize
			// See if we need to skip this key.
			if len(skipKey) > 0 {
				if y.SameKey(key, skipKey) {
					discardStats.collect(vs)
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}
			if !y.SameKey(key, lastKey) {
				// Only break if we are on a different key, and have reached capacity. We want
				// to ensure that all versions of the key are stored in the same sstable, and
				// not divided across multiple tables at the same level.
				if len(skippedTbls) > 0 {
					var over bool
					skippedTbls, over = overSkipTables(key, skippedTbls)
					if over && !builder.Empty() {
						break
					}
				}
				if shouldFinishFile(key, lastKey, guard, int64(builder.EstimateSize()), lc.kv.opt.MaxTableSize) {
					break
				}
				if len(splitHints) != 0 && y.CompareKeysWithVer(key, splitHints[0]) >= 0 {
					splitHints = splitHints[1:]
					for len(splitHints) > 0 && y.CompareKeysWithVer(key, splitHints[0]) >= 0 {
						splitHints = splitHints[1:]
					}
					break
				}
				lastKey = y.SafeCopy(lastKey, key)
			}

			version := y.ParseTs(key)

			// Only consider the versions which are below the minReadTs, otherwise, we might end up discarding the
			// only valid version for a running transaction.
			if version <= minReadTs {
				// key is the latest readable version of this key, so we simply discard all the rest of the versions.
				skipKey = y.SafeCopy(skipKey, key)

				if isDeleted(vs.Meta) {
					// If this key range has overlap with lower levels, then keep the deletion
					// marker with the latest version, discarding the rest. We have set skipKey,
					// so the following key versions would be skipped. Otherwise discard the deletion marker.
					if !hasOverlap {
						continue
					}
				} else if filter != nil {
					switch filter.Filter(key, vs.Value, vs.UserMeta) {
					case DecisionMarkTombstone:
						discardStats.collect(vs)
						if hasOverlap {
							// There may have ole versions for this key, so convert to delete tombstone.
							builder.Add(key, y.ValueStruct{Meta: bitDelete})
						}
						continue
					case DecisionDrop:
						discardStats.collect(vs)
						continue
					case DecisionKeep:
					}
				}
			}
			builder.Add(key, vs)
			numWrite++
			bytesWrite += kvSize
		}
		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		log.Infof("LOG Compact. Iteration took: %v\n", time.Since(timeStart))
		if err = builder.Finish(); err != nil {
			return
		}
		fd.Close()
		var tbl *table.Table
		// TODO: may have problem with cache manager.
		tbl, err = table.OpenTable(fileName, isRemote, lc.kv.opt.TableLoadingMode, lc.kv.cacheManger)
		if err != nil {
			return
		}

		if len(tbl.Smallest()) == 0 {
			tbl.DecrRef()
		} else {
			newTables = append(newTables, tbl)
		}
	}

	stats := &y.CompactionStats{
		KeysRead:     numRead,
		BytesRead:    bytesRead,
		KeysWrite:    numWrite,
		BytesWrite:   bytesWrite,
		KeysDiscard:  int(discardStats.numSkips),
		BytesDiscard: int(discardStats.skippedBytes),
	}
	cd.nextLevel.metrics.UpdateCompactionStats(stats)
	// Ensure created files' directory entries are visible.  We don't mind the extra latency
	// from not doing this ASAP after all file creation has finished because this is a
	// background operation.
	err = syncDir(lc.kv.opt.Dir)
	if err != nil {
		log.Error(err)
		return
	}
	sortTables(newTables)
	log.Infof("Discard stats: %s", discardStats)
	if len(discardStats.ptrs) > 0 {
		lc.kv.blobManger.discardCh <- discardStats
	}
	assertTablesOrder(newTables)
	return
}

func buildChangeSet(cd *compactDef, newTables []*table.Table) protos.ManifestChangeSet {
	changes := []*protos.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, makeTableCreateChange(table.ID(), cd.nextLevel.level))
	}
	for _, table := range cd.top {
		changes = append(changes, makeTableDeleteChange(table.ID()))
	}
	for _, table := range cd.bot {
		changes = append(changes, makeTableDeleteChange(table.ID()))
	}
	return protos.ManifestChangeSet{Changes: changes}
}

type compactDef struct {
	thisLevel *levelHandler
	nextLevel *levelHandler

	top []*table.Table
	bot []*table.Table

	skippedTbls []*table.Table

	thisRange keyRange
	nextRange keyRange

	thisSize int64
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

func (cd *compactDef) smallest() []byte {
	if len(cd.bot) > 0 && bytes.Compare(cd.nextRange.left, cd.thisRange.left) < 0 {
		return cd.nextRange.left
	}
	return cd.thisRange.left
}

func (cd *compactDef) biggest() []byte {
	if len(cd.bot) > 0 && bytes.Compare(cd.nextRange.right, cd.thisRange.right) > 0 {
		return cd.nextRange.right
	}
	return cd.thisRange.right
}

/*
type rangeWithSize struct {
	start []byte
	end   []byte
	sz    int
}

func (cd *compactDef) getInputBounds() []rangeWithSize {
	bounds := make([][]byte, 0, len(cd.bot)+1)
	for _, tbl := range cd.bot {
		smallest := y.KeyWithTs(y.ParseKey(tbl.Smallest()), math.MaxUint64)
		bounds = append(bounds, smallest)
	}
	biggest := y.KeyWithTs(y.ParseKey(cd.bot[len(cd.bot)-1].Biggest()), 0)
	bounds = append(bounds, biggest)

	ranges := make([]rangeWithSize, 0, len(bounds))
	for i := 0; i < len(bounds)-1; i++ {
		start, end := bounds[i], bounds[i+1]
		sz := cd.sizeInRange(cd.top, cd.thisLevel.level, start, end)
		sz += cd.sizeInRange(cd.bot, cd.nextLevel.level, start, end)
		ranges = append(ranges, rangeWithSize{start: start, end: end, sz: sz})
	}

	ranges[0].start = nil
	ranges[len(ranges)-1].end = nil

	return ranges
}

func (cd *compactDef) sizeInRange(tbls []*table.Table, level int, start, end []byte) int {
	var sz int
	left, right := 0, len(tbls)
	if level != 0 {
		left, right = getTablesInRange(tbls, start, end)
	}
	for _, tbl := range tbls[left:right] {
		sz += tbl.ApproximateSizeInRange(start, end)
	}
	return sz
}
*/

func (lc *levelsController) fillTablesL0(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	if len(cd.thisLevel.tables) == 0 {
		return false
	}

	cd.top = make([]*table.Table, len(cd.thisLevel.tables))
	copy(cd.top, cd.thisLevel.tables)

	cd.thisRange = infRange

	kr := getKeyRange(cd.top)
	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, kr)
	overlappingTables := cd.nextLevel.tables[left:right]
	lc.fillBottomTables(cd, overlappingTables)

	if len(overlappingTables) == 0 { // the bottom-most level
		cd.nextRange = kr
	} else {
		cd.nextRange = getKeyRange(overlappingTables)
	}

	if !lc.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
		return false
	}

	return true
}

const minSkippedTableSize = 1024 * 1024

func (lc *levelsController) fillBottomTables(cd *compactDef, overlappingTables []*table.Table) {
	for _, t := range overlappingTables {
		// If none of the top tables contains the range in an overlapping bottom table,
		// we can skip it during compaction to reduce write amplification.
		var added bool
		for _, topTbl := range cd.top {
			iter := topTbl.NewIteratorNoRef(false)
			iter.Seek(t.Smallest())
			if iter.Valid() && y.CompareKeysWithVer(iter.Key(), t.Biggest()) <= 0 {
				cd.bot = append(cd.bot, t)
				added = true
				break
			}
		}
		if !added {
			if t.Size() >= minSkippedTableSize {
				// We need to limit the minimum size of the table to be skipped,
				// otherwise the number of tables in a level will keep growing
				// until we meet too many open files error.
				cd.skippedTbls = append(cd.skippedTbls, t)
			} else {
				cd.bot = append(cd.bot, t)
			}
		}
	}
}

func (lc *levelsController) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	if len(cd.thisLevel.tables) == 0 {
		return false
	}

	tbls := make([]*table.Table, len(cd.thisLevel.tables))
	copy(tbls, cd.thisLevel.tables)

	// Find the biggest table, and compact that first.
	// TODO: Try other table picking strategies.
	sort.Slice(tbls, func(i, j int) bool {
		return tbls[i].Size() > tbls[j].Size()
	})

	for _, t := range tbls {
		cd.thisSize = t.Size()
		cd.thisRange = keyRange{
			left:  t.Smallest(),
			right: t.Biggest(),
		}
		if lc.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) {
			continue
		}
		cd.top = []*table.Table{t}
		cd.bot = cd.bot[:0]
		cd.skippedTbls = cd.skippedTbls[:0]
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
		overlappingTables := cd.nextLevel.tables[left:right]
		lc.fillBottomTables(cd, overlappingTables)

		if len(overlappingTables) == 0 {
			cd.bot = []*table.Table{}
			cd.nextRange = cd.thisRange
			if !lc.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(overlappingTables)

		if lc.cstatus.overlapsWith(cd.nextLevel.level, cd.nextRange) {
			continue
		}

		if !lc.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

/*
// determineSubCompactPlan returns the number of sub compactors and the estimated size of each compaction job.
func (lc *levelsController) determineSubCompactPlan(bounds []rangeWithSize) (int, int) {
	n := lc.kv.opt.MaxSubCompaction
	if len(bounds) < n {
		n = len(bounds)
	}

	var size int
	for _, bound := range bounds {
		size += bound.sz
	}

	const minFileFillPercent = 4.0 / 5.0
	maxOutPutFiles := int(math.Ceil(float64(size) / minFileFillPercent / float64(lc.kv.opt.MaxTableSize)))
	if maxOutPutFiles < n {
		n = maxOutPutFiles
	}
	return n, size / n
}

func (lc *levelsController) runSubCompacts(l int, cd compactDef, limiter *rate.Limiter) ([]*table.Table, error) {
	type jobResult struct {
		tbls []*table.Table
		err  error
	}

	inputBounds := cd.getInputBounds()
	numSubCompact, avgSize := lc.determineSubCompactPlan(inputBounds)
	if numSubCompact == 1 {
		return lc.compactBuildTables(l, cd, limiter, nil, nil)
	}

	results := make([]jobResult, numSubCompact)
	var wg sync.WaitGroup
	var currSize, begin, jobNo int

	for i := range inputBounds {
		currSize += inputBounds[i].sz
		if currSize >= avgSize || i == len(inputBounds)-1 {
			start, end := inputBounds[begin].start, inputBounds[i].end

			wg.Add(1)
			go func(job int) {
				newTables, err := lc.compactBuildTables(l, cd, limiter, start, end)
				results[job].tbls = newTables
				results[job].err = err
				wg.Done()
			}(jobNo)

			currSize = 0
			begin = i + 1
			jobNo++
		}
	}

	log.Infof("Started %d SubCompaction Jobs", jobNo)
	wg.Wait()

	var numTables int
	for _, result := range results {
		if result.err != nil {
			return nil, result.err
		}
		numTables += len(result.tbls)
	}

	newTables := make([]*table.Table, 0, numTables)
	for _, result := range results {
		newTables = append(newTables, result.tbls...)
	}

	return newTables, nil
}

func (lc *levelsController) shouldStartSubCompaction(cd compactDef) bool {
	if lc.kv.opt.MaxSubCompaction <= 1 || len(cd.bot) == 0 {
		return false
	}
	if cd.thisLevel.level == 0 {
		return true
	}
	if cd.thisLevel.level == 1 {
		// Only speed up large L1 compaction.
		return len(cd.bot)+len(cd.top) >= 10
	}
	return false
}
*/

func (lc *levelsController) runCompactDef(l int, cd compactDef, limiter *rate.Limiter) error {
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	var newTables []*table.Table
	var changeSet protos.ManifestChangeSet
	if l > 0 && len(cd.bot) == 0 && len(cd.skippedTbls) == 0 {
		// skip level 0, since it may has many table overlap with each other
		newTables = cd.top
		changeSet = protos.ManifestChangeSet{Changes: []*protos.ManifestChange{
			makeTableMoveDownChange(newTables[0].ID(), cd.nextLevel.level),
		}}
	} else {
		var err error
		newTables, err = lc.compactBuildTables(l, cd, limiter, nil)
		defer forceDecrRefs(newTables)
		if err != nil {
			return err
		}
		changeSet = buildChangeSet(&cd, newTables)
	}

	// We write to the manifest _before_ we delete files (and after we created files)
	if err := lc.kv.manifest.addChanges(changeSet.Changes, nil); err != nil {
		return err
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	if err := nextLevel.replaceTables(newTables, &cd); err != nil {
		return err
	}
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	log.Infof("LOG Compact %d->%d, del %d tables, add %d tables, took %v\n",
		l, l+1, len(cd.top)+len(cd.bot), len(newTables), time.Since(timeStart))
	return nil
}

// doCompact picks some table on level l and compacts it away to the next level.
func (lc *levelsController) doCompact(p compactionPriority) (bool, error) {
	l := p.level
	y.Assert(l+1 < lc.kv.opt.TableBuilderOptions.MaxLevels) // Sanity check.

	cd := compactDef{
		thisLevel: lc.levels[l],
		nextLevel: lc.levels[l+1],
	}

	log.Infof("Got compaction priority: %+v", p)

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	if l == 0 {
		if !lc.fillTablesL0(&cd) {
			log.Infof("fillTables failed for level: %d\n", l)
			return false, nil
		}
	} else {
		if !lc.fillTables(&cd) {
			log.Infof("fillTables failed for level: %d\n", l)
			return false, nil
		}
	}
	defer lc.cstatus.delete(cd) // Remove the ranges from compaction status.

	log.Infof("Running compaction: %d", cd.thisLevel.level)
	if err := lc.runCompactDef(l, cd, lc.kv.limiter); err != nil {
		// This compaction couldn't be done successfully.
		log.Infof("\tLOG Compact FAILED with error: %+v: %+v", err, cd)
		return false, err
	}

	log.Infof("Compaction Done for level: %d", cd.thisLevel.level)
	return true, nil
}

func (lc *levelsController) addLevel0Table(t *table.Table, head *protos.HeadInfo) error {
	// We update the manifest _before_ the table becomes part of a levelHandler, because at that
	// point it could get used in some compaction.  This ensures the manifest file gets updated in
	// the proper order. (That means this update happens before that of some compaction which
	// deletes the table.)
	err := lc.kv.manifest.addChanges([]*protos.ManifestChange{
		makeTableCreateChange(t.ID(), 0),
	}, head)
	if err != nil {
		return err
	}

	for !lc.levels[0].tryAddLevel0Table(t) {
		// Stall. Make sure all levels are healthy before we unstall.
		log.Warnf("STALLED STALLED STALLED STALLED STALLED STALLED STALLED STALLED: %v\n",
			time.Since(lastUnstalled))
		lc.cstatus.RLock()
		for i := 0; i < lc.kv.opt.TableBuilderOptions.MaxLevels; i++ {
			log.Infof("level=%d. Status=%s Size=%d\n",
				i, lc.cstatus.levels[i].debug(), lc.levels[i].getTotalSize())
		}
		lc.cstatus.RUnlock()
		timeStart := time.Now()
		// Before we unstall, we need to make sure that level 0 and 1 are healthy. Otherwise, we
		// will very quickly fill up level 0 again and if the compaction strategy favors level 0,
		// then level 1 is going to super full.
		for i := 0; ; i++ {
			// Passing 0 for deltaSize to compactable means we're treating incomplete compactions as
			// not having finished -- we wait for them to finish.  Also, it's crucial this behavior
			// replicates pickCompactLevels' behavior in computing compactability in order to
			// guarantee progress.
			if !lc.isL0Compactable() && !lc.levels[1].isCompactable(0) {
				break
			}
			time.Sleep(10 * time.Millisecond)
			if i%100 == 0 {
				prios := lc.pickCompactLevels()
				log.Warnf("Waiting to add level 0 table. Compaction priorities: %+v\n", prios)
				i = 0
			}
		}
		log.Infof("UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED: %v\n",
			time.Since(timeStart))
		lastUnstalled = time.Now()
	}

	return nil
}

func (s *levelsController) close() error {
	err := s.cleanupLevels()
	return errors.Wrap(err, "levelsController.Close")
}

// get returns the found value if any. If not found, we return nil.
func (s *levelsController) get(key []byte, keyHash uint64, refs RefMap) y.ValueStruct {
	// It's important that we iterate the levels from 0 on upward.  The reason is, if we iterated
	// in opposite order, or in parallel (naively calling all the h.RLock() in some order) we could
	// read level L's tables post-compaction and level L+1's tables pre-compaction.  (If we do
	// parallelize this, we will need to call the h.RLock() function by increasing order of level
	// number.)
	start := time.Now()
	defer s.kv.metrics.LSMGetDuration.Observe(time.Since(start).Seconds())
	for _, h := range s.levels {
		vs := h.get(key, keyHash, refs) // Calls h.RLock() and h.RUnlock().
		if vs.Valid() {
			return vs
		}
	}
	return y.ValueStruct{}
}

func (s *levelsController) multiGet(pairs []keyValuePair, refs RefMap) {
	start := time.Now()
	for _, h := range s.levels {
		h.multiGet(pairs, refs)
	}
	s.kv.metrics.LSMMultiGetDuration.Observe(time.Since(start).Seconds())
}

func appendIteratorsReversed(out []y.Iterator, th []*table.Table, reversed bool) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, table.NewConcatIterator(th[i:i+1], reversed))
	}
	return out
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelsController) appendIterators(
	iters []y.Iterator, opts IteratorOptions) []y.Iterator {
	// Just like with get, it's important we iterate the levels from 0 on upward, to avoid missing
	// data when there's a compaction.
	for _, level := range s.levels {
		iters = level.appendIterators(iters, opts)
	}
	return iters
}

type TableInfo struct {
	ID    uint64
	Level int
	Left  []byte
	Right []byte
}

func (lc *levelsController) getTableInfo() (result []TableInfo) {
	for _, l := range lc.levels {
		for _, t := range l.tables {
			info := TableInfo{
				ID:    t.ID(),
				Level: l.level,
				Left:  t.Smallest(),
				Right: t.Biggest(),
			}
			result = append(result, info)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Level != result[j].Level {
			return result[i].Level < result[j].Level
		}
		return result[i].ID < result[j].ID
	})
	return
}

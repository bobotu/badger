package surf

// SuffixType is SuRF's suffix type.
type SuffixType uint8

const (
	// NoneSuffix means don't store suffix for keys.
	NoneSuffix SuffixType = iota
	// HashSuffix means store a small hash of keys.
	HashSuffix
	// RealSuffix means store a prefix of keys.
	RealSuffix
	// MixedSuffix means store a small hash with prefix of keys.
	MixedSuffix
)

func (st SuffixType) String() string {
	switch st {
	case HashSuffix:
		return "Hash"
	case RealSuffix:
		return "Real"
	case MixedSuffix:
		return "Mixed"
	default:
		return "None"
	}
}

const labelTerminator = 0xff

type Builder struct {
	sparseStartLevel uint32
	valueSize        uint32
	totalCount       int

	// LOUDS-Sparse bitvecs
	lsLabels    [][]byte
	lsHasChild  [][]uint64
	lsLoudsBits [][]uint64

	// LOUDS-Dense bitvecs
	ldLabels   [][]uint64
	ldHasChild [][]uint64
	ldIsPrefix [][]uint64

	// suffix
	suffixType    SuffixType
	hashSuffixLen uint32
	realSuffixLen uint32
	suffixes      [][]uint64
	suffixCounts  []uint32

	// value
	values      [][]byte
	valueCounts []uint32

	// prefix
	hasPrefix [][]uint64
	prefixes  [][][]byte

	nodeCounts           []uint32
	isLastItemTerminator []bool
}

func NewBuilder(valueSize uint32, suffixType SuffixType, hashSuffixLen, realSuffixLen uint32) *Builder {
	switch suffixType {
	case HashSuffix:
		realSuffixLen = 0
	case RealSuffix:
		hashSuffixLen = 0
	case NoneSuffix:
		realSuffixLen = 0
		hashSuffixLen = 0
	}

	return &Builder{
		valueSize:     valueSize,
		suffixType:    suffixType,
		hashSuffixLen: hashSuffixLen,
		realSuffixLen: realSuffixLen,
	}
}

func (b *Builder) Build(keys, vals [][]byte, bitsPerKeyHint int) *SuRF {
	b.totalCount = len(keys)
	b.initNodes(keys, vals, 0, 0, 0)
	b.determineCutoffLevel(bitsPerKeyHint)
	b.buildDense()

	var prefixVec prefixVec
	numItemsPerLevel := make([]uint32, b.treeHeight())
	for level := range numItemsPerLevel {
		numItemsPerLevel[level] = b.nodeCounts[level]
	}
	prefixVec.init(b.hasPrefix, numItemsPerLevel, b.prefixes)

	surf := new(SuRF)
	surf.ld.init(b)
	surf.ld.prefixVec = &prefixVec
	surf.ls.init(b)
	surf.ls.prefixVec = &prefixVec
	surf.prefixVec = prefixVec
	return surf
}

func (b *Builder) initNodes(keys, vals [][]byte, prefixDepth, depth, level int) {
	b.ensureLevel(level)

	nodeStartPos := b.numItems(level)

	groupStart, groupEnd := 0, 0
	if depth >= len(keys[groupStart]) {
		b.lsLabels[level] = append(b.lsLabels[level], labelTerminator)
		b.isLastItemTerminator[level] = true
		b.insertSuffix(keys[groupStart], level, depth)
		b.insertValue(vals[groupStart], level)
		b.moveToNextItemSlot(level)
		groupStart, groupEnd = 1, 1
	}

	for {
		groupEnd++
		startKey := keys[groupStart]
		if groupEnd < len(keys) && startKey[depth] == keys[groupEnd][depth] {
			continue
		}

		if groupEnd >= len(keys) && groupStart == 0 {
			break
		}

		b.lsLabels[level] = append(b.lsLabels[level], startKey[depth])
		b.moveToNextItemSlot(level)
		if groupEnd-groupStart == 1 {
			b.insertSuffix(startKey, level, depth)
			b.insertValue(vals[groupStart], level)
		} else {
			setBit(b.lsHasChild[level], b.numItems(level)-1)
			b.initNodes(keys[groupStart:groupEnd], vals[groupStart:groupEnd], depth+1, depth+1, level+1)
		}

		groupStart = groupEnd
		if groupEnd >= len(keys) {
			break
		}
	}

	if groupStart == 0 {
		// node at this level is one-way node, compress it to next node
		b.initNodes(keys, vals, prefixDepth, depth+1, level)
	} else {
		if depth-prefixDepth > 0 {
			prefix := keys[0][prefixDepth:depth]
			setBit(b.hasPrefix[level], b.nodeCounts[level])
			b.insertPrefix(prefix, level)
		}
		setBit(b.lsLoudsBits[level], nodeStartPos)

		b.nodeCounts[level]++
		if b.nodeCounts[level]%wordSize == 0 {
			b.hasPrefix[level] = append(b.hasPrefix[level], 0)
		}
	}
}

func (b *Builder) buildDense() {
	var level int
	for level = 0; uint32(level) < b.sparseStartLevel; level++ {
		b.initDenseVectors(level)
		if b.numItems(level) == 0 {
			continue
		}

		var nodeID uint32
		if b.isTerminator(level, 0) {
			setBit(b.ldIsPrefix[level], 0)
		} else {
			b.setLabelAndHasChildVec(level, nodeID, 0)
		}

		var pos uint32
		numItems := b.numItems(level)
		for pos = 1; pos < numItems; pos++ {
			if b.isStartOfNode(level, pos) {
				nodeID++
				if b.isTerminator(level, pos) {
					setBit(b.ldIsPrefix[level], nodeID)
					continue
				}
			}
			b.setLabelAndHasChildVec(level, nodeID, pos)
		}
	}
}

func (b *Builder) ensureLevel(level int) {
	if level >= b.treeHeight() {
		b.addLevel()
	}
}

func (b *Builder) suffixLen() uint32 {
	return b.hashSuffixLen + b.realSuffixLen
}

func (b *Builder) treeHeight() int {
	return len(b.nodeCounts)
}

func (b *Builder) numItems(level int) uint32 {
	return uint32(len(b.lsLabels[level]))
}

func (b *Builder) addLevel() {
	b.lsLabels = append(b.lsLabels, []byte{})
	b.lsHasChild = append(b.lsHasChild, []uint64{})
	b.lsLoudsBits = append(b.lsLoudsBits, []uint64{})
	b.hasPrefix = append(b.hasPrefix, []uint64{})
	b.suffixes = append(b.suffixes, []uint64{})
	b.suffixCounts = append(b.suffixCounts, 0)
	b.values = append(b.values, []byte{})
	b.valueCounts = append(b.valueCounts, 0)
	b.prefixes = append(b.prefixes, [][]byte{})

	b.nodeCounts = append(b.nodeCounts, 0)
	b.isLastItemTerminator = append(b.isLastItemTerminator, false)

	level := b.treeHeight() - 1
	b.lsHasChild[level] = append(b.lsHasChild[level], 0)
	b.lsLoudsBits[level] = append(b.lsLoudsBits[level], 0)
	b.hasPrefix[level] = append(b.hasPrefix[level], 0)
}

func (b *Builder) moveToNextItemSlot(level int) {
	if b.numItems(level)%wordSize == 0 {
		b.lsHasChild[level] = append(b.lsHasChild[level], 0)
		b.lsLoudsBits[level] = append(b.lsLoudsBits[level], 0)
	}
}

func (b *Builder) insertSuffix(key []byte, level, depth int) {
	if level >= b.treeHeight() {
		b.addLevel()
	}
	suffix := constructSuffix(key, uint32(depth)+1, b.suffixType, b.realSuffixLen, b.hashSuffixLen)

	suffixLen := b.suffixLen()
	pos := b.suffixCounts[level] * suffixLen
	if pos == uint32(len(b.suffixes[level])*wordSize) {
		b.suffixes[level] = append(b.suffixes[level], 0)
	}
	wordID := pos / wordSize
	offset := pos % wordSize
	remain := wordSize - offset
	if suffixLen <= remain {
		shift := remain - suffixLen
		b.suffixes[level][wordID] += suffix << shift
	} else {
		left := suffix >> (suffixLen - remain)
		right := suffix << (wordSize - (suffixLen - remain))
		b.suffixes[level][wordID] += left
		b.suffixes[level] = append(b.suffixes[level], right)
	}
	b.suffixCounts[level]++
}

func (b *Builder) insertValue(value []byte, level int) {
	b.values[level] = append(b.values[level], value[:b.valueSize]...)
	b.valueCounts[level]++
}

func (b *Builder) insertPrefix(prefix []byte, level int) {
	b.prefixes[level] = append(b.prefixes[level], append([]byte{}, prefix...))
}

func (b *Builder) determineCutoffLevel(bitsPerKeyHint int) {
	height := b.treeHeight()
	if height == 0 {
		return
	}

	sizeHint := uint64(b.totalCount * bitsPerKeyHint)
	suffixAndValueSize := uint64(b.totalCount) * uint64(b.suffixLen())
	var level int
	for level = height - 1; level > 0; level-- {
		ds := b.denseSizeNoSuffix(level)
		ss := b.sparseSizeNoSuffix(level)
		sz := ds + ss + suffixAndValueSize
		if sz <= sizeHint {
			break
		}
	}
	b.sparseStartLevel = uint32(level)
}

func (b *Builder) denseSizeNoSuffix(level int) uint64 {
	var total uint64
	for l := 0; l < level; l++ {
		total += uint64(2 * denseFanout * b.nodeCounts[l])
		if l > 0 {
			total += uint64(b.nodeCounts[l-1])
		}
	}
	return total
}

func (b *Builder) sparseSizeNoSuffix(level int) uint64 {
	var total uint64
	height := b.treeHeight()
	for l := level; l < height; l++ {
		n := uint64(len(b.lsLabels[l]))
		total += n*8 + 2*n
	}
	return total
}

func (b *Builder) setLabelAndHasChildVec(level int, nodeID, pos uint32) {
	label := b.lsLabels[level][pos]
	setBit(b.ldLabels[level], nodeID*denseFanout+uint32(label))
	if readBit(b.lsHasChild[level], pos) {
		setBit(b.ldHasChild[level], nodeID*denseFanout+uint32(label))
	}
}

func (b *Builder) initDenseVectors(level int) {
	vecLength := b.nodeCounts[level] * (denseFanout / wordSize)
	prefixVecLen := b.nodeCounts[level] / wordSize
	if b.nodeCounts[level]%wordSize != 0 {
		prefixVecLen++
	}

	b.ldLabels = append(b.ldLabels, make([]uint64, vecLength))
	b.ldHasChild = append(b.ldHasChild, make([]uint64, vecLength))
	b.ldIsPrefix = append(b.ldIsPrefix, make([]uint64, prefixVecLen))
}

func (b *Builder) isStartOfNode(level int, pos uint32) bool {
	return readBit(b.lsLoudsBits[level], pos)
}

func (b *Builder) isTerminator(level int, pos uint32) bool {
	label := b.lsLabels[level][pos]
	return (label == labelTerminator) && !readBit(b.lsHasChild[level], pos)
}

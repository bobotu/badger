package cache

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

type CacheEntry interface {
	// cache file id
	CacheID() string
	// deallocate entry
	Deallocate() error
	// init entry
	Init() error
	// file size
	CacheSize() int
}

type CacheManager interface {
	// scan uploading files
	Open() error
	// Add file to cache
	Add(id string, entry CacheEntry, upload bool, isInLocal bool) error
	// File with ID is no longer needed, can be deleted from S3 and local.
	Free(id string) error
	// This file is needed, if not stay in local cache fetch from S3 and call `CacheEntry.Init`
	Pin(id string) error
	// Call `CacheEntry.Deallocate` to release entry, then it is safe from cache manager to remove cache of this file.
	Release(id string) error
}

type CacheEntryImpl struct {
	userEntry CacheEntry
	id        string
	pinCnt    int32
	inLocal   bool
	fileSize  int
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

func (entry *CacheEntryImpl) IsInLocal() bool {
	return entry.inLocal
}

func (entry *CacheEntryImpl) CacheSize() int {
	return entry.userEntry.CacheSize()
}

func (entry *CacheEntryImpl) SetInLocal(inLocal bool) {
	entry.inLocal = inLocal
}

func (entry *CacheEntryImpl) CacheID() string {
	return entry.userEntry.CacheID()
}

func (entry *CacheEntryImpl) Pinned() bool {
	return atomic.LoadInt32(&entry.pinCnt) > 0
}

func (entry *CacheEntryImpl) Pin() error {
	atomic.AddInt32(&entry.pinCnt, 1)
	return nil
}

func (entry *CacheEntryImpl) Unpin() error {
	atomic.AddInt32(&entry.pinCnt, -1)
	return nil
}

type CacheManagerImpl struct {
	// file dir, fileDir + id = filePath
	fileDir string
	// max files in cache manager
	maxSize int
	// client
	minioclient IMinioClient

	mu sync.Mutex
	// lru cache
	cache *LRU
	// current size
	localSize int
}

func canEvict(key, value interface{}) bool {
	entry := value.(*CacheEntryImpl)
	if entry.IsInLocal() && !entry.Pinned() {
		return true
	}
	return false
}

func NewCacheManager(fileDir string, maxSize int) CacheManager {
	cache, err := NewLRU(nil, canEvict)
	if err != nil {
		return nil
	}
	client := InitMinioClient()
	mgr := &CacheManagerImpl{
		fileDir:     fileDir,
		maxSize:     maxSize,
		minioclient: client,
		cache:       cache,
	}
	go func() {
		t := time.NewTicker(30 * time.Second)
		for range t.C {
			mgr.mu.Lock()
			var size int
			for el := mgr.cache.evictList.Front(); el != nil; el = el.Next() {
				e := el.Value.(*entry).value.(*CacheEntryImpl)
				size += e.CacheSize()
				log.Warnf("id %s, size: %d, cnt: %d", e.CacheID(), e.CacheSize(), e.pinCnt)
			}
			log.Warnf("total size: %d", size)
			mgr.mu.Unlock()
		}
	}()
	return mgr
}

func (mgr *CacheManagerImpl) getFileName(id string) string {
	return id
}

func (mgr *CacheManagerImpl) getUploadingFileName(id string) string {
	return fmt.Sprintf("%s.uploading", id)
}

func (mgr *CacheManagerImpl) getFilePath(id string) string {
	return path.Join(mgr.fileDir, mgr.getFileName(id))
}

func (mgr *CacheManagerImpl) getUploadingFilePath(id string) string {
	return path.Join(mgr.fileDir, mgr.getUploadingFileName(id))
}

func (mgr *CacheManagerImpl) getEntry(id string) (CacheEntry, error) {
	value, ok := mgr.cache.Peek(id)
	if !ok {
		return nil, errors.Errorf("%s not exist", id)
	}
	entry := value.(*CacheEntryImpl).userEntry
	return entry, nil
}

func (mgr *CacheManagerImpl) ensureFileSize(newSize int) {
	log.Infof("ensure file size, current %d, new %d, max %d", mgr.localSize, newSize, mgr.maxSize)
	for mgr.localSize+newSize > mgr.maxSize {
		_, value, ok := mgr.cache.GetOldestCanEvict()
		if !ok {
			log.Warn("cache full")
			return
		}
		entry := value.(*CacheEntryImpl)
		if !entry.IsInLocal() {
			panic("unexpected error: %d can evict but not in local")
		}
		if entry.CacheID() == "." {
			panic(fmt.Sprintf("%+v", entry.userEntry))
		}
		removed, err := mgr.removeLocalFile(entry.CacheID())
		if err != nil {
			log.Warn(err)
			return
		}
		if removed {
			mgr.localSize -= entry.CacheSize()
		}
		entry.SetInLocal(false)
	}
}

func (mgr *CacheManagerImpl) uploadRemoteFile(id string) error {
	uploadingFilePath := mgr.getUploadingFilePath(id)
	fileName := mgr.getFileName(id)
	filePath := mgr.getFilePath(id)
	log.Infof("uploading file: %s %s %s", fileName, filePath, uploadingFilePath)
	exist, err := fileExist(uploadingFilePath)
	if err != nil {
		return err
	}
	if exist {
		if err := os.Remove(uploadingFilePath); err != nil {
			return err
		}
	}
	f, err := y.OpenSyncedFile(uploadingFilePath, true)
	f.Close()
	if err != nil {
		return err
	}
	if err := mgr.minioclient.PutObject(fileName, filePath); err != nil {
		return err
	}
	os.Remove(uploadingFilePath)
	return nil
}

func (mgr *CacheManagerImpl) downloadRemoteFile(id string) (bool, error) {
	filePath := mgr.getFilePath(id)
	fileName := mgr.getFileName(id)
	exist, err := fileExist(filePath)
	if err != nil {
		return false, err
	}
	if exist {
		os.Remove(filePath)
	}
	log.Infof("download remote file: %s", id)
	if err = mgr.minioclient.GetObject(fileName, filePath); err != nil {
		return false, err
	}
	return true, nil
}

func (mgr *CacheManagerImpl) removeRemoteFile(id string) error {
	fileName := mgr.getFilePath(id)
	log.Infof("remove remote file: %s", id)
	return mgr.minioclient.RMObject(fileName)
}

func (mgr *CacheManagerImpl) removeLocalFile(id string) (removed bool, err error) {
	filePath := mgr.getFilePath(id)
	log.Infof("remove local file: %s", id)
	e, err := mgr.getEntry(id)
	if err != nil {
		return false, err
	}
	exist, err := fileExist(filePath)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	if err := e.Deallocate(); err != nil {
		return false, err
	}

	err = os.Remove(filePath)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (mgr *CacheManagerImpl) entryInLocal(entry *CacheEntryImpl) bool {
	filePath := mgr.getFilePath(entry.CacheID())
	exist, _ := fileExist(filePath)
	if entry.IsInLocal() && exist {
		return true
	}
	entry.SetInLocal(false)
	if exist {
		os.Remove(filePath)
	}
	return false
}

func (mgr *CacheManagerImpl) Open() error {
	fileInfos, err := ioutil.ReadDir(mgr.fileDir)
	if err != nil {
		return errors.Wrapf(err, "Error while opening cache uploading files")
	}
	for _, fileInfo := range fileInfos {
		if !strings.HasSuffix(fileInfo.Name(), ".uploading") {
			continue
		}
		fsz := len(fileInfo.Name())
		origFileId := fileInfo.Name()[:fsz-10]
		log.Infof("recover uploading: %s", origFileId)
		err := mgr.uploadRemoteFile(origFileId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mgr *CacheManagerImpl) Add(id string, entry CacheEntry, upload bool, isInLocal bool) error {
	id = filepath.Base(id)
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if mgr.cache.Contains(id) {
		log.Infof("%s duplicate", id)
		return nil
	}

	log.Warnf("add cache entry, id: %s, upload: %v, size: %d", id, upload, entry.CacheSize())
	if upload {
		if err := mgr.uploadRemoteFile(id); err != nil {
			return err
		}
	}
	e := &CacheEntryImpl{userEntry: entry, inLocal: isInLocal}
	if isInLocal {
		log.Errorf("add %s ensure size: %d", entry.CacheID(), entry.CacheSize())
		mgr.ensureFileSize(entry.CacheSize())
		mgr.localSize += entry.CacheSize()
	}
	mgr.cache.Add(id, e)
	return nil
}

func (mgr *CacheManagerImpl) Free(id string) error {
	id = filepath.Base(id)
	log.Infof("free file: %s", id)
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	value, ok := mgr.cache.Peek(id)
	if !ok {
		return errors.Errorf("%s not exist in cache list", id)
	}
	entry := value.(*CacheEntryImpl)
	if entry.Pinned() {
		return errors.Errorf("%s is pinned", id)
	}
	err := mgr.removeRemoteFile(id)
	if err != nil {
		return err
	}
	removed, err := mgr.removeLocalFile(id)
	if err != nil {
		return err
	}
	if removed {
		mgr.localSize -= entry.CacheSize()
		entry.SetInLocal(false)
	}
	mgr.cache.Remove(id)
	return nil
}

func (mgr *CacheManagerImpl) Pin(id string) error {
	id = filepath.Base(id)
	// log.Infof("pin file: %s", id)
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	value, ok := mgr.cache.Get(id)
	if !ok {
		return errors.Errorf("%s not in cache list", id)
	}
	entry := value.(*CacheEntryImpl)
	if entry.IsInLocal() {
		return entry.Pin()
	}
	log.Errorf("add %s ensure size: %d", entry.CacheID(), entry.CacheSize())
	mgr.ensureFileSize(entry.CacheSize())
	download, err := mgr.downloadRemoteFile(id)
	if err != nil {
		return err
	}
	if download {
		mgr.localSize += entry.CacheSize()
		entry.SetInLocal(true)
	}
	entry.userEntry.Init()
	return entry.Pin()
}

func (mgr *CacheManagerImpl) Release(id string) error {
	id = filepath.Base(id)
	// log.Infof("release file %s", id)
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	value, ok := mgr.cache.Peek(id)
	if !ok {
		return errors.Errorf("%s not in cache list", id)
	}
	entry := value.(*CacheEntryImpl)
	return entry.Unpin()
}

func (mgr *CacheManagerImpl) Len() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.cache.Len()
}

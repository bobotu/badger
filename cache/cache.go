package cache

import (
	"fmt"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
)

type CacheEntry interface {
	// cache file id
	CacheID() string
	// deallocate entry
	Deallocate() error
	// init entry
	Init() error
	// is file pinned
	Pinned() bool
	// pin file
	Pin() error
	// unpin file
	Unpin() error
	// file size
	CacheSize() int
	// file in local ssd
	IsInLocal() bool
	// SetInLocal
	SetInLocal(inLocal bool)
}

type CacheManager interface {
	// scan uploading files
	Open() error
	// Add file to cache
	Add(id string, entry CacheEntry, upload bool) error
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
	pinned    int
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
	return entry.fileSize
}

func (entry *CacheEntryImpl) SetInLocal(inLocal bool) {
	entry.inLocal = inLocal
}

func (entry *CacheEntryImpl) CacheID() string {
	return entry.id
}

func (entry *CacheEntryImpl) Pinned() bool {
	return entry.pinned > 0
}

func (entry *CacheEntryImpl) Pin() error {
	entry.pinned++
	return nil
}

func (entry *CacheEntryImpl) Unpin() error {
	if entry.pinned > 0 {
		entry.pinned--
		return nil
	}
	return errors.Errorf("entry not pinned")
}

func (entry *CacheEntryImpl) Deallocate() error {
	fmt.Println("cache entry deallocate")
	return nil
}

func (entry *CacheEntryImpl) Init() error {
	fmt.Println("cache entry init")
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
	entry := value.(CacheEntry)
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
	entry := value.(CacheEntry)
	return entry, nil
}

func (mgr *CacheManagerImpl) ensureFileSize(newSize int) error {
	log.Debugf("ensure file size, current %d, new %d, max %d", mgr.localSize, newSize, mgr.maxSize)
	for mgr.localSize+newSize > mgr.maxSize {
		_, value, ok := mgr.cache.GetOldestCanEvict()
		if !ok {
			return errors.Errorf("cache full")
		}
		entry := value.(CacheEntry)
		if !entry.IsInLocal() {
			panic("unexpected error: %d can evict but not in local")
		}
		removed, err := mgr.removeLocalFile(entry.CacheID())
		if err != nil {
			return err
		}
		if removed {
			mgr.localSize -= entry.CacheSize()
			entry.SetInLocal(false)
		}
	}
	return nil
}

func (mgr *CacheManagerImpl) uploadRemoteFile(id string) error {
	uploadingFilePath := mgr.getUploadingFilePath(id)
	fileName := mgr.getFileName(id)
	filePath := mgr.getFilePath(id)
	log.Debugf("uploading file: %s", fileName)
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
	log.Debugf("download remote file: %s", id)
	if err = mgr.minioclient.GetObject(fileName, filePath); err != nil {
		return false, err
	}
	return true, nil
}

func (mgr *CacheManagerImpl) removeRemoteFile(id string) error {
	fileName := mgr.getFilePath(id)
	log.Debugf("remove remote file: %s", id)
	return mgr.minioclient.RMObject(fileName)
}

func (mgr *CacheManagerImpl) removeLocalFile(id string) (removed bool, err error) {
	filePath := mgr.getFilePath(id)
	log.Debugf("remove local file: %s", id)
	exist, err := fileExist(filePath)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	err = os.Remove(filePath)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (mgr *CacheManagerImpl) entryInLocal(entry CacheEntry) bool {
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
		log.Debugf("recover uploading: %s", origFileId)
		err := mgr.uploadRemoteFile(origFileId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mgr *CacheManagerImpl) Add(id string, entry CacheEntry, upload bool) error {
	if upload {
		if err := mgr.uploadRemoteFile(id); err != nil {
			return err
		}
	}
	log.Debugf("add cache entry, id: %s, entry: %v, upload: %v", id, entry, upload)
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if entry.IsInLocal() {
		err := mgr.ensureFileSize(entry.CacheSize())
		if err != nil {
			return err
		}
		mgr.localSize += entry.CacheSize()
	}
	mgr.cache.Add(id, entry)
	return nil
}

func (mgr *CacheManagerImpl) Free(id string) error {
	log.Debugf("free file: %s", id)
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	value, ok := mgr.cache.Peek(id)
	if !ok {
		return errors.Errorf("%s not exist in cache list", id)
	}
	entry := value.(CacheEntry)
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
	log.Debugf("pin file: %s", id)
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	value, ok := mgr.cache.Get(id)
	if !ok {
		return errors.Errorf("%s not in cache list", id)
	}
	entry := value.(CacheEntry)
	if entry.IsInLocal() {
		return entry.Pin()
	}
	err := mgr.ensureFileSize(entry.CacheSize())
	if err != nil {
		return err
	}
	download, err := mgr.downloadRemoteFile(id)
	if err != nil {
		return err
	}
	if download {
		mgr.localSize += entry.CacheSize()
		entry.SetInLocal(true)
	}
	return entry.Pin()
}

func (mgr *CacheManagerImpl) Release(id string) error {
	log.Debugf("release file %s", id)
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	value, ok := mgr.cache.Peek(id)
	if !ok {
		return errors.Errorf("%s not in cache list", id)
	}
	entry := value.(CacheEntry)
	return entry.Unpin()
}

func (mgr *CacheManagerImpl) Len() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.cache.Len()
}

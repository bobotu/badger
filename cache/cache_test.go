package cache

import (
	"fmt"
	"github.com/coocood/badger/y"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

func getFilePath(dir, name string) string {
	return path.Join(dir, name)
}

func TestCacheMgr(t *testing.T) {
	tmpdir := os.TempDir()
	fmt.Println(tmpdir)
	cacheMgr := NewCacheManager(tmpdir, 3).(*CacheManagerImpl)

	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		filePath := getFilePath(tmpdir, fileName)
		fmt.Println(filePath)
		f, err := y.OpenSyncedFile(filePath, true)
		assert.NoError(t, err)
		n, err := f.Write([]byte("x"))
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.NoError(t, f.Close())
		entry := &CacheEntryImpl{
			id:       fileName,
			pinCnt:   0,
			inLocal:  true,
			fileSize: 1,
		}
		err = cacheMgr.Add(fileName, entry, true)
		assert.NoError(t, err)
	}

	for i := 3; i < 6; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		filePath := getFilePath(tmpdir, fileName)
		fmt.Println(filePath)
		f, err := y.OpenSyncedFile(filePath, true)
		assert.NoError(t, err)
		n, err := f.Write([]byte("x"))
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.NoError(t, f.Close())
		entry := &CacheEntryImpl{
			id:       fileName,
			pinCnt:   0,
			inLocal:  true,
			fileSize: 1,
		}
		err = cacheMgr.Add(fileName, entry, false)
		assert.NoError(t, err)
	}

	test0, err := cacheMgr.getEntry("test0.txt")
	assert.NoError(t, err)
	assert.Equal(t, false, test0.IsInLocal())
	assert.Equal(t, false, test0.Pinned())

	assert.NoError(t, cacheMgr.Pin("test0.txt"))

	for i := 6; i < 9; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		filePath := getFilePath(tmpdir, fileName)
		fmt.Println(filePath)
		f, err := y.OpenSyncedFile(filePath, true)
		assert.NoError(t, err)
		n, err := f.Write([]byte("x"))
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.NoError(t, f.Close())
		entry := &CacheEntryImpl{
			id:       fileName,
			pinCnt:   0,
			inLocal:  true,
			fileSize: 1,
		}
		err = cacheMgr.Add(fileName, entry, false)
		assert.NoError(t, err)
	}

	assert.Equal(t, 9, cacheMgr.Len())
	assert.NoError(t, cacheMgr.Pin("test8.txt"))
	assert.NoError(t, cacheMgr.Release("test0.txt"))

	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		err = cacheMgr.Free(fileName)
		assert.NoError(t, err)
	}
	assert.Equal(t, 6, cacheMgr.Len())

	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		filePath := getFilePath(tmpdir, fileName)
		fmt.Println(filePath)
		f, err := y.OpenSyncedFile(filePath, true)
		assert.NoError(t, err)
		n, err := f.Write([]byte("x"))
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.NoError(t, f.Close())
		entry := &CacheEntryImpl{
			id:       fileName,
			pinCnt:   0,
			inLocal:  true,
			fileSize: 1,
		}
		err = cacheMgr.Add(fileName, entry, true)
		assert.NoError(t, err)
	}

	assert.Error(t, cacheMgr.Free("test8.txt"))

	for i := 0; i < 9; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		entry, err := cacheMgr.getEntry(fileName)
		assert.NoError(t, err)
		if i == 1 || i == 2 || i == 8 {
			assert.Equal(t, true, entry.IsInLocal())
			if i == 8 {
				assert.Equal(t, true, entry.Pinned())
			} else {
				assert.Equal(t, false, entry.Pinned())
			}
		} else {
			assert.Equal(t, false, entry.IsInLocal())
			assert.Equal(t, false, entry.Pinned())
		}
	}

	assert.NoError(t, cacheMgr.Release("test8.txt"))
	test8, err := cacheMgr.getEntry("test8.txt")
	assert.NoError(t, err)
	assert.Equal(t, false, test8.Pinned())
	assert.Equal(t, true, test8.IsInLocal())

	assert.NoError(t, cacheMgr.Pin("test0.txt"))
	assert.NoError(t, cacheMgr.Pin("test2.txt"))
	assert.NoError(t, cacheMgr.Pin("test8.txt"))

	for i := 3; i < 6; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		err = cacheMgr.Free(fileName)
		assert.NoError(t, err)
	}
	assert.Equal(t, 6, cacheMgr.Len())

	for i := 3; i < 6; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		filePath := getFilePath(tmpdir, fileName)
		fmt.Println(filePath)
		f, err := y.OpenSyncedFile(filePath, true)
		assert.NoError(t, err)
		n, err := f.Write([]byte("x"))
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.NoError(t, f.Close())
		entry := &CacheEntryImpl{
			id:       fileName,
			pinCnt:   0,
			inLocal:  true,
			fileSize: 1,
		}
		err = cacheMgr.Add(fileName, entry, true)
		assert.Error(t, err)
	}
	assert.Equal(t, 6, cacheMgr.Len())

	assert.NoError(t, cacheMgr.Release("test0.txt"))
	assert.NoError(t, cacheMgr.Release("test8.txt"))

	for i := 3; i < 6; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		filePath := getFilePath(tmpdir, fileName)
		fmt.Println(filePath)
		f, err := y.OpenSyncedFile(filePath, true)
		assert.NoError(t, err)
		n, err := f.Write([]byte("x"))
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.NoError(t, f.Close())
		entry := &CacheEntryImpl{
			id:       fileName,
			pinCnt:   0,
			inLocal:  true,
			fileSize: 1,
		}
		err = cacheMgr.Add(fileName, entry, true)
		assert.NoError(t, err)
	}

	assert.Equal(t, 9, cacheMgr.Len())
	for i := 0; i < 9; i++ {
		fileName := fmt.Sprintf("test%d.txt", i)
		entry, err := cacheMgr.getEntry(fileName)
		assert.NoError(t, err)
		if i == 2 || i == 4 || i == 5 {
			assert.Equal(t, true, entry.IsInLocal())
			if i == 2 {
				assert.Equal(t, true, entry.Pinned())
			} else {
				assert.Equal(t, false, entry.Pinned())
			}
		} else {
			assert.Equal(t, false, entry.IsInLocal())
			assert.Equal(t, false, entry.Pinned())
		}
	}

}

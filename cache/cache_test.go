package cache

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestCacheMgr(t *testing.T) {
	tmpdir := os.TempDir()
	fmt.Println(tmpdir)
	cacheMgr := NewCacheManager(tmpdir, 10)

	for i := 0; i < 10; i++ {
		fileName := fmt.Sprintf("%s/test%d.txt", tmpdir, i)
		f, err := os.Open(fileName)
		assert.NoError(t, err)
		f.Write([]byte("x"))
		f.Close()
		entry := &CacheEntryImpl{
			id: fileName,
			pinned: 0,
			inLocal: false,
			fileSize: 1,
		}
		err = cacheMgr.Add(fileName, entry, true)
		assert.NoError(t, err)
	}
}

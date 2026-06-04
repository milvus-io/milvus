// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package bloomfilter

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPkStatsMmapManager_FileBacked_LoadAndRelease(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)
	defer mgr.Close()

	data := []byte("test bloom filter data for mmap manager")

	handle, err := mgr.Load("remote/path/stats", data)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(handle.Data()))
	assert.Equal(t, string(data), string(handle.Data()))
	filePath := handle.filePath
	stat, err := os.Stat(filePath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), stat.Mode().Perm())
	assert.Equal(t, 1, mgr.ActiveHandles())

	mgr.Release(handle)
	assert.Nil(t, handle.Data())
	assert.Equal(t, 0, mgr.ActiveHandles())
	_, err = os.Stat(filePath)
	assert.Error(t, err)
}

func TestPkStatsMmapManager_FileBacked_RepeatedLoadIndependentHandles(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)
	defer mgr.Close()

	data := []byte("test bloom filter data for repeated mmap loads")

	handle1, err := mgr.Load("remote/path/stats", data)
	require.NoError(t, err)
	handle2, err := mgr.Load("remote/path/stats", data)
	require.NoError(t, err)
	assert.NotEqual(t, handle1.key, handle2.key)
	assert.NotEqual(t, handle1.filePath, handle2.filePath)
	assert.Equal(t, 2, mgr.ActiveHandles())

	mgr.Release(handle1)
	assert.Equal(t, 1, mgr.ActiveHandles())
	assert.Equal(t, string(data), string(handle2.Data()))

	mgr.Release(handle2)
	assert.Equal(t, 0, mgr.ActiveHandles())
}

func TestPkStatsMmapManager_MemoryBacked_LoadAndRelease(t *testing.T) {
	mgr := NewPkStatsMmapManager("", false)
	defer mgr.Close()

	data := []byte("test bloom filter data for memory manager")

	handle, err := mgr.Load("remote/path/stats", data)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(handle.Data()))
	assert.Equal(t, string(data), string(handle.Data()))
	assert.Equal(t, 1, mgr.ActiveHandles())

	mgr.Release(handle)
	assert.Nil(t, handle.Data())
	assert.Equal(t, 0, mgr.ActiveHandles())
}

func TestPkStatsMmapManager_DifferentPaths(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)
	defer mgr.Close()

	data1 := []byte("bloom filter data 1")
	data2 := []byte("bloom filter data 2 - different content")

	handle1, err := mgr.Load("path/a", data1)
	require.NoError(t, err)

	handle2, err := mgr.Load("path/b", data2)
	require.NoError(t, err)

	assert.Equal(t, string(data1), string(handle1.Data()))
	assert.Equal(t, string(data2), string(handle2.Data()))
	assert.Equal(t, 2, mgr.ActiveHandles())

	mgr.Release(handle1)
	mgr.Release(handle2)
	assert.Equal(t, 0, mgr.ActiveHandles())
}

func TestPkStatsMmapManager_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)
	defer mgr.Close()

	data := make([]byte, 4096) // Page-aligned
	for i := range data {
		data[i] = byte(i % 256)
	}

	var wg sync.WaitGroup
	const goroutines = 20
	handles := make([]*PkStatsMmapHandle, goroutines)

	// Concurrent Load
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			handle, err := mgr.Load("concurrent/path", data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), len(handle.Data()))
			handles[idx] = handle
		}(i)
	}
	wg.Wait()
	assert.Equal(t, goroutines, mgr.ActiveHandles())

	// Concurrent Release
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(handle *PkStatsMmapHandle) {
			defer wg.Done()
			mgr.Release(handle)
		}(handles[i])
	}
	wg.Wait()

	assert.Equal(t, 0, mgr.ActiveHandles())
}

func TestPkStatsMmapManager_Close(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)

	data := []byte("test data")
	handle1, err := mgr.Load("path1", data)
	require.NoError(t, err)
	handle2, err := mgr.Load("path2", data)
	require.NoError(t, err)
	assert.Equal(t, 2, mgr.ActiveHandles())

	mgr.Close()

	assert.Equal(t, 0, mgr.ActiveHandles())
	mgr.Release(handle1)
	mgr.Release(handle2)
}

func TestPkStatsMmapManager_ReleaseNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)
	defer mgr.Close()

	// Should not panic
	mgr.Release(nil)
	mgr.Release(&PkStatsMmapHandle{key: "non/existent/path"})
}

func TestPkStatsMmapManager_MemoryBacked_RepeatedLoadIndependentHandles(t *testing.T) {
	mgr := NewPkStatsMmapManager("", false)
	defer mgr.Close()

	data := []byte("independent bloom filter data")

	handle1, err := mgr.Load("segment/100/stats", data)
	require.NoError(t, err)
	handle2, err := mgr.Load("segment/100/stats", data)
	require.NoError(t, err)

	assert.NotEqual(t, handle1.key, handle2.key)
	assert.Equal(t, string(handle1.Data()), string(handle2.Data()))
	assert.Equal(t, 2, mgr.ActiveHandles())

	mgr.Release(handle1)
	assert.Equal(t, 1, mgr.ActiveHandles())
	assert.Equal(t, string(data), string(handle2.Data()))

	mgr.Release(handle2)
	assert.Equal(t, 0, mgr.ActiveHandles())
}

func TestPkStatsMmapManager_FileBackedFailureBranches(t *testing.T) {
	tmpDir := t.TempDir()
	blocker := filepath.Join(tmpDir, "file")
	require.NoError(t, os.WriteFile(blocker, []byte("not a dir"), 0o600))

	mgr := NewPkStatsMmapManager(blocker, true)
	_, err := mgr.Load("path/a", []byte("data"))
	assert.Error(t, err)

	_, err = mgr.mmapFile(filepath.Join(tmpDir, "missing"), 1)
	assert.Error(t, err)

	emptyFile := filepath.Join(tmpDir, "empty")
	require.NoError(t, os.WriteFile(emptyFile, nil, 0o600))
	_, err = mgr.mmapFile(emptyFile, 0)
	assert.Error(t, err)

	writeErrMgr := NewPkStatsMmapManager(t.TempDir(), true)
	localPath := writeErrMgr.localCachePath(makeMmapEntryKey("write-error", 1))
	require.NoError(t, os.MkdirAll(localPath, 0o755))
	_, err = writeErrMgr.Load("write-error", []byte("data"))
	assert.Error(t, err)

	mmapErrMgr := NewPkStatsMmapManager(t.TempDir(), true)
	_, err = mmapErrMgr.Load("empty-data", nil)
	assert.Error(t, err)
	assert.Equal(t, 0, mmapErrMgr.ActiveHandles())
}

func TestPkStatsMmapManager_InvalidMunmapWarnings(t *testing.T) {
	releaseMgr := NewPkStatsMmapManager(t.TempDir(), true)
	releaseHandle := &PkStatsMmapHandle{key: "bad-release"}
	releaseMgr.mappings[releaseHandle.key] = &mmapEntry{
		data:     []byte("not mmap data"),
		filePath: filepath.Join(t.TempDir(), "release-cache"),
	}
	releaseMgr.Release(releaseHandle)
	assert.Equal(t, 0, releaseMgr.ActiveHandles())

	closeMgr := NewPkStatsMmapManager(t.TempDir(), true)
	closeMgr.mappings["bad-close"] = &mmapEntry{
		data:     []byte("not mmap data"),
		filePath: filepath.Join(t.TempDir(), "close-cache"),
	}
	closeMgr.Close()
	assert.Equal(t, 0, closeMgr.ActiveHandles())
}

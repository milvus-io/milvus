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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPkStatsMmapManager_FileBacked_GetOrLoadAndRelease(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)
	defer mgr.Close()

	data := []byte("test bloom filter data for mmap manager")

	// First load
	result, err := mgr.GetOrLoad("remote/path/stats", data)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(result))
	assert.Equal(t, string(data), string(result))

	// Second load should return same data with increased refcount
	result2, err := mgr.GetOrLoad("remote/path/stats", data)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(result2))

	// Release once - should not unmap yet
	mgr.Release("remote/path/stats")

	// Still accessible via refcount
	ok := mgr.AddRef("remote/path/stats")
	assert.True(t, ok)

	// Release two more times (original + AddRef)
	mgr.Release("remote/path/stats")
	mgr.Release("remote/path/stats")

	// Should be unmapped now
	ok = mgr.AddRef("remote/path/stats")
	assert.False(t, ok)
}

func TestPkStatsMmapManager_MemoryBacked_GetOrLoadAndRelease(t *testing.T) {
	mgr := NewPkStatsMmapManager("", false)
	defer mgr.Close()

	data := []byte("test bloom filter data for memory manager")

	// First load
	result, err := mgr.GetOrLoad("remote/path/stats", data)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(result))
	assert.Equal(t, string(data), string(result))

	// Second load should return same data with increased refcount
	result2, err := mgr.GetOrLoad("remote/path/stats", data)
	require.NoError(t, err)
	assert.Equal(t, string(data), string(result2))

	// Release once
	mgr.Release("remote/path/stats")

	// Still accessible
	ok := mgr.AddRef("remote/path/stats")
	assert.True(t, ok)

	// Release remaining refs
	mgr.Release("remote/path/stats")
	mgr.Release("remote/path/stats")

	// Should be gone now
	ok = mgr.AddRef("remote/path/stats")
	assert.False(t, ok)
}

func TestPkStatsMmapManager_TryAddRef(t *testing.T) {
	mgr := NewPkStatsMmapManager("", false)
	defer mgr.Close()

	// Not loaded yet
	data, ok := mgr.TryAddRef("path/a")
	assert.False(t, ok)
	assert.Nil(t, data)

	// Load it
	original := []byte("bloom filter data")
	_, err := mgr.GetOrLoad("path/a", original)
	require.NoError(t, err)

	// TryAddRef should succeed and return same data
	data, ok = mgr.TryAddRef("path/a")
	assert.True(t, ok)
	assert.Equal(t, string(original), string(data))

	// Release all refs (GetOrLoad + TryAddRef)
	mgr.Release("path/a")
	mgr.Release("path/a")

	// Should be gone
	data, ok = mgr.TryAddRef("path/a")
	assert.False(t, ok)
	assert.Nil(t, data)
}

func TestPkStatsMmapManager_DifferentPaths(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)
	defer mgr.Close()

	data1 := []byte("bloom filter data 1")
	data2 := []byte("bloom filter data 2 - different content")

	result1, err := mgr.GetOrLoad("path/a", data1)
	require.NoError(t, err)

	result2, err := mgr.GetOrLoad("path/b", data2)
	require.NoError(t, err)

	assert.Equal(t, string(data1), string(result1))
	assert.Equal(t, string(data2), string(result2))

	mgr.Release("path/a")
	mgr.Release("path/b")
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

	// Concurrent GetOrLoad
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := mgr.GetOrLoad("concurrent/path", data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), len(result))
		}()
	}
	wg.Wait()

	// Concurrent Release
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.Release("concurrent/path")
		}()
	}
	wg.Wait()

	// Should be fully released
	ok := mgr.AddRef("concurrent/path")
	assert.False(t, ok)
}

func TestPkStatsMmapManager_Close(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)

	data := []byte("test data")
	_, err := mgr.GetOrLoad("path1", data)
	require.NoError(t, err)
	_, err = mgr.GetOrLoad("path2", data)
	require.NoError(t, err)

	mgr.Close()

	ok := mgr.AddRef("path1")
	assert.False(t, ok)
	ok = mgr.AddRef("path2")
	assert.False(t, ok)
}

func TestPkStatsMmapManager_ReleaseNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := NewPkStatsMmapManager(tmpDir, true)
	defer mgr.Close()

	// Should not panic
	mgr.Release("non/existent/path")
}

func TestPkStatsMmapManager_MemoryBacked_SharedData(t *testing.T) {
	mgr := NewPkStatsMmapManager("", false)
	defer mgr.Close()

	data := []byte("shared bloom filter data")

	// Simulate delegator loading first
	result1, err := mgr.GetOrLoad("segment/100/stats", data)
	require.NoError(t, err)

	// Simulate segment loader hitting cache via TryAddRef
	result2, ok := mgr.TryAddRef("segment/100/stats")
	require.True(t, ok)

	// Both should reference the same underlying data
	assert.Equal(t, string(result1), string(result2))

	// Release both refs
	mgr.Release("segment/100/stats")
	mgr.Release("segment/100/stats")

	// Should be cleaned up
	_, ok = mgr.TryAddRef("segment/100/stats")
	assert.False(t, ok)
}

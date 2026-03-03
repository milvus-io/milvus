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
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// mmapEntry holds a reference-counted data region (file-mmap or memory).
type mmapEntry struct {
	data     []byte // mmap'd region or in-memory buffer
	refCount int32  // protected by PkStatsMmapManager.mu
	fileSize int64
	filePath string // local cache file path (empty for memory mode)
}

// PkStatsMmapManager manages bloom filter data with reference counting.
// It supports two modes:
//   - file-backed: writes data to a local file and mmaps it (off Go heap)
//   - memory-backed: keeps data as a Go []byte (on heap, but deduplicated)
//
// Both modes share the same data across multiple callers via ref counting,
// eliminating duplicate bloom filter copies (e.g. delegator + segment).
type PkStatsMmapManager struct {
	mu         sync.RWMutex
	cacheDir   string
	fileBacked bool
	mappings   map[string]*mmapEntry // remotePath → entry
}

// NewPkStatsMmapManager creates a new manager.
// If fileBacked is true, data is written to cacheDir and mmap'd (off Go heap).
// If fileBacked is false, data is kept in memory (on Go heap, but deduplicated).
func NewPkStatsMmapManager(cacheDir string, fileBacked bool) *PkStatsMmapManager {
	return &PkStatsMmapManager{
		cacheDir:   cacheDir,
		fileBacked: fileBacked,
		mappings:   make(map[string]*mmapEntry),
	}
}

// GetOrLoad returns the data for a bloom filter identified by remotePath.
// If already loaded, it increments the refcount and returns the existing data.
// Otherwise, it stores the provided data (via file-mmap or in-memory) and returns it.
func (m *PkStatsMmapManager) GetOrLoad(remotePath string, data []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Fast path: already loaded, just bump refcount
	if entry, ok := m.mappings[remotePath]; ok {
		entry.refCount++
		return entry.data, nil
	}

	if m.fileBacked {
		return m.loadFileBacked(remotePath, data)
	}
	return m.loadMemoryBacked(remotePath, data)
}

// loadFileBacked writes data to a local file and mmaps it.
// Caller must hold m.mu write lock.
func (m *PkStatsMmapManager) loadFileBacked(remotePath string, data []byte) ([]byte, error) {
	localPath := m.localCachePath(remotePath)

	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errors.Wrapf(err, "failed to create cache directory %s", dir)
	}

	if err := os.WriteFile(localPath, data, 0o644); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache file %s", localPath)
	}

	mmapData, err := m.mmapFile(localPath, int64(len(data)))
	if err != nil {
		os.Remove(localPath)
		return nil, err
	}

	entry := &mmapEntry{
		data:     mmapData,
		refCount: 1,
		fileSize: int64(len(data)),
		filePath: localPath,
	}
	m.mappings[remotePath] = entry

	log.Info("file-mmap'd bloom filter",
		zap.String("remotePath", remotePath),
		zap.String("localPath", localPath),
		zap.Int("size", len(data)))

	return mmapData, nil
}

// loadMemoryBacked stores data directly in memory.
// Caller must hold m.mu write lock.
func (m *PkStatsMmapManager) loadMemoryBacked(remotePath string, data []byte) ([]byte, error) {
	entry := &mmapEntry{
		data:     data,
		refCount: 1,
		fileSize: int64(len(data)),
	}
	m.mappings[remotePath] = entry

	log.Info("memory-cached bloom filter",
		zap.String("remotePath", remotePath),
		zap.Int("size", len(data)))

	return data, nil
}

// TryAddRef attempts to increment the refcount for an already-loaded path.
// If the path is loaded, returns the data and true.
// If not loaded, returns nil and false.
func (m *PkStatsMmapManager) TryAddRef(remotePath string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.mappings[remotePath]
	if !ok {
		return nil, false
	}
	entry.refCount++
	return entry.data, true
}

// Release decrements the refcount for a remote path.
// When refcount reaches zero, the entry is removed (and unmapped if file-backed).
func (m *PkStatsMmapManager) Release(remotePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.mappings[remotePath]
	if !ok {
		return
	}

	entry.refCount--
	newRef := entry.refCount
	if newRef <= 0 {
		if m.fileBacked && entry.filePath != "" {
			if err := unix.Munmap(entry.data); err != nil {
				log.Warn("failed to munmap bloom filter",
					zap.String("remotePath", remotePath),
					zap.Error(err))
			}
			os.Remove(entry.filePath)
		}
		delete(m.mappings, remotePath)

		log.Info("released bloom filter entry",
			zap.String("remotePath", remotePath),
			zap.Bool("fileBacked", m.fileBacked))
	}
}

// AddRef increments the refcount for an already-loaded remote path.
// Returns false if the path is not loaded.
func (m *PkStatsMmapManager) AddRef(remotePath string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.mappings[remotePath]
	if !ok {
		return false
	}
	entry.refCount++
	return true
}

// Close releases all entries and cleans up.
func (m *PkStatsMmapManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for remotePath, entry := range m.mappings {
		if m.fileBacked && entry.filePath != "" {
			if err := unix.Munmap(entry.data); err != nil {
				log.Warn("failed to munmap bloom filter during close",
					zap.String("remotePath", remotePath),
					zap.Error(err))
			}
			os.Remove(entry.filePath)
		}
	}
	m.mappings = make(map[string]*mmapEntry)
}

// localCachePath generates a local cache file path from the remote path.
func (m *PkStatsMmapManager) localCachePath(remotePath string) string {
	h := xxh3.HashString128(remotePath)
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], h.Hi)
	binary.BigEndian.PutUint64(b[8:], h.Lo)
	return filepath.Join(m.cacheDir, "bf_cache", hex.EncodeToString(b[:]))
}

// mmapFile opens a file and mmaps it read-only.
func (m *PkStatsMmapManager) mmapFile(path string, size int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file %s", path)
	}
	defer f.Close()

	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to mmap file %s", path)
	}

	return data, nil
}

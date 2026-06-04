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
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/milvus-io/milvus/pkg/v3/log"
)

// PkStatsMmapHandle owns one loaded PK stats data region.
type PkStatsMmapHandle struct {
	key      string
	data     []byte
	filePath string // local cache file path (empty for memory mode)
}

// Data returns the loaded PK stats data.
func (h *PkStatsMmapHandle) Data() []byte {
	if h == nil {
		return nil
	}
	return h.data
}

// mmapEntry holds one owned data region (file-mmap or memory).
type mmapEntry struct {
	handle   *PkStatsMmapHandle
	data     []byte // mmap'd region or in-memory buffer
	fileSize int64
	filePath string // local cache file path (empty for memory mode)
}

// PkStatsMmapManager manages loaded bloom filter data.
// It supports two modes:
//   - file-backed: writes data to a local file and mmaps it (off Go heap)
//   - memory-backed: keeps binary stats as a Go []byte for zero-copy BF reads
type PkStatsMmapManager struct {
	mu         sync.RWMutex
	cacheDir   string
	fileBacked bool
	nextID     uint64
	mappings   map[string]*mmapEntry // handle key to entry
}

// NewPkStatsMmapManager creates a new manager.
// If fileBacked is true, data is written to cacheDir and mmap'd (off Go heap).
// If fileBacked is false, binary stats data is kept in memory (on Go heap).
func NewPkStatsMmapManager(cacheDir string, fileBacked bool) *PkStatsMmapManager {
	return &PkStatsMmapManager{
		cacheDir:   cacheDir,
		fileBacked: fileBacked,
		mappings:   make(map[string]*mmapEntry),
	}
}

// FileBacked returns whether this manager stores data in local mmap files.
func (m *PkStatsMmapManager) FileBacked() bool {
	return m.fileBacked
}

// ActiveHandles returns the number of live loaded handles.
func (m *PkStatsMmapManager) ActiveHandles() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.mappings)
}

// Load stores the provided data (via file-mmap or in-memory) and returns an
// owned handle. Each call returns an independent handle; callers must release it.
func (m *PkStatsMmapManager) Load(remotePath string, data []byte) (*PkStatsMmapHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.nextEntryKeyLocked(remotePath)

	if m.fileBacked {
		return m.loadFileBacked(remotePath, key, data)
	}
	return m.loadMemoryBacked(remotePath, key, data)
}

// loadFileBacked writes data to a local file and mmaps it.
// Caller must hold m.mu write lock.
func (m *PkStatsMmapManager) loadFileBacked(remotePath string, key string, data []byte) (*PkStatsMmapHandle, error) {
	localPath := m.localCachePath(key)

	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errors.Wrapf(err, "failed to create cache directory %s", dir)
	}

	if err := os.WriteFile(localPath, data, 0o600); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache file %s", localPath)
	}

	mmapData, err := m.mmapFile(localPath, int64(len(data)))
	if err != nil {
		os.Remove(localPath)
		return nil, err
	}

	entry := &mmapEntry{
		data:     mmapData,
		fileSize: int64(len(data)),
		filePath: localPath,
	}
	handle := &PkStatsMmapHandle{
		key:      key,
		data:     mmapData,
		filePath: localPath,
	}
	entry.handle = handle
	m.mappings[key] = entry

	log.Info("file-mmap'd bloom filter",
		zap.String("remotePath", remotePath),
		zap.String("localPath", localPath),
		zap.Int("size", len(data)))

	return handle, nil
}

// loadMemoryBacked stores data directly in memory.
// Caller must hold m.mu write lock.
func (m *PkStatsMmapManager) loadMemoryBacked(remotePath string, key string, data []byte) (*PkStatsMmapHandle, error) {
	entry := &mmapEntry{
		data:     data,
		fileSize: int64(len(data)),
	}
	handle := &PkStatsMmapHandle{
		key:  key,
		data: data,
	}
	entry.handle = handle
	m.mappings[key] = entry

	log.Info("memory-cached bloom filter",
		zap.String("remotePath", remotePath),
		zap.Int("size", len(data)))

	return handle, nil
}

// Release releases an owned PK stats data handle.
func (m *PkStatsMmapManager) Release(handle *PkStatsMmapHandle) {
	if handle == nil || handle.key == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.mappings[handle.key]
	if !ok {
		clearMmapHandle(handle)
		return
	}

	if m.fileBacked && entry.filePath != "" {
		if err := unix.Munmap(entry.data); err != nil {
			log.Warn("failed to munmap bloom filter",
				zap.String("key", handle.key),
				zap.Error(err))
		}
		os.Remove(entry.filePath)
	}
	delete(m.mappings, handle.key)
	clearMmapHandle(handle)

	log.Info("released bloom filter entry",
		zap.Bool("fileBacked", m.fileBacked))
}

// Close releases all entries and cleans up.
func (m *PkStatsMmapManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, entry := range m.mappings {
		if m.fileBacked && entry.filePath != "" {
			if err := unix.Munmap(entry.data); err != nil {
				log.Warn("failed to munmap bloom filter during close",
					zap.String("key", key),
					zap.Error(err))
			}
			os.Remove(entry.filePath)
		}
		clearMmapHandle(entry.handle)
	}
	m.mappings = make(map[string]*mmapEntry)
}

func clearMmapHandle(handle *PkStatsMmapHandle) {
	if handle == nil {
		return
	}
	handle.key = ""
	handle.data = nil
	handle.filePath = ""
}

func (m *PkStatsMmapManager) nextEntryKeyLocked(remotePath string) string {
	m.nextID++
	return makeMmapEntryKey(remotePath, m.nextID)
}

func makeMmapEntryKey(remotePath string, id uint64) string {
	h := xxh3.HashString128(remotePath)
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], h.Hi)
	binary.BigEndian.PutUint64(b[8:], h.Lo)
	return hex.EncodeToString(b[:]) + "-" + strconv.FormatUint(id, 10)
}

// localCachePath generates a local cache file path from a handle key.
func (m *PkStatsMmapManager) localCachePath(key string) string {
	return filepath.Join(m.cacheDir, "bf_cache", key)
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

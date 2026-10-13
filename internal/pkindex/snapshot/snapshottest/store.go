// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package snapshottest provides an in-memory snapshot.TableStore for tests.
//
// It does not import package snapshot, so the internal tests of snapshot can
// import it without an import cycle.
package snapshottest

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex"
)

// errInjected is the cause of an injected failure.
var errInjected = errors.New("injected failure")

// Store keeps objects in memory. It is safe for concurrent use.
type Store struct {
	mu            sync.Mutex
	objects       map[string][]byte
	uploadFails   int
	downloadFails int
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{objects: make(map[string][]byte)}
}

// UploadFile reads the local file at path and stores its bytes under key.
func (s *Store) UploadFile(ctx context.Context, key, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uploadFails > 0 {
		s.uploadFails--
		return pkindex.MarkIO(errInjected, "upload %s", key)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return pkindex.MarkIO(err, "read %s", path)
	}
	s.objects[key] = data
	return nil
}

// DownloadFile writes the bytes under key to path through a temporary file
// and a rename. A missing key returns an error marked pkindex.ErrCorrupted.
func (s *Store) DownloadFile(ctx context.Context, key, path string) error {
	s.mu.Lock()
	if s.downloadFails > 0 {
		s.downloadFails--
		s.mu.Unlock()
		return pkindex.MarkIO(errInjected, "download %s", key)
	}
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return errors.Mark(errors.Wrapf(os.ErrNotExist, "object %s", key), pkindex.ErrCorrupted)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".download-*")
	if err != nil {
		return pkindex.MarkIO(err, "create temp file for %s", path)
	}
	defer os.Remove(tmp.Name()) // no effect after the rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return pkindex.MarkIO(err, "write %s", tmp.Name())
	}
	if err := tmp.Close(); err != nil {
		return pkindex.MarkIO(err, "close %s", tmp.Name())
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return pkindex.MarkIO(err, "rename to %s", path)
	}
	return nil
}

// FailUploads makes the next n UploadFile calls return an error marked
// pkindex.ErrIO.
func (s *Store) FailUploads(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploadFails = n
}

// FailDownloads makes the next n DownloadFile calls return an error marked
// pkindex.ErrIO.
func (s *Store) FailDownloads(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.downloadFails = n
}

// Keys returns the stored keys in sorted order.
func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Len returns the number of stored objects.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

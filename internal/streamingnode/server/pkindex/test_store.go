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

//go:build test

package pkindex

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
)

// errFakeInjected is the cause of an injected failure of a FakeStore.
var errFakeInjected = errors.New("injected failure")

// FakeStore is an in-memory TableStore for tests. It is safe for concurrent use.
type FakeStore struct {
	mu            sync.Mutex
	objects       map[string][]byte
	uploadFails   int
	downloadFails int
}

// NewFakeStore returns an empty FakeStore.
func NewFakeStore() *FakeStore {
	return &FakeStore{objects: make(map[string][]byte)}
}

// UploadFile reads the local file at path and stores its bytes under key.
func (s *FakeStore) UploadFile(ctx context.Context, key, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uploadFails > 0 {
		s.uploadFails--
		return pkroot.MarkIO(errFakeInjected, "upload %s", key)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return pkroot.MarkIO(err, "read %s", path)
	}
	s.objects[key] = data
	return nil
}

// DownloadFile writes the bytes under key to path through a temporary file
// and a rename. A missing key returns an error marked pkroot.ErrCorrupted.
func (s *FakeStore) DownloadFile(ctx context.Context, key, path string) error {
	s.mu.Lock()
	if s.downloadFails > 0 {
		s.downloadFails--
		s.mu.Unlock()
		return pkroot.MarkIO(errFakeInjected, "download %s", key)
	}
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return errors.Mark(errors.Wrapf(os.ErrNotExist, "object %s", key), pkroot.ErrCorrupted)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".download-*")
	if err != nil {
		return pkroot.MarkIO(err, "create temp file for %s", path)
	}
	defer os.Remove(tmp.Name()) // no effect after the rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return pkroot.MarkIO(err, "write %s", tmp.Name())
	}
	if err := tmp.Close(); err != nil {
		return pkroot.MarkIO(err, "close %s", tmp.Name())
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return pkroot.MarkIO(err, "rename to %s", path)
	}
	return nil
}

// FailUploads makes the next n UploadFile calls return an error marked
// pkroot.ErrIO.
func (s *FakeStore) FailUploads(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploadFails = n
}

// FailDownloads makes the next n DownloadFile calls return an error marked
// pkroot.ErrIO.
func (s *FakeStore) FailDownloads(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.downloadFails = n
}

// Keys returns the stored keys in sorted order.
func (s *FakeStore) Keys() []string {
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
func (s *FakeStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

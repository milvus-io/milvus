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

package manifest

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

// FakeObjectStore is an in-memory ObjectStore for tests. Now sets the modification time
// of written objects. ReadErr, WriteErr and RemoveErr inject faults; each one
// applies to the next matching call only and is then cleared.
type FakeObjectStore struct {
	mu   sync.Mutex
	objs map[string][]byte
	mod  map[string]time.Time
	Now  func() time.Time

	// ReadErr is returned by the next Read. When ReadErrKey is not empty,
	// ReadErr is returned only by the next Read of that key.
	ReadErr    error
	ReadErrKey string
	// WriteErr is returned by the next Write, which does not write.
	WriteErr error
	// RemoveErr is returned by the next MultiRemove, which removes nothing.
	RemoveErr error
}

// NewFakeObjectStore returns an empty FakeObjectStore whose clock is time.Now.
func NewFakeObjectStore() *FakeObjectStore {
	return &FakeObjectStore{objs: map[string][]byte{}, mod: map[string]time.Time{}, Now: time.Now}
}

// Write stores a copy of data under key.
func (s *FakeObjectStore) Write(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.WriteErr; err != nil {
		s.WriteErr = nil
		return err
	}
	s.objs[key] = append([]byte(nil), data...)
	s.mod[key] = s.Now()
	return nil
}

// Read returns the object under key, or an error marked with
// pkerr.ErrCorrupted when key does not exist.
func (s *FakeObjectStore) Read(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ReadErr; err != nil && (s.ReadErrKey == "" || s.ReadErrKey == key) {
		s.ReadErr, s.ReadErrKey = nil, ""
		return nil, err
	}
	b, ok := s.objs[key]
	if !ok {
		return nil, errors.Mark(errors.Newf("object %s not found", key), pkerr.ErrCorrupted)
	}
	return b, nil
}

// Walk calls fn for every object under prefix in key order until fn returns false.
func (s *FakeObjectStore) Walk(_ context.Context, prefix string, fn func(string, time.Time) bool) error {
	s.mu.Lock()
	keys := []string{}
	mods := map[string]time.Time{}
	for k := range s.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
			mods[k] = s.mod[k]
		}
	}
	s.mu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		if !fn(k, mods[k]) {
			return nil
		}
	}
	return nil
}

// MultiRemove removes keys. A key that does not exist is skipped.
func (s *FakeObjectStore) MultiRemove(_ context.Context, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.RemoveErr; err != nil {
		s.RemoveErr = nil
		return err
	}
	for _, k := range keys {
		delete(s.objs, k)
		delete(s.mod, k)
	}
	return nil
}

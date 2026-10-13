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

// Package fakes holds in-memory stores for the tests of the primary key index
// coordinator.
package fakes

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

var (
	// ErrKeyExists is returned by Create when the key already exists.
	ErrKeyExists = errors.New("key already exists")
	// ErrValueMismatch is returned by Swap when the stored value is not the expected value.
	ErrValueMismatch = errors.New("stored value is not the expected value")
)

// Pointers is an in-memory manifest pointer store. The fields below inject
// faults; each one applies to the next call only and is then cleared.
type Pointers struct {
	mu sync.Mutex
	kv map[string]string

	// BeforeWrite is called by the next Create or Swap before it checks its
	// condition, without the store lock held.
	BeforeWrite func()
	// WriteErr is returned by the next Create or Swap. When WriteApplies is
	// true, that call writes the value, without a check of its condition,
	// before it returns the error.
	WriteErr     error
	WriteApplies bool
	// LoadErr is returned by the next Load.
	LoadErr error
	// ListErr is returned by the next List.
	ListErr error
}

// NewPointers returns an empty Pointers.
func NewPointers() *Pointers { return &Pointers{kv: map[string]string{}} }

// Load returns the value of key and whether key exists. It returns ctx.Err()
// when ctx is done.
func (p *Pointers) Load(ctx context.Context, key string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.LoadErr; err != nil {
		p.LoadErr = nil
		return "", false, err
	}
	v, ok := p.kv[key]
	return v, ok, nil
}

// Create writes value only when key does not exist.
func (p *Pointers) Create(_ context.Context, key, value string) error {
	return p.write(key, value, func() error {
		if _, ok := p.kv[key]; ok {
			return errors.Wrapf(ErrKeyExists, "key %s", key)
		}
		return nil
	})
}

// Swap writes value only when the value of key equals expected.
func (p *Pointers) Swap(_ context.Context, key, expected, value string) error {
	return p.write(key, value, func() error {
		if v, ok := p.kv[key]; !ok || v != expected {
			return errors.Wrapf(ErrValueMismatch, "key %s", key)
		}
		return nil
	})
}

// write runs the injected faults, then writes value when check, called with
// the store lock held, returns nil.
func (p *Pointers) write(key, value string, check func() error) error {
	p.mu.Lock()
	hook := p.BeforeWrite
	p.BeforeWrite = nil
	p.mu.Unlock()
	if hook != nil {
		hook()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.WriteErr; err != nil {
		applies := p.WriteApplies
		p.WriteErr, p.WriteApplies = nil, false
		if applies {
			p.kv[key] = value
		}
		return err
	}
	if err := check(); err != nil {
		return err
	}
	p.kv[key] = value
	return nil
}

// Put writes value without a condition.
func (p *Pointers) Put(key, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.kv[key] = value
}

// List returns all keys with prefix and their values.
func (p *Pointers) List(_ context.Context, prefix string) ([]string, []string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ListErr; err != nil {
		p.ListErr = nil
		return nil, nil, err
	}
	var keys, vals []string
	for k, v := range p.kv {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
			vals = append(vals, v)
		}
	}
	return keys, vals, nil
}

// Store is an in-memory manifest.ObjectStore. Now sets the modification time
// of written objects. ReadErr, WriteErr and RemoveErr inject faults; each one
// applies to the next matching call only and is then cleared.
type Store struct {
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

// NewStore returns an empty Store whose clock is time.Now.
func NewStore() *Store {
	return &Store{objs: map[string][]byte{}, mod: map[string]time.Time{}, Now: time.Now}
}

// Write stores a copy of data under key.
func (s *Store) Write(_ context.Context, key string, data []byte) error {
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
func (s *Store) Read(_ context.Context, key string) ([]byte, error) {
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
func (s *Store) Walk(_ context.Context, prefix string, fn func(string, time.Time) bool) error {
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
func (s *Store) MultiRemove(_ context.Context, keys []string) error {
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

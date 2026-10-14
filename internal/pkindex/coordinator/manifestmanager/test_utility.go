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

package manifestmanager

import (
	"context"
	"maps"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
)

var (
	// ErrFakeKeyExists is returned by Save when a key that must not exist exists.
	ErrFakeKeyExists = errors.New("key already exists")
	// ErrFakeValueMismatch is returned by Save when a key does not hold the expected value.
	ErrFakeValueMismatch = errors.New("stored value is not the expected value")
)

// FakeMetaStore is an in-memory MetaStore for tests. The fields below
// inject faults; each one applies to the next call only and is then cleared.
type FakeMetaStore struct {
	mu sync.Mutex
	kv map[string]string

	// BeforeWrite is called by the next Save before it checks its
	// predicates, without the store lock held.
	BeforeWrite func()
	// WriteErr is returned by the next Save. When WriteApplies is true, that
	// call writes the values, without a check of its predicates, before it
	// returns the error.
	WriteErr     error
	WriteApplies bool
	// LoadErr is returned by the next Load. When LoadErrKey is not empty,
	// LoadErr is returned only by the next Load of that key.
	LoadErr    error
	LoadErrKey string
	// MultiLoadErr is returned by the next MultiLoad.
	MultiLoadErr error
	// ListErr is returned by the next List.
	ListErr error
	// MultiLoads counts the MultiLoad calls.
	MultiLoads int
	// LastSave holds the saves of the last Save call.
	LastSave map[string]string
}

// NewFakeMetaStore returns an empty FakeMetaStore.
func NewFakeMetaStore() *FakeMetaStore { return &FakeMetaStore{kv: map[string]string{}} }

// Load returns the value of key and whether key exists. It returns ctx.Err()
// when ctx is done.
func (p *FakeMetaStore) Load(ctx context.Context, key string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.LoadErr; err != nil && (p.LoadErrKey == "" || p.LoadErrKey == key) {
		p.LoadErr, p.LoadErrKey = nil, ""
		return "", false, err
	}
	v, ok := p.kv[key]
	return v, ok, nil
}

// MultiLoad returns the value of each key and whether it exists.
func (p *FakeMetaStore) MultiLoad(ctx context.Context, keys []string) ([]string, []bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.MultiLoads++
	if err := p.MultiLoadErr; err != nil {
		p.MultiLoadErr = nil
		return nil, nil, err
	}
	values := make([]string, len(keys))
	found := make([]bool, len(keys))
	for i, k := range keys {
		values[i], found[i] = p.kv[k]
	}
	return values, found, nil
}

// Save runs the injected faults, then writes saves when every predicate holds.
func (p *FakeMetaStore) Save(_ context.Context, saves map[string]string, preds ...predicates.Predicate) error {
	p.mu.Lock()
	hook := p.BeforeWrite
	p.BeforeWrite = nil
	p.mu.Unlock()
	if hook != nil {
		hook()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.LastSave = maps.Clone(saves)
	if err := p.WriteErr; err != nil {
		applies := p.WriteApplies
		p.WriteErr, p.WriteApplies = nil, false
		if applies {
			maps.Copy(p.kv, saves)
		}
		return err
	}
	for _, pred := range preds {
		v, ok := p.kv[pred.Key()]
		notExist := pred.Type() == predicates.PredTypeNotExist
		switch {
		case ok && notExist:
			return errors.Wrapf(ErrFakeKeyExists, "key %s", pred.Key())
		case !ok && !notExist, ok && !pred.IsTrue(v):
			return errors.Wrapf(ErrFakeValueMismatch, "key %s", pred.Key())
		}
	}
	maps.Copy(p.kv, saves)
	return nil
}

// Put writes value without a predicate.
func (p *FakeMetaStore) Put(key, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.kv[key] = value
}

// List returns all keys with prefix and their values.
func (p *FakeMetaStore) List(_ context.Context, prefix string) ([]string, []string, error) {
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

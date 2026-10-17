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

package authority

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

// This file is compiled only with the "test" build tag. It provides an in-memory
// engine.RW so that unit tests, WAL chain tests and integration tests can exercise
// the primary key index before the persistent engine is wired in.
// It keeps nothing across restarts and must not be used in production.

// errMemoryEngineTombstoneValue reports a put whose value is the reserved tombstone
// encoding, which the persistent engine rejects too.
var errMemoryEngineTombstoneValue = errors.New("memory engine value is the reserved tombstone encoding")

var _ engine.RW = (*MemoryEngine)(nil)

// MemoryEngine is an in-memory engine.RW. A deleted key is absent, so MultiGet
// returns nil for it, as the persistent engine does.
type MemoryEngine struct {
	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

// NewMemoryEngine creates an empty in-memory engine.
func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{data: make(map[string][]byte)}
}

func (e *MemoryEngine) MultiGet(_ context.Context, keys [][]byte) ([][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, engine.ErrClosed
	}
	values := make([][]byte, len(keys))
	for i, key := range keys {
		if value, ok := e.data[string(key)]; ok {
			values[i] = value
		}
	}
	return values, nil
}

func (e *MemoryEngine) Write(_ context.Context, muts []engine.Mutation) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return engine.ErrClosed
	}
	for _, mut := range muts {
		if !mut.Delete && codec.IsTombstone(mut.Value) {
			return errMemoryEngineTombstoneValue
		}
	}
	for _, mut := range muts {
		if mut.Delete {
			delete(e.data, string(mut.Key))
			continue
		}
		e.data[string(mut.Key)] = append([]byte(nil), mut.Value...)
	}
	return nil
}

// Close makes every later call fail with engine.ErrClosed.
func (e *MemoryEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	e.data = nil
	return nil
}

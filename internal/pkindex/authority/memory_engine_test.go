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
package authority

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

var _ engine.RW = (*MemoryEngine)(nil)

// MemoryEngine is an in-memory engine.RW for the tests of this package. A
// deleted key is absent, so MultiGet returns nil for it, as the persistent
// engine does.
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

// writeRecords writes the records of pks to eng: a put of segmentID for each
// key of puts and a tombstone for each key of deletes, deletes first.
func writeRecords(ctx context.Context, eng engine.RW, segmentID int64, puts, deletes []PK) error {
	muts := make([]engine.Mutation, 0, len(puts)+len(deletes))
	for _, pk := range deletes {
		muts = append(muts, engine.Mutation{Key: pk.Encode(), Delete: true})
	}
	value := codec.EncodePKEntry(codec.PKEntry{SegmentID: segmentID})
	for _, pk := range puts {
		muts = append(muts, engine.Mutation{Key: pk.Encode(), Value: value})
	}
	if len(muts) == 0 {
		return nil
	}
	return eng.Write(ctx, muts)
}

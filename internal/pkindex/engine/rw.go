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

package engine

import (
	"context"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

func (e *Engine) Write(ctx context.Context, muts []Mutation) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return errClosed
	}
	// RLock keeps e.active the active generation for the whole call, so the
	// batch and its bytes both go to the generation that took it
	return e.active.write(muts)
}

func (e *Engine) MultiGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, errClosed
	}
	e.commitMu.RLock()
	defer e.commitMu.RUnlock()

	out := make([][]byte, len(keys))
	for i, key := range keys {
		v, err := e.lookup(key)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// lookup resolves one key across the layers, newest first: the active
// generation, the frozen generations, then the committed tables. The first
// layer that holds the key decides, and a tombstone decides "absent".
func (e *Engine) lookup(key []byte) ([]byte, error) {
	if v, found, err := e.active.get(key); err != nil || found {
		return v, err
	}
	for _, g := range e.frozen {
		if v, found, err := g.get(key); err != nil || found {
			return v, err
		}
	}
	for _, cr := range e.committed {
		if v, found, err := cr.get(key); err != nil || found {
			return v, err
		}
	}
	return nil, nil
}

// get reads key from this committed table, with the same found and tombstone
// rules as generation.get. An empty table, and a key outside the table's
// range, are pruned without touching the file. The bounds are compared as
// bytes even when nil: the largest key of a non-empty table with a nil or
// empty MaxKey is the empty key, and proto3 bytes carry an empty key as nil.
func (cr installedTable) get(key []byte) (value []byte, found bool, err error) {
	if cr.info.NumEntries == 0 {
		return nil, false, nil
	}
	if sst.Comparer.Compare(key, cr.info.MinKey) < 0 || sst.Comparer.Compare(key, cr.info.MaxKey) > 0 {
		return nil, false, nil
	}
	v, ok, err := cr.reader.Get(key)
	if err != nil || !ok {
		return nil, false, err
	}
	if codec.IsTombstone(v) {
		return nil, true, nil
	}
	return v, true, nil
}

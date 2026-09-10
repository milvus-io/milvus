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

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"

	"github.com/milvus-io/milvus/internal/pkindex/core/codec"
	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
)

func (e *Engine) Write(ctx context.Context, muts []Mutation) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return errClosed
	}
	b := e.active.db.NewBatch()
	defer b.Close()
	for _, m := range muts {
		v := m.Value
		switch {
		case m.Delete:
			// a codec tombstone, not a pebble native tombstone: it must stay
			// visible to Get so it masks entries in the layers below
			v = codec.EncodeTombstone()
		case codec.IsTombstone(v):
			return errors.Wrapf(errReservedTombstoneValue, "key %x", m.Key)
		}
		if err := b.Set(m.Key, v, nil); err != nil {
			return sst.MarkPebbleErr(errors.Wrapf(err, "stage write to generation %d", e.active.gen))
		}
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return sst.MarkPebbleErr(errors.Wrapf(err, "commit write to generation %d", e.active.gen))
	}
	return nil
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
		v, err := e.getOne(key)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (e *Engine) getOne(key []byte) ([]byte, error) {
	// generations newest-first: active, then the frozen ones
	if v, found, err := e.getGeneration(e.active, key); err != nil || found {
		return v, err
	}
	for _, g := range e.draining {
		if v, found, err := e.getGeneration(g, key); err != nil || found {
			return v, err
		}
	}
	// committed tables in installed (recency) order, range-pruned
	for _, cr := range e.committed {
		if cr.info.MinKey != nil && sst.Comparer.Compare(key, cr.info.MinKey) < 0 {
			continue
		}
		if cr.info.MaxKey != nil && sst.Comparer.Compare(key, cr.info.MaxKey) > 0 {
			continue
		}
		v, ok, err := cr.reader.Get(key)
		if err != nil {
			return nil, err
		}
		if ok {
			if codec.IsTombstone(v) {
				return nil, nil
			}
			return v, nil
		}
	}
	return nil, nil
}

// getGeneration reports found=true once a layer holds the key, whether as a
// value or as a tombstone; a tombstone resolves to a nil value and stops the
// walk so lower layers cannot resurrect it.
func (e *Engine) getGeneration(g *generation, key []byte) (value []byte, found bool, err error) {
	v, closer, err := g.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, sst.MarkPebbleErr(errors.Wrapf(err, "read generation %s", g.dir))
	}
	defer closer.Close()
	if codec.IsTombstone(v) {
		return nil, true, nil
	}
	// the value is only valid until closer.Close, so hand back a copy
	return append([]byte{}, v...), true, nil
}

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

package compaction

import (
	"container/heap"
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// Source is one input table. *sst.Reader is one.
type Source interface {
	NewIter() (*sst.Iter, error)
}

// Operator changes or drops an entry that won the last-writer-wins merge.
// The merge itself is not an operator; it always runs first. key and value
// alias iterator buffers: the operator must not modify them in place, and the
// returned slice must stay valid until the Add of the output returns.
type Operator interface {
	Apply(key, value []byte) (out []byte, keep bool, err error)
}

// Output takes the merged entries in key order. The key and value slices
// are valid only during Add.
type Output interface {
	Add(key, value []byte) error
}

// Stats counts the entries of one merge.
type Stats struct {
	Read    int64 // entries read from the sources
	Dropped int64 // entries shadowed by a newer one, dropped by an operator, or dropped tombstones
	Written int64
}

type mergeEntry struct {
	src int
	it  *sst.Iter
}

// mergeHeap orders the heads of the sources by key, then by source order, so
// that the newest source comes first for equal keys.
type mergeHeap []mergeEntry

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(a, b int) bool {
	if c := sst.Comparer.Compare(h[a].it.Key(), h[b].it.Key()); c != 0 {
		return c < 0
	}
	return h[a].src < h[b].src
}
func (h mergeHeap) Swap(a, b int) { h[a], h[b] = h[b], h[a] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeEntry)) }
func (h *mergeHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// Merge writes the entries of sources, newest source first, to out in key
// order. For one key only the entry of the newest source is kept. The kept
// entry goes through ops in order; an operator that returns keep=false drops
// it. A kept tombstone is dropped when dropTombstones is set. progress, when
// not nil, counts the entries read. The merge holds one iterator per source
// and one copied key; nothing else is kept in memory. It stops with the
// error of ctx when ctx ends.
func Merge(ctx context.Context, sources []Source, ops []Operator, dropTombstones bool, out Output, progress *atomic.Int64) (Stats, error) {
	var stats Stats
	iters := make([]*sst.Iter, 0, len(sources))
	defer func() {
		// close errors of read-only iterators are ignored
		for _, it := range iters {
			it.Close()
		}
	}()
	h := make(mergeHeap, 0, len(sources))
	for i, s := range sources {
		it, err := s.NewIter()
		if err != nil {
			return stats, err
		}
		iters = append(iters, it)
		if it.First() {
			h = append(h, mergeEntry{src: i, it: it})
		} else if err := it.Error(); err != nil {
			return stats, err
		}
	}
	heap.Init(&h)

	var cur []byte
	haveCur := false
	for h.Len() > 0 {
		if stats.Read&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
		}
		e := h[0]
		key, value := e.it.Key(), e.it.Value()
		stats.Read++
		if progress != nil {
			progress.Add(1)
		}
		if !haveCur || sst.Comparer.Compare(key, cur) != 0 {
			cur = append(cur[:0], key...)
			haveCur = true
			if err := emit(key, value, ops, dropTombstones, out, &stats); err != nil {
				return stats, err
			}
		} else {
			stats.Dropped++
		}
		if e.it.Next() {
			heap.Fix(&h, 0)
		} else {
			if err := e.it.Error(); err != nil {
				return stats, err
			}
			heap.Pop(&h)
		}
	}
	return stats, nil
}

func emit(key, value []byte, ops []Operator, dropTombstones bool, out Output, stats *Stats) error {
	keep := true
	for _, op := range ops {
		var err error
		if value, keep, err = op.Apply(key, value); err != nil {
			return err
		}
		if !keep {
			break
		}
	}
	if keep && dropTombstones && codec.IsTombstone(value) {
		keep = false
	}
	if !keep {
		stats.Dropped++
		return nil
	}
	stats.Written++
	return out.Add(key, value)
}

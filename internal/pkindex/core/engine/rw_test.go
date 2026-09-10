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
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/core/codec"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

func TestWriteAndRead(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	require.NoError(t, e.Write(ctx, []Mutation{
		{Key: pk(1), Value: entry(100)},
		{Key: pk(2), Value: entry(200)},
	}))

	requireSegment(t, e, pk(1), 100)
	assert.Nil(t, getOne(t, e, pk(3)), "never-written key must be absent")

	// delete masks
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Delete: true}}))
	assert.Nil(t, getOne(t, e, pk(1)), "deleted key must be absent")

	// re-insert after delete
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(101)}}))
	requireSegment(t, e, pk(1), 101)
}

// The tombstone encoding is reserved: a caller deletes with a nil Value, and a
// Value that happens to equal the tombstone is a caller bug, not a delete.
func TestWriteRejectsReservedTombstoneValue(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(1), Value: entry(10)}}))

	err := e.Write(ctx, []Mutation{
		{Key: pk(2), Value: entry(20)},
		{Key: pk(1), Value: codec.EncodeTombstone()},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errReservedTombstoneValue), "got %v", err)
	assert.Contains(t, err.Error(), "key ", "the offending key must be in the message")
	// a caller bug, so it belongs to no category a retry or a boundary acts on
	assert.False(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.False(t, errors.Is(err, pkerr.ErrIO))
	assert.False(t, errors.Is(err, pkerr.ErrCorrupted))

	// the rejected batch leaves no trace
	requireSegment(t, e, pk(1), 10)
	assert.Nil(t, getOne(t, e, pk(2)))
}

func TestCommittedReadAndGenerationOverride(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	baseDir := t.TempDir()

	table := buildCommitted(t, baseDir, 1, map[int64][]byte{
		10: entry(1),
		20: entry(2),
		30: codec.EncodeTombstone(), // deleted in an earlier merge round
	})
	require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{table}))

	requireSegment(t, e, pk(10), 1)

	// committed tombstone means absent
	assert.Nil(t, getOne(t, e, pk(30)))

	// outside every table's range: pruned, absent
	assert.Nil(t, getOne(t, e, pk(5)))
	assert.Nil(t, getOne(t, e, pk(35)))

	// the active generation overrides the committed set
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(10), Value: entry(11)}}))
	requireSegment(t, e, pk(10), 11)

	// a delete in the active generation masks the committed set
	require.NoError(t, e.Write(ctx, []Mutation{{Key: pk(20), Delete: true}}))
	assert.Nil(t, getOne(t, e, pk(20)))
}

func TestCommittedRecencyOrder(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	older := buildCommitted(t, t.TempDir(), 1, map[int64][]byte{7: entry(1), 8: entry(1)})
	newer := buildCommitted(t, t.TempDir(), 2, map[int64][]byte{7: entry(2)})

	// newest-first: overlapping key 7 resolves from the newer table
	require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{newer, older}))
	requireSegment(t, e, pk(7), 2)
	// key 8 only in the older table still resolves
	requireSegment(t, e, pk(8), 1)
}

func TestConcurrentInstallAndRead(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	t1 := buildCommitted(t, t.TempDir(), 1, map[int64][]byte{1: entry(10)})
	t2 := buildCommitted(t, t.TempDir(), 2, map[int64][]byte{1: entry(20)})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				vs, err := e.MultiGet(ctx, [][]byte{pk(1)})
				if !assert.NoError(t, err) || len(vs) != 1 || vs[0] == nil {
					continue // vs[0]==nil only before the first install
				}
				got, err := codec.DecodePKEntry(vs[0])
				assert.NoError(t, err)
				assert.Contains(t, []int64{10, 20}, got.SegmentID,
					"a read must always observe one complete committed set")
			}
		}()
	}
	for i := 0; i < 50; i++ {
		require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{t1}))
		require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{t2}))
	}
	close(stop)
	wg.Wait()
}

func TestCommittedReadersShareNodeCache(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	table := buildCommitted(t, t.TempDir(), 1, map[int64][]byte{1: entry(10), 2: entry(20)})
	require.NoError(t, e.InstallCommitted(ctx, []CommittedTable{table}))

	c := e.cfg.Shared.cache
	before := c.Metrics()
	requireSegment(t, e, pk(1), 10)
	requireSegment(t, e, pk(1), 10)
	after := c.Metrics()
	assert.Greater(t, after.Count, before.Count, "baseline blocks must land in the node-level cache")
	assert.Greater(t, after.Hits, before.Hits, "a repeated baseline probe must hit the node-level cache")
}

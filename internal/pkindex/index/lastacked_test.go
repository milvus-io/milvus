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

package index

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
)

func insertRequest(idx *Index, pks ...int64) snpkindex.Request {
	return snpkindex.Request{Kind: snpkindex.KindInsert, VChannel: idx.vchannel, PKs: int64PKs(pks...)}
}

func TestLateApplyHoldsTheWatermark(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	intent, err := idx.Decide(ctx, insertRequest(idx, 1))
	require.NoError(t, err)
	ch.Tick(10)
	assert.EqualValues(t, 0, idx.LastAppliedTimeTick(), "the writer is still in flight")
	intent.Apply(ctx, snpkindex.Applied{SegmentID: 5})
	assert.EqualValues(t, 10, idx.LastAppliedTimeTick())
	assert.Panics(t, func() { intent.Apply(ctx, snpkindex.Applied{SegmentID: 5}) })
	assert.Panics(t, func() { intent.Discard(ctx, nil) })
	assert.EqualValues(t, 10, idx.LastAppliedTimeTick())
}

func TestDiscardEndsTheWriter(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	intent, err := idx.Decide(ctx, insertRequest(idx, 1))
	require.NoError(t, err)
	assert.Empty(t, intent.Decision().CompanionDeletePKs)
	assert.GreaterOrEqual(t, intent.LockWait(), time.Duration(0))
	ch.Tick(10)
	intent.Discard(ctx, errors.New("append failed"))
	assert.EqualValues(t, 10, idx.LastAppliedTimeTick())
	assert.False(t, idx.dirty(), "a discarded write does not touch the engine")
	assert.Panics(t, func() { intent.Discard(ctx, nil) })
}

func TestDecideErrorEndsTheWriter(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	// a closed engine makes the probe of Decide fail
	require.NoError(t, idx.eng.Close())
	_, err := idx.Decide(ctx, insertRequest(idx, 1))
	require.Error(t, err)
	ch.Tick(10)
	assert.EqualValues(t, 10, idx.LastAppliedTimeTick())
}

func TestCompanionDeleteDecision(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 1, 1)

	intent, err := idx.Decide(ctx, insertRequest(idx, 1, 2))
	require.NoError(t, err)
	assert.Equal(t, int64PKs(1), intent.Decision().CompanionDeletePKs)
	intent.Apply(ctx, snpkindex.Applied{SegmentID: 2})
	assert.EqualValues(t, 2, segmentOf(t, idx, 1))
}

func TestWriteMarksTheIndexDirty(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	assert.False(t, idx.dirty())

	// a transaction body only records pending writes
	body, err := idx.Decide(ctx, snpkindex.Request{Kind: snpkindex.KindInsert, VChannel: "v0", PKs: int64PKs(1), TxnID: 7})
	require.NoError(t, err)
	body.Apply(ctx, snpkindex.Applied{SegmentID: 3})
	assert.False(t, idx.dirty())
	assert.Equal(t, -1, idx.heapPos)
	assert.Equal(t, []int64{7}, idx.PendingTxnIDs())

	insert(t, idx, 4, 2)
	assert.True(t, idx.dirtyFlag.Load())
	assert.GreaterOrEqual(t, idx.heapPos, 0)

	idx.DropTxn(7)
	assert.Empty(t, idx.PendingTxnIDs())
}

func TestLastAckedIgnoresIdleVChannels(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	assert.EqualValues(t, uint64(math.MaxUint64), ch.LastAcked())

	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	ch.Tick(5)
	assert.EqualValues(t, 5, ch.LastAcked(), "no Index is dirty")

	insert(t, a, 1, 1)
	insert(t, b, 1, 1)
	ch.Tick(10)
	assert.EqualValues(t, 5, ch.LastAcked(), "a and b hold it at their commit")
	a.submitHandover()
	b.submitHandover()
	assert.EqualValues(t, 10, ch.LastAcked())

	// b stays idle and does not hold back the writes of a
	insert(t, a, 2, 2)
	ch.Tick(20)
	assert.EqualValues(t, 10, ch.LastAcked())
	a.submitHandover()
	ch.Tick(30)
	assert.EqualValues(t, 30, ch.LastAcked())

	// b becomes dirty again with an old commit, the value does not go down
	insert(t, b, 3, 3)
	assert.EqualValues(t, 10, ch.lastAckedRaw())
	assert.EqualValues(t, 30, ch.LastAcked())
}

// lastAckedRaw is LastAcked without the clamp.
func (c *Channel) lastAckedRaw() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAckedLocked()
}

// bruteLastAcked computes the raw LastAcked over every Index.
func bruteLastAcked(c *Channel) uint64 {
	v := c.wm.lastApplied()
	for _, idx := range c.list() {
		if idx.dirty() && idx.LastCommittedTimeTick() < v {
			v = idx.LastCommittedTimeTick()
		}
	}
	return v
}

func TestLastAckedHeapMatchesBruteForce(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	vchannels := []string{"va", "vb", "vc", "vd"}
	indexes := make([]*Index, 0, len(vchannels))
	for _, v := range vchannels {
		indexes = append(indexes, env.add(ch, v))
	}

	r := rand.New(rand.NewSource(42))
	var open []*Intent
	tt := uint64(0)
	prev := uint64(0)
	pk := int64(0)
	for i := 0; i < 1000; i++ {
		idx := indexes[r.Intn(len(indexes))]
		switch op := r.Intn(20); {
		case op < 7:
			pk++
			intent, err := idx.Decide(ctx, insertRequest(idx, pk))
			require.NoError(t, err)
			intent.Apply(ctx, snpkindex.Applied{SegmentID: pk})
		case op < 9:
			// a transaction body takes no stripe lock, so it can stay open
			// across later steps and hold the watermark back
			intent, err := idx.Decide(ctx, snpkindex.Request{Kind: snpkindex.KindInsert, VChannel: idx.vchannel, PKs: int64PKs(pk), TxnID: int64(i + 1)})
			require.NoError(t, err)
			open = append(open, intent)
		case op < 11 && len(open) > 0:
			k := r.Intn(len(open))
			open[k].Apply(ctx, snpkindex.Applied{SegmentID: 1})
			open = append(open[:k], open[k+1:]...)
		case op < 18:
			tt++
			ch.Tick(tt)
		default:
			if idx.dirty() {
				idx.submitHandover()
			}
		}
		require.Equal(t, bruteLastAcked(ch), ch.lastAckedRaw(), "step %d", i)
		got := ch.LastAcked()
		require.GreaterOrEqual(t, got, prev, "step %d", i)
		prev = got
	}
	for _, intent := range open {
		intent.Apply(ctx, snpkindex.Applied{SegmentID: 1})
	}
	assert.Empty(t, env.client.applyErrList())
	assert.Empty(t, env.fatalList())
}

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

package pkindex

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
)

func insertRequest(idx *index.Index, pks ...int64) authority.DecideRequest {
	return authority.DecideRequest{Kind: authority.KindInsert, VChannel: idx.VChannel(), PKs: int64PKs(pks...)}
}

func TestLateApplyHoldsTheWatermark(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	intent, err := idx.Authority().Decide(ctx, insertRequest(idx, 1))
	require.NoError(t, err)
	ch.ObserveTimeTick(10)
	assert.EqualValues(t, 0, ch.wm.lastApplied(), "the writer is still in flight")
	intent.Apply(ctx, authority.Applied{SegmentID: 5})
	assert.EqualValues(t, 10, ch.wm.lastApplied())
	assert.Panics(t, func() { intent.Apply(ctx, authority.Applied{SegmentID: 5}) })
	assert.Panics(t, func() { intent.Discard(ctx, nil) })
	assert.EqualValues(t, 10, ch.wm.lastApplied())
}

func TestDiscardEndsTheWriter(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	intent, err := idx.Authority().Decide(ctx, insertRequest(idx, 1))
	require.NoError(t, err)
	assert.Empty(t, intent.Decision().CompanionDeletePKs)
	assert.GreaterOrEqual(t, intent.LockWait(), time.Duration(0))
	ch.ObserveTimeTick(10)
	intent.Discard(ctx, errors.New("append failed"))
	assert.EqualValues(t, 10, ch.wm.lastApplied())
	assert.False(t, idx.Dirty(), "a discarded write does not touch the engine")
	assert.Panics(t, func() { intent.Discard(ctx, nil) })
}

func TestDecideErrorEndsTheWriter(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	// a closed engine makes the probe of Decide fail
	require.NoError(t, idx.EngineForTest().Close())
	_, err := idx.Authority().Decide(ctx, insertRequest(idx, 1))
	require.Error(t, err)
	ch.ObserveTimeTick(10)
	assert.EqualValues(t, 10, ch.wm.lastApplied())
}

func TestCompanionDeleteDecision(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 1, 1)

	intent, err := idx.Authority().Decide(ctx, insertRequest(idx, 1, 2))
	require.NoError(t, err)
	assert.Equal(t, int64PKs(1), intent.Decision().CompanionDeletePKs)
	intent.Apply(ctx, authority.Applied{SegmentID: 2})
	assert.EqualValues(t, 2, segmentOf(t, idx, 1))
}

func TestWriteMarksTheIndexDirty(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	assert.False(t, idx.Dirty())

	// a transaction body only records pending writes
	body, err := idx.Authority().Decide(ctx, authority.DecideRequest{Kind: authority.KindInsert, VChannel: "v0", PKs: int64PKs(1), TxnID: 7})
	require.NoError(t, err)
	body.Apply(ctx, authority.Applied{SegmentID: 3})
	assert.False(t, idx.Dirty())
	assert.False(t, inHeap(ch, idx))
	assert.Equal(t, []int64{7}, idx.Authority().PendingTxnIDs())

	insert(t, idx, 4, 2)
	assert.True(t, idx.DirtyForTest())
	assert.True(t, inHeap(ch, idx))

	idx.Authority().DropTxn(7)
	assert.Empty(t, idx.Authority().PendingTxnIDs())
}

func TestLastAckedIgnoresIdleVChannels(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	assert.EqualValues(t, uint64(math.MaxUint64), ch.LastAcked())

	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	ch.ObserveTimeTick(5)
	assert.EqualValues(t, 5, ch.LastAcked(), "no Index is dirty")

	insert(t, a, 1, 1)
	insert(t, b, 1, 1)
	ch.ObserveTimeTick(10)
	assert.EqualValues(t, 5, ch.LastAcked(), "a and b hold it at their commit")
	ch.submitHandover(a)
	ch.submitHandover(b)
	assert.EqualValues(t, 10, ch.LastAcked())

	// b stays idle and does not hold back the writes of a
	insert(t, a, 2, 2)
	ch.ObserveTimeTick(20)
	assert.EqualValues(t, 10, ch.LastAcked())
	ch.submitHandover(a)
	ch.ObserveTimeTick(30)
	assert.EqualValues(t, 30, ch.LastAcked())

	// b becomes dirty again with an old commit, the value does not go down
	insert(t, b, 3, 3)
	assert.EqualValues(t, 10, ch.lastAckedRaw())
	assert.EqualValues(t, 30, ch.LastAcked())
}

// lastAckedRaw is LastAcked without the clamp.
func (c *PChannelIndexManager) lastAckedRaw() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAckedLocked()
}

// bruteLastAcked computes the raw LastAcked over every Index.
func bruteLastAcked(c *PChannelIndexManager) uint64 {
	v := c.wm.lastApplied()
	for _, idx := range c.list() {
		if idx.Dirty() && idx.LastCommittedTimeTick() < v {
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
	indexes := make([]*index.Index, 0, len(vchannels))
	for _, v := range vchannels {
		indexes = append(indexes, env.add(ch, v))
	}

	r := rand.New(rand.NewSource(42))
	var open []*authority.WriteGuard
	tt := uint64(0)
	prev := uint64(0)
	pk := int64(0)
	for i := 0; i < 1000; i++ {
		idx := indexes[r.Intn(len(indexes))]
		switch op := r.Intn(20); {
		case op < 7:
			pk++
			intent, err := idx.Authority().Decide(ctx, insertRequest(idx, pk))
			require.NoError(t, err)
			intent.Apply(ctx, authority.Applied{SegmentID: pk})
		case op < 9:
			// a transaction body takes no stripe lock, so it can stay open
			// across later steps and hold the watermark back
			intent, err := idx.Authority().Decide(ctx, authority.DecideRequest{Kind: authority.KindInsert, VChannel: idx.VChannel(), PKs: int64PKs(pk), TxnID: int64(i + 1)})
			require.NoError(t, err)
			open = append(open, intent)
		case op < 11 && len(open) > 0:
			k := r.Intn(len(open))
			open[k].Apply(ctx, authority.Applied{SegmentID: 1})
			open = append(open[:k], open[k+1:]...)
		case op < 18:
			tt++
			ch.ObserveTimeTick(tt)
		default:
			if idx.Dirty() {
				ch.submitHandover(idx)
			}
		}
		require.Equal(t, bruteLastAcked(ch), ch.lastAckedRaw(), "step %d", i)
		got := ch.LastAcked()
		require.GreaterOrEqual(t, got, prev, "step %d", i)
		prev = got
	}
	for _, intent := range open {
		intent.Apply(ctx, authority.Applied{SegmentID: 1})
	}
	assert.Empty(t, env.client.applyErrList())
	assert.Empty(t, env.fatalList())
}

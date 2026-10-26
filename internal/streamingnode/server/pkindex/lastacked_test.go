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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
)

func insertRequest(idx *index.Index, pks ...int64) dedup.DecideRequest {
	return dedup.DecideRequest{Kind: dedup.KindInsert, PKs: int64PKs(pks...)}
}

func TestCompanionDeleteDecision(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, ch, idx, 1, 1)

	intent, err := idx.Decider().Decide(ctx, insertRequest(idx, 1, 2))
	require.NoError(t, err)
	assert.Equal(t, int64PKs(1), intent.CompanionDeletePKs())
	intent.Appended(2)
	// the observe applies the companion delete and the insert, as one message
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Deletes: int64PKs(1), Puts: puts(2, 1, 2), TimeTick: 2}))
	assert.EqualValues(t, 2, segmentOf(t, idx, 1))
	assert.EqualValues(t, 2, segmentOf(t, idx, 2))
}

func TestWriteMarksTheIndexDirty(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	assert.False(t, idx.Dirty())

	// a transaction body only records pending writes
	body, err := idx.Decider().Decide(ctx, dedup.DecideRequest{Kind: dedup.KindInsert, PKs: int64PKs(1), TxnID: 7})
	require.NoError(t, err)
	body.Appended(3)
	assert.False(t, idx.Dirty())
	assert.False(t, inHeap(ch, idx))
	assert.Equal(t, []int64{7}, dedup.PendingTxnIDsForTest(idx.Decider()))

	insert(t, ch, idx, 4, 2)
	assert.True(t, idx.DirtyForTest())
	assert.True(t, inHeap(ch, idx))

	idx.Decider().DropEndedTxns(func(txnID int64) bool { return txnID == 7 })
	assert.Empty(t, dedup.PendingTxnIDsForTest(idx.Decider()))
}

func TestLastAckedIgnoresIdleVChannels(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	assert.EqualValues(t, uint64(math.MaxUint64), ch.LastAcked())

	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	ch.ObserveTimeTick(5)
	assert.EqualValues(t, 5, ch.LastAcked(), "no Index is dirty")

	insert(t, ch, a, 1, 1)
	insert(t, ch, b, 1, 1)
	ch.ObserveTimeTick(10)
	assert.EqualValues(t, 5, ch.LastAcked(), "a and b hold it at their commit")
	handover(t, a)
	handover(t, b)
	assert.EqualValues(t, 10, ch.LastAcked())

	// b stays idle and does not hold back the writes of a
	insert(t, ch, a, 2, 2)
	ch.ObserveTimeTick(20)
	assert.EqualValues(t, 10, ch.LastAcked())
	handover(t, a)
	ch.ObserveTimeTick(30)
	assert.EqualValues(t, 30, ch.LastAcked())

	// b becomes dirty again with an old commit, the value does not go down
	insert(t, ch, b, 3, 3)
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
	v := c.LastObserved()
	for _, idx := range c.list() {
		if idx.Dirty() && idx.LastCommittedTimeTick() < v {
			v = idx.LastCommittedTimeTick()
		}
	}
	return v
}

func TestLastAckedHeapMatchesBruteForce(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	vchannels := []string{"va", "vb", "vc", "vd"}
	indexes := make([]*index.Index, 0, len(vchannels))
	for _, v := range vchannels {
		indexes = append(indexes, env.add(ch, v))
	}

	r := rand.New(rand.NewSource(42))
	tt := uint64(0)
	prev := uint64(0)
	pk := int64(0)
	for i := 0; i < 1000; i++ {
		idx := indexes[r.Intn(len(indexes))]
		switch op := r.Intn(20); {
		case op < 9:
			pk++
			insert(t, ch, idx, pk, pk)
		case op < 18:
			tt++
			ch.ObserveTimeTick(tt)
		default:
			if idx.Dirty() {
				handover(t, idx)
			}
		}
		require.Equal(t, bruteLastAcked(ch), ch.lastAckedRaw(), "step %d", i)
		got := ch.LastAcked()
		require.GreaterOrEqual(t, got, prev, "step %d", i)
		prev = got
	}
	assert.Empty(t, env.client.applyErrList())
	assert.Empty(t, env.fatalList())
}

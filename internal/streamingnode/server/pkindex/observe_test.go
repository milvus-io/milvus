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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestObserveTimeTickOnlyMovesLastObservedForward(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(7)
	assert.EqualValues(t, 7, ch.LastObserved(), "the seed is the global checkpoint")
	ch.ObserveTimeTick(20, WALTailSnapshot{})
	assert.EqualValues(t, 20, ch.LastObserved())
	ch.ObserveTimeTick(15, WALTailSnapshot{})
	assert.EqualValues(t, 20, ch.LastObserved(), "never goes back")
}

func TestObserveTimeTickRemovesInDoubtEntriesOfIdleVChannels(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	intent, err := idx.Decider().Decide(ctx, dedup.DecideRequest{Kind: dedup.KindInsert, PKs: int64PKs(1)})
	require.NoError(t, err)
	intent.InDoubt(10)
	require.Equal(t, 1, dedup.InflightLenForTest(idx.Decider()))

	ch.ObserveTimeTick(9, WALTailSnapshot{})
	assert.Equal(t, 1, dedup.InflightLenForTest(idx.Decider()), "the write may still be applied after 9")
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	assert.Equal(t, 0, dedup.InflightLenForTest(idx.Decider()), "a TimeTick reaches an idle vchannel")
}

func TestManagerApplyAddsADirtiedIndexToTheHeap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	assert.EqualValues(t, 10, ch.LastAcked(), "a clean Index follows lastObserved")

	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: 11}))
	ch.ObserveTimeTick(12, WALTailSnapshot{})
	assert.EqualValues(t, 0, ch.lastAckedUnclamped(), "a dirty Index holds LastAcked at its last commit")
	assert.Contains(t, ch.dirtyIndexes(), idx)

	require.NoError(t, ch.Apply(ctx, "no-such-vchannel", index.Write{Puts: puts(1, 1), TimeTick: 11}), "a vchannel without an Index is ignored")
}

// tsAt returns a time tick whose physical part is d after a fixed origin.
func tsAt(d time.Duration) uint64 {
	origin := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	return tsoutil.ComposeTSByTime(origin.Add(d))
}

func TestRotateRunsOnlyInTheDriver(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	env.setParams(func(p *Params) { p.ActiveBytes = 1 })
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1, 2), TimeTick: 5}))
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	assert.Equal(t, 0, idx.EngineForTest().Stats().FrozenGenerations, "the observe never rotates")
	assert.True(t, ch.wantsHandover(), "the size trigger is met")
	require.Equal(t, []*index.Index{idx}, picks(ch, farFuture))

	ch.step(farFuture)
	assert.EqualValues(t, 10, idx.LastCommittedTimeTick(), "the driver rotated and committed")
}

func TestCoveredIsAtMostLastObservedAtTheRotate(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")

	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: 5}))
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	// a write the observe applied after the TimeTick, not covered by lastObserved yet
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(2, 2), TimeTick: 15}))

	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.EqualValues(t, 10, p.Covered(), "covered is the lastObserved read before the rotate")
	assert.LessOrEqual(t, p.Covered(), ch.LastObserved())
	assert.Equal(t, 1, idx.EngineForTest().Stats().FrozenGenerations)
	assert.Zero(t, idx.EngineForTest().Stats().ActiveBytes, "the write above covered sits in the frozen generation")
	assert.EqualValues(t, 2, segmentOf(t, idx, 2))
	require.NoError(t, idx.CommitForTest(ctx, p))

	// a restart replays everything above the checkpoint, so the write above
	// covered is applied again: same key, same value
	idx.SetLastCommittedForTest(10)
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(2, 2), TimeTick: 15}))
	assert.EqualValues(t, 2, segmentOf(t, idx, 2))
}

// TestASlowStageDoesNotBlockTheObserve: StageFrozen writes the whole
// generation to a table under the cycle lock of the engine. A big generation
// makes it take long enough to overlap the observe calls below, which must
// not wait for it: Apply and ObserveTimeTick never take that lock.
func TestASlowStageDoesNotBlockTheObserve(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	pks := make([]int64, 300000)
	for i := range pks {
		pks[i] = int64(i)
	}
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, pks...), TimeTick: 5}))
	ch.ObserveTimeTick(10, WALTailSnapshot{})

	staged := make(chan struct{})
	go func() {
		defer close(staged)
		_, err := idx.PrepareForTest(ctx) // RotateIncrement + StageFrozen
		assert.NoError(t, err)
	}()
	var worst time.Duration
	overlapped := false
	for i := 0; i < 200; i++ {
		start := time.Now()
		require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(2, int64(1000000+i)), TimeTick: uint64(20 + i)}))
		ch.ObserveTimeTick(uint64(20+i), WALTailSnapshot{})
		if d := time.Since(start); d > worst {
			worst = d
		}
		select {
		case <-staged:
		default:
			overlapped = true
		}
	}
	<-staged
	assert.True(t, overlapped, "the stage must still run while the observe calls happen; grow the generation if not")
	assert.Less(t, worst, 100*time.Millisecond, "an observe call waited for the stage")
}

func TestTriggersFireOnlyFromAnObservedTimeTick(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, asyncScheduler{})
	env.setParams(func(p *Params) { p.ActiveBytes = 1 })
	ch := env.openChannel(0)
	ch.Start() // the real driver goroutine, no ticker
	defer ch.Close()
	idx := env.add(ch, "v0")

	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: 5}))
	time.Sleep(200 * time.Millisecond)
	assert.Zero(t, ch.steps.Load(), "no step without an observed TimeTick")
	assert.Empty(t, env.client.requestList())

	ch.ObserveTimeTick(10, WALTailSnapshot{})
	require.Eventually(t, func() bool { return idx.LastCommittedTimeTick() == 10 }, waitTimeout, time.Millisecond)
}

func TestTailTriggerReadsTheTailOfTheObserve(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 100
	})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: 5}))

	ch.ObserveTimeTick(10, WALTailSnapshot{Bytes: 99, CompletedTimeTick: 10})
	assert.False(t, ch.wantsHandover(), "below the tail threshold")
	ch.ObserveTimeTick(11, WALTailSnapshot{Bytes: 100, CompletedTimeTick: 0})
	assert.False(t, ch.wantsHandover(), "the index does not hold the checkpoint back")
	ch.ObserveTimeTick(12, WALTailSnapshot{Bytes: 100, CompletedTimeTick: 10})
	assert.True(t, ch.wantsHandover())
	assert.Equal(t, []*index.Index{idx}, picks(ch, farFuture))
}

func TestStallTriggerCountsObservedTime(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.StallTimeout = 10 * time.Minute
	})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: tsAt(0)}))

	ch.ObserveTimeTick(tsAt(time.Second), WALTailSnapshot{CompletedTimeTick: tsAt(time.Second)})
	assert.False(t, ch.wantsHandover(), "the stall clock just started")
	ch.ObserveTimeTick(tsAt(9*time.Minute), WALTailSnapshot{CompletedTimeTick: tsAt(9 * time.Minute)})
	assert.False(t, ch.wantsHandover())
	ch.ObserveTimeTick(tsAt(11*time.Minute), WALTailSnapshot{CompletedTimeTick: tsAt(11 * time.Minute)})
	assert.True(t, ch.wantsHandover(), "held the checkpoint back for the stall timeout, by observed time")
	assert.Equal(t, []*index.Index{idx}, picks(ch, farFuture))

	// the clock resets when the index no longer holds the checkpoint back
	ch.step(farFuture)
	ch.ObserveTimeTick(tsAt(12*time.Minute), WALTailSnapshot{CompletedTimeTick: tsAt(12 * time.Minute)})
	assert.Zero(t, ch.stallSince.Load())
	assert.False(t, ch.wantsHandover())
}

func TestExplicitFlushRequestsAHandoverOnceLastObservedReachesIt(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	env.setParams(func(p *Params) { p.ActiveBytes = math.MaxUint64 })
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: 25}))

	// the observe saw a ManualFlush of v0 at tt 30
	ch.ObserveFlush("v0", 30)
	ch.ObserveTimeTick(20, WALTailSnapshot{})
	assert.False(t, ch.wantsHandover(), "lastObserved is below the flush")
	assert.Empty(t, picks(ch, farFuture))

	ch.ObserveTimeTick(30, WALTailSnapshot{})
	assert.True(t, ch.wantsHandover())
	ch.step(farFuture)
	assert.EqualValues(t, 30, idx.LastCommittedTimeTick(), "the handover covers the flush")
	assert.False(t, ch.wantsHandover(), "the flush is served")

	ch.ObserveFlush("no-such-vchannel", 40) // ignored
	ch.ObserveFlushAll(40)
	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 2), TimeTick: 35}))
	ch.ObserveTimeTick(40, WALTailSnapshot{})
	assert.Equal(t, []*index.Index{idx}, picks(ch, farFuture))
}

func TestDriverWakesForTheBackoffOfAFailedHandover(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, asyncScheduler{})
	env.setParams(func(p *Params) {
		p.ActiveBytes = 1
		p.BackoffBase = 50 * time.Millisecond
		p.MaxBackoff = 50 * time.Millisecond
	})
	ch := env.openChannel(0)
	ch.Start()
	defer ch.Close()
	idx := env.add(ch, "v0")
	env.store.FailUploads(1)

	require.NoError(t, ch.Apply(ctx, "v0", index.Write{Puts: puts(1, 1), TimeTick: 5}))
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	// the first attempt fails on the upload; no further TimeTick arrives, the
	// backoff timer alone must bring the retry
	require.Eventually(t, func() bool { return idx.LastCommittedTimeTick() == 10 }, waitTimeout, time.Millisecond)
	assert.GreaterOrEqual(t, ch.steps.Load(), int64(2))
}

// TestManagerApplyIgnoresADroppedIndex: the observe looks the Index up and
// then writes it. A DropCollection in between closes the Index, and the
// write is moot: Apply reports no error, so lastObserved keeps moving. A
// closed Index that is still attached reports the closed engine.
func TestManagerApplyIgnoresADroppedIndex(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	dropped := env.add(ch, "v0")
	require.NoError(t, ch.DropCollection(ctx, "v0"))
	require.NoError(t, ch.apply(ctx, dropped, index.Write{Puts: puts(1, 1), TimeTick: 5}))

	closed := env.add(ch, "v1")
	closed.Close(ctx)
	err := ch.Apply(ctx, "v1", index.Write{Puts: puts(1, 1), TimeTick: 5})
	require.True(t, errors.Is(err, engine.ErrClosed), "%v", err)
}

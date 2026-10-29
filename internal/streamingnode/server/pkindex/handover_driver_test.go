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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// tsAt returns a time tick whose physical part is d after a fixed origin.
func tsAt(d time.Duration) uint64 {
	origin := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	return tsoutil.ComposeTSByTime(origin.Add(d))
}

func TestSizeTrigger(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	pks := make([]int64, 0, 100)
	for i := range int64(100) {
		pks = append(pks, i)
	}
	insert(t, ch, a, 1, pks...)
	insert(t, ch, b, 1, 1)
	small := b.ActiveBytes()
	require.Greater(t, a.ActiveBytes(), small)
	env.setParams(func(p *Params) { p.ActiveBytes = small + 1 })

	assert.Equal(t, []*index.Index{a}, picks(ch, farFuture))
}

func TestTailTriggerPicksTheOldestCommit(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	env.setParams(func(p *Params) { p.ActiveBytes = math.MaxUint64 })

	insert(t, ch, a, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.submitHandover(a)
	require.EqualValues(t, 10, a.LastCommittedTimeTick())
	insert(t, ch, a, 1, 2)
	insert(t, ch, b, 1, 1)
	ch.ObserveTimeTick(20, WALTailSnapshot{})

	ch.ObserveTimeTick(20, WALTailSnapshot{Bytes: 0, CompletedTimeTick: 1000})
	assert.Empty(t, picks(ch, farFuture), "the tail is below every threshold")
	env.setParams(func(p *Params) { p.TailBytes = 100 })
	ch.ObserveTimeTick(20, WALTailSnapshot{Bytes: 100, CompletedTimeTick: 1000})
	assert.Equal(t, []*index.Index{b}, picks(ch, farFuture))

	ch.submitHandover(b)
	require.EqualValues(t, 20, b.LastCommittedTimeTick())
	insert(t, ch, b, 1, 2)
	assert.Equal(t, []*index.Index{a}, picks(ch, farFuture))
}

func TestSoftPressureTrigger(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.SoftWatermark = 50
	})
	insert(t, ch, a, 1, 1)

	ch.ObserveTimeTick(10, WALTailSnapshot{Bytes: 40, CompletedTimeTick: 1000})
	assert.Empty(t, picks(ch, farFuture))
	ch.ObserveTimeTick(10, WALTailSnapshot{Bytes: 60, CompletedTimeTick: 1000})
	assert.Equal(t, []*index.Index{a}, picks(ch, farFuture))
}

func TestTailTriggerNeedsTheIndexBehind(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 100
		p.SoftWatermark = 200
	})
	insert(t, ch, a, 1, 1)
	ch.ObserveTimeTick(50, WALTailSnapshot{})
	// a has committed nothing, so LastAcked is 0
	require.Zero(t, ch.LastAcked())

	// the tail is above both thresholds, but the completed point of the WAL
	// recovery is not past the index: another component holds the tail
	ch.ObserveTimeTick(50, WALTailSnapshot{Bytes: 300, CompletedTimeTick: 0})
	assert.Empty(t, picks(ch, farFuture))

	ch.ObserveTimeTick(50, WALTailSnapshot{Bytes: 300, CompletedTimeTick: 40})
	assert.Equal(t, []*index.Index{a}, picks(ch, farFuture))
}

func TestTailTriggerIgnoresTheLastAckedClamp(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 100
	})
	insert(t, ch, a, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.submitHandover(a)
	require.False(t, a.Dirty())
	ch.ObserveTimeTick(100, WALTailSnapshot{})
	require.EqualValues(t, 100, ch.LastAcked())

	// b joins after LastAcked returned 100, and it committed nothing
	b := env.add(ch, "vb")
	insert(t, ch, b, 1, 1)
	require.EqualValues(t, 100, ch.LastAcked(), "the clamp hides b")
	require.Zero(t, ch.lastAckedUnclamped())

	ch.ObserveTimeTick(100, WALTailSnapshot{Bytes: 300, CompletedTimeTick: 50})
	assert.Equal(t, []*index.Index{b}, picks(ch, farFuture))
}

func TestNoWALTailNeverTriggers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.mgr.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "test-dml_1", Term: testTerm}, OnFatal: env.onFatal})
	env.client.assign(testTerm)
	ch.start(false)
	a := env.add(ch, "va")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 0
		p.SoftWatermark = 0
	})
	insert(t, ch, a, 1, 1)
	ch.ObserveTimeTick(50, WALTailSnapshot{})
	assert.Empty(t, picks(ch, farFuture))
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

// TestStallPickRestartsTheClock: a stall pick resets the stall clock, so the
// next stall pick comes a stall timeout later, not at the next TimeTick.
func TestStallPickRestartsTheClock(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.StallTimeout = 10 * time.Minute
		p.BackoffBase = time.Hour
		p.MaxBackoff = 2 * time.Hour
	})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	// the handover fails, so the index keeps holding the checkpoint back
	env.client.set(func(c *FakeCommitter) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(tsAt(0), WALTailSnapshot{CompletedTimeTick: tsAt(0)})
	ch.ObserveTimeTick(tsAt(11*time.Minute), WALTailSnapshot{CompletedTimeTick: tsAt(11 * time.Minute)})
	require.Equal(t, []*index.Index{idx}, picks(ch, time.Now()), "the stall trigger picks")
	ch.step(time.Now())
	require.True(t, idx.HasPending(), "the handover failed, the index still holds the checkpoint back")
	assert.Zero(t, ch.stallSince.Load(), "the pick restarted the clock")

	ch.ObserveTimeTick(tsAt(12*time.Minute), WALTailSnapshot{CompletedTimeTick: tsAt(12 * time.Minute)})
	assert.EqualValues(t, tsAt(12*time.Minute), ch.stallSince.Load(), "the clock starts again while the index holds the checkpoint back")
	assert.Zero(t, ch.stalledFor())
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

// TestTailTriggerWaitsForTheHolderInBackoff: triggers 2 to 4 hand over the
// Index with the oldest commit, the one that holds the WAL checkpoint back.
// While that Index is in backoff, no younger Index is handed over in its
// place: it would not move LastAcked.
func TestTailTriggerWaitsForTheHolderInBackoff(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 100
		p.BackoffBase = time.Hour
		p.MaxBackoff = 2 * time.Hour
	})
	// a fails its handover and goes into backoff with the oldest commit
	env.client.set(func(c *FakeCommitter) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, ch, a, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.submitHandover(a)
	require.Zero(t, a.LastCommittedTimeTick())
	require.True(t, a.HasPending())
	insert(t, ch, b, 1, 1)
	now := time.Now()

	ch.ObserveTimeTick(20, WALTailSnapshot{Bytes: 300, CompletedTimeTick: 1000})
	assert.Empty(t, picks(ch, now), "the holder is in backoff, b would not move LastAcked")
	assert.False(t, ch.wantsHandover())
	assert.Equal(t, []*index.Index{a}, picks(ch, now.Add(3*time.Hour)), "the backoff is over")
}

// TestTailTriggerWaitsForTheHolderThatIsNotReady: the same for an Index that
// the recovery has not marked ready.
func TestTailTriggerWaitsForTheHolderThatIsNotReady(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.recoveredChannel(0, "va")
	a := ch.get("va")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 100
	})
	insert(t, ch, a, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.start(false)
	b := env.add(ch, "vb")
	ch.submitHandover(b)
	require.EqualValues(t, 10, b.LastCommittedTimeTick())
	insert(t, ch, b, 1, 1)

	ch.ObserveTimeTick(20, WALTailSnapshot{Bytes: 300, CompletedTimeTick: 1000})
	assert.Empty(t, picks(ch, farFuture), "the holder is not ready")
	a.MarkReady(context.Background())
	assert.Equal(t, []*index.Index{a}, picks(ch, farFuture))
}

// TestTailTriggerWaitsForTheHolderWithARunningHandover: while the Index with
// the oldest commit runs a handover, triggers 2 to 4 pick nothing.
func TestTailTriggerWaitsForTheHolderWithARunningHandover(t *testing.T) {
	env := newTestEnv(t, asyncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 100
	})
	insert(t, ch, b, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	require.True(t, handover(t, b))
	require.EqualValues(t, 10, b.LastCommittedTimeTick())
	insert(t, ch, a, 1, 1)
	insert(t, ch, b, 1, 2)
	ch.ObserveTimeTick(20, WALTailSnapshot{})

	// a, with the oldest commit, starts a handover that blocks in the commit
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	env.client.set(func(c *FakeCommitter) {
		c.commitEntered = entered
		c.commitBlock = block
	})
	ch.submitHandover(a)
	<-entered
	ch.ObserveTimeTick(30, WALTailSnapshot{Bytes: 300, CompletedTimeTick: 1000})
	assert.Empty(t, picks(ch, farFuture), "the holder runs, b would not move LastAcked")
	assert.False(t, ch.wantsHandover())

	close(block)
	require.Eventually(t, func() bool { return a.LastCommittedTimeTick() == 20 }, waitTimeout, time.Millisecond)
	ch.ObserveTimeTick(30, WALTailSnapshot{Bytes: 300, CompletedTimeTick: 1000})
	assert.Equal(t, []*index.Index{b}, picks(ch, farFuture), "b holds the checkpoint back now")
}

func TestRetryableCommitErrorBacksOff(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Hour
		p.MaxBackoff = 2 * time.Hour
	})
	idx := env.add(ch, "v0")
	env.client.set(func(c *FakeCommitter) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	now := time.Now()
	ch.step(now)

	assert.Empty(t, env.fatalList())
	assert.Equal(t, 1, scheduleOf(ch, idx).failures)
	require.NotNil(t, idx.PendingForTest())
	assert.Empty(t, picks(ch, now), "the backoff is not over")
	assert.Equal(t, []*index.Index{idx}, picks(ch, now.Add(2*time.Hour)), "the backoff is over")
}

func TestBackoffParamsApplyAfterASuccess(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Hour
		p.MaxBackoff = time.Hour
	})
	idx := env.add(ch, "v0")
	env.client.set(func(c *FakeCommitter) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.step(farFuture)
	require.Equal(t, 1, scheduleOf(ch, idx).failures)
	assert.Empty(t, picks(ch, time.Now().Add(time.Minute)), "the wait is about one hour")

	// a smaller max applies after the next success
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Millisecond
		p.MaxBackoff = time.Millisecond
	})
	ch.step(farFuture)
	require.Zero(t, scheduleOf(ch, idx).failures)
	require.EqualValues(t, 10, idx.LastCommittedTimeTick())

	env.client.set(func(c *FakeCommitter) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, ch, idx, 2, 2)
	ch.ObserveTimeTick(20, WALTailSnapshot{})
	ch.step(farFuture)
	require.Equal(t, 1, scheduleOf(ch, idx).failures)
	assert.Equal(t, []*index.Index{idx}, picks(ch, time.Now().Add(time.Second)), "the wait is the new max")
	assert.Empty(t, env.fatalList())
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

// TestPendingIndexInBackoffDoesNotWakeTheDriver: a failed handover leaves a
// pending request and a backoff. The backoff timer of the driver brings the
// retry; an observed TimeTick does not wake the driver for it.
func TestPendingIndexInBackoffDoesNotWakeTheDriver(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Hour
		p.MaxBackoff = 2 * time.Hour
	})
	env.client.set(func(c *FakeCommitter) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.step(time.Now())
	require.True(t, idx.HasPending())
	// the failed handover itself signaled the driver, for the backoff timer
	<-ch.wake

	ch.ObserveTimeTick(20, WALTailSnapshot{})
	assert.False(t, ch.wantsHandover())
	select {
	case <-ch.wake:
		t.Fatal("the TimeTick woke the driver for an Index in backoff")
	default:
	}
	_, next, _ := ch.pickHandovers(time.Now())
	assert.False(t, next.IsZero(), "the driver sets its timer for the retry")
}

func TestDriverClearsATaskThatNeverRan(t *testing.T) {
	env := newTestEnv(t, droppingScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	ch.step(farFuture)
	assert.True(t, scheduleOf(ch, idx).running.Load())
	// the next step sees the finished handle and picks the Index again
	assert.Equal(t, []*index.Index{idx}, picks(ch, farFuture))
}

func TestDriverRunsHandovers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.mgr.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "test-dml_1", Term: testTerm}, OnFatal: env.onFatal})
	env.client.assign(testTerm)
	ch.Start()
	idx := env.add(ch, "v0")
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(10, WALTailSnapshot{})
	require.Eventually(t, func() bool { return idx.LastCommittedTimeTick() == 10 }, waitTimeout, 10*time.Millisecond)
	ch.Close()
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

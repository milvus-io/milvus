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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSizeTrigger(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	pks := make([]int64, 0, 100)
	for i := range int64(100) {
		pks = append(pks, i)
	}
	insert(t, a, 1, pks...)
	insert(t, b, 1, 1)
	small := b.ActiveBytes()
	require.Greater(t, a.ActiveBytes(), small)
	env.setParams(func(p *Params) { p.ActiveBytes = small + 1 })

	assert.Equal(t, []*index.Index{a}, ch.pickHandovers(farFuture))
}

func TestTailTriggerPicksTheOldestCommit(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	env.setParams(func(p *Params) { p.ActiveBytes = math.MaxUint64 })

	insert(t, a, 1, 1)
	ch.ObserveTimeTick(10)
	ch.submitHandover(a)
	require.EqualValues(t, 10, a.LastCommittedTimeTick())
	insert(t, a, 1, 2)
	insert(t, b, 1, 1)
	ch.ObserveTimeTick(20)

	env.recoveryTimeTick.Store(1000)
	assert.Empty(t, ch.pickHandovers(farFuture), "the tail is below every threshold")
	env.setParams(func(p *Params) { p.TailBytes = 100 })
	env.tailBytes.Store(100)
	assert.Equal(t, []*index.Index{b}, ch.pickHandovers(farFuture))

	ch.submitHandover(b)
	require.EqualValues(t, 20, b.LastCommittedTimeTick())
	insert(t, b, 1, 2)
	assert.Equal(t, []*index.Index{a}, ch.pickHandovers(farFuture))
}

func TestSoftPressureTrigger(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.SoftWatermark = 50
	})
	insert(t, a, 1, 1)
	env.recoveryTimeTick.Store(1000)

	env.tailBytes.Store(40)
	assert.Empty(t, ch.pickHandovers(farFuture))
	env.tailBytes.Store(60)
	assert.Equal(t, []*index.Index{a}, ch.pickHandovers(farFuture))
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
	insert(t, a, 1, 1)
	ch.ObserveTimeTick(50)
	// a has committed nothing, so LastAcked is 0
	require.Zero(t, ch.LastAcked())

	// the tail is above both thresholds, but the WAL recovery is not past
	// the index: another component holds the tail
	env.tailBytes.Store(300)
	env.recoveryTimeTick.Store(0)
	assert.Empty(t, ch.pickHandovers(farFuture))

	env.recoveryTimeTick.Store(40)
	assert.Equal(t, []*index.Index{a}, ch.pickHandovers(farFuture))
}

func TestTailTriggerIgnoresTheLastAckedClamp(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 100
	})
	insert(t, a, 1, 1)
	ch.ObserveTimeTick(10)
	ch.submitHandover(a)
	require.False(t, a.Dirty())
	ch.ObserveTimeTick(100)
	require.EqualValues(t, 100, ch.LastAcked())

	// b joins after LastAcked returned 100, and it committed nothing
	b := env.add(ch, "vb")
	insert(t, b, 1, 1)
	require.EqualValues(t, 100, ch.LastAcked(), "the clamp hides b")
	require.Zero(t, ch.lastAckedUnclamped())

	env.tailBytes.Store(300)
	env.recoveryTimeTick.Store(50)
	assert.Equal(t, []*index.Index{b}, ch.pickHandovers(farFuture))
}

func TestNoWALTailNeverTriggers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.mgr.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "test-dml_1", Term: testTerm}, OnFatal: env.onFatal})
	require.NoError(t, env.client.ClaimPChannel(context.Background(), "test-dml_1", testTerm))
	ch.start(false)
	a := env.add(ch, "va")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 0
		p.SoftWatermark = 0
	})
	insert(t, a, 1, 1)
	ch.ObserveTimeTick(50)
	assert.Empty(t, ch.pickHandovers(farFuture))
}

// refNames returns the vchannels of refs.
func refNames(refs []VChannelRef) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref.VChannel)
	}
	return out
}

// TestIndexCreatedBeforeStartWaitsForTheRecovery checks that an Index opened
// before Start is not ready until the recovery marks it, does not hand over,
// and that an Index opened after Start is ready at once.
func TestIndexCreatedBeforeStartWaitsForTheRecovery(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.recoveredChannel(0, "v0")
	v0 := ch.get("v0")
	assert.False(t, v0.Ready())
	insert(t, v0, 10, 1)
	ch.ObserveTimeTick(10)
	assert.True(t, inHeap(ch, v0), "a dirty Index holds LastAcked back while it waits")
	assert.Empty(t, ch.pickHandovers(farFuture), "no handover before ready")

	ch.start(false)
	assert.False(t, v0.Ready(), "Start does not mark an existing Index, the recovery does")
	v1, created, err := ch.CreateCollection(context.Background(), "v1", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	assert.True(t, v1.Ready(), "a collection created after Start is ready at once")

	v0.MarkReady(context.Background(), false)
	assert.True(t, v0.Ready())
	assert.Equal(t, []*index.Index{v0}, ch.pickHandovers(farFuture))
	assert.Empty(t, env.fatalList())
}

func TestFencedCommitStopsTheHandovers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{errors.Wrap(index.ErrFenced, "v0")} })
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, index.ErrFenced))
	assert.True(t, scheduleOf(ch, idx).stopped.Load())
	assert.Len(t, idx.EngineForTest().FrozenGenerations(), 1, "the generations stay frozen")
	// the Index keeps serving writes, but it does not hand over again
	insert(t, idx, 2, 2)
	ch.ObserveTimeTick(20)
	assert.True(t, idx.Dirty())
	assert.Empty(t, ch.pickHandovers(farFuture))
	assert.EqualValues(t, 2, segmentOf(t, idx, 2))
}

func TestRetryableCommitErrorBacksOff(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Hour
		p.MaxBackoff = 2 * time.Hour
	})
	idx := env.add(ch, "v0")
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(10)
	now := time.Now()
	ch.step(now)

	assert.Empty(t, env.fatalList())
	assert.Equal(t, 1, scheduleOf(ch, idx).failures)
	require.NotNil(t, idx.PendingForTest())
	assert.Empty(t, ch.pickHandovers(now), "the backoff is not over")
	assert.Equal(t, []*index.Index{idx}, ch.pickHandovers(now.Add(2*time.Hour)), "the backoff is over")
}

// dropCommittedFiles makes the committed tables of idx missing on this node.
func dropCommittedFiles(t *testing.T, idx *index.Index) {
	require.NoError(t, os.RemoveAll(idx.CommittedDirForTest()))
	idx.ForgetInstalledPathsForTest()
}

func TestMissingObjectIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)
	require.EqualValues(t, 10, idx.LastCommittedTimeTick())

	// the next install needs the first tables, which are gone everywhere
	dropCommittedFiles(t, idx)
	env.store.missing.Store(true)
	insert(t, idx, 2, 2)
	ch.ObserveTimeTick(20)
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, pkroot.ErrCorrupted))
	assert.True(t, scheduleOf(ch, idx).stopped.Load())
}

func TestIsFatal(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		fatal bool
	}{
		{"fenced", errors.Wrap(index.ErrFenced, "v0"), true},
		{"rejected", errors.Wrap(index.ErrRejected, "v0"), true},
		{"corrupted", errors.Mark(errors.New("bad block"), pkroot.ErrCorrupted), true},
		{"unavailable", errors.Mark(errors.New("connection reset"), pkroot.ErrUnavailable), false},
		{"io", errors.Mark(errors.New("disk full"), pkroot.ErrIO), false},
		// The Committer translates the codes of the manifest service. A code
		// that it did not translate is retryable.
		{"untranslated service internal", merr.WrapErrServiceInternal("bug"), false},
		{"deadline", context.DeadlineExceeded, false},
		{"canceled", context.Canceled, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.fatal, index.IsFatal(c.err))
		})
	}
}

func TestRejectedCommitIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{errors.Wrap(index.ErrRejected, "duplicate table")} })
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, index.ErrRejected))
	assert.True(t, scheduleOf(ch, idx).stopped.Load())
	assert.Zero(t, scheduleOf(ch, idx).failures, "no retry is scheduled")
	assert.Len(t, idx.EngineForTest().FrozenGenerations(), 1, "the generations stay frozen")
	assert.Empty(t, ch.pickHandovers(farFuture))
	assert.Len(t, env.client.requestList(), 1)
}

func TestBackoffParamsApplyAfterASuccess(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Hour
		p.MaxBackoff = time.Hour
	})
	idx := env.add(ch, "v0")
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)
	require.Equal(t, 1, scheduleOf(ch, idx).failures)
	assert.Empty(t, ch.pickHandovers(time.Now().Add(time.Minute)), "the wait is about one hour")

	// a smaller max applies after the next success
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Millisecond
		p.MaxBackoff = time.Millisecond
	})
	ch.step(farFuture)
	require.Zero(t, scheduleOf(ch, idx).failures)
	require.EqualValues(t, 10, idx.LastCommittedTimeTick())

	env.client.set(func(c *fakeClient) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, idx, 2, 2)
	ch.ObserveTimeTick(20)
	ch.step(farFuture)
	require.Equal(t, 1, scheduleOf(ch, idx).failures)
	assert.Equal(t, []*index.Index{idx}, ch.pickHandovers(time.Now().Add(time.Second)), "the wait is the new max")
	assert.Empty(t, env.fatalList())
}

func TestRemoveWaitsForARunningHandover(t *testing.T) {
	env := newTestEnv(t, asyncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	env.client.set(func(c *fakeClient) {
		c.commitEntered = entered
		c.commitBlock = block
	})
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)
	<-entered

	removed := make(chan struct{})
	go func() {
		ch.remove(context.Background(), "v0")
		close(removed)
	}()
	select {
	case <-removed:
		t.Fatal("Remove returned while the handover runs")
	case <-time.After(200 * time.Millisecond):
	}
	close(block)
	select {
	case <-removed:
	case <-time.After(waitTimeout):
		t.Fatal("Remove did not return after the handover ended")
	}
	assert.Nil(t, ch.get("v0"))
	assert.False(t, scheduleOf(ch, idx).running.Load())
}

func TestDriverClearsATaskThatNeverRan(t *testing.T) {
	env := newTestEnv(t, droppingScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)
	assert.True(t, scheduleOf(ch, idx).running.Load())
	// the next step sees the finished handle and picks the Index again
	assert.Equal(t, []*index.Index{idx}, ch.pickHandovers(farFuture))
}

func TestClosedChannelAndIndex(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ch.Close()

	_, _, err := ch.CreateCollection(context.Background(), "v1", testCollectionID, schemapb.DataType_Int64)
	assert.True(t, errors.Is(err, engine.ErrClosed))
	_, err = idx.Authority().Decide(context.Background(), insertRequest(idx, 1))
	assert.True(t, errors.Is(err, engine.ErrClosed))
	assert.True(t, errors.Is(err, pkroot.ErrUnavailable))
	assert.Nil(t, ch.get("v0"))
}

func TestManagerCloseClosesChannels(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	env.mgr.Close()
	assert.Nil(t, ch.get("v0"))
	closed := env.channel(0)
	_, _, err := closed.CreateCollection(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	assert.True(t, errors.Is(err, engine.ErrClosed))
}

func TestDriverRunsHandovers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.mgr.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "test-dml_1", Term: testTerm}, OnFatal: env.onFatal})
	require.NoError(t, env.client.ClaimPChannel(context.Background(), "test-dml_1", testTerm))
	ch.Start()
	idx := env.add(ch, "v0")
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(10)
	require.Eventually(t, func() bool { return idx.LastCommittedTimeTick() == 10 }, waitTimeout, 10*time.Millisecond)
	ch.Close()
}

func TestAddOfAnExistingIndexReportsNotCreated(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1)

	_, created, err := ch.CreateCollection(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	assert.False(t, created, "a caller that rolls back must not remove the Index in use")
	assert.Same(t, idx, ch.get("v0"))
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
}

func TestDropRemovesTheLocalFiles(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1)
	ch.ObserveTimeTick(10)
	ch.step(farFuture)
	require.DirExists(t, idx.CommittedDirForTest())

	require.NoError(t, ch.DropCollection(context.Background(), "v0"))
	assert.Nil(t, ch.get("v0"))
	assert.NoDirExists(t, filepath.Join(env.root, "v0"))
	require.NoError(t, ch.DropCollection(context.Background(), "v0"), "a drop of a vchannel with no Index is a no-op")

	idx = env.add(ch, "v0")
	assert.Zero(t, segmentOf(t, idx, 1), "the old key is gone")
	assert.Empty(t, env.fatalList())
}

func TestDropOfAClosedChannelFails(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	ch.Close()

	err := ch.DropCollection(context.Background(), "v0")
	require.ErrorIs(t, err, engine.ErrClosed)
	assert.DirExists(t, filepath.Join(env.root, "v0"))
}

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
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/snapshot"
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
	small := b.eng.Stats().ActiveBytes
	require.Greater(t, a.eng.Stats().ActiveBytes, small)
	env.setParams(func(p *Params) { p.ActiveBytes = small + 1 })

	assert.Equal(t, []*Index{a}, ch.pickHandovers(farFuture))
}

func TestTailTriggerPicksTheOldestCommit(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	a := env.add(ch, "va")
	b := env.add(ch, "vb")
	env.setParams(func(p *Params) { p.ActiveBytes = math.MaxUint64 })

	insert(t, a, 1, 1)
	ch.Tick(10)
	a.submitHandover()
	require.EqualValues(t, 10, a.LastCommittedTimeTick())
	insert(t, a, 1, 2)
	insert(t, b, 1, 1)
	ch.Tick(20)

	env.recoveryTimeTick.Store(1000)
	assert.Empty(t, ch.pickHandovers(farFuture), "the tail is below every threshold")
	env.setParams(func(p *Params) { p.TailBytes = 100 })
	env.tailBytes.Store(100)
	assert.Equal(t, []*Index{b}, ch.pickHandovers(farFuture))

	b.submitHandover()
	require.EqualValues(t, 20, b.LastCommittedTimeTick())
	insert(t, b, 1, 2)
	assert.Equal(t, []*Index{a}, ch.pickHandovers(farFuture))
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
	assert.Equal(t, []*Index{a}, ch.pickHandovers(farFuture))
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
	ch.Tick(50)
	// a has committed nothing, so LastAcked is 0
	require.Zero(t, ch.LastAcked())

	// the tail is above both thresholds, but the WAL recovery is not past
	// the index: another component holds the tail
	env.tailBytes.Store(300)
	env.recoveryTimeTick.Store(0)
	assert.Empty(t, ch.pickHandovers(farFuture))

	env.recoveryTimeTick.Store(40)
	assert.Equal(t, []*Index{a}, ch.pickHandovers(farFuture))
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
	ch.Tick(10)
	a.submitHandover()
	require.False(t, a.dirty())
	ch.Tick(100)
	require.EqualValues(t, 100, ch.LastAcked())

	// b joins after LastAcked returned 100, and it committed nothing
	b := env.add(ch, "vb")
	insert(t, b, 1, 1)
	require.EqualValues(t, 100, ch.LastAcked(), "the clamp hides b")
	require.Zero(t, ch.lastAckedUnclamped())

	env.tailBytes.Store(300)
	env.recoveryTimeTick.Store(50)
	assert.Equal(t, []*Index{b}, ch.pickHandovers(farFuture))
}

func TestNoWALTailNeverTriggers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.mgr.openChannel(types.PChannelInfo{Name: "test-dml_1", Term: testTerm}, 0, nil, env.onFatal, false)
	a := env.add(ch, "va")
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.TailBytes = 0
		p.SoftWatermark = 0
	})
	insert(t, a, 1, 1)
	ch.Tick(50)
	assert.Empty(t, ch.pickHandovers(farFuture))
}

func TestNoHandoverBeforeTheClaim(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	block := make(chan struct{})
	env.client.set(func(c *fakeClient) { c.claimBlock = block })
	ch := env.channel(0)
	created, err := ch.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	idx := ch.Get("v0")
	insert(t, idx, 1, 1)
	ch.Tick(10)

	assert.Empty(t, ch.pickHandovers(farFuture))
	close(block)
	waitClaimed(t, idx)
	assert.Equal(t, []*Index{idx}, ch.pickHandovers(farFuture))
}

func TestClaimRetriesAfterARetryableError(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.client.set(func(c *fakeClient) {
		c.claimErrs = []error{merr.WrapErrServiceUnavailable("down"), merr.WrapErrServiceUnavailable("down")}
	})
	ch := env.channel(0)
	env.add(ch, "v0")
	assert.Empty(t, env.fatalList())
}

func TestFencedClaimIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.client.set(func(c *fakeClient) { c.claimErrs = []error{errors.Wrap(snapshot.ErrFenced, "v0")} })
	ch := env.channel(0)
	created, err := ch.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	err = env.waitFatal()
	assert.True(t, errors.Is(err, snapshot.ErrFenced))
	idx := ch.Get("v0")
	assert.False(t, idx.claimedDone())
	insert(t, idx, 1, 1)
	assert.Empty(t, ch.pickHandovers(farFuture))
}

func TestFencedCommitStopsTheHandovers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{errors.Wrap(snapshot.ErrFenced, "v0")} })
	insert(t, idx, 1, 1)
	ch.Tick(10)
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, snapshot.ErrFenced))
	assert.True(t, idx.handoverStopped.Load())
	// the Index keeps serving writes, but it does not hand over again
	insert(t, idx, 2, 2)
	ch.Tick(20)
	assert.True(t, idx.dirty())
	assert.Empty(t, ch.pickHandovers(farFuture))
	assert.EqualValues(t, 2, segmentOf(t, idx, 2))
}

func TestRetryableCommitErrorBacksOff(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Hour
		p.MaxBackoff = 2 * time.Hour
	})
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, idx, 1, 1)
	ch.Tick(10)
	now := time.Now()
	ch.step(now)

	assert.Empty(t, env.fatalList())
	assert.Equal(t, 1, idx.backoff.Failures())
	require.NotNil(t, idx.pending.Load())
	assert.Empty(t, ch.pickHandovers(now), "the backoff is not over")
	assert.Equal(t, []*Index{idx}, ch.pickHandovers(now.Add(2*time.Hour)), "the backoff is over")
}

// dropCommittedFiles makes the committed tables of idx missing on this node.
func dropCommittedFiles(t *testing.T, idx *Index) {
	require.NoError(t, os.RemoveAll(idx.committedDir))
	idx.installMu.Lock()
	idx.installedPaths = nil
	idx.installMu.Unlock()
}

func TestMissingObjectIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 1, 1)
	ch.Tick(10)
	ch.step(farFuture)
	require.EqualValues(t, 10, idx.LastCommittedTimeTick())

	// the next install needs the first tables, which are gone everywhere
	dropCommittedFiles(t, idx)
	env.store.missing.Store(true)
	insert(t, idx, 2, 2)
	ch.Tick(20)
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
	assert.True(t, idx.handoverStopped.Load())
}

func TestIsFatal(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		fatal bool
	}{
		{"fenced", errors.Wrap(snapshot.ErrFenced, "v0"), true},
		{"rejected", errors.Wrap(snapshot.ErrRejected, "v0"), true},
		{"corrupted", errors.Mark(errors.New("bad block"), pkindex.ErrCorrupted), true},
		{"unavailable", errors.Mark(errors.New("connection reset"), pkindex.ErrUnavailable), false},
		{"io", errors.Mark(errors.New("disk full"), pkindex.ErrIO), false},
		// The Committer translates the codes of the manifest service. A code
		// that it did not translate is retryable.
		{"untranslated service internal", merr.WrapErrServiceInternal("bug"), false},
		{"deadline", context.DeadlineExceeded, false},
		{"canceled", context.Canceled, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.fatal, isFatal(c.err))
		})
	}
}

func TestRejectedCommitIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{errors.Wrap(snapshot.ErrRejected, "duplicate table")} })
	insert(t, idx, 1, 1)
	ch.Tick(10)
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, snapshot.ErrRejected))
	assert.True(t, idx.handoverStopped.Load())
	assert.Zero(t, idx.backoff.Failures(), "no retry is scheduled")
	assert.Empty(t, ch.pickHandovers(farFuture))
	assert.Len(t, env.client.requestList(), 1)
}

func TestRejectedClaimIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.client.set(func(c *fakeClient) {
		c.claimErrs = []error{errors.Wrap(snapshot.ErrRejected, "schema mismatch")}
	})
	ch := env.channel(0)
	created, err := ch.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	err = env.waitFatal()
	assert.True(t, errors.Is(err, snapshot.ErrRejected))
	idx := ch.Get("v0")
	assert.False(t, idx.claimedDone())
	assert.True(t, idx.handoverStopped.Load())
}

func TestBackoffParamsAreReadAtEachFailure(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Hour
		p.MaxBackoff = time.Hour
	})
	env.client.set(func(c *fakeClient) {
		c.commitErrs = []error{merr.WrapErrServiceUnavailable("down"), merr.WrapErrServiceUnavailable("down")}
	})
	insert(t, idx, 1, 1)
	ch.Tick(10)
	ch.step(farFuture)
	require.Equal(t, 1, idx.backoff.Failures())
	assert.False(t, idx.backoff.Ready(time.Now().Add(time.Minute)), "the wait is one hour")

	// a smaller max applies to the next failure
	env.setParams(func(p *Params) { p.MaxBackoff = time.Millisecond })
	ch.step(farFuture)
	require.Equal(t, 2, idx.backoff.Failures())
	assert.Equal(t, time.Millisecond, idx.backoff.Max)
	assert.True(t, idx.backoff.Ready(time.Now().Add(time.Second)), "the wait is the new max")
	assert.Empty(t, env.fatalList())
}

func TestFailReadsTheBackoffParams(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	// the claim loop uses the same helper with its own Backoff
	var bo snapshot.Backoff
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Hour
		p.MaxBackoff = 2 * time.Hour
	})
	now := time.Now()
	idx.fail(&bo, now)
	assert.Equal(t, time.Hour, bo.Base)
	assert.False(t, bo.Ready(now.Add(time.Minute)))
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Millisecond
		p.MaxBackoff = time.Millisecond
	})
	idx.fail(&bo, now)
	assert.Equal(t, time.Millisecond, bo.Max)
	assert.True(t, bo.Ready(now.Add(time.Second)))
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
	ch.Tick(10)
	ch.step(farFuture)
	<-entered

	removed := make(chan struct{})
	go func() {
		ch.Remove(context.Background(), "v0")
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
	assert.Nil(t, ch.Get("v0"))
	assert.False(t, idx.running.Load())
}

func TestDriverClearsATaskThatNeverRan(t *testing.T) {
	env := newTestEnv(t, droppingScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 1, 1)
	ch.Tick(10)
	ch.step(farFuture)
	assert.True(t, idx.running.Load())
	// the next step sees the finished handle and picks the Index again
	assert.Equal(t, []*Index{idx}, ch.pickHandovers(farFuture))
}

func TestClosedChannelAndIndex(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ch.Close()

	_, err := ch.Add(context.Background(), "v1", testCollectionID, schemapb.DataType_Int64)
	assert.True(t, errors.Is(err, engine.ErrClosed))
	_, err = idx.Decide(context.Background(), insertRequest(idx, 1))
	assert.True(t, errors.Is(err, engine.ErrClosed))
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.Nil(t, ch.Get("v0"))
}

func TestManagerCloseClosesChannels(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	env.mgr.Close()
	assert.Nil(t, ch.Get("v0"))
	closed := env.channel(0)
	_, err := closed.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	assert.True(t, errors.Is(err, engine.ErrClosed))
}

func TestDriverRunsHandovers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.mgr.OpenChannel(types.PChannelInfo{Name: "test-dml_1", Term: testTerm}, 0, nil, env.onFatal)
	idx := env.add(ch, "v0")
	insert(t, idx, 1, 1)
	ch.Tick(10)
	require.Eventually(t, func() bool { return idx.LastCommittedTimeTick() == 10 }, waitTimeout, 10*time.Millisecond)
	ch.Close()
}

func TestAddOfAnExistingIndexReportsNotCreated(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1)

	created, err := ch.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	assert.False(t, created, "a caller that rolls back must not remove the Index in use")
	assert.Same(t, idx, ch.Get("v0"))
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
}

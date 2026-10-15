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
	pkerr "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
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
	require.EqualValues(t, 10, a.lastCommittedTimeTick())
	insert(t, a, 1, 2)
	insert(t, b, 1, 1)
	ch.Tick(20)

	env.recoveryTimeTick.Store(1000)
	assert.Empty(t, ch.pickHandovers(farFuture), "the tail is below every threshold")
	env.setParams(func(p *Params) { p.TailBytes = 100 })
	env.tailBytes.Store(100)
	assert.Equal(t, []*Index{b}, ch.pickHandovers(farFuture))

	b.submitHandover()
	require.EqualValues(t, 20, b.lastCommittedTimeTick())
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
	ch.StartClaim()
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
	_, created, err := ch.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	idx := ch.Get("v0")
	insert(t, idx, 1, 1)
	ch.Tick(10)

	assert.Empty(t, ch.pickHandovers(farFuture))
	close(block)
	waitReady(t, idx)
	assert.Equal(t, []*Index{idx}, ch.pickHandovers(farFuture))
}

// refNames returns the vchannels of refs.
func refNames(refs []VChannelRef) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref.VChannel)
	}
	return out
}

// TestOneClaimAndOneLookupForManyVChannels checks that a Channel claims its
// pchannel once, looks up the manifests of the indexes of the existing
// collections in one call, and makes an Index of a new collection ready
// without a lookup.
func TestOneClaimAndOneLookupForManyVChannels(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	vchannels := []string{"v0", "v1", "v2", "v3", "v4"}
	ch := env.recoveredChannel(0, vchannels...)
	for _, v := range vchannels {
		idx := ch.Get(v)
		waitReady(t, idx)
		assert.True(t, idx.fresh())
		assert.Equal(t, testBasePath(v), idx.sstBasePath)
	}
	assert.Equal(t, 1, env.client.claimCount())
	lookups := env.client.lookupList()
	require.Len(t, lookups, 1, "one GetRecoveryManifests call for the existing collections")
	for _, ref := range lookups[0] {
		assert.Equal(t, testCollectionID, ref.CollectionID)
	}
	assert.ElementsMatch(t, vchannels, refNames(lookups[0]))

	created := env.add(ch, "v5")
	assert.True(t, created.fresh())
	assert.Equal(t, 1, env.client.claimCount(), "a new collection does not claim")
	assert.Len(t, env.client.lookupList(), 1, "a new collection does not look up")
	assert.Empty(t, env.fatalList())
}

// TestNewCollectionBeforeTheClaimWaitsForIt checks that an Index of a new
// collection that opens while the claim runs is not looked up, and becomes
// ready when the claim and the lookups are done.
func TestNewCollectionBeforeTheClaimWaitsForIt(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	block := make(chan struct{})
	env.client.set(func(c *fakeClient) { c.claimBlock = block })
	ch := env.recoveredChannel(0, "v0")
	_, created, err := ch.Add(context.Background(), "v1", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	assert.False(t, ch.Get("v1").readyDone())

	close(block)
	waitReady(t, ch.Get("v0"))
	waitReady(t, ch.Get("v1"))
	lookups := env.client.lookupList()
	require.Len(t, lookups, 1)
	assert.Equal(t, []string{"v0"}, refNames(lookups[0]))
}

func TestLookupSplitsIntoBatches(t *testing.T) {
	old := recoveryBatch
	recoveryBatch = 2
	t.Cleanup(func() { recoveryBatch = old })
	env := newTestEnv(t, syncScheduler{})
	vchannels := []string{"v0", "v1", "v2", "v3", "v4"}
	ch := env.recoveredChannel(0, vchannels...)
	for _, v := range vchannels {
		waitReady(t, ch.Get(v))
	}
	lookups := env.client.lookupList()
	require.Len(t, lookups, 3)
	var all []string
	for _, l := range lookups {
		assert.LessOrEqual(t, len(l), 2)
		all = append(all, refNames(l)...)
	}
	assert.ElementsMatch(t, vchannels, all)
}

func TestLookupRetriesOnlyTheFailedItems(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.client.seed("v2", ManifestView{Version: 3, CheckpointTimeTick: 5})
	env.client.set(func(c *fakeClient) {
		c.itemErrs = map[string][]error{"v1": {merr.WrapErrServiceUnavailable("down")}}
	})
	ch := env.recoveredChannel(0, "v0", "v1", "v2")
	for _, v := range []string{"v0", "v1", "v2"} {
		waitReady(t, ch.Get(v))
	}
	lookups := env.client.lookupList()
	require.Len(t, lookups, 2)
	assert.ElementsMatch(t, []string{"v0", "v1", "v2"}, refNames(lookups[0]))
	assert.Equal(t, []string{"v1"}, refNames(lookups[1]), "only the failed item is retried")
	assert.True(t, ch.Get("v0").fresh())
	assert.True(t, ch.Get("v1").fresh())
	assert.False(t, ch.Get("v2").fresh(), "a found manifest makes the Index not fresh")
	assert.Empty(t, env.fatalList())
}

func TestFatalItemErrorIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.client.set(func(c *fakeClient) {
		c.itemErrs = map[string][]error{"v1": {errors.Wrap(ErrRejected, "corrupted pointer")}}
	})
	ch := env.recoveredChannel(0, "v0", "v1")
	err := env.waitFatal()
	assert.True(t, errors.Is(err, ErrRejected))
	assert.False(t, ch.Get("v1").readyDone())
	insert(t, ch.Get("v1"), 1, 1)
	assert.Empty(t, ch.pickHandovers(farFuture))
}

func TestLookupRetriesAfterARetryableError(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.client.set(func(c *fakeClient) {
		c.lookupErrs = []error{merr.WrapErrServiceUnavailable("down")}
	})
	ch := env.recoveredChannel(0, "v0")
	idx := ch.Get("v0")
	waitReady(t, idx)
	assert.True(t, idx.fresh())
	assert.Len(t, env.client.lookupList(), 2)
	assert.Empty(t, env.fatalList())
}

func TestRejectedLookupIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.client.set(func(c *fakeClient) {
		c.lookupErrs = []error{errors.Wrap(ErrRejected, "invalid request")}
	})
	ch := env.recoveredChannel(0, "v0")
	err := env.waitFatal()
	assert.True(t, errors.Is(err, ErrRejected))
	idx := ch.Get("v0")
	assert.False(t, idx.readyDone())
	insert(t, idx, 1, 1)
	assert.Empty(t, ch.pickHandovers(farFuture))
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
	env.client.set(func(c *fakeClient) { c.claimErrs = []error{errors.Wrap(ErrFenced, "v0")} })
	ch := env.channel(0)
	_, created, err := ch.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	err = env.waitFatal()
	assert.True(t, errors.Is(err, ErrFenced))
	idx := ch.Get("v0")
	assert.False(t, idx.readyDone())
	insert(t, idx, 1, 1)
	assert.Empty(t, ch.pickHandovers(farFuture))
}

func TestFencedCommitStopsTheHandovers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{errors.Wrap(ErrFenced, "v0")} })
	insert(t, idx, 1, 1)
	ch.Tick(10)
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, ErrFenced))
	assert.True(t, idx.handoverStopped.Load())
	assert.Len(t, idx.eng.FrozenGenerations(), 1, "the generations stay frozen")
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
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Hour
		p.MaxBackoff = 2 * time.Hour
	})
	idx := env.add(ch, "v0")
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, idx, 1, 1)
	ch.Tick(10)
	now := time.Now()
	ch.step(now)

	assert.Empty(t, env.fatalList())
	assert.Equal(t, 1, idx.failures)
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
	require.EqualValues(t, 10, idx.lastCommittedTimeTick())

	// the next install needs the first tables, which are gone everywhere
	dropCommittedFiles(t, idx)
	env.store.missing.Store(true)
	insert(t, idx, 2, 2)
	ch.Tick(20)
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
	assert.True(t, idx.handoverStopped.Load())
}

func TestIsFatal(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		fatal bool
	}{
		{"fenced", errors.Wrap(ErrFenced, "v0"), true},
		{"rejected", errors.Wrap(ErrRejected, "v0"), true},
		{"corrupted", errors.Mark(errors.New("bad block"), pkerr.ErrCorrupted), true},
		{"unavailable", errors.Mark(errors.New("connection reset"), pkerr.ErrUnavailable), false},
		{"io", errors.Mark(errors.New("disk full"), pkerr.ErrIO), false},
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
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{errors.Wrap(ErrRejected, "duplicate table")} })
	insert(t, idx, 1, 1)
	ch.Tick(10)
	ch.step(farFuture)

	err := env.waitFatal()
	assert.True(t, errors.Is(err, ErrRejected))
	assert.True(t, idx.handoverStopped.Load())
	assert.Zero(t, idx.failures, "no retry is scheduled")
	assert.Len(t, idx.eng.FrozenGenerations(), 1, "the generations stay frozen")
	assert.Empty(t, ch.pickHandovers(farFuture))
	assert.Len(t, env.client.requestList(), 1)
}

func TestRejectedClaimIsFatal(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.client.set(func(c *fakeClient) {
		c.claimErrs = []error{errors.Wrap(ErrRejected, "schema mismatch")}
	})
	ch := env.channel(0)
	_, created, err := ch.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, created)
	err = env.waitFatal()
	assert.True(t, errors.Is(err, ErrRejected))
	idx := ch.Get("v0")
	assert.False(t, idx.readyDone())
	insert(t, idx, 1, 1)
	assert.Empty(t, ch.pickHandovers(farFuture))
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
	ch.Tick(10)
	ch.step(farFuture)
	require.Equal(t, 1, idx.failures)
	assert.Empty(t, ch.pickHandovers(time.Now().Add(time.Minute)), "the wait is about one hour")

	// a smaller max applies after the next success
	env.setParams(func(p *Params) {
		p.BackoffBase = time.Millisecond
		p.MaxBackoff = time.Millisecond
	})
	ch.step(farFuture)
	require.Zero(t, idx.failures)
	require.EqualValues(t, 10, idx.lastCommittedTimeTick())

	env.client.set(func(c *fakeClient) { c.commitErrs = []error{merr.WrapErrServiceUnavailable("down")} })
	insert(t, idx, 2, 2)
	ch.Tick(20)
	ch.step(farFuture)
	require.Equal(t, 1, idx.failures)
	assert.Equal(t, []*Index{idx}, ch.pickHandovers(time.Now().Add(time.Second)), "the wait is the new max")
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

	_, _, err := ch.Add(context.Background(), "v1", testCollectionID, schemapb.DataType_Int64)
	assert.True(t, errors.Is(err, engine.ErrClosed))
	_, err = idx.Decide(context.Background(), insertRequest(idx, 1))
	assert.True(t, errors.Is(err, engine.ErrClosed))
	assert.True(t, errors.Is(err, pkerr.ErrUnavailable))
	assert.Nil(t, ch.Get("v0"))
}

func TestManagerCloseClosesChannels(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	env.mgr.Close()
	assert.Nil(t, ch.Get("v0"))
	closed := env.channel(0)
	_, _, err := closed.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	assert.True(t, errors.Is(err, engine.ErrClosed))
}

func TestDriverRunsHandovers(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.mgr.OpenChannel(types.PChannelInfo{Name: "test-dml_1", Term: testTerm}, 0, nil, env.onFatal)
	ch.StartClaim()
	idx := env.add(ch, "v0")
	insert(t, idx, 1, 1)
	ch.Tick(10)
	require.Eventually(t, func() bool { return idx.lastCommittedTimeTick() == 10 }, waitTimeout, 10*time.Millisecond)
	ch.Close()
}

func TestAddOfAnExistingIndexReportsNotCreated(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1)

	_, created, err := ch.Add(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	assert.False(t, created, "a caller that rolls back must not remove the Index in use")
	assert.Same(t, idx, ch.Get("v0"))
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
}

func TestDropRemovesTheLocalFiles(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, idx, 10, 1)
	ch.Tick(10)
	ch.step(farFuture)
	require.DirExists(t, idx.committedDir)

	require.NoError(t, ch.Drop(context.Background(), "v0"))
	assert.Nil(t, ch.Get("v0"))
	assert.NoDirExists(t, filepath.Join(env.root, "v0"))
	require.NoError(t, ch.Drop(context.Background(), "v0"), "a drop of a vchannel with no Index is a no-op")

	idx = env.add(ch, "v0")
	assert.Zero(t, segmentOf(t, idx, 1), "the old key is gone")
	assert.Empty(t, env.fatalList())
}

func TestDropOfAClosedChannelFails(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	ch.Close()

	err := ch.Drop(context.Background(), "v0")
	require.ErrorIs(t, err, engine.ErrClosed)
	assert.DirExists(t, filepath.Join(env.root, "v0"))
}

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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// The tests in this file call prepare and commit of a ready Index directly,
// without the driver, to check the steps of one handover.

// handoverIndex returns a ready Index of v0 on a PChannelIndexManager without the driver.
func handoverIndex(t *testing.T) (*testEnv, *PChannelIndexManager, *index.Index) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	return env, ch, env.add(ch, "v0")
}

func refIDs(refs []index.TableRef) []int64 {
	ids := make([]int64, 0, len(refs))
	for _, r := range refs {
		ids = append(ids, int64(r.ID))
	}
	return ids
}

// storeIDs returns the SST IDs of the objects of store under basePath.
func storeIDs(t *testing.T, store *countingStore, basePath string) []int64 {
	var ids []int64
	for _, k := range store.Keys() {
		id, ok := pkroot.ParseSSTKey(basePath, k)
		require.True(t, ok, k)
		ids = append(ids, id)
	}
	return ids
}

// TestSSTKeyExtension checks that pkroot.SSTKey writes the extension of
// sst.Extension.
func TestSSTKeyExtension(t *testing.T) {
	assert.True(t, strings.HasSuffix(pkroot.SSTKey("base", 1), sst.Extension))
}

func TestHandoverNormal(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1, 2, 3)
	ch.ObserveTimeTick(50)

	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NotNil(t, p.Request())
	assert.Equal(t, uint64(50), p.Covered())
	assert.Empty(t, env.client.requestList())
	assert.Zero(t, idx.LastCommittedTimeTick())
	assert.Equal(t, 0, env.store.Len(), "prepare does not upload")
	assert.True(t, p.UploadedAt().IsZero())

	require.NoError(t, idx.CommitForTest(ctx, p))

	reqs := env.client.requestList()
	require.Len(t, reqs, 1)
	req := reqs[0]
	assert.Equal(t, uint64(50), req.CheckpointTimeTick)
	assert.Equal(t, testCollectionID, req.CollectionID)
	assert.Equal(t, "v0", req.VChannel)
	assert.Equal(t, testTerm, req.PChannelTerm)
	assert.Equal(t, schemapb.DataType_Int64, req.PKDataType)
	require.NotEmpty(t, req.AddTables)

	for _, ref := range req.AddTables {
		assert.Greater(t, ref.Size, int64(0))
		assert.Equal(t, int64(3), ref.NumEntries)
		assert.NotZero(t, ref.TableFormat)
		assert.Equal(t, codec.EncodeInt64PK(1), ref.MinKey)
		assert.Equal(t, codec.EncodeInt64PK(3), ref.MaxKey)
		path := index.CommittedPathForTest(idx.CommittedDirForTest(), ref.ID)
		assert.FileExists(t, path)
		assert.Equal(t, path, p.Local()[ref.ID])
	}
	addIDs := refIDs(req.AddTables)
	assert.ElementsMatch(t, addIDs, storeIDs(t, env.store, idx.SSTBasePathForTest()))

	assert.Equal(t, int64(1), idx.InstalledVersionForTest(), "the first flush creates version 1")
	for _, pk := range []int64{1, 2, 3} {
		assert.EqualValues(t, 10, segmentOf(t, idx, pk))
	}
	stats := idx.EngineForTest().Stats()
	assert.Equal(t, 0, stats.FrozenGenerations)
	assert.Equal(t, len(addIDs), stats.CommittedTables)
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
}

func TestHandoverNothingToPublish(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	idx.SetLastCommittedForTest(10)
	ch.ObserveTimeTick(20)

	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Nil(t, p.Request())
	assert.Equal(t, uint64(20), p.Covered())
	assert.Len(t, p.Gens(), 1)

	require.NoError(t, idx.CommitForTest(ctx, p))

	assert.Empty(t, env.client.requestList())
	assert.Equal(t, 0, env.store.Len())
	assert.Zero(t, idx.InstalledVersionForTest())
	assert.Equal(t, uint64(20), idx.LastCommittedTimeTick())
	assert.Equal(t, 0, idx.EngineForTest().Stats().FrozenGenerations)
}

func TestHandoverCommitFailsBeforeApplyThenRetry(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1, 2, 3)
	ch.ObserveTimeTick(50)

	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)

	env.client.set(func(c *fakeClient) { c.commitErrs = []error{pkroot.MarkIO(errors.New("disk gone"), "commit")} })
	err = idx.CommitForTest(ctx, p)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrIO))
	objects := env.store.Len()
	assert.Equal(t, len(p.Request().AddTables), objects)
	assert.Empty(t, env.client.appliedList())
	assert.Equal(t, 1, idx.EngineForTest().Stats().FrozenGenerations)
	assert.Zero(t, idx.InstalledVersionForTest())
	assert.Zero(t, idx.LastCommittedTimeTick())

	require.NoError(t, idx.CommitForTest(ctx, p))

	assert.Equal(t, objects, env.store.Len())
	reqs := env.client.requestList()
	require.Len(t, reqs, 2)
	assert.Equal(t, reqs[0], reqs[1])
	assert.Equal(t, []bool{true}, env.client.appliedList())
	assert.Equal(t, uint64(50), env.client.manifest("v0").CheckpointTimeTick)
	assert.Equal(t, 0, idx.EngineForTest().Stats().FrozenGenerations)
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
	for _, pk := range []int64{1, 2, 3} {
		assert.EqualValues(t, 10, segmentOf(t, idx, pk))
	}
}

// TestHandoverRejectsAManifestThatWentBack checks the safety check after the
// flush: a returned manifest below the installed version, or with a checkpoint
// below the covered time tick, does not have the request in effect. A
// manifest with another SST base path does not list the uploaded tables. It is
// rejected, nothing is installed, and the generations stay frozen.
func TestHandoverRejectsAManifestThatWentBack(t *testing.T) {
	cases := map[string]struct {
		installed int64
		rewrite   func(m index.ManifestView) index.ManifestView
	}{
		"version below the installed one": {installed: 100},
		"checkpoint below the covered time tick": {rewrite: func(m index.ManifestView) index.ManifestView {
			m.CheckpointTimeTick = 49
			return m
		}},
		"another SST base path": {rewrite: func(m index.ManifestView) index.ManifestView {
			m.SSTBasePath = "other/sst"
			return m
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			env, ch, idx := handoverIndex(t)
			env.client.set(func(c *fakeClient) { c.rewrite = tc.rewrite })
			insert(t, idx, 10, 1)
			ch.ObserveTimeTick(50)
			idx.SetInstalledVersionForTest(tc.installed)

			p, err := idx.PrepareForTest(ctx)
			require.NoError(t, err)
			require.NotNil(t, p)
			err = idx.CommitForTest(ctx, p)
			require.Error(t, err)
			assert.True(t, errors.Is(err, index.ErrRejected))
			assert.Equal(t, 0, idx.EngineForTest().Stats().CommittedTables, "nothing is installed")
			assert.Zero(t, idx.LastCommittedTimeTick())
			assert.Equal(t, tc.installed, idx.InstalledVersionForTest())
			assert.Equal(t, 1, idx.EngineForTest().Stats().FrozenGenerations)
		})
	}
}

// TestHandoverAcceptsTheSameVersion checks that a returned manifest at the
// installed version passes the safety check when its checkpoint covers the
// handover.
func TestHandoverAcceptsTheSameVersion(t *testing.T) {
	ctx := context.Background()
	_, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1)
	ch.ObserveTimeTick(50)
	idx.SetInstalledVersionForTest(1)

	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NoError(t, idx.CommitForTest(ctx, p))

	assert.Equal(t, int64(1), idx.InstalledVersionForTest())
	assert.Equal(t, 0, idx.EngineForTest().Stats().FrozenGenerations, "the generations of the handover retire")
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
}

func TestHandoverWatermarkDidNotMove(t *testing.T) {
	env, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1, 2)
	ch.ObserveTimeTick(30)
	idx.SetLastCommittedForTest(30)

	p, err := idx.PrepareForTest(context.Background())
	require.NoError(t, err)
	assert.Nil(t, p)

	assert.Equal(t, 0, env.store.Len())
	assert.Equal(t, uint64(30), idx.LastCommittedTimeTick())
	stats := idx.EngineForTest().Stats()
	assert.Equal(t, 0, stats.FrozenGenerations)
	assert.Greater(t, stats.ActiveBytes, uint64(0))
}

func TestCommitReuploadFailureKeepsThePending(t *testing.T) {
	ctx := context.Background()
	env, ch, idx := handoverIndex(t)
	insert(t, idx, 10, 1)
	ch.ObserveTimeTick(50)

	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	old := time.Now().Add(-2 * time.Hour)
	p.SetUploadedAt(old)
	env.store.FailUploads(1)
	err = idx.CommitForTest(ctx, p)
	assert.True(t, errors.Is(err, pkroot.ErrIO))
	assert.Empty(t, env.client.requestList(), "no commit after a failed re-upload")
	assert.Equal(t, old, p.UploadedAt())
	assert.Equal(t, 1, idx.EngineForTest().Stats().FrozenGenerations)

	require.NoError(t, idx.CommitForTest(ctx, p))
	assert.Equal(t, uint64(50), idx.LastCommittedTimeTick())
}

// The tests below call pickHandovers of a started PChannelIndexManager without
// the driver, to check the explicit request and the stall triggers. The sizes
// never trigger.

// requestEnv returns a started PChannelIndexManager whose sizes never trigger,
// with a ready Index of v0 that a handover of the fake client commits.
func requestEnv(t *testing.T) (*testEnv, *PChannelIndexManager, *index.Index) {
	env := newTestEnv(t, syncScheduler{})
	env.setParams(func(p *Params) { p.ActiveBytes = math.MaxUint64 })
	ch := env.channel(0)
	return env, ch, env.add(ch, "v0")
}

// TestRequestHandoverWaitsForTheWatermark checks that a request fires once the
// watermark of the pchannel reaches the requested time tick, that the
// handover then commits at or above it, and that a satisfied request does not
// fire again.
func TestRequestHandoverWaitsForTheWatermark(t *testing.T) {
	_, ch, idx := requestEnv(t)
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(5)
	ch.RequestHandover("v0", 10)
	assert.EqualValues(t, 10, ch.RequestedForTest("v0"))
	assert.Empty(t, ch.pickHandovers(farFuture), "the watermark is below the request")

	ch.ObserveTimeTick(10)
	assert.Equal(t, []*index.Index{idx}, ch.pickHandovers(farFuture))
	ch.submitHandover(idx)
	require.GreaterOrEqual(t, idx.LastCommittedTimeTick(), uint64(10))
	assert.EqualValues(t, 10, ch.RequestedForTest("v0"), "a request is never reset")

	insert(t, idx, 1, 2)
	ch.ObserveTimeTick(11)
	assert.Empty(t, ch.pickHandovers(farFuture), "the request is satisfied")
}

// TestRequestHandoverRepeatsUntilACommitCoversIt checks that a handover
// that commits below the requested time tick leaves the request in force.
func TestRequestHandoverRepeatsUntilACommitCoversIt(t *testing.T) {
	_, ch, idx := requestEnv(t)
	insert(t, idx, 1, 1)
	ch.ObserveTimeTick(10)
	ch.RequestHandover("v0", 10)
	assert.Equal(t, []*index.Index{idx}, ch.pickHandovers(farFuture))
	ch.submitHandover(idx)
	require.EqualValues(t, 10, idx.LastCommittedTimeTick())

	insert(t, idx, 1, 2)
	idx.SetLastCommittedForTest(7)
	assert.Equal(t, []*index.Index{idx}, ch.pickHandovers(farFuture), "the commit did not cover the request")
	assert.Equal(t, []*index.Index{idx}, ch.pickHandovers(farFuture), "picked at every step")
}

// TestRequestHandoverKeepsTheHighestTick checks that requests keep the
// highest time tick, that a request of an unknown vchannel is ignored, and
// that RequestHandoverAll requests every vchannel.
func TestRequestHandoverKeepsTheHighestTick(t *testing.T) {
	env, ch, a := requestEnv(t)
	b := env.add(ch, "v1")
	ch.RequestHandover("v0", 10)
	ch.RequestHandover("v0", 8)
	assert.EqualValues(t, 10, ch.RequestedForTest("v0"))
	ch.RequestHandover("v0", 12)
	assert.EqualValues(t, 12, ch.RequestedForTest("v0"))

	ch.RequestHandover("unknown", 20)
	assert.Zero(t, ch.RequestedForTest("unknown"))
	assert.Zero(t, ch.RequestedForTest("v1"))

	ch.RequestHandoverAll(11)
	assert.EqualValues(t, 12, ch.RequestedForTest("v0"), "a lower request does not lower it")
	assert.EqualValues(t, 11, ch.RequestedForTest("v1"))

	insert(t, a, 1, 1)
	insert(t, b, 1, 1)
	ch.ObserveTimeTick(12)
	assert.ElementsMatch(t, []*index.Index{a, b}, ch.pickHandovers(farFuture))
}

// TestRequestHandoverOfACleanOrNotReadyIndexWaits checks that a request does
// not pick an Index with nothing to commit, nor one that is not ready, and
// stays in force until the Index is dirty and ready.
func TestRequestHandoverOfACleanOrNotReadyIndexWaits(t *testing.T) {
	ctx := context.Background()
	t.Run("clean", func(t *testing.T) {
		_, ch, idx := requestEnv(t)
		ch.ObserveTimeTick(10)
		ch.RequestHandover("v0", 10)
		assert.Empty(t, ch.pickHandovers(farFuture))
		assert.EqualValues(t, 10, ch.RequestedForTest("v0"), "the request waits for a write")

		insert(t, idx, 1, 1)
		ch.ObserveTimeTick(11)
		assert.Equal(t, []*index.Index{idx}, ch.pickHandovers(farFuture))
	})
	t.Run("not ready", func(t *testing.T) {
		env := newTestEnv(t, syncScheduler{})
		env.setParams(func(p *Params) { p.ActiveBytes = math.MaxUint64 })
		ch := env.recoveredChannel(0, "v0")
		idx := ch.get("v0")
		ch.start(false)
		require.False(t, idx.Ready())
		insert(t, idx, 1, 1)
		ch.ObserveTimeTick(10)
		ch.RequestHandover("v0", 10)
		assert.Empty(t, ch.pickHandovers(farFuture))
		assert.EqualValues(t, 10, ch.RequestedForTest("v0"), "the request waits for the recovery")

		idx.MarkReady(ctx, false)
		assert.Equal(t, []*index.Index{idx}, ch.pickHandovers(farFuture))
	})
}

// TestStalledCheckpointHandsOverTheOldestIndex checks that an index that held
// the WAL checkpoint back for the stall timeout hands the oldest dirty Index
// over, and that the stall restarts when the index no longer holds the
// checkpoint back.
func TestStalledCheckpointHandsOverTheOldestIndex(t *testing.T) {
	ctx := context.Background()
	env := NewFakeEnv(t, t.TempDir())
	env.SetStallTimeoutForTest(time.Second)
	var tt atomic.Uint64
	tt.Store(50)
	c := env.Manager.New(PChannelConfig{
		PChannel: types.PChannelInfo{Name: "p", Term: 1},
		Tail:     func() WALTail { return WALTail{RecoveryTimeTick: tt.Load()} },
	})
	t.Cleanup(c.Close)
	old, _, err := c.CreateCollection(ctx, "v1", 1, schemapb.DataType_Int64)
	require.NoError(t, err)
	young, _, err := c.CreateCollection(ctx, "v2", 2, schemapb.DataType_Int64)
	require.NoError(t, err)
	c.start(false)
	old.MarkReady(ctx, false)
	young.MarkReady(ctx, false)
	old.SetLastCommittedForTest(10)
	young.SetLastCommittedForTest(20)
	insert(t, old, 1, 1)
	insert(t, young, 1, 1)
	c.ObserveTimeTick(60)
	require.EqualValues(t, 10, c.lastAckedUnclamped())

	now := time.Now()
	assert.Empty(t, c.pickHandovers(now), "the stall starts now")
	assert.Empty(t, c.pickHandovers(now.Add(500*time.Millisecond)))
	assert.Equal(t, []*index.Index{old}, c.pickHandovers(now.Add(2*time.Second)))

	// once the index no longer holds the checkpoint back, the stall resets
	tt.Store(5)
	assert.Empty(t, c.pickHandovers(now.Add(3*time.Second)))
	tt.Store(50)
	assert.Empty(t, c.pickHandovers(now.Add(3500*time.Millisecond)), "a new stall starts")
	assert.Equal(t, []*index.Index{old}, c.pickHandovers(now.Add(4500*time.Millisecond)))
}

// TestStallClockRunsThroughABackoff checks that an Index that waits in its
// backoff after a failed handover keeps the stall clock running.
func TestStallClockRunsThroughABackoff(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	env.setParams(func(p *Params) {
		p.ActiveBytes = math.MaxUint64
		p.BackoffBase = time.Hour
		p.MaxBackoff = time.Hour
		p.StallTimeout = time.Second
	})
	ch := env.channel(0)
	a := env.add(ch, "va")
	insert(t, a, 1, 1)
	ch.ObserveTimeTick(50)
	env.recoveryTimeTick.Store(40)

	now := time.Now()
	assert.Empty(t, ch.pickHandovers(now), "the stall starts now")
	require.Equal(t, now, ch.stallSince)

	// the commit fails, the Index keeps its pending handover and backs off
	env.client.set(func(c *fakeClient) { c.commitErrs = []error{pkroot.MarkIO(errors.New("disk gone"), "commit")} })
	ch.submitHandover(a)
	require.True(t, a.HasPending())
	require.True(t, now.Add(2*time.Second).Before(scheduleOf(ch, a).backoff.NextInstant()))

	assert.Empty(t, ch.pickHandovers(now.Add(2*time.Second)), "the Index waits in its backoff")
	assert.Equal(t, now, ch.stallSince, "the stall clock keeps running")
	assert.Equal(t, []*index.Index{a}, ch.pickHandovers(now.Add(2*time.Hour)), "the pending handover is retried after the backoff")
}

// TestStallTimeoutZeroNeverTriggers checks that a zero stall timeout turns
// the stall trigger off.
func TestStallTimeoutZeroNeverTriggers(t *testing.T) {
	ctx := context.Background()
	env := NewFakeEnv(t, t.TempDir())
	c := env.Manager.New(PChannelConfig{
		PChannel: types.PChannelInfo{Name: "p", Term: 1},
		Tail:     func() WALTail { return WALTail{RecoveryTimeTick: 50} },
	})
	t.Cleanup(c.Close)
	c.start(false)
	idx, _, err := c.CreateCollection(ctx, "v1", 1, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.True(t, idx.Ready())
	insert(t, idx, 1, 1)
	c.ObserveTimeTick(60)
	require.Less(t, c.lastAckedUnclamped(), uint64(50), "the index holds the checkpoint back")
	now := time.Now()
	assert.Empty(t, c.pickHandovers(now))
	assert.Empty(t, c.pickHandovers(now.Add(24*time.Hour)))
}

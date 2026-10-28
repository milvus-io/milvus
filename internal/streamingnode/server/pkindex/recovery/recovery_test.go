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

package recovery

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// testMeta returns the vchannel meta of a collection with one primary key
// field of type pk.
func testMeta(vchannel string, collectionID int64, pk schemapb.DataType, autoID bool) *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel: vchannel,
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: collectionID,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: pk, AutoID: autoID},
			}}}},
		},
	}
}

func TestRecoverOpensLooksUpAndMarksReadiness(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	env.Committer.SetManifest("v1", &index.FakeManifest{View: index.ManifestView{Version: 2, CheckpointTimeTick: 100, SSTBasePath: "root/1/v1"}})
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}, Seed: 80},
		Metas: map[string]*streamingpb.VChannelMeta{
			"v1": testMeta("v1", 1, schemapb.DataType_Int64, false),
			"v2": testMeta("v2", 2, schemapb.DataType_VarChar, false),
			"v3": testMeta("v3", 3, schemapb.DataType_Int64, true), // autoID: no index
		},
		Sched: env.Manager.Scheduler(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })

	m := env.Manager.GetForTest("p")
	require.NotNil(t, m)
	v1 := m.IndexForTest("v1")
	require.NotNil(t, v1)
	assert.Equal(t, uint64(100), v1.LastCommittedTimeTick())
	assert.False(t, v1.Ready(), "waits for the install")
	v2 := m.IndexForTest("v2")
	require.NotNil(t, v2)
	assert.True(t, v2.Ready(), "no manifest: ready at once")
	assert.Nil(t, m.IndexForTest("v3"))
	assert.Equal(t, uint64(80), m.LastAcked(), "nothing dirty: the watermark seed")
	assert.Equal(t, uint64(80), rec.LastAcked())
}

func TestRecoverFailsOnAFencedLookupAndLeavesNothingOpen(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	env.Committer.SetLookupErr(index.ErrFenced)
	_, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          env.Manager.Scheduler(),
	})
	require.ErrorIs(t, err, index.ErrFenced)
	assert.Nil(t, env.Manager.GetForTest("p"))
}

func TestRecoverSkipsDroppedVChannelsAndRejectsAnUnsupportedPrimaryKey(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	dropped := testMeta("v2", 2, schemapb.DataType_Int64, false)
	dropped.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}},
		Metas: map[string]*streamingpb.VChannelMeta{
			"v1": testMeta("v1", 1, schemapb.DataType_Int64, false),
			"v2": dropped,
		},
		Sched: env.Manager.Scheduler(),
	})
	require.NoError(t, err)
	m := env.Manager.GetForTest("p")
	require.NotNil(t, m)
	assert.NotNil(t, m.IndexForTest("v1"))
	assert.Nil(t, m.IndexForTest("v2"), "a dropped vchannel gets no index")
	rec.Close(ctx)
	require.Nil(t, env.Manager.GetForTest("p"))

	_, err = Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 4}},
		Metas: map[string]*streamingpb.VChannelMeta{
			"v1": testMeta("v1", 1, schemapb.DataType_Int64, false),
			"v4": testMeta("v4", 4, schemapb.DataType_Float, false),
		},
		Sched: env.Manager.Scheduler(),
	})
	require.Error(t, err)
	assert.Nil(t, env.Manager.GetForTest("p"), "an unsupported primary key leaves nothing open")
}

func TestRunStartsTheDriverAndCloseForgetsThePChannel(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          env.Manager.Scheduler(),
	})
	require.NoError(t, err)
	m := env.Manager.GetForTest("p")
	require.NotNil(t, m)
	require.NoError(t, rec.Run(ctx))

	idx, created, err := m.CreateCollection(ctx, "v2", 2, 0)
	require.NoError(t, err)
	require.True(t, created)
	assert.True(t, idx.Ready(), "after Run a new collection is ready at once")

	rec.Close(ctx)
	assert.Nil(t, env.Manager.GetForTest("p"))
	rec.Close(ctx) // a second Close does nothing
	err = rec.Run(ctx)
	require.Error(t, err, "Run after Close")
	assert.True(t, errors.Is(err, pkroot.ErrUnavailable), "got %v", err)
}

func TestRecoverFailsOnAStoppedManager(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	env.Manager.Close()
	_, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkroot.ErrUnavailable), "got %v", err)
	assert.Nil(t, env.Manager.GetForTest("p"))
}

func TestRecoverRetriesATransientLookupFailure(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	env.Committer.SetLookupFailures(2)
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          env.Manager.Scheduler(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	assert.True(t, env.Manager.GetForTest("p").IndexForTest("v1").Ready(), "the lookup went through after the retries")
}

// readCommitter is a Committer whose ReadManifest of some body keys is
// replaced. The other calls go to the embedded Committer.
type readCommitter struct {
	pkindex.Committer
	reads map[string]func(ctx context.Context) (index.ManifestView, error) // by body key
}

func (c *readCommitter) ReadManifest(ctx context.Context, bodyKey string) (index.ManifestView, error) {
	if read, ok := c.reads[bodyKey]; ok {
		return read(ctx)
	}
	return c.Committer.ReadManifest(ctx, bodyKey)
}

func TestRecoverRejectsAManifestBodyWithAnotherVersion(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	m := &index.FakeManifest{View: index.ManifestView{Version: 2, CheckpointTimeTick: 100, SSTBasePath: "root/1/v1"}}
	env.Committer.SetManifest("v1", m)
	env.Manager.SetCommitterForTest(&readCommitter{Committer: env.Committer, reads: map[string]func(context.Context) (index.ManifestView, error){
		pkindex.FakeRecoveryManifest("v1", m).BodyKey: func(context.Context) (index.ManifestView, error) {
			view := m.View
			view.Version = 3
			return view, nil
		},
	}})
	_, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          env.Manager.Scheduler(),
	})
	require.ErrorIs(t, err, index.ErrRejected)
	assert.Nil(t, env.Manager.GetForTest("p"))
}

func TestRecoverAFatalManifestReadStopsTheOtherReads(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	m1 := &index.FakeManifest{View: index.ManifestView{Version: 2, CheckpointTimeTick: 100, SSTBasePath: "root/1/v1"}}
	m2 := &index.FakeManifest{View: index.ManifestView{Version: 1, CheckpointTimeTick: 50, SSTBasePath: "root/2/v2"}}
	env.Committer.SetManifest("v1", m1)
	env.Committer.SetManifest("v2", m2)
	env.Manager.SetCommitterForTest(&readCommitter{Committer: env.Committer, reads: map[string]func(context.Context) (index.ManifestView, error){
		// v1: the body names another version, a fatal error
		pkindex.FakeRecoveryManifest("v1", m1).BodyKey: func(context.Context) (index.ManifestView, error) {
			view := m1.View
			view.Version = 3
			return view, nil
		},
		// v2: always a transient failure, retried until the read ctx ends
		pkindex.FakeRecoveryManifest("v2", m2).BodyKey: func(context.Context) (index.ManifestView, error) {
			time.Sleep(time.Millisecond)
			return index.ManifestView{}, errors.New("injected transient failure")
		},
	}})
	start := time.Now()
	_, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}},
		Metas: map[string]*streamingpb.VChannelMeta{
			"v1": testMeta("v1", 1, schemapb.DataType_Int64, false),
			"v2": testMeta("v2", 2, schemapb.DataType_Int64, false),
		},
		Sched: env.Manager.Scheduler(),
	})
	require.ErrorIs(t, err, index.ErrRejected)
	assert.Less(t, time.Since(start), 5*time.Second, "the transient retry of v2 ends with the fatal error of v1")
	assert.Nil(t, env.Manager.GetForTest("p"))
}

// batchCommitter records the size of every GetRecoveryManifests batch.
type batchCommitter struct {
	pkindex.Committer
	mu      sync.Mutex
	batches []int
}

func (c *batchCommitter) GetRecoveryManifests(ctx context.Context, refs []pkindex.VChannelRef) ([]pkindex.RecoveryManifest, error) {
	c.mu.Lock()
	c.batches = append(c.batches, len(refs))
	c.mu.Unlock()
	return c.Committer.GetRecoveryManifests(ctx, refs)
}

func TestRecoverLooksManifestsUpInBatches(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	committer := &batchCommitter{Committer: env.Committer}
	env.Manager.SetCommitterForTest(committer)
	env.Committer.SetManifest("v5", &index.FakeManifest{View: index.ManifestView{Version: 1, CheckpointTimeTick: 10, SSTBasePath: "root/5/v5"}})
	old := manifestLookupBatch
	manifestLookupBatch = 2
	t.Cleanup(func() { manifestLookupBatch = old })

	metas := make(map[string]*streamingpb.VChannelMeta)
	for i := int64(1); i <= 5; i++ {
		v := "v" + string(rune('0'+i))
		metas[v] = testMeta(v, i, schemapb.DataType_Int64, false)
	}
	rec, err := Recover(ctx, env.Manager, Param{PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}}, Metas: metas, Sched: env.Manager.Scheduler()})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })

	committer.mu.Lock()
	batches := append([]int(nil), committer.batches...)
	committer.mu.Unlock()
	sort.Sort(sort.Reverse(sort.IntSlice(batches)))
	assert.Equal(t, []int{2, 2, 1}, batches)
	m := env.Manager.GetForTest("p")
	for v := range metas {
		require.NotNil(t, m.IndexForTest(v), v)
	}
	assert.Equal(t, uint64(10), m.IndexForTest("v5").LastCommittedTimeTick())
	assert.False(t, m.IndexForTest("v5").Ready())
	assert.True(t, m.IndexForTest("v1").Ready())
}

// countingScheduler counts the submitted tasks and passes them on.
type countingScheduler struct {
	nodescheduler.Scheduler
	submitted atomic.Int32
}

func (s *countingScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.submitted.Add(1)
	return s.Scheduler.Submit(task)
}

func TestRunSkipsTheInstallOfADroppedIndex(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	env.Committer.SetManifest("v1", &index.FakeManifest{View: index.ManifestView{Version: 1, CheckpointTimeTick: 10, SSTBasePath: "root/1/v1"}})
	inner := nodescheduler.New(1)
	t.Cleanup(inner.Close)
	sched := &countingScheduler{Scheduler: inner}
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          sched,
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	m := env.Manager.GetForTest("p")
	require.False(t, m.IndexForTest("v1").Ready(), "waits for the install")

	after := rec.ObserveMessage(ctx, dropCollectionMsg("v1", 1, 20))
	require.NotNil(t, after)
	after(ctx)
	require.Nil(t, m.IndexForTest("v1"))
	require.NoError(t, rec.Run(ctx))
	assert.Zero(t, sched.submitted.Load(), "no install of a dropped index")
}

func TestRunRunsOnce(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	env.Committer.SetManifest("v1", &index.FakeManifest{View: index.ManifestView{Version: 1, CheckpointTimeTick: 10, SSTBasePath: "root/1/v1"}})
	inner := nodescheduler.New(1)
	t.Cleanup(inner.Close)
	sched := &countingScheduler{Scheduler: inner}
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          sched,
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	require.NoError(t, rec.Run(ctx))
	require.NoError(t, rec.Run(ctx))
	assert.Equal(t, int32(1), sched.submitted.Load(), "the install is submitted once")
	require.Eventually(t, env.Manager.GetForTest("p").IndexForTest("v1").Ready, 10*time.Second, 10*time.Millisecond)
}

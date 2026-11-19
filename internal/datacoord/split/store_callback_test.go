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

package split

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

const (
	testCollection = int64(100)
	testSource     = "by-dev-rootcoord-dml_0_100v0"
	testTarget1    = "by-dev-rootcoord-dml_1_100v1"
	testTarget2    = "by-dev-rootcoord-dml_2_100v2"
)

// fakeCatalog is a split task catalog and a DataView catalog in memory. log
// records the order of the writes of both.
type fakeCatalog struct {
	mu      sync.Mutex
	tasks   map[int64]*datapb.SplitShardTask
	views   []*viewpb.DataViewOfCollection
	log     []string
	saveErr error
	dropErr error
	listErr error
	viewErr error
}

func newFakeCatalog() *fakeCatalog {
	return &fakeCatalog{tasks: make(map[int64]*datapb.SplitShardTask)}
}

func (c *fakeCatalog) SaveSplitShardTask(_ context.Context, task *datapb.SplitShardTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.saveErr != nil {
		return c.saveErr
	}
	c.tasks[task.GetTaskId()] = proto.Clone(task).(*datapb.SplitShardTask)
	c.log = append(c.log, "task:"+task.GetState().String())
	return nil
}

func (c *fakeCatalog) ListSplitShardTask(context.Context) ([]*datapb.SplitShardTask, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listErr != nil {
		return nil, c.listErr
	}
	tasks := make([]*datapb.SplitShardTask, 0, len(c.tasks))
	for _, task := range c.tasks {
		tasks = append(tasks, proto.Clone(task).(*datapb.SplitShardTask))
	}
	return tasks, nil
}

func (c *fakeCatalog) DropSplitShardTask(_ context.Context, task *datapb.SplitShardTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropErr != nil {
		return c.dropErr
	}
	delete(c.tasks, task.GetTaskId())
	return nil
}

func (c *fakeCatalog) SaveDataView(_ context.Context, view *viewpb.DataViewOfCollection) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.viewErr != nil {
		return c.viewErr
	}
	c.views = append(c.views, proto.Clone(view).(*viewpb.DataViewOfCollection))
	c.log = append(c.log, "view")
	return nil
}

func (c *fakeCatalog) ListAllDataViews(context.Context) ([]*viewpb.DataViewOfCollection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.views, nil
}

func (c *fakeCatalog) DropDataView(context.Context, *viewpb.DataViewOfCollection) error { return nil }

func (c *fakeCatalog) DropDataViews(context.Context, int64) error { return nil }

func (c *fakeCatalog) writes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.log...)
}

func (c *fakeCatalog) resetLog() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = nil
}

func mustStore(t *testing.T, catalog *fakeCatalog) *Store {
	store, err := NewStore(context.Background(), catalog)
	require.NoError(t, err)
	return store
}

func testRecord(taskID int64) Record {
	return Record{
		TaskID:         taskID,
		CollectionID:   testCollection,
		Source:         testSource,
		Targets:        []Target{{VChannel: testTarget1, GenesisTimeTick: 110}, {VChannel: testTarget2, GenesisTimeTick: 120}},
		SwitchTimeTick: 100,
	}
}

// splitResult is the acknowledged SplitShard message of a split, as the ack
// callback receives it. ticks are the time ticks of the copies on the source,
// the first target and the second target.
func splitResult(taskID int64, ticks [3]uint64) message.BroadcastResultSplitShardMessageV2 {
	header := &message.SplitShardMessageHeader{
		CollectionId:    testCollection,
		SplitTaskId:     taskID,
		SourceVchannel:  testSource,
		TargetVchannels: []string{testTarget1, testTarget2},
	}
	vchannels := []string{testSource, testTarget1, testTarget2}
	msg := message.NewSplitShardMessageBuilderV2().WithHeader(header).WithBody(&message.SplitShardMessageBody{}).
		WithBroadcast(vchannels, message.OptBuildBroadcastAppendFirst(testSource)).
		MustBuildBroadcast().WithBroadcastID(1)
	msg = message.WithBroadcastControlChannel(msg, funcutil.GetControlChannel("by-dev-rootcoord-dml_0"))
	results := map[string]*message.AppendResult{}
	for idx, vchannel := range vchannels {
		id := rmq.NewRmqID(int64(ticks[idx]))
		results[vchannel] = &message.AppendResult{MessageID: id, LastConfirmedMessageID: id, TimeTick: ticks[idx]}
	}
	return message.BroadcastResultSplitShardMessageV2{
		Message: message.MustAsBroadcastSplitShardMessageV2(msg),
		Results: results,
	}
}

// consumedCopy is the copy of the SplitShard message on a vchannel, as a
// StreamingNode acknowledges it.
func consumedCopy(t *testing.T, result message.BroadcastResultSplitShardMessageV2, vchannel string) message.ImmutableMessage {
	header := result.Message.Header()
	vchannels := append([]string{header.GetSourceVchannel()}, header.GetTargetVchannels()...)
	broadcast := message.NewSplitShardMessageBuilderV2().WithHeader(header).WithBody(&message.SplitShardMessageBody{}).
		WithBroadcast(vchannels, message.OptBuildBroadcastAppendFirst(header.GetSourceVchannel())).
		MustBuildBroadcast().WithBroadcastID(1)
	for _, msg := range broadcast.SplitIntoMutableMessage() {
		if msg.VChannel() == vchannel {
			id := result.Results[vchannel].MessageID
			return msg.WithTimeTick(result.Results[vchannel].TimeTick).WithLastConfirmed(id).IntoImmutableMessage(id)
		}
	}
	require.FailNow(t, "the split has no copy on the vchannel", vchannel)
	return nil
}

func TestStoreRecordOpensTheWindowOnce(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	require.Nil(t, store.Get(7))
	require.Nil(t, store.WindowOf(testCollection))
	require.Zero(t, store.WindowCount())
	require.Nil(t, store.TransformPins(testCollection))
	require.False(t, store.IsL0CompactionSuspended(testTarget1))

	task, err := store.Record(ctx, testRecord(7))
	require.NoError(t, err)
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskWindowOpen, task.GetState())
	require.EqualValues(t, 100, task.GetSwitchTimeTick())
	require.Equal(t, task, store.Get(7))
	require.Equal(t, task, store.WindowOf(testCollection))
	require.Equal(t, 1, store.WindowCount())
	require.Len(t, store.List(), 1)
	require.Equal(t, map[string]uint64{testTarget1: 100, testTarget2: 100}, store.TransformPins(testCollection))
	require.Nil(t, store.TransformPins(testCollection+1))
	require.True(t, store.IsL0CompactionSuspended(testTarget1))
	require.True(t, store.IsL0CompactionSuspended(testTarget2))
	require.False(t, store.IsL0CompactionSuspended(testSource))
	require.Equal(t, []string{testSource}, store.Sources(testCollection, datapb.SplitShardTaskState_SplitShardTaskRetired))
	require.Empty(t, store.Sources(testCollection, datapb.SplitShardTaskState_SplitShardTaskWindowOpen))
	require.Equal(t, []string{testSource, testTarget1, testTarget2}, store.VChannels(testCollection))

	// The callback runs again with the ticks of another attempt: the record
	// keeps the ticks it has and nothing is written.
	catalog.resetLog()
	again := testRecord(7)
	again.SwitchTimeTick = 500
	again.Targets[0].GenesisTimeTick = 510
	kept, err := store.Record(ctx, again)
	require.NoError(t, err)
	require.Equal(t, task, kept)
	require.Empty(t, catalog.writes())

	// The same task id for other shards is refused.
	other := testRecord(7)
	other.Source = testTarget1
	_, err = store.Record(ctx, other)
	require.Error(t, err)
	other = testRecord(7)
	other.Targets = other.Targets[:1]
	_, err = store.Record(ctx, other)
	require.Error(t, err)
}

func TestStoreRecordRefusesAnIncompleteRecord(t *testing.T) {
	ctx := context.Background()
	store := mustStore(t, newFakeCatalog())
	for _, mutate := range []func(record *Record){
		func(record *Record) { record.TaskID = 0 },
		func(record *Record) { record.Source = "" },
		func(record *Record) { record.SwitchTimeTick = 0 },
		func(record *Record) { record.Targets = nil },
		func(record *Record) { record.Targets[1].GenesisTimeTick = 0 },
		func(record *Record) { record.Targets[0].VChannel = "" },
	} {
		record := testRecord(7)
		mutate(&record)
		_, err := store.Record(ctx, record)
		require.Error(t, err)
	}
	require.Empty(t, store.List())
}

func TestStoreTransformPinFollowsAGenesisBelowTheFence(t *testing.T) {
	store := mustStore(t, newFakeCatalog())
	record := testRecord(7)
	record.Targets[0].GenesisTimeTick = 90
	_, err := store.Record(context.Background(), record)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{testTarget1: 90, testTarget2: 100}, store.TransformPins(testCollection))
}

func TestStoreSourceAckedAndAdvance(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	targets := []string{testTarget1, testTarget2}
	require.Error(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskRewriteDone))
	require.Error(t, store.WaitResidualFlushed(ctx, 7))

	_, err := store.Record(ctx, testRecord(7))
	require.NoError(t, err)
	// No state is reached before the residual has flushed, and the states up
	// to that one are not reached through Advance.
	require.Error(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskRewriteDone))
	require.Error(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskResidualFlushed))

	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	require.ErrorIs(t, store.WaitResidualFlushed(waitCtx, 7), context.DeadlineExceeded)
	cancel()

	done := make(chan error, 1)
	go func() { done <- store.WaitResidualFlushed(ctx, 7) }()
	require.NoError(t, store.MarkSourceAcked(ctx, 7, testCollection, testSource, targets))
	require.NoError(t, <-done)
	task := store.Get(7)
	require.True(t, task.GetSourceAcked())
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskResidualFlushed, task.GetState())
	require.NotNil(t, store.WindowOf(testCollection))

	// Acknowledged again: nothing is written.
	catalog.resetLog()
	require.NoError(t, store.MarkSourceAcked(ctx, 7, testCollection, testSource, targets))
	require.Empty(t, catalog.writes())

	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskRewriteDone))
	require.True(t, store.IsL0CompactionSuspended(testTarget1))
	require.NotNil(t, store.TransformPins(testCollection))
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
	// Adoption closes the data window, but L0 and history stay protected
	// until the coordinator revokes g1 and the accumulated history is folded.
	require.Nil(t, store.WindowOf(testCollection))
	require.Zero(t, store.WindowCount())
	require.True(t, store.IsL0CompactionSuspended(testTarget1))
	require.NotEmpty(t, store.TransformPins(testCollection))
	require.NoError(t, store.ResumeL0(ctx, 7))
	require.NoError(t, store.ReleasePins(ctx, 7))
	require.False(t, store.IsL0CompactionSuspended(testTarget1))
	require.Nil(t, store.TransformPins(testCollection))
	require.Equal(t, []string{testSource}, store.Sources(testCollection, datapb.SplitShardTaskState_SplitShardTaskRetired))
	// A split never moves back.
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskRewriteDone))
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopted, store.Get(7).GetState())
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskRetired))
	require.Empty(t, store.Sources(testCollection, datapb.SplitShardTaskState_SplitShardTaskRetired))
	require.NoError(t, store.WaitResidualFlushed(ctx, 7))
}

// On a cluster where the StreamingNode acknowledges the source before the ack
// callback runs, the acknowledgement is kept and the callback completes it.
func TestStoreSourceAckedBeforeTheRecord(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	require.NoError(t, store.MarkSourceAcked(ctx, 7, testCollection, testSource, []string{testTarget1, testTarget2}))
	pending := store.Get(7)
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPending, pending.GetState())
	// A pending record keeps other splits of the collection out, and pins
	// nothing: its ticks are not known yet.
	require.NotNil(t, store.WindowOf(testCollection))
	require.Nil(t, store.TransformPins(testCollection))
	require.True(t, store.IsL0CompactionSuspended(testTarget1))

	task, err := store.Record(ctx, testRecord(7))
	require.NoError(t, err)
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskResidualFlushed, task.GetState())
	require.True(t, task.GetSourceAcked())
	require.EqualValues(t, 100, task.GetSwitchTimeTick())
	require.EqualValues(t, 110, task.GetTargets()[0].GetGenesisTimeTick())
	require.NoError(t, store.WaitResidualFlushed(ctx, 7))
}

func TestStoreRecoversFromTheCatalog(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	_, err := store.Record(ctx, testRecord(7))
	require.NoError(t, err)
	second := testRecord(8)
	second.CollectionID = testCollection + 1
	_, err = store.Record(ctx, second)
	require.NoError(t, err)
	require.NoError(t, store.MarkSourceAcked(ctx, 8, second.CollectionID, testSource, nil))

	recovered := mustStore(t, catalog)
	require.Len(t, recovered.List(), 2)
	require.True(t, proto.Equal(store.Get(7), recovered.Get(7)))
	require.True(t, proto.Equal(store.Get(8), recovered.Get(8)))
	require.Equal(t, 2, recovered.WindowCount())
	require.Equal(t, map[string]uint64{testTarget1: 100, testTarget2: 100}, recovered.TransformPins(testCollection))
	require.True(t, recovered.IsL0CompactionSuspended(testTarget2))

	catalog.listErr = errors.New("mock")
	_, err = NewStore(ctx, catalog)
	require.Error(t, err)
}

func TestStoreKeepsItsCacheWhenTheCatalogFails(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	catalog.saveErr = errors.New("mock")
	_, err := store.Record(ctx, testRecord(7))
	require.Error(t, err)
	require.Nil(t, store.Get(7))
	require.Error(t, store.MarkSourceAcked(ctx, 7, testCollection, testSource, nil))
	require.Nil(t, store.Get(7))

	catalog.saveErr = nil
	_, err = store.Record(ctx, testRecord(7))
	require.NoError(t, err)
	require.NoError(t, store.MarkSourceAcked(ctx, 7, testCollection, testSource, nil))
	catalog.saveErr = errors.New("mock")
	require.Error(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskRewriteDone))
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskResidualFlushed, store.Get(7).GetState())

	catalog.dropErr = errors.New("mock")
	require.Error(t, store.DropCollection(ctx, testCollection))
	require.NotNil(t, store.Get(7))
}

func TestStoreDrop(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	_, err := store.Record(ctx, testRecord(7))
	require.NoError(t, err)
	require.NoError(t, store.MarkSourceAcked(ctx, 8, testCollection+1, "s8", []string{"t81", "t82"}))
	require.NoError(t, store.MarkSourceAcked(ctx, 9, testCollection+2, "s9", []string{"t91", "t92"}))

	// Only a pending record of a collection that is gone, older than the
	// deadline, is swept.
	gone := func(collectionID int64) bool { return collectionID != testCollection+2 }
	require.NoError(t, store.DropPendingBefore(ctx, time.Now().Add(-time.Hour), gone))
	require.Len(t, store.List(), 3)
	require.NoError(t, store.DropPendingBefore(ctx, time.Now().Add(time.Hour), gone))
	require.Nil(t, store.Get(8))
	require.NotNil(t, store.Get(9))
	require.NotNil(t, store.Get(7))

	require.NoError(t, store.DropCollection(ctx, testCollection))
	require.Nil(t, store.Get(7))
	require.Len(t, mustStore(t, catalog).List(), 1)
}

// recordingDeclarer is a DataView manager that only records its calls into
// the write log of the catalog.
type recordingDeclarer struct {
	catalog *fakeCatalog
	events  []dataview.DeclareShardsDataViewEvent
	err     error
}

func (d *recordingDeclarer) DeclareShards(_ context.Context, event dataview.DeclareShardsDataViewEvent) (*viewpb.DataVersion, error) {
	if d.err != nil {
		return nil, d.err
	}
	d.events = append(d.events, event)
	d.catalog.mu.Lock()
	d.catalog.log = append(d.catalog.log, "declare")
	d.catalog.mu.Unlock()
	return &viewpb.DataVersion{StreamingVersion: 1}, nil
}

func TestCallbackDeclaresTheTargetsBeforeItRecordsTheSplit(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	declarer := &recordingDeclarer{catalog: catalog}
	callback := &Callback{Store: store, DataView: func() ShardDeclarer { return declarer }}

	require.NoError(t, callback.OnAcked(ctx, splitResult(7, [3]uint64{100, 110, 120})))
	require.Equal(t, []string{"declare", "task:SplitShardTaskWindowOpen"}, catalog.writes())
	require.Len(t, declarer.events, 1)
	require.Equal(t, testCollection, declarer.events[0].CollectionID)
	require.Equal(t, []string{testTarget1, testTarget2}, declarer.events[0].VChannels)
	// The targets are declared with their Transform frontier at T_switch.
	require.Equal(t, map[string]uint64{testTarget1: 100, testTarget2: 100}, declarer.events[0].TransformStarts)

	task := store.Get(7)
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskWindowOpen, task.GetState())
	require.Equal(t, testSource, task.GetSourceVchannel())
	require.EqualValues(t, 100, task.GetSwitchTimeTick())
	require.EqualValues(t, 110, task.GetTargets()[0].GetGenesisTimeTick())
	require.EqualValues(t, 120, task.GetTargets()[1].GetGenesisTimeTick())
	require.Equal(t, map[string]uint64{testTarget1: 100, testTarget2: 100}, store.TransformPins(testCollection))
	require.True(t, store.IsL0CompactionSuspended(testTarget1))
}

func TestCallbackIsIdempotentAtEachStep(t *testing.T) {
	ctx := context.Background()
	result := splitResult(7, [3]uint64{100, 110, 120})

	t.Run("the DataView manager is not ready", func(t *testing.T) {
		catalog := newFakeCatalog()
		callback := &Callback{Store: mustStore(t, catalog), DataView: func() ShardDeclarer { return nil }}
		require.ErrorIs(t, callback.OnAcked(ctx, result), merr.ErrServiceNotReady)
		require.Empty(t, catalog.writes())
	})

	t.Run("the declaration fails: nothing is recorded, the retry does both", func(t *testing.T) {
		catalog := newFakeCatalog()
		store := mustStore(t, catalog)
		declarer := &recordingDeclarer{catalog: catalog, err: errors.New("mock")}
		callback := &Callback{Store: store, DataView: func() ShardDeclarer { return declarer }}
		require.Error(t, callback.OnAcked(ctx, result))
		require.Nil(t, store.Get(7))
		require.Empty(t, catalog.writes())

		declarer.err = nil
		require.NoError(t, callback.OnAcked(ctx, result))
		require.Equal(t, []string{"declare", "task:SplitShardTaskWindowOpen"}, catalog.writes())
	})

	t.Run("the record fails after the declaration: the retry declares again and records", func(t *testing.T) {
		catalog := newFakeCatalog()
		store := mustStore(t, catalog)
		declarer := &recordingDeclarer{catalog: catalog}
		callback := &Callback{Store: store, DataView: func() ShardDeclarer { return declarer }}
		catalog.saveErr = errors.New("mock")
		require.Error(t, callback.OnAcked(ctx, result))
		require.Equal(t, []string{"declare"}, catalog.writes())
		require.Nil(t, store.Get(7))

		catalog.saveErr = nil
		require.NoError(t, callback.OnAcked(ctx, result))
		require.Equal(t, []string{"declare", "declare", "task:SplitShardTaskWindowOpen"}, catalog.writes())
	})

	t.Run("the whole callback runs again with other ticks: the record and the declared frontier do not change", func(t *testing.T) {
		catalog := newFakeCatalog()
		store := mustStore(t, catalog)
		declarer := &recordingDeclarer{catalog: catalog}
		callback := &Callback{Store: store, DataView: func() ShardDeclarer { return declarer }}
		require.NoError(t, callback.OnAcked(ctx, result))
		first := store.Get(7)

		require.NoError(t, callback.OnAcked(ctx, splitResult(7, [3]uint64{500, 510, 520})))
		require.Equal(t, first, store.Get(7))
		require.Equal(t, []string{"declare", "task:SplitShardTaskWindowOpen", "declare"}, catalog.writes())
		require.Equal(t, declarer.events[0], declarer.events[1])
	})

	t.Run("a result without the ticks it needs is refused", func(t *testing.T) {
		catalog := newFakeCatalog()
		declarer := &recordingDeclarer{catalog: catalog}
		callback := &Callback{Store: mustStore(t, catalog), DataView: func() ShardDeclarer { return declarer }}
		noSource := splitResult(7, [3]uint64{100, 110, 120})
		delete(noSource.Results, testSource)
		require.Error(t, callback.OnAcked(ctx, noSource))
		noTarget := splitResult(7, [3]uint64{100, 110, 0})
		require.Error(t, callback.OnAcked(ctx, noTarget))
		require.Empty(t, catalog.writes())
	})
}

func newDataViewWithSource(t *testing.T, catalog *fakeCatalog, store *Store) dataview.Manager {
	manager := dataview.NewManager(catalog, nil, dataview.WithPendingTransformProjector(
		func(_ context.Context, view *viewpb.DataViewOfCollection) (map[string]uint64, error) {
			return store.TransformPins(view.GetCollectionId()), nil
		}))
	_, err := manager.OnCreateCollection(context.Background(), dataview.CreateCollectionDataViewEvent{
		CollectionID: testCollection, VChannels: []string{testSource}, TransformStarts: map[string]uint64{testSource: 50},
	})
	require.NoError(t, err)
	return manager
}

func shardOf(t *testing.T, manager dataview.Manager, vchannel string) *viewpb.DataViewOfShard {
	ref, err := manager.Latest(context.Background(), testCollection)
	require.NoError(t, err)
	defer ref.Deref()
	for _, shard := range ref.DataView().GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard
		}
	}
	return nil
}

// A cluster that received the message through replication never ran the
// trigger. Its callback, with the ticks of its own WAL, leaves the record and
// the DataView the issuing cluster has, except for the ticks.
func TestCallbackOnAClusterThatNeverRanTheTrigger(t *testing.T) {
	ctx := context.Background()
	run := func(ticks [3]uint64) (*datapb.SplitShardTask, dataview.Manager) {
		catalog := newFakeCatalog()
		store := mustStore(t, catalog)
		manager := newDataViewWithSource(t, catalog, store)
		callback := &Callback{Store: store, DataView: func() ShardDeclarer { return manager }}
		require.NoError(t, callback.OnAcked(ctx, splitResult(7, ticks)))
		return store.Get(7), manager
	}
	primary, primaryView := run([3]uint64{100, 110, 120})
	secondary, secondaryView := run([3]uint64{900, 910, 920})

	require.Equal(t, primary.GetState(), secondary.GetState())
	require.Equal(t, primary.GetSourceVchannel(), secondary.GetSourceVchannel())
	require.Equal(t, targetVChannels(primary), targetVChannels(secondary))
	require.EqualValues(t, 900, secondary.GetSwitchTimeTick())
	for _, vchannel := range []string{testSource, testTarget1, testTarget2} {
		require.NotNil(t, shardOf(t, primaryView, vchannel))
		require.NotNil(t, shardOf(t, secondaryView, vchannel))
	}
	require.EqualValues(t, 100, shardOf(t, primaryView, testTarget1).GetGrowingTransformStartAfterTimetick())
	require.EqualValues(t, 900, shardOf(t, secondaryView, testTarget1).GetGrowingTransformStartAfterTimetick())
}

// A coordinator that restarts inside the window reloads the split from the
// catalog and the shards from the DataView, and the callback entered again
// after the restart changes neither.
func TestCallbackRestartInsideTheWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	manager := newDataViewWithSource(t, catalog, store)
	callback := &Callback{Store: store, DataView: func() ShardDeclarer { return manager }}
	result := splitResult(7, [3]uint64{100, 110, 120})
	require.NoError(t, callback.OnAcked(ctx, result))

	restarted := mustStore(t, catalog)
	recovered, err := dataview.RecoverManager(ctx, catalog,
		func(context.Context, int64) (bool, error) { return true, nil },
		func(context.Context, int64, *viewpb.DataViewOfCollection) ([]dataview.LoadableSegment, error) {
			return nil, nil
		},
		[]int64{testCollection}, map[int64][]string{testCollection: {testTarget1, testTarget2}},
		dataview.WithPendingTransformProjector(func(_ context.Context, view *viewpb.DataViewOfCollection) (map[string]uint64, error) {
			return restarted.TransformPins(view.GetCollectionId()), nil
		}))
	require.NoError(t, err)
	require.True(t, proto.Equal(store.Get(7), restarted.Get(7)))
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskWindowOpen, restarted.Get(7).GetState())
	require.True(t, restarted.IsL0CompactionSuspended(testTarget1))
	// The source is still a shard of the DataView, next to the targets.
	for _, vchannel := range []string{testSource, testTarget1, testTarget2} {
		require.NotNil(t, shardOf(t, recovered, vchannel))
	}

	catalog.resetLog()
	again := &Callback{Store: restarted, DataView: func() ShardDeclarer { return recovered }}
	require.NoError(t, again.OnAcked(ctx, splitResult(7, [3]uint64{300, 310, 320})))
	require.Empty(t, catalog.writes())
	require.EqualValues(t, 100, restarted.Get(7).GetSwitchTimeTick())
	require.EqualValues(t, 100, shardOf(t, recovered, testTarget1).GetGrowingTransformStartAfterTimetick())
}

// A flush on a target that arrives while the callback is between its steps,
// or after it, finds the shard of the target, and the Transform frontier of
// the target stays at T_switch for as long as the split is not adopted.
func TestCallbackFlushOnATargetFindsItsShard(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	manager := newDataViewWithSource(t, catalog, store)
	callback := &Callback{Store: store, DataView: func() ShardDeclarer { return manager }}
	require.NoError(t, callback.OnAcked(ctx, splitResult(7, [3]uint64{100, 110, 120})))

	flush := func(segmentID int64, start uint64, certificate uint64) {
		view, commit, _, err := manager.PrepareFlush(ctx, dataview.FlushDataViewEvent{
			CollectionID: testCollection,
			Segments: []dataview.LoadableSegment{{
				SegmentID: segmentID, VChannel: testTarget1, PartitionID: 1, ManifestVersion: 1, TransformStartAfterTimetick: start,
			}},
			GrowingTransformStarts: map[string]uint64{testTarget1: certificate},
		})
		require.NoError(t, err)
		require.NoError(t, catalog.SaveDataView(ctx, view))
		commit()
	}
	flush(20, 130, 150)
	target := shardOf(t, manager, testTarget1)
	require.Equal(t, []int64{20}, target.GetPartitions()[0].GetSegmentIds())
	require.EqualValues(t, 100, target.GetGrowingTransformStartAfterTimetick())
	require.EqualValues(t, 130, target.GetCompactedTransformStartAfterTimetick())

	// After adoption, g2 publication and history folding release the pin.
	require.NoError(t, store.MarkSourceAcked(ctx, 7, testCollection, testSource, nil))
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
	require.NoError(t, store.ResumeL0(ctx, 7))
	require.NoError(t, store.ReleasePins(ctx, 7))
	flush(21, 160, 200)
	require.EqualValues(t, 200, shardOf(t, manager, testTarget1).GetGrowingTransformStartAfterTimetick())
}

func TestCallbackHoldsUntilTheResidualHasFlushed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	declarer := &recordingDeclarer{catalog: catalog}
	callback := &Callback{Store: store, DataView: func() ShardDeclarer { return declarer }}
	result := splitResult(7, [3]uint64{100, 110, 120})

	// A split that was never recorded has nothing to wait for.
	require.NoError(t, callback.OnApplied(ctx, result))

	require.NoError(t, callback.OnAcked(ctx, result))
	released := make(chan error, 1)
	go func() { released <- callback.OnApplied(ctx, result) }()
	select {
	case err := <-released:
		require.FailNow(t, "the callback returned before the residual has flushed", "%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// The copies on the targets and messages of other types do not release it.
	require.NoError(t, callback.OnSourceConsumed(ctx, consumedCopy(t, result, testTarget1)))
	require.False(t, store.Get(7).GetSourceAcked())
	select {
	case err := <-released:
		require.FailNow(t, "a copy on a target released the callback", "%v", err)
	case <-time.After(20 * time.Millisecond):
	}

	require.NoError(t, callback.OnSourceConsumed(ctx, consumedCopy(t, result, testSource)))
	select {
	case err := <-released:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the callback was not released by the acknowledgement of the source")
	}
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskResidualFlushed, store.Get(7).GetState())

	// A coordinator that restarts after the acknowledgement does not wait.
	restarted := &Callback{Store: mustStore(t, catalog), DataView: func() ShardDeclarer { return declarer }}
	require.NoError(t, restarted.OnAcked(ctx, result))
	require.NoError(t, restarted.OnApplied(ctx, result))
}

func TestCallbackWaitEndsWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	declarer := &recordingDeclarer{catalog: catalog}
	callback := &Callback{Store: store, DataView: func() ShardDeclarer { return declarer }}
	result := splitResult(7, [3]uint64{100, 110, 120})
	require.NoError(t, callback.OnAcked(ctx, result))
	released := make(chan error, 1)
	go func() { released <- callback.OnApplied(ctx, result) }()
	cancel()
	require.ErrorIs(t, <-released, context.Canceled)
}

// On a secondary the StreamingNode acknowledges the source before the ack
// callback runs: the callback records the split with its residual flushed and
// does not wait.
func TestCallbackSourceConsumedBeforeTheCallback(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	declarer := &recordingDeclarer{catalog: catalog}
	callback := &Callback{Store: store, DataView: func() ShardDeclarer { return declarer }}
	result := splitResult(7, [3]uint64{100, 110, 120})

	require.NoError(t, callback.OnSourceConsumed(ctx, consumedCopy(t, result, testSource)))
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPending, store.Get(7).GetState())
	require.NoError(t, callback.OnAcked(ctx, result))
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskResidualFlushed, store.Get(7).GetState())
	require.NoError(t, callback.OnApplied(ctx, result))

	catalog.saveErr = errors.New("mock")
	other := splitResult(8, [3]uint64{100, 110, 120})
	require.Error(t, callback.OnSourceConsumed(ctx, consumedCopy(t, other, testSource)))

	// A message of another type is not a split.
	flush := message.NewManualFlushMessageBuilderV2().
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: testCollection}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithVChannel(testSource).MustBuildMutable().
		WithTimeTick(1).WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(1))
	require.NoError(t, callback.OnSourceConsumed(ctx, flush))
}

// A pending record names its targets but pins nothing: its targets are not
// flushable until the callback records the split. Its source is.
func TestStoreFlushableVChannelsNeedThePin(t *testing.T) {
	ctx := context.Background()
	store := mustStore(t, newFakeCatalog())
	require.Empty(t, store.FlushableVChannels(testCollection))

	require.NoError(t, store.MarkSourceAcked(ctx, 7, testCollection, testSource, []string{testTarget1, testTarget2}))
	require.Equal(t, []string{testSource}, store.FlushableVChannels(testCollection))
	// The placement of new targets still keeps away from them.
	require.ElementsMatch(t, []string{testSource, testTarget1, testTarget2}, store.VChannels(testCollection))
	require.Empty(t, store.TransformPins(testCollection))

	_, err := store.Record(ctx, testRecord(7))
	require.NoError(t, err)
	require.Equal(t, []string{testSource, testTarget1, testTarget2}, store.FlushableVChannels(testCollection))
	require.NotEmpty(t, store.TransformPins(testCollection))
	require.Empty(t, store.FlushableVChannels(testCollection+1))
}

// A callback that waits for the residual of its source is visible: a gauge
// of its age from the record of the split, removed when the wait ends, and an
// error log once the wait passes the threshold.
func TestCallbackWaitForTheResidualIsVisible(t *testing.T) {
	ctx := context.Background()
	interval := residualWaitLogInterval
	residualWaitLogInterval = 5 * time.Millisecond
	t.Cleanup(func() { residualWaitLogInterval = interval })
	threshold := paramtable.Get().DataCoordCfg.ShardSplitResidualWaitAlertThreshold.Key
	paramtable.Get().Save(threshold, "1000000")
	t.Cleanup(func() { paramtable.Get().Reset(threshold) })

	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	declarer := &recordingDeclarer{catalog: catalog}
	callback := &Callback{Store: store, DataView: func() ShardDeclarer { return declarer }}
	result := splitResult(7, [3]uint64{100, 110, 120})
	require.NoError(t, callback.OnAcked(ctx, result))
	// Recorded an hour ago, before a coordinator restart.
	store.mu.Lock()
	store.tasks[7].RecordTime = time.Now().Add(-time.Hour).Unix()
	store.mu.Unlock()

	label := strconv.FormatInt(testCollection, 10)
	released := make(chan error, 1)
	go func() { released <- callback.OnApplied(ctx, result) }()
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.DataCoordShardSplitResidualWaitSeconds.WithLabelValues(label)) >= 3600
	}, 5*time.Second, time.Millisecond)
	// Past the threshold: logged as an error, still waiting.
	paramtable.Get().Save(threshold, "60")
	time.Sleep(4 * residualWaitLogInterval)
	select {
	case err := <-released:
		require.FailNow(t, "the callback returned before the residual has flushed", "%v", err)
	default:
	}

	require.NoError(t, callback.OnSourceConsumed(ctx, consumedCopy(t, result, testSource)))
	require.NoError(t, <-released)
	// The series is gone with the wait.
	require.False(t, metrics.DataCoordShardSplitResidualWaitSeconds.DeleteLabelValues(label))
}

// A callback that arrives after the DataViews of its collection were dropped
// records nothing: nothing would remove the record later.
func TestCallbackAfterTheDataViewIsDroppedRecordsNothing(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store := mustStore(t, catalog)
	manager := newDataViewWithSource(t, catalog, store)
	_, err := manager.OnDropCollection(ctx, testCollection)
	require.NoError(t, err)
	callback := &Callback{Store: store, DataView: func() ShardDeclarer { return manager }}
	result := splitResult(7, [3]uint64{100, 110, 120})
	require.NoError(t, callback.OnAcked(ctx, result))
	require.Nil(t, store.Get(7))
	require.Empty(t, catalog.tasks)
	// Nothing to wait for either.
	require.NoError(t, callback.OnApplied(ctx, result))
}

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

package datacoord

import (
	"context"
	"math"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/bulkimport"
	"github.com/milvus-io/milvus/internal/datacoord/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/ddl"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/segment"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	types2 "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	splitTestSource  = "by-dev-rootcoord-dml_0_100v0"
	splitTestTarget1 = "by-dev-rootcoord-dml_1_100v1"
	splitTestTarget2 = "by-dev-rootcoord-dml_2_100v2"
)

// splitTestCatalog keeps split records and DataViews in memory.
type splitTestCatalog struct {
	tasks map[int64]*datapb.SplitShardTask
	views []*viewpb.DataViewOfCollection
}

func (c *splitTestCatalog) SaveSplitShardTask(_ context.Context, task *datapb.SplitShardTask) error {
	c.tasks[task.GetTaskId()] = proto.Clone(task).(*datapb.SplitShardTask)
	return nil
}

func (c *splitTestCatalog) ListSplitShardTask(context.Context) ([]*datapb.SplitShardTask, error) {
	tasks := make([]*datapb.SplitShardTask, 0, len(c.tasks))
	for _, task := range c.tasks {
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (c *splitTestCatalog) DropSplitShardTask(_ context.Context, task *datapb.SplitShardTask) error {
	delete(c.tasks, task.GetTaskId())
	return nil
}

func (c *splitTestCatalog) SaveDataView(_ context.Context, view *viewpb.DataViewOfCollection) error {
	c.views = append(c.views, view)
	return nil
}

func (c *splitTestCatalog) ListAllDataViews(context.Context) ([]*viewpb.DataViewOfCollection, error) {
	return c.views, nil
}

func (c *splitTestCatalog) DropDataView(context.Context, *viewpb.DataViewOfCollection) error {
	return nil
}

func (c *splitTestCatalog) DropDataViews(context.Context, int64) error { return nil }

func newSplitTestStore(t *testing.T) (*split.Store, *splitTestCatalog) {
	catalog := &splitTestCatalog{tasks: make(map[int64]*datapb.SplitShardTask)}
	store, err := split.NewStore(context.Background(), catalog)
	require.NoError(t, err)
	return store, catalog
}

func splitTestRecord(taskID int64) split.Record {
	return split.Record{
		TaskID:       taskID,
		CollectionID: 100,
		Source:       splitTestSource,
		Targets: []split.Target{
			{VChannel: splitTestTarget1, GenesisTimeTick: 110},
			{VChannel: splitTestTarget2, GenesisTimeTick: 120},
		},
		SwitchTimeTick: 100,
	}
}

// splitTestMessage builds the broadcast of a split of collection 100 and its
// result, with the source at tick 100 and the targets at 110 and 120.
func splitTestMessage(taskID int64) (message.BroadcastMutableMessage, message.BroadcastResultSplitShardMessageV2) {
	vchannels := []string{splitTestSource, splitTestTarget1, splitTestTarget2}
	msg := message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId: 100, SplitTaskId: taskID, SourceVchannel: splitTestSource,
			TargetVchannels: []string{splitTestTarget1, splitTestTarget2},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		WithBroadcast(vchannels, message.OptBuildBroadcastAppendFirst(splitTestSource)).
		MustBuildBroadcast().WithBroadcastID(1)
	results := map[string]*message.AppendResult{}
	for idx, vchannel := range vchannels {
		tick := uint64(100 + 10*idx)
		results[vchannel] = &message.AppendResult{MessageID: rmq.NewRmqID(int64(tick)), LastConfirmedMessageID: rmq.NewRmqID(int64(tick)), TimeTick: tick}
	}
	return msg, message.BroadcastResultSplitShardMessageV2{Message: message.MustAsBroadcastSplitShardMessageV2(msg), Results: results}
}

func TestSplitShardHooksNeedRecoveredMetadata(t *testing.T) {
	ctx := context.Background()
	s := &Server{}
	msg, result := splitTestMessage(7)
	require.ErrorIs(t, s.SplitShardAckHook(ctx, result), merr.ErrServiceNotReady)
	require.ErrorIs(t, s.SplitShardAppliedHook(ctx, result), merr.ErrServiceNotReady)
	source := msg.SplitIntoMutableMessage()[0].WithTimeTick(100).WithLastConfirmed(rmq.NewRmqID(100)).IntoImmutableMessage(rmq.NewRmqID(100))
	require.ErrorIs(t, s.ObserveSplitShardConsumerAck(ctx, source), merr.ErrServiceNotReady)
	_, err := s.SplitShard(ctx, 100, splitTestSource)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)

	require.Nil(t, s.ShardSplitStore())

	// The records are recovered but the DataView manager is not.
	s.splitStore, _ = newSplitTestStore(t)
	require.Equal(t, s.splitStore, s.ShardSplitStore())
	require.ErrorIs(t, s.SplitShardAckHook(ctx, result), merr.ErrServiceNotReady)
	require.Nil(t, s.splitStore.Get(7))

	// Nothing refuses or pins while the records are not recovered.
	bare := &Server{}
	require.NoError(t, bare.checkSplitWindow(100, "Import"))
	require.NoError(t, bare.checkFlushOfUndeclaredShard(100, splitTestTarget1))
	require.NoError(t, bare.dropSplitSourcesOfCollection(ctx, 100))
	require.NoError(t, bare.truncateSplitSources(ctx, 100, nil))
}

// The three hooks, as the coordinators call them for one split on the cluster
// that issues it: declare and record, publish, then wait for the source.
func TestSplitShardHooksCarryASplitThroughItsCallback(t *testing.T) {
	ctx := context.Background()
	store, catalog := newSplitTestStore(t)
	s := &Server{splitStore: store}
	s.dataViewManager = dataview.NewManager(catalog, nil, dataview.WithPendingTransformProjector(
		func(_ context.Context, view *viewpb.DataViewOfCollection) (map[string]uint64, error) {
			return s.splitTransformPins(store, view, nil), nil
		}))
	_, err := s.dataViewManager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: 100, VChannels: []string{splitTestSource}, TransformStarts: map[string]uint64{splitTestSource: 50},
	})
	require.NoError(t, err)
	msg, result := splitTestMessage(7)

	require.NoError(t, s.SplitShardAckHook(ctx, result))
	shards, err := s.dataViewShards(ctx, 100)
	require.NoError(t, err)
	// The source stays a shard of the DataView next to the two targets.
	require.ElementsMatch(t, []string{splitTestSource, splitTestTarget1, splitTestTarget2}, shards)
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskWindowOpen, store.Get(7).GetState())

	released := make(chan error, 1)
	go func() { released <- s.SplitShardAppliedHook(ctx, result) }()
	copies := msg.SplitIntoMutableMessage()
	for _, mutable := range copies {
		id := result.Results[mutable.VChannel()].MessageID
		consumed := mutable.WithTimeTick(result.Results[mutable.VChannel()].TimeTick).WithLastConfirmed(id).IntoImmutableMessage(id)
		require.NoError(t, s.ObserveSplitShardConsumerAck(ctx, consumed))
	}
	require.NoError(t, <-released)
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskResidualFlushed, store.Get(7).GetState())

	_, err = s.dataViewShards(ctx, 999)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	_, err = (&Server{}).dataViewShards(ctx, 100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
}

func TestSplitShardIssuer(t *testing.T) {
	ctx := context.Background()
	s := &Server{}
	request := split.Request{CollectionID: 100, SplitTaskID: 7, Source: splitTestSource, Targets: [2]string{splitTestTarget1, splitTestTarget2}}
	require.ErrorIs(t, s.issueSplitShard(ctx, request, nil), merr.ErrServiceUnavailable)

	var issued []split.Request
	issueErr := errors.New("mock")
	admitErr := errors.New("refused")
	s.SetSplitShardIssuer(func(ctx context.Context, request split.Request, admit func(ctx context.Context) error) error {
		issued = append(issued, request)
		require.ErrorIs(t, admit(ctx), admitErr)
		return issueErr
	})
	require.ErrorIs(t, s.issueSplitShard(ctx, request, func(context.Context) error { return admitErr }), issueErr)
	require.Equal(t, []split.Request{request}, issued)
}

func TestSplitTransformPinsMergeIntoThePendingBounds(t *testing.T) {
	ctx := context.Background()
	store, _ := newSplitTestStore(t)
	s := &Server{splitStore: store}
	view := &viewpb.DataViewOfCollection{CollectionId: 100}

	// No split: the bounds pass through untouched, nil included.
	require.Nil(t, s.splitTransformPins(store, view, nil))
	bounds := map[string]uint64{"other": 5}
	require.Equal(t, bounds, s.splitTransformPins(store, view, bounds))

	_, err := store.Record(ctx, splitTestRecord(7))
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{splitTestTarget1: 100, splitTestTarget2: 100}, s.splitTransformPins(store, view, nil))
	// The lower of a pin and an existing bound wins.
	merged := s.splitTransformPins(store, view, map[string]uint64{splitTestTarget1: 90, splitTestTarget2: 300, "other": 5})
	require.Equal(t, map[string]uint64{splitTestTarget1: 90, splitTestTarget2: 100, "other": 5}, merged)
}

func TestSplitWindowRefusesSnapshotOnThePrimary(t *testing.T) {
	ctx := context.Background()
	store, _ := newSplitTestStore(t)
	s := &Server{splitStore: store}
	require.NoError(t, s.checkSplitWindow(100, "CreateSnapshot"))
	_, err := store.Record(ctx, splitTestRecord(7))
	require.NoError(t, err)

	refusal := s.checkSplitWindow(100, "CreateSnapshot")
	require.ErrorIs(t, refusal, merr.ErrOperationNotSupported)
	// The request is valid; the state of the collection refuses it.
	require.Equal(t, merr.SystemError, merr.GetErrorType(refusal))
	require.NoError(t, s.checkSplitWindow(101, "CreateSnapshot"))

	patch := mockey.Mock((*snapshot.Manager).GetSnapshot).
		Return(nil, merr.WrapErrSnapshotNotFound("snap", "not found")).Build()
	defer patch.UnPatch()
	collection := &meta.CollectionInfo{
		ID: 100, DatabaseName: "default", Schema: &schemapb.CollectionSchema{Name: "collection"},
		VChannelNames: []string{splitTestTarget1, splitTestTarget2},
	}
	patchCollection := mockey.Mock((*embeddedHandler).GetCollection).Return(collection, nil).Build()
	defer patchCollection.UnPatch()
	patchStart := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(&embeddedBroadcastAPI{}, nil).Build()
	defer patchStart.UnPatch()
	patchClose := mockey.Mock((*embeddedBroadcastAPI).Close).Return().Build()
	defer patchClose.UnPatch()
	broadcasts := 0
	patchBroadcast := mockey.Mock((*embeddedBroadcastAPI).Broadcast).To(
		func(*embeddedBroadcastAPI, context.Context, message.BroadcastMutableMessage) (*types2.BroadcastAppendResult, error) {
			broadcasts++
			return &types2.BroadcastAppendResult{}, nil
		}).Build()
	defer patchBroadcast.UnPatch()
	s.snapshotManager = snapshot.NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil, nil)
	s.handler = &embeddedHandler{}
	s.meta = meta.NewMetaWithCollections(collection)
	s.stateCode.Store(commonpb.StateCode_Healthy)

	resp, err := s.CreateSnapshot(ctx, &datapb.CreateSnapshotRequest{Name: "snap", CollectionId: 100})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp), merr.ErrOperationNotSupported)
	require.Zero(t, broadcasts)

	// After the adoption the snapshot is taken again.
	require.NoError(t, store.MarkSourceAcked(ctx, 7, 100, splitTestSource, nil))
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
	resp, err = s.CreateSnapshot(ctx, &datapb.CreateSnapshotRequest{Name: "snap", CollectionId: 100})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp))
	require.Equal(t, 1, broadcasts)
}

func TestCheckFlushOfUndeclaredShard(t *testing.T) {
	ctx := context.Background()
	store, _ := newSplitTestStore(t)
	s := &Server{splitStore: store}
	s.meta = meta.NewMetaWithCollections(
		&meta.CollectionInfo{ID: 100, VChannelNames: []string{splitTestSource}},
		&meta.CollectionInfo{ID: 101},
	)
	// A shard RootCoord lists, and collections DataCoord cannot judge.
	require.NoError(t, s.checkFlushOfUndeclaredShard(100, splitTestSource))
	require.NoError(t, s.checkFlushOfUndeclaredShard(101, "any"))
	require.NoError(t, s.checkFlushOfUndeclaredShard(999, "any"))

	// A target before its split is recorded: not ready, and retriable.
	refusal := s.checkFlushOfUndeclaredShard(100, splitTestTarget1)
	require.ErrorIs(t, refusal, merr.ErrServiceNotReady)
	require.Equal(t, merr.SystemError, merr.GetErrorType(refusal))

	// Recorded: the targets are known, and so is the source once RootCoord has
	// stopped listing it.
	_, err := store.Record(ctx, splitTestRecord(7))
	require.NoError(t, err)
	require.NoError(t, s.checkFlushOfUndeclaredShard(100, splitTestTarget1))
	s.meta.AddCollection(&meta.CollectionInfo{ID: 100, VChannelNames: []string{splitTestTarget1, splitTestTarget2}})
	require.NoError(t, s.checkFlushOfUndeclaredShard(100, splitTestSource))
	require.ErrorIs(t, s.checkFlushOfUndeclaredShard(100, "by-dev-rootcoord-dml_9_100v9"), merr.ErrServiceNotReady)
}

func addSplitSegment(t *testing.T, mt *meta.Meta, id int64, vchannel string, state commonpb.SegmentState, level datapb.SegmentLevel, rows int64, ts uint64) {
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: id, CollectionID: 100, PartitionID: 10, InsertChannel: vchannel,
		State: state, Level: level, NumOfRows: rows, StorageVersion: storage.StorageV2,
		DmlPosition: &msgpb.MsgPosition{Timestamp: ts},
	})))
}

// A final flush of a segment of a target is refused until the callback has
// recorded the split: nothing is published, the segment stays growing, and
// the StreamingNode sends the flush again.
func TestSplitTargetFlushWaitsForTheCallback(t *testing.T) {
	ctx := context.Background()
	store, catalog := newSplitTestStore(t)
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: 100, VChannelNames: []string{splitTestSource}})
	s := &Server{splitStore: store, meta: mt}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	s.dataViewManager = dataview.NewManager(catalog, nil)
	_, err = s.dataViewManager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: 100, VChannels: []string{splitTestSource}, TransformStarts: map[string]uint64{splitTestSource: 50},
	})
	require.NoError(t, err)
	addSplitSegment(t, mt, 30, splitTestTarget1, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1, 10, 0)

	req := &datapb.SaveBinlogPathsRequest{
		SegmentID: 30, CollectionID: 100, PartitionID: 10,
		Flushed: true, SegLevel: datapb.SegmentLevel_L1, WithFullBinlogs: true, StorageVersion: storage.StorageV2,
		Field2BinlogPaths:                  []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 30, EntriesNum: 10}}}},
		GrowingTransformStartAfterTimetick: 150,
	}
	resp, err := s.SaveBinlogPaths(ctx, req)
	refusal := merr.CheckRPCCall(resp, err)
	require.ErrorIs(t, refusal, merr.ErrServiceNotReady)
	require.Equal(t, merr.SystemError, merr.GetErrorType(refusal))
	shards, err := s.dataViewShards(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, []string{splitTestSource}, shards)
	require.Equal(t, commonpb.SegmentState_Growing, mt.GetSegment(ctx, 30).GetState())

	// Declared but not recorded yet, as after a restart between the two steps
	// of the callback: still refused.
	_, err = s.dataViewManager.DeclareShards(ctx, dataview.DeclareShardsDataViewEvent{
		CollectionID: 100, VChannels: []string{splitTestTarget1}, TransformStarts: map[string]uint64{splitTestTarget1: 100},
	})
	require.NoError(t, err)
	resp, err = s.SaveBinlogPaths(ctx, req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrServiceNotReady)

	// Once the callback has recorded the split the check lets the flush in.
	_, result := splitTestMessage(7)
	require.NoError(t, s.SplitShardAckHook(ctx, result))
	require.NoError(t, s.checkFlushOfUndeclaredShard(100, splitTestTarget1))
}

// The sequence of a secondary: the StreamingNode acknowledges the copy on the
// source before the ack callback runs, so a pending record names the targets
// while nothing pins their frontier and they are not declared. A replicated
// flush on a target in that interval is refused; after the callback it is
// accepted and the frontier of the target stays at the pin.
func TestSplitTargetFlushOnASecondaryWaitsForThePin(t *testing.T) {
	ctx := context.Background()
	store, catalog := newSplitTestStore(t)
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: 100, VChannelNames: []string{splitTestSource}})
	segmentManager := segment.NewMockManager(t)
	trigger := compaction.NewMockTrigger(t)
	trigger.EXPECT().TriggerCompaction(mock.Anything, mock.Anything).Return(0, nil).Maybe()
	s := &Server{
		splitStore: store, meta: mt, segmentManager: segmentManager,
		compactionTrigger: trigger,
		flushCh:           make(chan UniqueID, 10),
	}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	s.dataViewManager = dataview.NewManager(catalog, nil, dataview.WithPendingTransformProjector(
		func(_ context.Context, view *viewpb.DataViewOfCollection) (map[string]uint64, error) {
			return s.splitTransformPins(store, view, nil), nil
		}))
	_, err = s.dataViewManager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: 100, VChannels: []string{splitTestSource}, TransformStarts: map[string]uint64{splitTestSource: 50},
	})
	require.NoError(t, err)

	// 1. The source copy is acknowledged first: a pending record.
	msg, result := splitTestMessage(7)
	var sourceCopy message.MutableMessage
	for _, mutable := range msg.SplitIntoMutableMessage() {
		if mutable.VChannel() == splitTestSource {
			sourceCopy = mutable
		}
	}
	require.NotNil(t, sourceCopy)
	id := result.Results[splitTestSource].MessageID
	require.NoError(t, s.ObserveSplitShardConsumerAck(ctx, sourceCopy.WithTimeTick(100).WithLastConfirmed(id).IntoImmutableMessage(id)))
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPending, store.Get(7).GetState())

	// 2. A replicated flush of a target segment: refused, nothing changes.
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 30, CollectionID: 100, PartitionID: 10, InsertChannel: splitTestTarget1,
		State: commonpb.SegmentState_Growing, Level: datapb.SegmentLevel_L1, NumOfRows: 10, StorageVersion: storage.StorageV2,
		TransformStartAfterTimetick: 120,
	})))
	req := &datapb.SaveBinlogPathsRequest{
		SegmentID: 30, CollectionID: 100, PartitionID: 10,
		Flushed: true, SegLevel: datapb.SegmentLevel_L1, WithFullBinlogs: true, StorageVersion: storage.StorageV2,
		Field2BinlogPaths:                  []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 30, EntriesNum: 10}}}},
		GrowingTransformStartAfterTimetick: 150,
		CheckPoints: []*datapb.CheckPoint{{
			SegmentID: 30, NumOfRows: 10,
			Position: &msgpb.MsgPosition{ChannelName: splitTestTarget1, Timestamp: 150, MsgID: []byte{1}},
		}},
	}
	resp, err := s.SaveBinlogPaths(ctx, req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrServiceNotReady)
	require.Equal(t, commonpb.SegmentState_Growing, mt.GetSegment(ctx, 30).GetState())
	shards, err := s.dataViewShards(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, []string{splitTestSource}, shards)

	// 3. The ack callback: declared, recorded, pinned. The flush is accepted
	// and the frontier of the target stays at T_switch.
	require.NoError(t, s.SplitShardAckHook(ctx, result))
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskResidualFlushed, store.Get(7).GetState())
	resp, err = s.SaveBinlogPaths(ctx, req)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, commonpb.SegmentState_Flushed, mt.GetSegment(ctx, 30).GetState())
	ref, err := s.dataViewManager.Latest(ctx, 100)
	require.NoError(t, err)
	defer ref.Deref()
	var target *viewpb.DataViewOfShard
	for _, shard := range ref.DataView().GetShards() {
		if shard.GetVchannel() == splitTestTarget1 {
			target = shard
		}
	}
	require.NotNil(t, target)
	require.EqualValues(t, 100, target.GetGrowingTransformStartAfterTimetick())
}

// DropCollection and Truncate are broadcast to the shards RootCoord lists.
// The split source is not one of them and is handled with the collection.
func TestSplitSourceIsDroppedAndTruncatedWithItsCollection(t *testing.T) {
	ctx := context.Background()
	store, catalog := newSplitTestStore(t)
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	segmentManager := segment.NewMockManager(t)
	inspector := compaction.NewMockCompactionInspector(t)
	inspector.EXPECT().RemoveTasksByChannel(splitTestSource).Return().Once()
	s := &Server{splitStore: store, meta: mt, segmentManager: segmentManager, compactionInspector: inspector}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	s.dataViewManager = dataview.NewManager(catalog, nil)

	_, err = store.Record(ctx, splitTestRecord(7))
	require.NoError(t, err)
	addSplitSegment(t, mt, 40, splitTestSource, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1, 10, 90)
	addSplitSegment(t, mt, 41, splitTestSource, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0, 1, 95)
	addSplitSegment(t, mt, 42, splitTestTarget1, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1, 10, 150)
	addSplitSegment(t, mt, 43, splitTestTarget1, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1, 10, 250)

	// The trigger reads the size of a shard from its data segments.
	_, rows := s.splitShardStats(splitTestSource)
	require.EqualValues(t, 10, rows)

	// Truncate at 200 on the targets: the source goes completely, the target
	// up to the flush time tick.
	require.NoError(t, s.DropSegmentsByTime(ctx, 100, map[string]uint64{splitTestTarget1: 200, splitTestTarget2: 200}))
	for id, state := range map[int64]commonpb.SegmentState{
		40: commonpb.SegmentState_Dropped, 41: commonpb.SegmentState_Dropped,
		42: commonpb.SegmentState_Dropped, 43: commonpb.SegmentState_Flushed,
	} {
		require.Equal(t, state, mt.GetSegment(ctx, id).GetState(), "segment %d", id)
	}

	// A source that is adopted has no segment of its own left to truncate, and
	// one the truncate names itself is left to the ordinary path.
	addSplitSegment(t, mt, 44, splitTestSource, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1, 10, 90)
	require.NoError(t, s.truncateSplitSources(ctx, 100, map[string]uint64{splitTestSource: 1}))
	require.Equal(t, commonpb.SegmentState_Flushed, mt.GetSegment(ctx, 44).GetState())
	require.NoError(t, store.MarkSourceAcked(ctx, 7, 100, splitTestSource, nil))
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
	require.NoError(t, s.truncateSplitSources(ctx, 100, map[string]uint64{splitTestTarget1: math.MaxUint64}))
	require.Equal(t, commonpb.SegmentState_Flushed, mt.GetSegment(ctx, 44).GetState())

	// Drop the collection: the source is not retired, so its data is dropped
	// and its channel checkpoint is marked dropped; the records go.
	require.NoError(t, s.DropCollectionDataView(ctx, 100))
	require.Equal(t, commonpb.SegmentState_Dropped, mt.GetSegment(ctx, 44).GetState())
	require.True(t, funcutil.IsDroppedChannelCheckpoint(mt.GetChannelCheckpoint(splitTestSource)))
	require.Empty(t, store.List())
	// Dropped again by a retried callback: nothing left to do.
	require.NoError(t, s.DropCollectionDataView(ctx, 100))
}

func TestSplitManagerOverTheServer(t *testing.T) {
	ctx := context.Background()
	store, _ := newSplitTestStore(t)
	s := &Server{splitStore: store, allocator: allocator.NewMockAllocator(t)}
	s.meta = meta.NewMetaWithCollections(
		&meta.CollectionInfo{ID: 100, VChannelNames: []string{splitTestSource}, Schema: &schemapb.CollectionSchema{Name: "c"}},
		&meta.CollectionInfo{ID: 101, State: etcdpb.CollectionState_CollectionDropping},
	)
	manager := s.newSplitManager(ctx)
	defer manager.Stop()
	s.splitManager = manager

	// The trigger is off by default: a round issues nothing.
	require.Empty(t, manager.TriggerOnce())

	// A direct request goes through the dependencies of the server. Without a
	// cluster behind it, it stops at the first one it cannot reach.
	roleErr := errors.New("mock")
	patch := mockey.Mock(ddl.GetReplicationRole).Return(replicateutil.RolePrimary, false, roleErr).Build()
	_, err := s.SplitShard(ctx, 100, splitTestSource)
	require.ErrorIs(t, err, roleErr)
	patch.UnPatch()

	// On a primary the gates refuse first: the read path of this build does
	// not serve a split source.
	patch = mockey.Mock(ddl.GetReplicationRole).Return(replicateutil.RolePrimary, false, nil).Build()
	defer patch.UnPatch()
	_, err = s.SplitShard(ctx, 100, splitTestSource)
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	require.ErrorContains(t, err, "read path")

	// With the gates open, every registered node at the declared release: a
	// collection that is being dropped is not found, and the live one is
	// refused for its unfinished import.
	split.SetReadPathReady(true)
	split.SetMinimalNodeVersion(semver.MustParse("3.1.0"))
	defer func() {
		split.SetReadPathReady(false)
		split.SetMinimalNodeVersion(semver.Version{})
	}()
	session := sessionutil.NewMockSession(t)
	session.EXPECT().GetSessions(mock.Anything, mock.Anything).Return(map[string]*sessionutil.Session{
		"n-1": {SessionRaw: sessionutil.SessionRaw{ServerID: 1}, Version: semver.MustParse("3.1.0")},
	}, 0, nil)
	s.session = session
	_, err = s.SplitShard(ctx, 101, splitTestSource)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	importMeta := bulkimport.NewMockImportMeta(t)
	importMeta.EXPECT().GetJobBy(mock.Anything, mock.Anything, mock.Anything).Return([]bulkimport.ImportJob{nil})
	s.importMeta = importMeta
	_, err = s.SplitShard(ctx, 100, splitTestSource)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.ErrorContains(t, err, "import")
}

// Every dependency the server gives the trigger, read over a server without a
// cluster behind it.
func TestSplitDependenciesOfTheServer(t *testing.T) {
	ctx := context.Background()
	store, _ := newSplitTestStore(t)
	s := &Server{splitStore: store, allocator: allocator.NewMockAllocator(t)}
	schemaProperties := []*commonpb.KeyValuePair{{Key: "k", Value: "v"}}
	s.meta = meta.NewMetaWithCollections(
		&meta.CollectionInfo{
			ID: 100, VChannelNames: []string{splitTestSource},
			Schema: &schemapb.CollectionSchema{Name: "c", EnableNamespace: true, Properties: schemaProperties},
		},
		&meta.CollectionInfo{ID: 101, State: etcdpb.CollectionState_CollectionDropping},
	)
	deps := s.splitDependencies()

	collections := deps.Collections()
	require.Len(t, collections, 1)
	require.EqualValues(t, 100, collections[0].ID)
	require.Equal(t, []string{splitTestSource}, collections[0].VChannels)
	require.True(t, collections[0].EnableNamespace)
	require.Equal(t, schemaProperties, collections[0].SchemaProperties)
	_, ok := deps.Collection(101)
	require.False(t, ok)
	collection, ok := deps.Collection(100)
	require.True(t, ok)
	require.EqualValues(t, 100, collection.ID)

	patch := mockey.Mock(ddl.GetReplicationRole).Return(replicateutil.RoleSecondary, true, nil).Build()
	primary, err := deps.IsPrimary(ctx)
	require.NoError(t, err)
	require.False(t, primary)
	replicating, err := deps.IsReplicating(ctx)
	require.NoError(t, err)
	require.True(t, replicating)
	patch.UnPatch()

	importMeta := bulkimport.NewMockImportMeta(t)
	importMeta.EXPECT().GetJobBy(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.importMeta = importMeta
	require.False(t, deps.HasPendingImport(ctx, 100))

	copies, _ := copysegment.NewCopySegmentTaskTestMeta(t, copysegment.CreateTestCopyTask(100, 2001).(*copysegment.Task))
	require.NoError(t, copies.AddJob(ctx, copysegment.NewTestCopyJob(1, datapb.CopySegmentJobState_CopySegmentJobExecuting)))
	s.copySegmentMeta = copies
	require.True(t, deps.HasPendingRestore(ctx, 100))
	require.False(t, deps.HasPendingRestore(ctx, 101))
	require.NoError(t, copies.UpdateJob(ctx, 1, copysegment.UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted)))
	require.False(t, deps.HasPendingRestore(ctx, 100))

	manager := s.newSplitManager(ctx)
	require.NotNil(t, manager)
	manager.Stop()
}

func TestSplitNodeVersions(t *testing.T) {
	ctx := context.Background()
	s := &Server{}
	_, err := s.splitNodeVersions(ctx)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)

	session := sessionutil.NewMockSession(t)
	session.EXPECT().GetSessions(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, role string) (map[string]*sessionutil.Session, int64, error) {
			switch role {
			case typeutil.StreamingNodeRole:
				return map[string]*sessionutil.Session{
					"streamingnode-2": {SessionRaw: sessionutil.SessionRaw{ServerID: 2}, Version: semver.MustParse("3.0.0")},
					"streamingnode-1": {SessionRaw: sessionutil.SessionRaw{ServerID: 1}, Version: semver.MustParse("3.1.0")},
				}, 0, nil
			case typeutil.ProxyRole:
				return map[string]*sessionutil.Session{
					"proxy-3": {SessionRaw: sessionutil.SessionRaw{ServerID: 3}, Version: semver.MustParse("3.1.0")},
				}, 0, nil
			}
			return nil, 0, nil
		})
	s.session = session
	nodes, err := s.splitNodeVersions(ctx)
	require.NoError(t, err)
	require.Equal(t, []split.NodeVersion{
		{Role: typeutil.StreamingNodeRole, ServerID: 1, Version: semver.MustParse("3.1.0")},
		{Role: typeutil.StreamingNodeRole, ServerID: 2, Version: semver.MustParse("3.0.0")},
		{Role: typeutil.ProxyRole, ServerID: 3, Version: semver.MustParse("3.1.0")},
	}, nodes)

	listErr := errors.New("etcd")
	failing := sessionutil.NewMockSession(t)
	failing.EXPECT().GetSessions(mock.Anything, mock.Anything).Return(nil, 0, listErr)
	s.session = failing
	_, err = s.splitNodeVersions(ctx)
	require.ErrorIs(t, err, listErr)
}

func TestAvailablePChannels(t *testing.T) {
	ctx := context.Background()
	patchCluster := mockey.Mock(channel.GetClusterChannels).Return(message.ClusterChannels{Channels: []string{"p0", "p1", "p2"}}).Build()
	defer patchCluster.UnPatch()

	// Partial allocation returns both ready channels in one snapshot, without
	// retrying once per unavailable channel.
	patchAlloc := mockey.Mock((*snmanager.StreamingNodeManager).AllocVirtualChannels).To(
		func(_ *snmanager.StreamingNodeManager, _ context.Context, param balancer.AllocVChannelParam) ([]string, error) {
			require.True(t, param.RequireReady)
			require.True(t, param.AllowPartial)
			require.Equal(t, 3, param.Num)
			return []string{"p2_0v0", "p0_0v1"}, nil
		}).Build()
	pchannels, err := availablePChannels(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"p2", "p0"}, pchannels)
	patchAlloc.UnPatch()

	allocErr := errors.New("balancer is closed")
	patchAlloc = mockey.Mock((*snmanager.StreamingNodeManager).AllocVirtualChannels).Return(nil, allocErr).Build()
	defer patchAlloc.UnPatch()
	_, err = availablePChannels(ctx)
	require.ErrorIs(t, err, allocErr)

	patchCluster.UnPatch()
	patchEmpty := mockey.Mock(channel.GetClusterChannels).Return(message.ClusterChannels{}).Build()
	defer patchEmpty.UnPatch()
	pchannels, err = availablePChannels(ctx)
	require.NoError(t, err)
	require.Empty(t, pchannels)
}

func TestFlushSplitSource(t *testing.T) {
	ctx := context.Background()
	s := &Server{ddl: &ddl.Callbacks{}}
	startErr := errors.New("mock")
	patchStart := mockey.Mock((*ddl.Callbacks).StartLocalFlushBroadcast).Return(nil, startErr).Build()
	require.ErrorIs(t, s.flushSplitSource(ctx, 100, splitTestSource), startErr)
	patchStart.UnPatch()

	patchStart = mockey.Mock((*ddl.Callbacks).StartLocalFlushBroadcast).Return(&embeddedBroadcastAPI{}, nil).Build()
	defer patchStart.UnPatch()
	patchClose := mockey.Mock((*embeddedBroadcastAPI).Close).Return().Build()
	defer patchClose.UnPatch()
	var flushed message.BroadcastMutableMessage
	patchBroadcast := mockey.Mock((*embeddedBroadcastAPI).Broadcast).To(
		func(_ *embeddedBroadcastAPI, _ context.Context, msg message.BroadcastMutableMessage) (*types2.BroadcastAppendResult, error) {
			flushed = msg
			return &types2.BroadcastAppendResult{}, nil
		}).Build()
	defer patchBroadcast.UnPatch()
	require.NoError(t, s.flushSplitSource(ctx, 100, splitTestSource))
	// A ManualFlush on the source alone, local to this cluster, acknowledged
	// by the consuming side.
	require.Equal(t, message.MessageTypeManualFlush, flushed.MessageType())
	require.Equal(t, []string{splitTestSource}, flushed.BroadcastHeader().VChannels)
	require.True(t, flushed.BroadcastHeader().AckSyncUp)
	require.True(t, flushed.IsUnreplicable())
}

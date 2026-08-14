package vchannel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestPChannelRecoveryManagerCreatesAndRoutesVChannelModules(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1")
	manager.SwitchIntoMetaAndData()

	observeTestMessage(ctx, t, manager, newTestDeleteMessage(t, "v2", 10))
	assert.Nil(t, manager.Module("v2"))

	observeTestMessage(ctx, t, manager, newTestCreateCollectionMessage(t, "v2", 20))
	require.NotNil(t, manager.Module("v2"))
	assert.True(t, manager.Module("v2").metaAndData)

	snapshots := manager.ConsumeDirtySnapshots()
	require.NotEmpty(t, snapshots)
	assert.Contains(t, dirtySnapshotVChannels(snapshots), "v2")
}

func TestPChannelRecoveryManagerBroadcastsPChannelMessages(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1", "v2")
	manager.SwitchIntoMetaAndData()
	observeTestMessage(ctx, t, manager, newTestDeleteMessage(t, "v1", 10))
	observeTestMessage(ctx, t, manager, newTestDeleteMessage(t, "v2", 11))

	observeTestMessage(ctx, t, manager, newTestRecoveryBarrierMessage(t, 20))

	assert.Equal(t, uint64(20), manager.Module("v1").transformLog.LatestTimeTick())
	assert.Equal(t, uint64(20), manager.Module("v2").transformLog.LatestTimeTick())
}

func TestPChannelRecoveryManagerModuleIndexSupportsConcurrentRange(t *testing.T) {
	manager := newTestManager(t, "p1", "v1", "v2")

	observed := make(map[string]struct{})
	manager.modules.Range(func(vchannel string, module *VChannelRecoveryModule) bool {
		require.NotNil(t, module)
		observed[vchannel] = struct{}{}
		return true
	})

	assert.ElementsMatch(t, []string{"v1", "v2"}, mapKeys(observed))
}

func TestPChannelRecoveryManagerSwitchAggregatesWritePathRecoverySnapshot(t *testing.T) {
	manager := newTestManager(t, "p1", "v1", "v2")

	snapshots := moduleapi.FlattenModuleSnapshot(manager.SwitchIntoMetaAndData())

	writeSnapshots := make([]*moduleapi.WritePathRecoveryModuleSnapshot, 0)
	for _, snapshot := range snapshots {
		if typed, ok := snapshot.(*moduleapi.WritePathRecoveryModuleSnapshot); ok {
			writeSnapshots = append(writeSnapshots, typed)
		}
	}
	require.Len(t, writeSnapshots, 1)
	assert.ElementsMatch(t, []string{"v1", "v2"}, mapKeys(writeSnapshots[0].VChannels))
}

func TestGroupSegmentsByVChannel(t *testing.T) {
	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {SegmentId: 1, Vchannel: "v1"},
		2: {SegmentId: 2, Vchannel: "v2"},
		3: {SegmentId: 3, Vchannel: "v1"},
		4: {SegmentId: 4},
	}

	grouped := groupSegmentsByVChannel(segments)
	require.Len(t, grouped, 2)
	assert.ElementsMatch(t, []int64{1, 3}, mapKeys(grouped["v1"]))
	assert.ElementsMatch(t, []int64{2}, mapKeys(grouped["v2"]))
}

func TestPChannelRecoveryManagerReleasesInitialState(t *testing.T) {
	vchannelMeta := newTestVChannelMeta("v1")
	segmentMeta := &streamingpb.SegmentAssignmentMeta{
		SegmentId:    101,
		CollectionId: 100,
		PartitionId:  10,
		Vchannel:     "v1",
		State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		Stat:         &streamingpb.SegmentAssignmentStat{},
	}
	vchannelMetas := map[string]*streamingpb.VChannelMeta{"v1": vchannelMeta}
	segments := map[int64]*streamingpb.SegmentAssignmentMeta{101: segmentMeta}
	vchannelMeta.SegmentDataVersionSummary = &viewpb.DataVersion{StreamingVersion: 10, CompactVersion: 20}
	transformLogMeta := &streamingpb.VChannelTransformLogMeta{CheckpointTimeTick: 30}
	transformLogMetas := map[string]*streamingpb.VChannelTransformLogMeta{"v1": transformLogMeta}

	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel:          "p1",
		VChannelMetas:     vchannelMetas,
		Segments:          segments,
		TransformLogMetas: transformLogMetas,
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)

	assert.Nil(t, manager.config.VChannelMetas)
	assert.Nil(t, manager.config.Segments)
	assert.Nil(t, manager.config.TransformLogMetas)
	assert.Nil(t, manager.segmentsByVChannel)
	require.Same(t, vchannelMeta, vchannelMetas["v1"])
	require.Same(t, segmentMeta, segments[101])
	require.Same(t, transformLogMeta, transformLogMetas["v1"])

	module := manager.Module("v1")
	require.NotNil(t, module)
	require.Same(t, vchannelMeta, module.vchannelView.meta)
	require.True(t, proto.Equal(vchannelMeta, module.vchannelView.AssignmentMeta()))
	assert.Equal(t, qviews.DataVersion{StreamingVersion: 10, CompactVersion: 20}, module.vchannelView.SegmentDataVersionSummary())
	require.True(t, proto.Equal(transformLogMeta, module.transformLog.SnapshotMeta()))
	require.Contains(t, module.segments, int64(101))
	segmentID, vchannel := module.segments[101].IDAndVChannel()
	assert.Equal(t, int64(101), segmentID)
	assert.Equal(t, "v1", vchannel)
}

func TestPChannelRecoveryManagerSeedsPersistedTombstoneCleanup(t *testing.T) {
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel:      "p1",
		VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": newTestVChannelMeta("v1")},
		Segments: map[int64]*streamingpb.SegmentAssignmentMeta{
			101: {
				SegmentId:              101,
				CollectionId:           100,
				PartitionId:            10,
				Vchannel:               "v1",
				State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
				CheckpointTimeTick:     100,
				DataCheckpointTimeTick: 100,
				TombstoneTimeTick:      100,
				SealedAtDataVersion:    &viewpb.DataVersion{StreamingVersion: 2},
				Stat:                   &streamingpb.SegmentAssignmentStat{},
			},
		},
		TransformLogMetas: map[string]*streamingpb.VChannelTransformLogMeta{},
		NodeScheduler:     scheduler,
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)

	assert.True(t, manager.HasPendingCleanup())
	assert.Empty(t, manager.ConsumeCleanupSnapshots(moduleapi.CleanupContext{
		MetaPhysicalTimeTick: 101,
		DataPhysicalTimeTick: 101,
	}))

	upserts := manager.ConsumeDirtySnapshots()
	require.Len(t, upserts, 1)
	assert.Equal(t, moduleapi.SnapshotOpUpsertBase, upserts[0].Op())
	upserts[0].MarkPersisted()

	snapshots := manager.ConsumeCleanupSnapshots(moduleapi.CleanupContext{
		MetaPhysicalTimeTick: 101,
		DataPhysicalTimeTick: 101,
	})
	require.Len(t, snapshots, 1)
	assert.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
	assert.Equal(t, int64(101), snapshots[0].Key().SegmentID)
}

func TestPChannelRecoveryManagerConsumesDirtySnapshotsFromUpdatedModule(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1")
	manager.SwitchIntoMetaAndData()

	observeTestMessage(ctx, t, manager, newTestCreatePartitionMessage(t, "v1", 20))

	snapshots := manager.ConsumeDirtySnapshots()
	require.NotEmpty(t, snapshots)
	assert.Contains(t, dirtySnapshotModuleNames(snapshots), moduleapi.ModuleNameVChannel)
}

func TestPChannelRecoveryManagerDoesNotScanCleanModulesForDirtySnapshots(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1", "v2")
	manager.SwitchIntoMetaAndData()

	observeTestMessage(ctx, t, manager.Module("v2"), newTestCreatePartitionMessage(t, "v2", 20))
	observeTestMessage(ctx, t, manager, newTestCreatePartitionMessage(t, "v1", 20))

	snapshots := manager.ConsumeDirtySnapshots()
	assert.Contains(t, dirtySnapshotVChannels(snapshots), "v1")
	assert.NotContains(t, dirtySnapshotVChannels(snapshots), "v2")
}

func TestPChannelRecoveryManagerTracksAsyncModuleUpdates(t *testing.T) {
	manager := newTestManager(t, "p1", "v1")
	module := manager.Module("v1")
	require.NotNil(t, module.runtime.Notifier)

	module.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameTransformLog)
	dirty := manager.takeDirtyModules()
	assert.Same(t, module, dirty["v1"])
}

func TestPChannelRecoveryManagerKeepsInFlightDirtyVChannelSnapshots(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1", "v2")

	observeTestMessage(ctx, t, manager, newTestCreateCollectionMessage(t, "v3", 20))
	first := manager.ConsumeDirtySnapshots()
	require.NotEmpty(t, first)
	assert.Contains(t, dirtySnapshotVChannels(first), "v3")

	second := manager.ConsumeDirtySnapshots()
	require.NotEmpty(t, second)
	assert.Contains(t, dirtySnapshotVChannels(second), "v3")

	for _, snapshot := range second {
		snapshot.MarkPersisted()
	}
	assert.Empty(t, manager.ConsumeDirtySnapshots())
}

func TestPChannelRecoveryManagerProvidesTransformLogStream(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1")

	stream, err := manager.AcquireStream(ctx, "p1")
	require.NoError(t, err)
	require.NotNil(t, stream)
	assert.NoError(t, stream.Close())

	_, err = manager.AcquireStream(ctx, "other")
	assert.Error(t, err)
}

func TestPChannelRecoveryManagerSharesQueryTransformLogStream(t *testing.T) {
	manager := newTestManager(t, "p1", "v1", "v2")

	require.NotNil(t, manager.queryTransformLogStream)
	require.Same(t, manager.queryTransformLogStream, manager.Module("v1").queryTransformLogStream)
	require.Same(t, manager.queryTransformLogStream, manager.Module("v2").queryTransformLogStream)
}

func TestPChannelRecoveryManagerRemovesClosedVChannelTransformLog(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1")
	manager.SwitchIntoMetaAndData()

	stream, err := manager.AcquireStream(ctx, "p1")
	require.NoError(t, err)
	defer stream.Close()

	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            newNoopTransformLogHandler(),
	})
	require.NoError(t, err)
	require.NoError(t, sub.Close())

	observeTestMessage(ctx, t, manager, newTestDropCollectionMessage(t, "v1", 20))

	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            newNoopTransformLogHandler(),
	})
	require.Error(t, err)
}

func TestPChannelRecoveryManagerAcquireBuildsBeforeRecoveryBarrier(t *testing.T) {
	manager := newTestManager(t, "p1", "v1")
	manager.SwitchIntoMetaAndData()
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(100, 2, "v1", version, 3)

	ready := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:     key,
		Meta:    meta,
		OnReady: func() { close(ready) },
	})

	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for query runtime before the recovery barrier")
	}
	runtime, ok := manager.GetQueryRuntime(key)
	require.True(t, ok)
	require.NotNil(t, runtime)
}

func TestPChannelRecoveryManagerWALViewUsesDataObservedFrontier(t *testing.T) {
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	vchannelMeta := newTestVChannelMeta("v1")
	vchannelMeta.CollectionInfo.Partitions = []*streamingpb.PartitionInfoOfVChannel{
		{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
	}
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel:               "p1",
		DataCheckpointTimeTick: 7,
		VChannelMetas:          map[string]*streamingpb.VChannelMeta{"v1": vchannelMeta},
		Segments: map[int64]*streamingpb.SegmentAssignmentMeta{
			10: {
				CollectionId:           100,
				PartitionId:            10,
				SegmentId:              10,
				Vchannel:               "v1",
				State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
				CheckpointTimeTick:     7,
				DataCheckpointTimeTick: 7,
				PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
				Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
			},
		},
		TransformLogMetas: map[string]*streamingpb.VChannelTransformLogMeta{},
		NodeScheduler:     scheduler,
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	insert := newTestInsertMessage(t, "v1", 10, 20)
	metaOwner := message.NewOwnedImmutableMessage(insert, nil)
	metaDispatch := metaOwner.Clone()
	manager.ObserveMessage(context.Background(), metaDispatch)
	metaDispatch.Release()
	metaOwner.Release()
	manager.SwitchIntoMetaAndData()
	meta, _ := testQueryViewMetaAndKey(100, 2, "v1", qviews.DataVersion{}, 3)
	module := manager.Module("v1")
	require.Contains(t, module.segments, int64(10))

	module.mu.Lock()
	beforeData, ok := module.queryWALViewLocked(meta)
	module.mu.Unlock()
	require.True(t, ok)
	assert.Equal(t, uint64(7), beforeData.BaseGrowingTimeTick)
	require.Len(t, beforeData.SegmentSnapshot.Segments, 1)
	assert.Empty(t, beforeData.SegmentSnapshot.Segments[0].Data.InsertMessages)

	observeTestMessage(context.Background(), t, manager, insert)

	module.mu.Lock()
	afterData, ok := module.queryWALViewLocked(meta)
	module.mu.Unlock()
	require.True(t, ok)
	assert.Equal(t, uint64(20), afterData.BaseGrowingTimeTick)
	require.Len(t, afterData.SegmentSnapshot.Segments, 1)
	require.Len(t, afterData.SegmentSnapshot.Segments[0].Data.InsertMessages, 1)
	assert.Equal(t, uint64(20), afterData.SegmentSnapshot.Segments[0].Data.InsertMessages[0].TimeTick())
}

func TestPChannelRecoveryManagerAcquireWaitsForRecoveredSegmentFinalCommit(t *testing.T) {
	scheduler := &recordingScheduler{}
	lifecycle := &recordingSegmentLifecycle{}
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel:      "p1",
		VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": newTestVChannelMeta("v1")},
		Segments: map[int64]*streamingpb.SegmentAssignmentMeta{
			10: {
				CollectionId:           100,
				PartitionId:            10,
				SegmentId:              10,
				Vchannel:               "v1",
				State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
				CheckpointTimeTick:     30,
				DataCheckpointTimeTick: 20,
				PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
				Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 10},
			},
		},
		TransformLogMetas: map[string]*streamingpb.VChannelTransformLogMeta{},
		Runtime:           moduleapi.Runtime{Scheduler: scheduler},
		SegmentLifecycle:  lifecycle,
		NodeScheduler:     scheduler,
		QueryRuntimeModuleBuilders: []queryresource.QueryRuntimeModuleBuilder{
			testQueryRuntimeModuleBuilder{},
		},
	})
	require.NoError(t, err)
	defer manager.Close()
	manager.SwitchIntoMetaAndData()

	version := qviews.DataVersion{StreamingVersion: 1}
	meta, key := testQueryViewMetaAndKey(100, 2, "v1", version, 3)
	ready := false
	manager.Acquire(snview.AcquireResource{
		Key:     key,
		Meta:    meta,
		OnReady: func() { ready = true },
	})
	observeTestMessage(context.Background(), t, manager, newTestRecoveryBarrierMessage(t, 40))

	require.Len(t, scheduler.tasks, 1)
	assert.False(t, ready)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	require.Equal(t, []int64{10}, lifecycle.committedSegmentIDs)
	require.Len(t, scheduler.tasks, 2)
	assert.False(t, ready)

	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	require.Len(t, scheduler.tasks, 3)
	assert.False(t, ready)
	require.NoError(t, scheduler.tasks[2].Execute(context.Background()))
	assert.True(t, ready)
	require.NotNil(t, manager.Module("v1").segments[10].AssignmentMeta().GetSealedAtDataVersion())
}

func newTestManager(t *testing.T, pchannel string, vchannels ...string) *PChannelRecoveryManager {
	t.Helper()
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	metas := make(map[string]*streamingpb.VChannelMeta, len(vchannels))
	for _, vchannel := range vchannels {
		metas[vchannel] = newTestVChannelMeta(vchannel)
	}
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel:          pchannel,
		VChannelMetas:     metas,
		TransformLogMetas: map[string]*streamingpb.VChannelTransformLogMeta{},
		NodeScheduler:     scheduler,
		Runtime:           moduleapi.Runtime{},
		QueryRuntimeModuleBuilders: []queryresource.QueryRuntimeModuleBuilder{
			testQueryRuntimeModuleBuilder{},
		},
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	return manager
}

func testQueryViewMetaAndKey(
	collectionID int64,
	replicaID int64,
	vchannel string,
	dataVersion qviews.DataVersion,
	queryVersion int64,
) (*viewpb.QueryViewMeta, qviews.QueryViewKey) {
	version := qviews.QueryViewVersion{DataVersion: dataVersion, QueryVersion: queryVersion}
	meta := &viewpb.QueryViewMeta{
		CollectionId: collectionID,
		ReplicaId:    replicaID,
		Vchannel:     vchannel,
		Version:      version.IntoProto(),
	}
	key := qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: replicaID, VChannel: vchannel},
		QueryViewVersion: version,
	}
	return meta, key
}

type testQueryRuntimeModuleBuilder struct{}

func (testQueryRuntimeModuleBuilder) NewRuntime() (queryresource.QueryRuntimeModule, error) {
	return testQueryRuntimeModule{}, nil
}

type testQueryRuntimeModule struct{}

func (testQueryRuntimeModule) Prepare(context.Context, walview.VChannelWALView) error { return nil }
func (testQueryRuntimeModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {
}
func (testQueryRuntimeModule) Advance(qviews.DataVersion) {}
func (testQueryRuntimeModule) Close()                     {}

func newTestVChannelMeta(vchannel string) *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel:           vchannel,
		State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 1,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "c100"},
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					CheckpointTimeTick: 1,
				},
			},
		},
	}
}

func newTestCreateCollectionMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 100,
			PartitionIds: []int64{
				10,
			},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionSchema: &schemapb.CollectionSchema{Name: "c100"},
		}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTestInsertMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 100,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 10,
					Rows:        1,
					BinarySize:  1,
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{NumRows: 1}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTestCreatePartitionMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewCreatePartitionMessageBuilderV1().
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 100,
			PartitionId:  11,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTestDropCollectionMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

type noopTransformLogHandler struct{}

func newNoopTransformLogHandler() wal.TransformLogEventHandler {
	return noopTransformLogHandler{}
}

func (noopTransformLogHandler) Handle(wal.TransformLogStreamEvent) error {
	return nil
}

func (noopTransformLogHandler) Close() {}

func dirtySnapshotVChannels(snapshots []moduleapi.DirtySnapshot) []string {
	vchannels := make([]string, 0)
	for _, snapshot := range snapshots {
		if snapshot.ModuleName() != moduleapi.ModuleNameVChannel {
			continue
		}
		meta, ok := snapshot.Payload().(*streamingpb.VChannelMeta)
		if !ok {
			continue
		}
		vchannels = append(vchannels, proto.Clone(meta).(*streamingpb.VChannelMeta).GetVchannel())
	}
	return vchannels
}

func dirtySnapshotModuleNames(snapshots []moduleapi.DirtySnapshot) []moduleapi.ModuleName {
	names := make([]moduleapi.ModuleName, 0, len(snapshots))
	for _, snapshot := range snapshots {
		names = append(names, snapshot.ModuleName())
	}
	return names
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

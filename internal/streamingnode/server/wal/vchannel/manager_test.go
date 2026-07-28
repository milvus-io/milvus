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

	result := manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v2", 10))
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Nil(t, manager.Module("v2"))

	result = manager.ObserveMessage(ctx, newTestCreateCollectionMessage(t, "v2", 20))
	require.NotNil(t, result.Meta)
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
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10))
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v2", 11))

	result := manager.ObserveMessage(ctx, newTestRecoveryBarrierMessage(t, 20))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())
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

func TestPChannelRecoveryManagerSwitchAggregatesVChannelMetaSnapshot(t *testing.T) {
	manager := newTestManager(t, "p1", "v1", "v2")

	snapshots := moduleapi.FlattenModuleSnapshot(manager.SwitchIntoMetaAndData())

	vchannelSnapshots := make([]*moduleapi.VChannelModuleSnapshot, 0)
	for _, snapshot := range snapshots {
		if typed, ok := snapshot.(*moduleapi.VChannelModuleSnapshot); ok {
			vchannelSnapshots = append(vchannelSnapshots, typed)
		}
	}
	require.Len(t, vchannelSnapshots, 1)
	assert.ElementsMatch(t, []string{"v1", "v2"}, mapKeys(vchannelSnapshots[0].VChannels))
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
	versionSummary := &streamingpb.SegmentDataVersionSummary{
		DataVersion: &viewpb.DataVersion{StreamingVersion: 10, CompactVersion: 20},
	}
	transformLogMeta := &streamingpb.VChannelTransformLogMeta{CheckpointTimeTick: 30}
	versionSummaries := map[string]*streamingpb.SegmentDataVersionSummary{"v1": versionSummary}
	transformLogMetas := map[string]*streamingpb.VChannelTransformLogMeta{"v1": transformLogMeta}

	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel:                  "p1",
		VChannelMetas:             vchannelMetas,
		Segments:                  segments,
		SegmentDataVersionSummary: versionSummaries,
		TransformLogMetas:         transformLogMetas,
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)

	assert.Nil(t, manager.config.VChannelMetas)
	assert.Nil(t, manager.config.Segments)
	assert.Nil(t, manager.config.SegmentDataVersionSummary)
	assert.Nil(t, manager.config.TransformLogMetas)
	assert.Nil(t, manager.segmentsByVChannel)
	require.Same(t, vchannelMeta, vchannelMetas["v1"])
	require.Same(t, segmentMeta, segments[101])
	require.Same(t, versionSummary, versionSummaries["v1"])
	require.Same(t, transformLogMeta, transformLogMetas["v1"])

	module := manager.Module("v1")
	require.NotNil(t, module)
	require.True(t, proto.Equal(vchannelMeta, module.vchannelView.AssignmentMeta()))
	require.True(t, proto.Equal(versionSummary, module.segmentDataVersionSummary))
	require.True(t, proto.Equal(transformLogMeta, module.transformLog.SnapshotMeta()))
	require.Contains(t, module.segments, int64(101))
	segmentID, vchannel := module.segments[101].IDAndVChannel()
	assert.Equal(t, int64(101), segmentID)
	assert.Equal(t, "v1", vchannel)
}
func TestPChannelRecoveryManagerConsumesDirtySnapshotsFromUpdatedModule(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1")
	manager.SwitchIntoMetaAndData()

	result := manager.ObserveMessage(ctx, newTestCreatePartitionMessage(t, "v1", 20))
	require.NotNil(t, result.Meta)

	snapshots := manager.ConsumeDirtySnapshots()
	require.NotEmpty(t, snapshots)
	assert.Contains(t, dirtySnapshotModuleNames(snapshots), moduleapi.ModuleNameVChannel)
}

func TestPChannelRecoveryManagerDoesNotScanCleanModulesForDirtySnapshots(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1", "v2")
	manager.SwitchIntoMetaAndData()

	result := manager.Module("v2").ObserveMessage(ctx, newTestCreatePartitionMessage(t, "v2", 20))
	require.NotNil(t, result.Meta)
	result = manager.ObserveMessage(ctx, newTestCreatePartitionMessage(t, "v1", 20))
	require.NotNil(t, result.Meta)

	snapshots := manager.ConsumeDirtySnapshots()
	assert.Contains(t, dirtySnapshotVChannels(snapshots), "v1")
	assert.NotContains(t, dirtySnapshotVChannels(snapshots), "v2")
}

func TestPChannelRecoveryManagerTracksAsyncModuleUpdates(t *testing.T) {
	manager := newTestManager(t, "p1", "v1")
	module := manager.Module("v1")
	require.NotNil(t, module.runtime.Notifier)

	module.runtime.Notifier.NotifyBarrierUpdated()
	dirty := manager.takeDirtyModules()
	assert.Same(t, module, dirty["v1"])
}

func TestPChannelRecoveryManagerKeepsInFlightDirtyVChannelSnapshots(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1", "v2")

	manager.ObserveMessage(ctx, newTestCreateCollectionMessage(t, "v3", 20))
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

func TestPChannelRecoveryManagerAggregatesDataFrontier(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1", "v2")
	manager.SwitchIntoMetaAndData()
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10))

	v1Frontier := manager.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v1",
	})
	require.NotNil(t, v1Frontier)
	assert.Equal(t, uint64(0), v1Frontier.TimeTick())

	v2Frontier := manager.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v2",
	})
	require.NotNil(t, v2Frontier)
	assert.NotZero(t, v2Frontier.TimeTick())

	allFrontier := manager.DataFrontier(moduleapi.Scope{
		Type: moduleapi.ScopeAll,
		Kind: moduleapi.DataProgressDurable,
	})
	require.NotNil(t, allFrontier)
	assert.Equal(t, uint64(0), allFrontier.TimeTick())
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

	result := manager.ObserveMessage(ctx, newTestDropCollectionMessage(t, "v1", 20))
	require.NotNil(t, result.Meta)

	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            newNoopTransformLogHandler(),
	})
	require.Error(t, err)
}

func TestPChannelRecoveryManagerAcquireBuildsQueryRuntimeWithoutLoadConfigCallback(t *testing.T) {
	manager := newTestManager(t, "p1", "v1")
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
		t.Fatal("timed out waiting for ready callback")
	}
	runtime, ok := manager.GetQueryRuntime(key)
	require.True(t, ok)
	require.NotNil(t, runtime)
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

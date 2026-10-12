package vchannel

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func schemaGCTestMeta() *streamingpb.VChannelMeta {
	meta := &streamingpb.VChannelMeta{
		Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 50, TransformMaterializedTimeTick: 50,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
			Partitions:   []*streamingpb.PartitionInfoOfVChannel{{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL}},
		},
	}
	for version := uint64(1); version <= 5; version++ {
		meta.CollectionInfo.Schemas = append(meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
			Schema: &schemapb.CollectionSchema{Version: int32(version)}, CheckpointTimeTick: version * 10,
			State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		})
	}
	return meta
}

func TestSchemaGCRetainsReplayAndSegmentDependencies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		floor   uint64
		segment *streamingpb.SegmentAssignmentMeta
		dropped []uint64
	}{
		{name: "no published checkpoint"},
		{name: "before first schema", floor: 9},
		{name: "replay floor between versions", floor: 35, dropped: []uint64{10, 20}},
		{name: "replay floor at version", floor: 30, dropped: []uint64{10, 20}},
		{name: "latest always retained", floor: 100, dropped: []uint64{10, 20, 30, 40}},
		{name: "growing reference", floor: 35, segment: &streamingpb.SegmentAssignmentMeta{
			State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			Stat:  &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 15},
		}, dropped: []uint64{20}},
		{name: "flushed reference", floor: 35, segment: &streamingpb.SegmentAssignmentMeta{
			State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			Stat:  &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 25},
		}, dropped: []uint64{10}},
		{name: "pending tombstone deletion", floor: 35, segment: &streamingpb.SegmentAssignmentMeta{
			State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			Stat:  &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 15},
		}, dropped: []uint64{20}},
		{name: "legacy flushed encoding version", floor: 35, segment: &streamingpb.SegmentAssignmentMeta{
			State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, SchemaVersion: 1,
			Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 45},
		}, dropped: []uint64{20}},
		{name: "legacy sealed encoding version", floor: 35, segment: &streamingpb.SegmentAssignmentMeta{
			State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED, SchemaVersion: 1,
			Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 45},
		}, dropped: []uint64{20}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := NewVChannelViewFromMeta(schemaGCTestMeta())
			var segments []*streamingpb.SegmentAssignmentMeta
			if tc.segment != nil {
				segments = append(segments, tc.segment)
			}
			deletion, changed := view.SchemaCleanupPlan(tc.floor, segments)
			require.Nil(t, deletion)
			require.Equal(t, len(tc.dropped) > 0, changed)
			var dropped []uint64
			for _, schema := range view.AssignmentMeta().GetCollectionInfo().GetSchemas() {
				if schema.GetState() == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED {
					dropped = append(dropped, schema.GetCheckpointTimeTick())
				}
			}
			require.Equal(t, tc.dropped, dropped)
			_, latest := view.GetSchema(0)
			require.EqualValues(t, 5, latest.GetVersion())
		})
	}
}

func TestSchemaGCPersistsTombstonesBeforeDeletionAndResumesAfterRestart(t *testing.T) {
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1", VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": schemaGCTestMeta()},
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 35}
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup))
	snapshots := manager.ConsumeDirtySnapshots()
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpUpsert, snapshots[0].Op())
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup), "unpublished tombstones must not be deleted")
	snapshots[0].MarkPersisted()
	persisted := snapshots[0].Payload().(*streamingpb.VChannelMeta)
	restarted, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1", VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": persisted},
	})
	require.NoError(t, err)
	t.Cleanup(restarted.Close)
	deletes := restarted.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, deletes, 1)
	require.Equal(t, moduleapi.SnapshotOpDeleteSchemas, deletes[0].Op())
	view := restarted.Module("v1").vchannelView
	require.Len(t, view.AssignmentMeta().CollectionInfo.Schemas, 5)
	_, retiredSchema := view.GetSchema(15)
	require.Nil(t, retiredSchema)
	// Failed deletion has no callback: retry contains the same fixed identities.
	retry := restarted.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, retry, 1)
	require.True(t, proto.Equal(deletes[0].Payload(), retry[0].Payload()))
	deletes[0].MarkPersisted()
	require.Len(t, view.AssignmentMeta().CollectionInfo.Schemas, 3)
	require.Empty(t, restarted.ConsumeCleanupSnapshots(cleanup))
	_, schema := view.GetSchema(35)
	require.EqualValues(t, 3, schema.GetVersion())
}

func TestSchemaGCWaitsForSegmentCatalogDeletion(t *testing.T) {
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1", VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": schemaGCTestMeta()},
		Segments: map[int64]*streamingpb.SegmentAssignmentMeta{100: {
			SegmentId: 100, CollectionId: 1, PartitionId: 10, Vchannel: "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick: 25, Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 15},
		}},
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 35}
	deletes := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, deletes, 1)
	require.Equal(t, moduleapi.ModuleNameSegment, deletes[0].ModuleName())
	module := manager.Module("v1")
	require.Equal(t, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		module.vchannelView.AssignmentMeta().CollectionInfo.Schemas[0].State)
	for _, snapshot := range manager.ConsumeDirtySnapshots() {
		snapshot.MarkPersisted()
	}
	// A later scan still cannot retire the pending segment's schema.
	for _, snapshot := range manager.ConsumeCleanupSnapshots(cleanup) {
		snapshot.MarkPersisted()
	}
	require.Equal(t, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		module.vchannelView.AssignmentMeta().CollectionInfo.Schemas[0].State)
	deletes[0].MarkPersisted()
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup))
	require.Equal(t, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED,
		module.vchannelView.AssignmentMeta().CollectionInfo.Schemas[0].State)
}

func TestSchemaGCAllowsBusyBaseMetaAndPreservesConcurrentSchema(t *testing.T) {
	for _, newSchema := range []bool{false, true} {
		t.Run(map[bool]string{false: "base change", true: "schema change"}[newSchema], func(t *testing.T) {
			meta := schemaGCTestMeta()
			meta.CollectionInfo.Schemas[0].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
			view := NewVChannelViewFromMeta(meta)
			view.SetTransformMaterializedTimeTick(51)
			deletion, changed := view.SchemaCleanupPlan(35, nil)
			require.False(t, changed)
			require.NotNil(t, deletion, "base-only dirtiness must not starve GC")
			if newSchema {
				// A WAL schema update between cleanup selection and snapshot freezing.
				view.mu.Lock()
				view.meta.CollectionInfo.Schemas = append(view.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
					Schema: &schemapb.CollectionSchema{Version: 6}, CheckpointTimeTick: 60,
					State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				})
				view.meta.CheckpointTimeTick = 60
				view.schemaDirty = true
				view.mu.Unlock()
			}
			frozen, saveSchemas := view.ConsumeDirtyAndGetSnapshot()
			require.Equal(t, newSchema, saveSchemas)
			view.MarkSchemaCleanupPersisted(deletion)
			view.MarkSnapshotPersisted(frozen)
			next, _ := view.ConsumeDirtyAndGetSnapshot()
			require.NotNil(t, next)
			for _, schema := range next.CollectionInfo.Schemas {
				require.NotEqual(t, uint64(10), schema.CheckpointTimeTick)
			}
			view.MarkSnapshotPersisted(next)
			clean, _ := view.ConsumeDirtyAndGetSnapshot()
			require.Nil(t, clean, "completion must converge without resurrecting deleted history")
			_, latest := view.GetSchema(0)
			if newSchema {
				require.EqualValues(t, 6, latest.GetVersion())
			} else {
				require.EqualValues(t, 5, latest.GetVersion())
			}
		})
	}
}

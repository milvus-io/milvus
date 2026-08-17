package vchannel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestPChannelRecoveryManagerCleansDroppedVChannelInTwoPhases(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, nil)
	equalCheckpoint := moduleapi.CleanupContext{MetaPhysicalTimeTick: 10, DataPhysicalTimeTick: 10}

	require.Empty(t, manager.ConsumeCleanupSnapshots(equalCheckpoint))

	tombstoneSnapshots := manager.ConsumeDirtySnapshots()
	require.Len(t, tombstoneSnapshots, 1)
	assert.Equal(t, moduleapi.ModuleNameVChannel, tombstoneSnapshots[0].ModuleName())
	assert.Equal(t, moduleapi.SnapshotOpUpsertBase, tombstoneSnapshots[0].Op())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
		tombstoneSnapshots[0].Payload().(*streamingpb.VChannelMeta).GetState())
	tombstoneSnapshots[0].MarkPersisted()

	require.Empty(t, manager.ConsumeCleanupSnapshots(equalCheckpoint))

	pastTombstone := moduleapi.CleanupContext{MetaPhysicalTimeTick: 11, DataPhysicalTimeTick: 11}
	deleteSnapshots := manager.ConsumeCleanupSnapshots(pastTombstone)
	require.Len(t, deleteSnapshots, 2)
	assert.Equal(t, moduleapi.ModuleNameTransformLog, deleteSnapshots[0].ModuleName())
	assert.Equal(t, moduleapi.SnapshotOpDelete, deleteSnapshots[0].Op())
	assert.Equal(t, moduleapi.ModuleNameVChannel, deleteSnapshots[1].ModuleName())
	assert.Equal(t, moduleapi.SnapshotOpDelete, deleteSnapshots[1].Op())

	deleteSnapshots[0].MarkPersisted()
	deleteSnapshots[1].MarkPersisted()
	assert.Nil(t, manager.Module("v1"))
}

func TestPChannelRecoveryManagerDeletesSegmentsBeforeVChannel(t *testing.T) {
	segmentMeta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            10,
		SegmentId:              100,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		TombstoneTimeTick:      10,
	}
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
		map[int64]*streamingpb.SegmentAssignmentMeta{100: segmentMeta})
	cleanup := moduleapi.CleanupContext{MetaPhysicalTimeTick: 11, DataPhysicalTimeTick: 11}

	segmentDeletes := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, segmentDeletes, 1)
	assert.Equal(t, moduleapi.ModuleNameSegment, segmentDeletes[0].ModuleName())
	assert.Equal(t, moduleapi.SnapshotOpDelete, segmentDeletes[0].Op())
	segmentDeletes[0].MarkPersisted()

	vchannelDeletes := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, vchannelDeletes, 2)
	assert.Equal(t, moduleapi.ModuleNameTransformLog, vchannelDeletes[0].ModuleName())
	assert.Equal(t, moduleapi.ModuleNameVChannel, vchannelDeletes[1].ModuleName())
	for _, snapshot := range vchannelDeletes {
		snapshot.MarkPersisted()
	}
	assert.Nil(t, manager.Module("v1"))
}

func TestPChannelRecoveryManagerDoesNotRemarkRemovedModuleDirty(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	module := manager.Module("v1")
	require.NotNil(t, module)

	manager.removeModule(module)
	manager.markModuleUpdated(module)

	assert.Empty(t, manager.takeDirtyModules())
}

func newCleanupTestManager(
	t *testing.T,
	state streamingpb.VChannelState,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
) *PChannelRecoveryManager {
	t.Helper()
	tombstoneTimeTick := uint64(0)
	if state == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED {
		tombstoneTimeTick = 10
	}
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1",
		VChannelMetas: map[string]*streamingpb.VChannelMeta{
			"v1": {
				Vchannel:           "v1",
				State:              state,
				CheckpointTimeTick: 10,
				TombstoneTimeTick:  tombstoneTimeTick,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 1,
					Partitions: []*streamingpb.PartitionInfoOfVChannel{
						{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					},
					Schemas: []*streamingpb.CollectionSchemaOfVChannel{
						{
							Schema:             &schemapb.CollectionSchema{Name: "collection"},
							State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
							CheckpointTimeTick: 1,
						},
					},
				},
			},
		},
		Segments: segments,
		TransformLogMetas: map[string]*streamingpb.VChannelTransformLogMeta{
			"v1": {
				MaterializedTimeTick: 10,
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	return manager
}

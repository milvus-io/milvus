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
	equalCheckpoint := moduleapi.CleanupContext{PhysicalTimeTick: 10}

	require.Empty(t, manager.ConsumeCleanupSnapshots(equalCheckpoint))

	tombstoneSnapshots := manager.ConsumeDirtySnapshots()
	require.Len(t, tombstoneSnapshots, 1)
	assert.Equal(t, moduleapi.ModuleNameVChannel, tombstoneSnapshots[0].ModuleName())
	assert.Equal(t, moduleapi.SnapshotOpUpsertBase, tombstoneSnapshots[0].Op())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
		tombstoneSnapshots[0].Payload().(*streamingpb.VChannelMeta).GetState())
	tombstoneSnapshots[0].MarkPersisted()
	// Consuming a stable snapshot must not re-enqueue an otherwise clean
	// module for an extra no-op persist round.
	require.Empty(t, manager.ConsumeDirtySnapshots())

	require.Empty(t, manager.ConsumeCleanupSnapshots(equalCheckpoint))

	pastTombstone := moduleapi.CleanupContext{PhysicalTimeTick: 11}
	deleteSnapshots := manager.ConsumeCleanupSnapshots(pastTombstone)
	require.Len(t, deleteSnapshots, 1)
	assert.Equal(t, moduleapi.ModuleNameVChannel, deleteSnapshots[0].ModuleName())
	assert.Equal(t, moduleapi.SnapshotOpDelete, deleteSnapshots[0].Op())

	deleteSnapshots[0].MarkPersisted()
	assert.Nil(t, manager.Module("v1"))
}

func TestPChannelRecoveryManagerDeletesSegmentsBeforeVChannel(t *testing.T) {
	segmentMeta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:       1,
		PartitionId:        10,
		SegmentId:          100,
		Vchannel:           "v1",
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
		CheckpointTimeTick: 10,
	}
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
		map[int64]*streamingpb.SegmentAssignmentMeta{100: segmentMeta})
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 11}

	segmentDeletes := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, segmentDeletes, 1)
	assert.Equal(t, moduleapi.ModuleNameSegment, segmentDeletes[0].ModuleName())
	assert.Equal(t, moduleapi.SnapshotOpDelete, segmentDeletes[0].Op())
	segmentDeletes[0].MarkPersisted()

	vchannelDeletes := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, vchannelDeletes, 1)
	assert.Equal(t, moduleapi.ModuleNameVChannel, vchannelDeletes[0].ModuleName())
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

func TestPChannelRecoveryManagerKeepsChangesAfterSnapshotFrozen(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, nil)
	module := manager.Module("v1")
	require.NotNil(t, module)

	require.Empty(t, manager.ConsumeCleanupSnapshots(moduleapi.CleanupContext{
		PhysicalTimeTick: 10,
	}))
	first := manager.ConsumeDirtySnapshots()
	require.Len(t, first, 1)

	// Simulate a state change arriving while the first snapshot is in flight.
	module.vchannelView.mu.Lock()
	module.vchannelView.meta.CheckpointTimeTick = 11
	module.vchannelView.dirty = true
	module.vchannelView.mu.Unlock()
	manager.markModuleUpdated(module)
	first[0].MarkPersisted()

	followUp := manager.ConsumeDirtySnapshots()
	require.Len(t, followUp, 1)
	assert.Equal(t, uint64(11), followUp[0].Payload().(*streamingpb.VChannelMeta).GetCheckpointTimeTick())
	followUp[0].MarkPersisted()
	require.Empty(t, manager.ConsumeDirtySnapshots())
}

func newCleanupTestManager(
	t *testing.T,
	state streamingpb.VChannelState,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
) *PChannelRecoveryManager {
	t.Helper()
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1",
		VChannelMetas: map[string]*streamingpb.VChannelMeta{
			"v1": {
				Vchannel:                      "v1",
				State:                         state,
				CheckpointTimeTick:            10,
				TransformMaterializedTimeTick: 10,
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
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	return manager
}

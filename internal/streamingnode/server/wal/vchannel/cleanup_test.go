package vchannel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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

	pastTombstone := moduleapi.CleanupContext{PhysicalTimeTick: 11, SummaryRetired: func(string, uint64) bool { return true }}
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
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 11, SummaryRetired: func(string, uint64) bool { return true }}

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

func TestVChannelTombstoneWaitsForSummaryRetirement(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 11}
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup), "no retirement proof")
	retired := false
	cleanup.SummaryRetired = func(vc string, through uint64) bool {
		require.Equal(t, "v1", vc)
		require.Equal(t, uint64(10), through)
		return retired
	}
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup))
	require.NotNil(t, manager.Module("v1"))
	retired = true
	snapshots := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
	snapshots[0].MarkPersisted()
	require.Nil(t, manager.Module("v1"))
}

func TestTombstonedVChannelIgnoresFlushAndLateMaterialization(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	module := manager.Module("v1")
	before := module.vchannelView.AssignmentMeta()
	cleanup := manager.ConsumeCleanupSnapshots(moduleapi.CleanupContext{
		PhysicalTimeTick: 11, SummaryRetired: func(string, uint64) bool { return true },
	})
	require.Len(t, cleanup, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, cleanup[0].Op())
	raw := message.NewFlushAllMessageBuilderV2().WithVChannel("p1").
		WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).
		MustBuildMutable().WithTimeTick(20).WithLastConfirmed(walimplstest.NewTestMessageID(19)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(20))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(raw)
	retained := owner.Clone()
	require.True(t, module.ObserveMessage(context.Background(), retained))
	retained.Release()
	owner.Release()
	require.Equal(t, uint64(20), tracker.CompletedPoint().TimeTick, "tombstone must not retain new flush work")
	// A task queued before the tombstone transition may finish after cleanup
	// captured its delete snapshot. Its empty frontier must not revive metadata.
	module.markL0Materialized(20)
	require.Equal(t, before, module.vchannelView.AssignmentMeta())
	require.Empty(t, module.ConsumeDirtySnapshots())
	cleanup[0].MarkPersisted()
	require.Nil(t, manager.Module("v1"))
}

func TestDroppedVChannelStillRecordsMaterialization(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, nil)
	module := manager.Module("v1")
	module.markL0Materialized(20)
	snapshots := module.ConsumeDirtySnapshots()
	require.Len(t, snapshots, 1)
	require.Equal(t, uint64(20), snapshots[0].Payload().(*streamingpb.VChannelMeta).GetTransformMaterializedTimeTick())
}

func TestQueryViewPinsSegmentUntilVersionFloorIsPersisted(t *testing.T) {
	segmentMeta := &streamingpb.SegmentAssignmentMeta{CollectionId: 1, PartitionId: 10, SegmentId: 100, Vchannel: "v1", State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, CheckpointTimeTick: 10, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 2}}
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, map[int64]*streamingpb.SegmentAssignmentMeta{100: segmentMeta})
	module := manager.Module("v1")
	key := qviews.QueryViewKey{ShardID: qviews.ShardID{VChannel: "v1", ReplicaID: 1}, QueryViewVersion: qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 1}}}
	module.queryResources.AcquireLocked(snview.AcquireResource{Key: key, Meta: &viewpb.QueryViewMeta{Vchannel: "v1", Version: key.QueryViewVersion.IntoProto()}}, func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) { return walview.VChannelWALView{}, false })
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 11}
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup), "the old Up view retains its growing segment")
	module.ReleaseQueryResource(snview.ReleaseResource{Key: key})
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup), "persist the version floor before deleting segment metadata")
	snapshots := manager.ConsumeDirtySnapshots()
	var persistedFloor bool
	for _, snapshot := range snapshots {
		if meta, ok := snapshot.Payload().(*streamingpb.VChannelMeta); ok {
			require.Equal(t, int64(2), meta.GetSegmentDataVersionSummary().GetStreamingVersion())
			persistedFloor = true
		}
		snapshot.MarkPersisted()
	}
	require.True(t, persistedFloor)
	snapshots = manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.ModuleNameSegment, snapshots[0].ModuleName())
	require.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
	snapshots[0].MarkPersisted()
	require.Empty(t, module.segments)
}

func TestQueryResourceRejectsDroppedVChannel(t *testing.T) {
	for _, state := range []streamingpb.VChannelState{streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED} {
		manager := newCleanupTestManager(t, state, nil)
		module := manager.Module("v1")
		scheduler := nodescheduler.New(1)
		module.queryResources = queryresource.NewManager(queryresource.Config{Scheduler: scheduler})
		rejected := make(chan struct{})
		module.AcquireQueryResource(snview.AcquireResource{Meta: &viewpb.QueryViewMeta{Vchannel: "v1", Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}}}, OnUnrecoverable: func() { close(rejected) }})
		select {
		case <-rejected:
		case <-time.After(time.Second):
			t.Fatal("dropped channel preparation stalled")
		}
		_, ok := module.queryResources.OldestDataVersion()
		require.False(t, ok)
		scheduler.Close()
	}
}

func fenceCleanupTestVChannel(t *testing.T, manager *PChannelRecoveryManager) {
	t.Helper()
	module := manager.Module("v1")
	require.NotNil(t, module)
	module.vchannelView.mu.Lock()
	module.vchannelView.meta.SplitFenceTimeTick = 5
	module.vchannelView.meta.SplitFenceTaskId = 7
	module.vchannelView.mu.Unlock()
}

// TestRetiredSplitSourceWaitsForItsFinalCheckpointReport: a retired split
// source is not removed -- and so stays in the checkpoint updater's report set
// -- until DataCoord has accepted a checkpoint through its drop tick, which a
// secondary's drain predicate reads after the source is already retired.
func TestRetiredSplitSourceWaitsForItsFinalCheckpointReport(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	fenceCleanupTestVChannel(t, manager)
	reported := false
	cleanup := moduleapi.CleanupContext{
		PhysicalTimeTick: 11,
		SummaryRetired:   func(string, uint64) bool { return true },
		CheckpointReported: func(vc string, through uint64) bool {
			require.Equal(t, "v1", vc)
			require.Equal(t, uint64(10), through, "the drop tick bounds T_switch from above")
			return reported
		},
	}
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup))
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup), "still a cleanup candidate, still waiting")
	require.NotNil(t, manager.Module("v1"))
	assert.Contains(t, manager.activeVChannels(), "v1", "its checkpoint is still reported")

	reported = true
	snapshots := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
	snapshots[0].MarkPersisted()
	require.Nil(t, manager.Module("v1"))
}

// TestOrdinaryDropDoesNotWaitForCheckpointReport: a drop that was never a
// split source has no reader of its final checkpoint.
func TestOrdinaryDropDoesNotWaitForCheckpointReport(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	snapshots := manager.ConsumeCleanupSnapshots(moduleapi.CleanupContext{
		PhysicalTimeTick:   11,
		SummaryRetired:     func(string, uint64) bool { return true },
		CheckpointReported: func(string, uint64) bool { return false },
	})
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
}

// TestManagerGatesRetiredSplitSourceOnItsCheckpointUpdater: the manager wires
// its own updater into the cleanup context, and forgets the vchannel once the
// module is removed.
func TestManagerGatesRetiredSplitSourceOnItsCheckpointUpdater(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	fenceCleanupTestVChannel(t, manager)
	updater := newPChannelCheckpointUpdater("p1", manager.activeVChannels,
		func() *utility.WALCheckpoint { return nil }, nil, &stubStoredCheckpointReader{})
	manager.checkpointUpdater = updater
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 11, SummaryRetired: func(string, uint64) bool { return true }}

	updater.markStored("v1", 9)
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup), "a stored checkpoint below the drop tick is not enough")

	updater.markStored("v1", 10)
	snapshots := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, snapshots, 1)
	snapshots[0].MarkPersisted()
	require.Nil(t, manager.Module("v1"))
	assert.False(t, updater.Reported("v1", 10), "a removed vchannel is forgotten")
}

// TestRetiredSplitSourceWaitsWhenDataCoordDropsItsCheckpoint: DataCoord's
// batch UpdateChannelCheckpoint answers success while silently dropping a
// vchannel whose WAL it places on another node (a pchannel mid-rebalance).
// The retired source must not be cleaned up on that success: it stays, and
// stays reported, until a read back shows the checkpoint stored.
func TestRetiredSplitSourceWaitsWhenDataCoordDropsItsCheckpoint(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	fenceCleanupTestVChannel(t, manager)
	reporter := &stubCheckpointReporter{}
	reader := &stubStoredCheckpointReader{}
	updater := newPChannelCheckpointUpdater("p1", manager.activeVChannels,
		func() *utility.WALCheckpoint {
			return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 20, Magic: 1}
		}, reporter, reader)
	manager.checkpointUpdater = updater
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 21, SummaryRetired: func(string, uint64) bool { return true }}

	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup))
	for i := 0; i < 3; i++ {
		updater.execute()
		calls, cps := reporter.snap()
		require.Equal(t, i+1, calls)
		require.Len(t, cps, 1)
		require.Equal(t, "v1", cps[0].GetChannelName(), "the source is still reported")
		require.Equal(t, []string{"v1"}, reader.takeReads())
		require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup), "success with the source filtered out does not release it")
		require.NotNil(t, manager.Module("v1"))
	}

	reader.set("v1", 20, nil)
	updater.execute()
	snapshots := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
	snapshots[0].MarkPersisted()
	require.Nil(t, manager.Module("v1"))
}

// TestManagerWithoutStoredReaderDoesNotGate: an updater that cannot read back
// is not wired in, so cleanup does not wait on an answer that never comes.
func TestManagerWithoutStoredReaderDoesNotGate(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	fenceCleanupTestVChannel(t, manager)
	manager.checkpointUpdater = newPChannelCheckpointUpdater("p1", manager.activeVChannels,
		func() *utility.WALCheckpoint { return nil }, &stubCheckpointReporter{}, nil)
	snapshots := manager.ConsumeCleanupSnapshots(moduleapi.CleanupContext{
		PhysicalTimeTick: 11,
		SummaryRetired:   func(string, uint64) bool { return true },
	})
	require.Len(t, snapshots, 1)
}

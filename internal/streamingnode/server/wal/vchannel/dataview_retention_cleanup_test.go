package vchannel

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestDataViewRetentionWaitsForDurableDropCleanup(t *testing.T) {
	for _, phase := range []string{"dirty tombstone", "materialization pending", "checkpoint pending", "segment pending"} {
		t.Run(phase, func(t *testing.T) {
			var segments map[int64]*streamingpb.SegmentAssignmentMeta
			if phase == "segment pending" {
				segments = map[int64]*streamingpb.SegmentAssignmentMeta{100: {
					CollectionId: 1, SegmentId: 100, Vchannel: "v1",
					State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, CheckpointTimeTick: 10,
				}}
			}
			manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, segments)
			module := manager.Module("v1")
			module.summaryManager = &walsummary.Manager{}
			cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 11}
			switch phase {
			case "dirty tombstone":
				module.vchannelView.dirty = true
			case "materialization pending":
				module.vchannelView.persistedMaterializedTimeTick = 9
			case "checkpoint pending":
				cleanup.PhysicalTimeTick = 10
			}
			released := false
			patch := mockey.Mock((*walsummary.Manager).SetDataViewRetention).To(func(*walsummary.Manager, string, uint64) {
				released = true
			}).Build()
			defer patch.UnPatch()
			snapshots := manager.ConsumeCleanupSnapshots(cleanup)
			require.False(t, released)
			if phase == "segment pending" {
				require.Len(t, snapshots, 1)
				require.Equal(t, moduleapi.ModuleNameSegment, snapshots[0].ModuleName())
				require.False(t, released, "capturing a delete snapshot is not durable deletion")
				snapshots[0].MarkPersisted()
				require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup))
				require.True(t, released)
			}
		})
	}
}

func TestDataViewRetentionDropCompletesSummaryAndVChannelCleanup(t *testing.T) {
	ctx := t.Context()
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	store := walsummary.NewStore(storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())), "p1", 1)
	summary := walsummary.NewManager(walsummary.ManagerConfig{
		PChannel: "p1", Term: 1, Store: store, MaxRetentionBytes: 0,
		Runtime: moduleapi.Runtime{Scheduler: scheduler},
		FetchDataViewRetention: func(context.Context) (map[string]uint64, error) {
			// Even a stale Coord response must not undo the Drop release.
			return map[string]uint64{"v1": 1}, nil
		},
	})
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1", SummaryManager: summary,
		VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": {
			Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CreateCollectionTimeTick: 1, CheckpointTimeTick: 10, TransformMaterializedTimeTick: 10,
		}},
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	msg := message.NewDeleteMessageBuilderV1().WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{CollectionID: 1, PrimaryKeys: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}},
		}, Timestamps: []uint64{10}}).MustBuildMutable().WithTimeTick(10).
		WithLastConfirmed(walimplstest.NewTestMessageID(9)).IntoImmutableMessage(walimplstest.NewTestMessageID(10))
	summary.ObserveMessage(ctx, msg)
	summary.RequestFlushThrough(10)
	require.Eventually(t, func() bool { return summary.LastAcked() >= 10 }, 5*time.Second, time.Millisecond)
	require.NoError(t, summary.GCOnce(ctx))
	require.Len(t, summary.Manifest().Chunks, 1, "the origin still pins the Delete")

	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 11, SummaryRetired: summary.CanCleanupVChannel}
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup), "release the pin but keep the tombstone")
	require.NotNil(t, manager.Module("v1"))
	require.NoError(t, summary.GCOnce(ctx))
	require.Eventually(t, func() bool { return summary.CanCleanupVChannel("v1", 10) }, 5*time.Second, time.Millisecond)
	manifest, exists, err := store.ReadManifest(ctx)
	require.NoError(t, err)
	require.True(t, exists)
	require.Empty(t, manifest.Chunks)
	require.Equal(t, uint64(10), manifest.TransformFastForwardTimeTick["v1"])

	snapshots := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
	// A failed catalog delete leaves the module alive and the delete retryable.
	require.NotNil(t, manager.Module("v1"))
	retry := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, retry, 1)
	retry[0].MarkPersisted()
	require.Nil(t, manager.Module("v1"))
	snapshots[0].MarkPersisted() // a late completion is harmless
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup))
}

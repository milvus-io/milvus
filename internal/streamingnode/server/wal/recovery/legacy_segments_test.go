package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type legacyMigrationCatalog struct{ metastore.StreamingNodeCataLog }

func TestLegacyMigrationRewindsBeforeComponentRemoval(t *testing.T) {
	ctx := context.Background()
	store := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(90), TimeTick: 900, Magic: utility.RecoveryMagicStreamingInitialized})
	migration := &legacyRecoveryMigration{
		checkpoint:        &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(10), Magic: utility.RecoveryMagicRecoveryStorageV2, Term: 3},
		legacyTimeTick:    100,
		removedSegmentIDs: []int64{1},
	}
	catalog := &legacyMigrationCatalog{}
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	for _, failAt := range []int{1, 2, 0} {
		calls := 0
		patch := mockey.Mock((*legacyMigrationCatalog).SaveRecoverySnapshot).To(func(_ *legacyMigrationCatalog, _ context.Context, channel string, snapshot *metastore.WALRecoverySnapshot) error {
			calls++
			require.Equal(t, "test-pchannel", channel)
			require.EqualValues(t, 3, snapshot.ConsumeCheckpoint.GetTerm())
			if calls == 1 {
				require.EqualValues(t, 100, snapshot.ConsumeCheckpoint.GetTimeTick())
				require.Empty(t, snapshot.RemovedSegmentIDs)
				require.Empty(t, snapshot.SegmentAssignments)
				require.EqualValues(t, utility.RecoveryMagicStreamingInitialized, snapshot.ConsumeCheckpoint.GetRecoveryMagic())
			} else {
				require.Zero(t, snapshot.ConsumeCheckpoint.GetTimeTick())
				require.Equal(t, []int64{1}, snapshot.RemovedSegmentIDs)
				require.EqualValues(t, utility.RecoveryMagicRecoveryStorageV2, snapshot.ConsumeCheckpoint.GetRecoveryMagic())
			}
			if calls == failAt {
				return context.DeadlineExceeded
			}
			return nil
		}).Build()
		err := store.persistLegacyRecoveryMigration(ctx, migration)
		patch.UnPatch()
		if failAt == 0 {
			require.NoError(t, err)
			require.Equal(t, 2, calls)
		} else {
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, failAt, calls, "rewind failure must prevent component removal")
		}
	}
}

// Exercise migration and real segment observation together: a boundary Insert
// must populate the pending buffer, and a boundary CreateSegment must restore
// an allocation removed during migration. Neither may be treated as completed.
func TestLegacyMigrationReplaysInclusiveBoundary(t *testing.T) {
	for _, boundary := range []string{"insert", "create_segment"} {
		for _, recovery := range []string{"initial", "restart", "retry_after_rewind"} {
			t.Run(boundary+"/"+recovery, func(t *testing.T) {
				ctx := t.Context()
				catalog := &legacyMigrationCatalog{}
				resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
				checkpoint := &utility.WALCheckpoint{
					MessageID: walimplstest.NewTestMessageID(90), TimeTick: 900,
					Magic: utility.RecoveryMagicStreamingInitialized,
				}
				channelMeta := newLegacyRecoveryTestVChannel("v1", 1, 10)
				allocation := &streamingpb.SegmentAssignmentMeta{
					CollectionId: 1, PartitionId: 10, SegmentId: 1, Vchannel: "v1",
					CheckpointTimeTick: 900,
					State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					Stat:               &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 50},
				}
				if boundary == "create_segment" {
					allocation.Stat.CreateSegmentTimeTick = 100
				}
				durable := &datapb.SegmentInfo{
					ID: 1, CollectionID: 1, PartitionID: 10, InsertChannel: "v1",
					State: commonpb.SegmentState_Growing, NumOfRows: 5,
					DmlPosition: &msgpb.MsgPosition{Timestamp: 90},
				}
				if boundary == "create_segment" {
					durable = nil // Allocation must be removed and rebuilt by WAL.
				}
				seek := &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(10), TimeTick: 100}
				getCheckpoint := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).Return(seek, []int64{1}, nil).Build()
				defer getCheckpoint.UnPatch()
				getSegment := mockey.Mock((*recoveryStorageImpl).getLegacySegmentInfo).Return(durable, nil).Build()
				defer getSegment.UnPatch()
				vchannels := map[string]*streamingpb.VChannelMeta{"v1": channelMeta}
				segments := map[int64]*streamingpb.SegmentAssignmentMeta{1: allocation}
				savedCheckpoint := checkpoint.IntoProto()
				savedChannel := proto.Clone(channelMeta).(*streamingpb.VChannelMeta)
				savedSegment := proto.Clone(allocation).(*streamingpb.SegmentAssignmentMeta)
				saves := 0
				save := mockey.Mock((*legacyMigrationCatalog).SaveRecoverySnapshot).To(func(
					_ *legacyMigrationCatalog, _ context.Context, _ string, snapshot *metastore.WALRecoverySnapshot,
				) error {
					saves++
					if recovery == "retry_after_rewind" && saves == 2 {
						return context.DeadlineExceeded
					}
					// Round-trip the persisted checkpoint, not a pointer to the
					// caller's mutable snapshot or a process-local migration flag.
					encoded, err := proto.Marshal(snapshot.ConsumeCheckpoint)
					require.NoError(t, err)
					savedCheckpoint = &streamingpb.WALCheckpoint{}
					require.NoError(t, proto.Unmarshal(encoded, savedCheckpoint))
					if meta := snapshot.VChannels["v1"]; meta != nil {
						savedChannel = proto.Clone(meta).(*streamingpb.VChannelMeta)
					}
					if meta := snapshot.SegmentAssignments[1]; meta != nil {
						savedSegment = proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
					}
					for _, id := range snapshot.RemovedSegmentIDs {
						if id == 1 {
							savedSegment = nil
						}
					}
					return nil
				}).Build()
				defer save.UnPatch()
				newStore := func(cp *utility.WALCheckpoint) *recoveryStorageImpl {
					store := newTestRecoveryStorage(t, cp)
					t.Cleanup(store.closeRecoveryResources)
					return store
				}
				store := newStore(checkpoint)
				migrated, err := store.migrateLegacyRecoveryInfo(ctx, vchannels, segments)
				if recovery == "retry_after_rewind" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.False(t, migrated)
					require.EqualValues(t, 100, savedCheckpoint.GetTimeTick())
					require.Zero(t, savedChannel.GetCheckpointTimeTick(), "old metadata has not been normalized on disk")
					store = newStore(utility.NewWALCheckpointFromProto(savedCheckpoint))
					vchannels = map[string]*streamingpb.VChannelMeta{"v1": savedChannel}
					segments = map[int64]*streamingpb.SegmentAssignmentMeta{1: savedSegment}
					migrated, err = store.migrateLegacyRecoveryInfo(ctx, vchannels, segments)
				}
				require.NoError(t, err)
				require.True(t, migrated)
				require.Zero(t, savedCheckpoint.GetTimeTick())
				require.EqualValues(t, 900, savedCheckpoint.GetControlCheckpointTimeTick(), "control retains its independent frontier")
				require.Zero(t, store.ackTracker.CompletedPoint().TimeTick)
				if recovery == "restart" {
					store = newStore(utility.NewWALCheckpointFromProto(savedCheckpoint))
					vchannels = map[string]*streamingpb.VChannelMeta{"v1": savedChannel}
					segments = make(map[int64]*streamingpb.SegmentAssignmentMeta)
					if savedSegment != nil {
						segments[1] = savedSegment
					}
					migrated, err = store.migrateLegacyRecoveryInfo(ctx, vchannels, segments)
					require.NoError(t, err)
					require.False(t, migrated, "reopening V2 must not depend on an in-memory migrated flag")
				}

				// Leave persistence tasks queued so the snapshot demonstrates
				// replayed rows and the tracker cannot claim they are durable.
				submit := mockey.Mock(mockey.GetMethod(store.taskScheduler, "Submit")).Return(nodescheduler.TaskHandle(nil)).Build()
				defer submit.UnPatch()
				manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
					PChannel: store.channel.Name, VChannelMetas: vchannels, Segments: segments,
					Runtime: moduleapi.Runtime{Scheduler: store.taskScheduler, Notifier: store},
				})
				require.NoError(t, err)
				store.vchannelManager = manager
				store.installCheckpoint(store.checkpoint)
				stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, 3), closed: make(chan struct{})}
				insertTick := uint64(100)
				expectedRows := uint64(7)
				if boundary == "create_segment" {
					require.Empty(t, segments)
					stream.ch <- message.NewCreateSegmentMessageBuilderV2().WithVChannel("v1").
						WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: 10, SegmentId: 1}).
						WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(100).
						WithLastConfirmed(seek.MessageID).IntoImmutableMessage(walimplstest.NewTestMessageID(11))
					insertTick, expectedRows = 101, 2
				}
				stream.ch <- message.NewInsertMessageBuilderV1().WithVChannel("v1").
					WithHeader(&message.InsertMessageHeader{
						CollectionId: 1, Partitions: []*messagespb.PartitionSegmentAssignment{{
							PartitionId: 10, Rows: 2, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 1},
						}},
					}).WithBody(&msgpb.InsertRequest{NumRows: 2}).MustBuildMutable().WithTimeTick(insertTick).
					WithLastConfirmed(seek.MessageID).IntoImmutableMessage(walimplstest.NewTestMessageID(12))
				barrier := message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
					WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).
					MustBuildMutable().WithTimeTick(200).WithLastConfirmed(seek.MessageID).
					IntoImmutableMessage(walimplstest.NewTestMessageID(13))
				stream.ch <- barrier
				builder := &recordingRecoveryStreamBuilder{stream: stream}
				snapshot, err := store.runBoundedRecovery(ctx, builder, barrier)
				require.NoError(t, err)
				require.True(t, seek.MessageID.EQ(builder.param.StartCheckpoint))
				state, ok := snapshot.WritePathRecovery.GrowingSegments[1]
				require.True(t, ok, "the boundary allocation must survive recovery")
				require.Equal(t, expectedRows, state.Stat.GetModifiedRows(), "the boundary rows must enter the real segment buffer")
				require.Zero(t, store.ackTracker.CompletedPoint().TimeTick, "unpersisted rows must hold back the completed prefix")
			})
		}
	}
}

func TestLegacyMigrationReconcilesSegmentCatalogs(t *testing.T) {
	legacy := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {CollectionId: 10, PartitionId: 20, SegmentId: 1, Vchannel: "v1", CheckpointTimeTick: 900, Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 10, ModifiedRows: 999}},
		3: {CollectionId: 10, PartitionId: 20, SegmentId: 3, Vchannel: "v1", CheckpointTimeTick: 900, Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 200}},
	}
	durable := map[int64]*datapb.SegmentInfo{
		1: {ID: 1, CollectionID: 10, PartitionID: 20, InsertChannel: "v1", State: commonpb.SegmentState_Growing, NumOfRows: 5, DmlPosition: &msgpb.MsgPosition{Timestamp: 100}},
		2: {ID: 2, CollectionID: 10, PartitionID: 20, InsertChannel: "v1", State: commonpb.SegmentState_Sealed, NumOfRows: 7, StartPosition: &msgpb.MsgPosition{Timestamp: 20}, DmlPosition: &msgpb.MsgPosition{Timestamp: 100}, ManifestPath: "prefix", SchemaVersion: 3},
	}
	patch := mockey.Mock((*recoveryStorageImpl).getLegacySegmentInfo).To(func(_ *recoveryStorageImpl, _ context.Context, id int64) (*datapb.SegmentInfo, error) {
		return durable[id], nil
	}).Build()
	defer patch.UnPatch()
	store := &recoveryStorageImpl{}
	cp := &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(90), TimeTick: 100}
	recovered, removed, err := store.rebuildLegacySegmentSnapshots(context.Background(), legacy, map[int64]string{1: "v1", 2: "v1"}, cp)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{3}, removed)
	require.Len(t, recovered, 2)
	require.EqualValues(t, 5, recovered[1].GetStat().GetModifiedRows())
	require.EqualValues(t, 100, recovered[1].GetCheckpointTimeTick())
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED, recovered[2].GetState())
	require.Equal(t, "prefix", recovered[2].GetPersistedStorage().GetManifestPath())
	require.EqualValues(t, 3, recovered[2].GetSchemaVersion())
	// Without a replayable creation, missing state cannot be silently omitted.
	cp.TimeTick = 201
	_, _, err = store.rebuildLegacySegmentSnapshots(context.Background(), legacy, map[int64]string{2: "v1"}, cp)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestLegacyMigrationEmptyRetiredAllocation(t *testing.T) {
	info := &datapb.SegmentInfo{ID: 2, CollectionID: 10, PartitionID: 20, InsertChannel: "v1", State: commonpb.SegmentState_Sealed}
	allocation, err := legacyAllocationFromDurable(info, 100)
	require.NoError(t, err)
	snapshot, keep, err := rebuildLegacySegmentSnapshot(allocation, info)
	require.NoError(t, err)
	require.True(t, keep)
	require.Zero(t, snapshot.GetStat().GetModifiedRows())
	require.EqualValues(t, 100, snapshot.GetCheckpointTimeTick())
	info.NumOfRows = 1
	_, err = legacyAllocationFromDurable(info, 100)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

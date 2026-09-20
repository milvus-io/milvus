package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type legacyMigrationCatalog struct{ metastore.StreamingNodeCataLog }

func TestLegacyMigrationRewindsBeforeComponentRemoval(t *testing.T) {
	ctx := context.Background()
	store := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(90), TimeTick: 900, Magic: utility.RecoveryMagicStreamingInitialized})
	migration := &legacyRecoveryMigration{
		checkpoint:        &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(10), TimeTick: 100, Magic: utility.RecoveryMagicRecoveryStorageV2, Term: 3},
		removedSegmentIDs: []int64{1},
	}
	catalog := &legacyMigrationCatalog{}
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	for _, failAt := range []int{1, 2, 0} {
		calls := 0
		patch := mockey.Mock((*legacyMigrationCatalog).SaveRecoverySnapshot).To(func(_ *legacyMigrationCatalog, _ context.Context, channel string, snapshot *metastore.WALRecoverySnapshot) error {
			calls++
			require.Equal(t, "test-pchannel", channel)
			require.EqualValues(t, 100, snapshot.ConsumeCheckpoint.GetTimeTick())
			require.EqualValues(t, 3, snapshot.ConsumeCheckpoint.GetTerm())
			if calls == 1 {
				require.Empty(t, snapshot.RemovedSegmentIDs)
				require.Empty(t, snapshot.SegmentAssignments)
				require.EqualValues(t, utility.RecoveryMagicStreamingInitialized, snapshot.ConsumeCheckpoint.GetRecoveryMagic())
			} else {
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

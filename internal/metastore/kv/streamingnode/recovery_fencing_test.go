package streamingnode

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRecoverySnapshotFencesEveryBatch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		limit      int
		takeoverAt int
	}{
		{"atomic", 128, 0},
		{"atomic takeover", 128, 1},
		{"chunked", 1, 0},
		{"takeover before delete", 1, 1},
		{"takeover before vchannel", 1, 2},
		{"takeover before segment", 1, 3},
		{"takeover before checkpoint", 1, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &mocks.MetaKv{}
			cpKey := buildConsumeCheckpointKey("p1")
			segmentKey := buildSegmentAssignmentKey("p1", 1)
			deletedKey := buildSegmentAssignmentKey("p1", 2)
			vchannelKey := buildVChannelKey("p1", "v1")
			cp := &streamingpb.WALCheckpoint{TimeTick: 10, Term: 1}
			successor := checkpointBytesOf(t, &streamingpb.WALCheckpoint{TimeTick: 10, Term: 2})
			values := map[string]string{cpKey: checkpointBytesOf(t, cp), deletedKey: "old"}
			limit := mockey.Mock(mockey.GetMethod(store, "MaxTxnOps")).Return(tc.limit).Build()
			defer limit.UnPatch()
			load := mockey.Mock(mockey.GetMethod(store, "Load")).To(func(_ context.Context, key string) (string, error) {
				return values[key], nil
			}).Build()
			defer load.UnPatch()
			calls := 0
			write := mockey.Mock(mockey.GetMethod(store, "MultiSaveAndRemove")).To(
				func(_ context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
					calls++
					if calls == tc.takeoverAt {
						values[cpKey] = successor
						for _, key := range []string{segmentKey, deletedKey, vchannelKey} {
							values[key] = "new-owner"
						}
					}
					require.Len(t, preds, 1, "every batch must carry ownership")
					require.Equal(t, cpKey, preds[0].Key())
					if !preds[0].IsTrue(values[cpKey]) {
						return merr.WrapErrIoFailedReason("ownership changed")
					}
					for _, key := range removals {
						delete(values, key)
					}
					for key, value := range saves {
						values[key] = value
					}
					return nil
				}).Build()
			defer write.UnPatch()
			err := NewCataLog(store).SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
				ConsumeCheckpoint: cp, // Component-only update: checkpoint does not move.
				SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
					1: {SegmentId: 1},
				},
				RemovedSegmentIDs: []int64{2},
				VChannelBaseMetas: map[string]*streamingpb.VChannelMeta{
					"v1": {Vchannel: "v1", CollectionInfo: &streamingpb.CollectionInfoOfVChannel{}},
				},
			})
			if tc.takeoverAt != 0 {
				require.Error(t, err)
				require.Equal(t, tc.takeoverAt, calls, "stop at the rejected batch")
				require.Equal(t, successor, values[cpKey])
				for _, key := range []string{segmentKey, deletedKey, vchannelKey} {
					require.Equal(t, "new-owner", values[key])
				}
			} else {
				require.NoError(t, err)
				require.Contains(t, values, segmentKey)
				require.Contains(t, values, vchannelKey)
				require.NotContains(t, values, deletedKey)
				require.Equal(t, checkpointBytesOf(t, cp), values[cpKey])
			}
		})
	}
}

func TestRecoveryComponentOnlyUncertainCommitMustRetry(t *testing.T) {
	for _, applied := range []bool{false, true} {
		name := "not applied"
		if applied {
			name = "applied but response lost"
		}
		t.Run(name, func(t *testing.T) {
			store := &mocks.MetaKv{}
			cp := &streamingpb.WALCheckpoint{TimeTick: 10, Term: 1}
			limit := mockey.Mock(mockey.GetMethod(store, "MaxTxnOps")).Return(128).Build()
			defer limit.UnPatch()
			load := mockey.Mock(mockey.GetMethod(store, "Load")).Return(checkpointBytesOf(t, cp), nil).Build()
			defer load.UnPatch()
			attempts := 0
			var component string
			failure := merr.WrapErrIoFailedReason("lost transaction response")
			write := mockey.Mock(mockey.GetMethod(store, "MultiSaveAndRemove")).To(
				func(_ context.Context, saves map[string]string, _ []string, _ ...predicates.Predicate) error {
					attempts++
					if applied || attempts > 1 {
						component = saves[buildSegmentAssignmentKey("p1", 1)]
					}
					if attempts == 1 {
						return failure
					}
					return nil
				}).Build()
			defer write.UnPatch()
			catalog := NewCataLog(store)
			snapshot := &metastore.WALRecoverySnapshot{
				ConsumeCheckpoint:  cp,
				SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{1: {SegmentId: 1}},
			}
			require.ErrorIs(t, catalog.SaveRecoverySnapshot(context.Background(), "p1", snapshot), failure)
			require.NoError(t, catalog.SaveRecoverySnapshot(context.Background(), "p1", snapshot))
			require.NotEmpty(t, component)
			require.Equal(t, 2, attempts)
		})
	}
}

func TestRecoverySnapshotRequiresInitialOwnershipBeforeComponents(t *testing.T) {
	store := &mocks.MetaKv{}
	load := mockey.Mock(mockey.GetMethod(store, "Load")).Return("", merr.ErrIoKeyNotFound).Build()
	defer load.UnPatch()
	err := NewCataLog(store).SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint:  &streamingpb.WALCheckpoint{TimeTick: 10, Term: 1},
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{1: {SegmentId: 1}},
	})
	require.ErrorContains(t, err, "initialize consume checkpoint")
}

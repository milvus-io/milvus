package streamingcoord

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func setBroadcastDeletionBatchSize(t *testing.T, value string) {
	t.Helper()
	paramtable.Init()
	limit := &paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum
	previous := limit.SwapTempValue(value)
	t.Cleanup(func() { limit.SwapTempValue(previous) })
}

func TestRemoveBroadcastTasksUsesBoundedExactKeys(t *testing.T) {
	setBroadcastDeletionBatchSize(t, "2")
	catalog, stored, kv := newTestCatalog(t)
	ctx := context.Background()
	ids := []uint64{1, 2, 3, 4, 5}
	for _, id := range append(slices.Clone(ids), 11) {
		require.NoError(t, catalog.SaveBroadcastTask(ctx, id, &streamingpb.BroadcastTask{
			State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE,
		}))
	}
	stored["querycoord-collection-loadinfo/1"] = "keep"
	var batches [][]string
	kv.EXPECT().MultiRemove(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, keys []string) error {
			batches = append(batches, slices.Clone(keys))
			for _, key := range keys {
				delete(stored, key)
			}
			return nil
		}).Times(3)
	require.NoError(t, catalog.RemoveBroadcastTasks(ctx, nil))
	require.NoError(t, catalog.RemoveBroadcastTasks(ctx, ids))
	require.Equal(t, [][]string{
		{buildBroadcastTaskPath(1), buildBroadcastTaskPath(2)},
		{buildBroadcastTaskPath(3), buildBroadcastTaskPath(4)},
		{buildBroadcastTaskPath(5)},
	}, batches)
	require.Len(t, stored, 2)
	require.Contains(t, stored, buildBroadcastTaskPath(11), "deletion must not match key prefixes")
	require.Equal(t, "keep", stored["querycoord-collection-loadinfo/1"])
}

func TestRemoveBroadcastTasksRetriesLostCommitResponse(t *testing.T) {
	setBroadcastDeletionBatchSize(t, "2")
	catalog, stored, kv := newTestCatalog(t)
	ids := []uint64{1, 2, 3}
	for _, id := range ids {
		stored[buildBroadcastTaskPath(id)] = "tombstone"
	}
	var attempts [][]string
	kv.EXPECT().MultiRemove(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, keys []string) error {
			attempts = append(attempts, slices.Clone(keys))
			for _, key := range keys {
				delete(stored, key)
			}
			if len(attempts) == 1 {
				return merr.WrapErrServiceUnavailable("commit response lost")
			}
			return nil
		}).Times(3)
	require.NoError(t, catalog.RemoveBroadcastTasks(context.Background(), ids))
	require.Equal(t, attempts[0], attempts[1], "the real reliable-write wrapper must retry the same deletion")
	require.Equal(t, []string{buildBroadcastTaskPath(3)}, attempts[2])
	require.Empty(t, stored)
}

func TestRemoveBroadcastTasksCancellationAfterPartialProgress(t *testing.T) {
	setBroadcastDeletionBatchSize(t, "2")
	catalog, stored, kv := newTestCatalog(t)
	ids := []uint64{1, 2, 3, 4, 5}
	for _, id := range ids {
		require.NoError(t, catalog.SaveBroadcastTask(context.Background(), id, &streamingpb.BroadcastTask{
			State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE,
		}))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts [][]string
	kv.EXPECT().MultiRemove(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, keys []string) error {
			attempts = append(attempts, slices.Clone(keys))
			if len(attempts) == 2 {
				cancel()
				return ctx.Err()
			}
			for _, key := range keys {
				delete(stored, key)
			}
			return nil
		})
	require.ErrorIs(t, catalog.RemoveBroadcastTasks(ctx, ids), context.Canceled)
	require.Len(t, attempts, 2, "do not issue further batches after cancellation")
	tasks, err := catalog.ListBroadcastTask(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	for _, task := range tasks {
		require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, task.State)
	}
	require.NoError(t, catalog.RemoveBroadcastTasks(context.Background(), ids))
	require.Equal(t, attempts[0], attempts[2], "retry tolerates already removed IDs")
	require.Empty(t, stored)
	require.ErrorIs(t, catalog.RemoveBroadcastTasks(ctx, ids), context.Canceled)
	require.Len(t, attempts, 5, "an already canceled request must not access the KV store")
}

func TestRemoveBroadcastTasksUsesPositiveBatchLimit(t *testing.T) {
	for _, limit := range []string{"0", "-1"} {
		t.Run(limit, func(t *testing.T) {
			setBroadcastDeletionBatchSize(t, limit)
			catalog, _, kv := newTestCatalog(t)
			kv.EXPECT().MultiRemove(mock.Anything, []string{buildBroadcastTaskPath(1)}).
				Return(nil).Once()
			require.NoError(t, catalog.RemoveBroadcastTasks(context.Background(), []uint64{1}))
		})
	}
}

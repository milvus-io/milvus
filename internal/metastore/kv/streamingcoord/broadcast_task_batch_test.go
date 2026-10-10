package streamingcoord

import (
	"context"
	"maps"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/mocks/mock_kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSaveBroadcastTasksAtomicBatch(t *testing.T) {
	mockey.PatchConvey("existing task keys share one atomic update or deletion", t, func() {
		storage := map[string]string{}
		kv := &mock_kv.MockMetaKv{}
		writes := 0
		loseReply := false
		expectedSaves, expectedRemoves := 1, 0
		mockey.Mock((*mock_kv.MockMetaKv).MultiSaveAndRemove).To(func(_ *mock_kv.MockMetaKv, ctx context.Context, saves map[string]string, removes []string, preds ...predicates.Predicate) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			require.Empty(t, preds)
			require.Len(t, saves, expectedSaves)
			require.Len(t, removes, expectedRemoves)
			for key := range saves {
				require.True(t, strings.HasPrefix(key, BroadcastTaskPrefix))
			}
			for _, key := range removes {
				require.True(t, strings.HasPrefix(key, BroadcastTaskPrefix))
			}
			maps.Copy(storage, saves)
			for _, key := range removes {
				delete(storage, key)
			}
			writes++
			if loseReply {
				loseReply = false
				return merr.WrapErrServiceUnavailable("reply lost after atomic commit")
			}
			return nil
		}).Build()
		mockey.Mock((*mock_kv.MockMetaKv).LoadWithPrefix).To(func(_ *mock_kv.MockMetaKv, ctx context.Context, prefix string) ([]string, []string, error) {
			require.Equal(t, BroadcastTaskPrefix, prefix)
			var keys, values []string
			for key, value := range storage {
				keys = append(keys, key)
				values = append(values, value)
			}
			return keys, values, nil
		}).Build()
		c := NewCataLog(kv)
		ctx := context.Background()
		begin := &streamingpb.BroadcastTask{State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TXN_INFLIGHT}
		require.NoError(t, c.SaveBroadcastTasks(ctx, map[uint64]*streamingpb.BroadcastTask{7: begin}))
		body := &streamingpb.BroadcastTask{State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE}
		require.NoError(t, c.SaveBroadcastTasks(ctx, map[uint64]*streamingpb.BroadcastTask{8: body}))
		untouchedBody := storage[buildBroadcastTaskPath(8)]
		closed := proto.Clone(begin).(*streamingpb.BroadcastTask)
		closed.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
		expectedSaves = 2
		loseReply = true
		require.NoError(t, c.SaveBroadcastTasks(ctx, map[uint64]*streamingpb.BroadcastTask{7: closed, 9: closed}))
		require.Equal(t, untouchedBody, storage[buildBroadcastTaskPath(8)], "closing only updates Begin and Commit")
		tasks, err := c.ListBroadcastTask(ctx)
		require.NoError(t, err)
		require.Len(t, tasks, 3)
		for _, task := range tasks {
			require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, task.State)
		}
		expectedSaves, expectedRemoves = 0, 3
		deletions := map[uint64]*streamingpb.BroadcastTask{}
		for _, id := range []uint64{7, 8, 9} {
			deletions[id] = &streamingpb.BroadcastTask{State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE}
		}
		canceled, stop := context.WithCancel(ctx)
		stop()
		require.ErrorIs(t, c.SaveBroadcastTasks(canceled, deletions), context.Canceled)
		require.Len(t, storage, 3, "failed GC leaves every member intact")
		require.NoError(t, c.SaveBroadcastTasks(ctx, deletions))
		require.Empty(t, storage)
		require.Equal(t, 5, writes)
		invalid := &streamingpb.BroadcastTask{Message: &messagespb.Message{Properties: map[string]string{"bad": string([]byte{0xff})}}}
		require.Error(t, c.SaveBroadcastTasks(ctx, map[uint64]*streamingpb.BroadcastTask{1: invalid}))
		require.Empty(t, storage)
	})
}

package dml

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestInvalidateWriteSchemaOnAnyShardMismatch(t *testing.T) {
	cache := newTestCache()
	var invalidated []int64
	patch := mockey.Mock((*metacache.MetaCache).RemoveCollectionsByID).To(func(got *metacache.MetaCache, _ context.Context, id int64) []string {
		require.Same(t, cache, got)
		invalidated = append(invalidated, id)
		return nil
	}).Build()
	defer patch.UnPatch()
	for _, responses := range [][]streaming.AppendResponse{
		{{}},
		{{Error: status.NewOnShutdownError("not a schema error")}},
	} {
		invalidateMismatchedWriteSchema(t.Context(), cache, 10, streaming.AppendResponses{Responses: responses})
	}
	require.Empty(t, invalidated)
	responses := streaming.AppendResponses{Responses: []streaming.AppendResponse{
		{Error: status.NewOnShutdownError("first shard failed differently")},
		{}, // A successful shard must never be replayed by cache invalidation.
		{Error: status.NewSchemaVersionMismatch("write schema advanced")},
		{Error: status.NewSchemaVersionMismatch("another shard advanced")},
	}}
	invalidateMismatchedWriteSchema(t.Context(), cache, 10, responses)
	require.Equal(t, []int64{10}, invalidated)
	require.Len(t, responses.Responses, 4)
	require.NoError(t, responses.Responses[1].Error)
}

type invalidationTestWAL struct{ streaming.WALAccesser }

func TestInsertAppendInvalidatesLaterShardWithoutReplay(t *testing.T) {
	cache := newTestCache()
	task := &InsertTask{
		baseTask: baseTask{MetaCache: cache}, ctx: t.Context(), collectionID: 10,
		vChannels: []string{"p_10v0", "p_10v1", "p_10v2"},
		insertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{Base: &commonpb.MsgBase{}}},
		result:    &milvuspb.MutationResult{},
	}
	wal := &invalidationTestWAL{}
	patch := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer patch.UnPatch()
	patch = mockey.Mock(repackInsertDataForStreamingService).Return([]message.MutableMessage{}, nil).Build()
	defer patch.UnPatch()
	appends, invalidations := 0, 0
	patch = mockey.Mock((*invalidationTestWAL).AppendMessagesWithOptions).To(func(_ *invalidationTestWAL, _ context.Context, _ []message.MutableMessage, _ ...streaming.AppendOption) streaming.AppendResponses {
		appends++
		return streaming.AppendResponses{Responses: []streaming.AppendResponse{
			{Error: status.NewOnShutdownError("first shard shutting down")},
			{}, // This shard committed and must not be replayed.
			{Error: status.NewSchemaVersionMismatch("later shard advanced")},
		}}
	}).Build()
	defer patch.UnPatch()
	patch = mockey.Mock((*metacache.MetaCache).RemoveCollectionsByID).To(func(_ *metacache.MetaCache, _ context.Context, id int64) []string {
		require.EqualValues(t, 10, id)
		invalidations++
		return nil
	}).Build()
	defer patch.UnPatch()
	require.NoError(t, task.Execute(t.Context()))
	require.Equal(t, 1, appends)
	require.Equal(t, 1, invalidations)
	require.Contains(t, task.result.GetStatus().GetReason(), "first shard shutting down")
}

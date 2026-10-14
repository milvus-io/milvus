package dml

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
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

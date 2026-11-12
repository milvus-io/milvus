package dml

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

// A WAL ALTER can advance writes before the read schema is published. Expire
// the write selector on any shard's mismatch so the next request fetches the
// active encoding schema, even while DescribeCollection still returns the old
// read version. This does not replay a possibly partially committed request.
func invalidateMismatchedWriteSchema(ctx context.Context, cache Cache, collectionID int64, response streaming.AppendResponses) {
	for _, shard := range response.Responses {
		if shard.Error != nil && status.AsStreamingError(shard.Error).IsSchemaVersionMismatch() {
			cache.RemoveCollectionsByID(ctx, collectionID)
			return
		}
	}
}

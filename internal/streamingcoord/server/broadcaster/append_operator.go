package broadcaster

import (
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
)

// NewAppendOperator creates an append operator to handle the incoming messages for broadcaster.
func NewAppendOperator() AppendOperator {
	if streamingutil.IsStreamingServiceEnabled() {
		return streaming.WAL()
	}
	return nil
}

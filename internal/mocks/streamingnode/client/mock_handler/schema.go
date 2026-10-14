//go:build test && dynamic

package mock_handler

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// GetCollectionSchema is patched with mockey by schema-aware tests. Legacy
// handler tests do not exercise schema discovery.
func (*MockHandlerClient) GetCollectionSchema(context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
	panic("GetCollectionSchema must be patched with mockey")
}

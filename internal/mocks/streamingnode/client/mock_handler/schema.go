//go:build test && dynamic

package mock_handler

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// GetCollectionSchema is a mockey patch point for schema access tests.
func (*MockHandlerClient) GetCollectionSchema(context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
	panic("GetCollectionSchema must be patched with mockey")
}

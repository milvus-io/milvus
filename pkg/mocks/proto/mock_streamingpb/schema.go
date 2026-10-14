//go:build test && dynamic

package mock_streamingpb

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// GetCollectionSchema is a mockey patch point for schema-aware tests.
func (*MockStreamingNodeHandlerServiceClient) GetCollectionSchema(context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
	panic("GetCollectionSchema must be patched with mockey")
}

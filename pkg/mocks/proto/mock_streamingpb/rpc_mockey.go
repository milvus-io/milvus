//go:build test && dynamic

package mock_streamingpb

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// SubscribeTransform completes the generated legacy client interface. Tests of
// the new RPC use mockey or the real gRPC service, without expanding testify mocks.
func (*MockStreamingNodeHandlerServiceClient) SubscribeTransform(context.Context, ...grpc.CallOption) (streamingpb.StreamingNodeHandlerService_SubscribeTransformClient, error) {
	panic("SubscribeTransform must be patched with mockey")
}

// ValidateRuntime completes the RPC already declared by the QueryView proto.
func (*MockStreamingNodeManagerServiceClient) ValidateRuntime(context.Context, *streamingpb.StreamingNodeManagerValidateRuntimeRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeManagerValidateRuntimeResponse, error) {
	panic("ValidateRuntime must be patched with mockey")
}

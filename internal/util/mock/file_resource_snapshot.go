//go:build test && dynamic

package mock

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

// Tests using this RPC provide its behavior with mockey.
func (*GrpcRootCoordClient) GetFileResourceSnapshot(context.Context, *milvuspb.ListFileResourcesRequest, ...grpc.CallOption) (*rootcoordpb.FileResourceSnapshot, error) {
	panic("patch GetFileResourceSnapshot with mockey")
}

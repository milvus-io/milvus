//go:build test && dynamic

package mocks

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

// These new RPC stubs are patched with mockey by tests using the pull path.
func (*MockMixCoordClient) GetFileResourceSnapshot(context.Context, *milvuspb.ListFileResourcesRequest, ...grpc.CallOption) (*rootcoordpb.FileResourceSnapshot, error) {
	panic("patch GetFileResourceSnapshot with mockey")
}

func (*MockRootCoordClient) GetFileResourceSnapshot(context.Context, *milvuspb.ListFileResourcesRequest, ...grpc.CallOption) (*rootcoordpb.FileResourceSnapshot, error) {
	panic("patch GetFileResourceSnapshot with mockey")
}

func (*MixCoord) GetFileResourceSnapshot(context.Context, *milvuspb.ListFileResourcesRequest) (*rootcoordpb.FileResourceSnapshot, error) {
	panic("patch GetFileResourceSnapshot with mockey")
}

func (*MockRootCoord) GetFileResourceSnapshot(context.Context, *milvuspb.ListFileResourcesRequest) (*rootcoordpb.FileResourceSnapshot, error) {
	panic("patch GetFileResourceSnapshot with mockey")
}

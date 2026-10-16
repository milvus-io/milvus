package ddl

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// QueryView compatibility patch points for the mock moved by the Proxy package
// split. Tests needing these APIs supply behavior with mockey.

func (coord *MixCoordMock) EnsureCollectionReady(ctx context.Context, req *querypb.EnsureCollectionReadyRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("patch QueryView client method with mockey")
}

func (coord *MixCoordMock) GetQueryViewLoadInfo(ctx context.Context, in *querypb.GetQueryViewLoadInfoRequest, opts ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
	panic("patch QueryView client method with mockey")
}

func (coord *MixCoordMock) GetStreamingNodeQueryViewResources(ctx context.Context, in *datapb.GetStreamingNodeQueryViewResourcesRequest, opts ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
	panic("patch QueryView client method with mockey")
}

func (coord *MixCoordMock) WatchQueryViewSegmentLoadInfo(ctx context.Context, opts ...grpc.CallOption) (querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient, error) {
	panic("patch QueryView client method with mockey")
}

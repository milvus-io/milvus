package walmanager

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type testMixCoordClient struct {
	*mocks.MockMixCoordClient
}

func (c testMixCoordClient) GetQueryViewSegmentLoadInfo(context.Context, *querypb.GetQueryViewSegmentLoadInfoRequest, ...grpc.CallOption) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
	return &querypb.GetQueryViewSegmentLoadInfoResponse{}, nil
}

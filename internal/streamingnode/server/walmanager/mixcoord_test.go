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

func (c testMixCoordClient) WatchQueryViewSegmentLoadInfo(context.Context, ...grpc.CallOption) (querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient, error) {
	return nil, nil
}

//go:build test && dynamic

package mocks

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// These interface adapters are patched with mockey in tests of the new RPC.
// generate-mockery-types filters their generated counterparts and EXPECT helpers
// with scripts/mockery_adapter_filter, keeping these methods authoritative.
func (*MixCoord) GetDataViewRetention(context.Context, *datapb.GetDataViewRetentionRequest) (*datapb.GetDataViewRetentionResponse, error) {
	return nil, merr.Wrap(merr.ErrServiceUnimplemented, "GetDataViewRetention is not configured by this test")
}

func (*MockDataCoord) GetDataViewRetention(context.Context, *datapb.GetDataViewRetentionRequest) (*datapb.GetDataViewRetentionResponse, error) {
	return nil, merr.Wrap(merr.ErrServiceUnimplemented, "GetDataViewRetention is not configured by this test")
}

func (*MockMixCoordClient) GetDataViewRetention(context.Context, *datapb.GetDataViewRetentionRequest, ...grpc.CallOption) (*datapb.GetDataViewRetentionResponse, error) {
	return nil, merr.Wrap(merr.ErrServiceUnimplemented, "GetDataViewRetention is not configured by this test")
}

func (*MockDataCoordClient) GetDataViewRetention(context.Context, *datapb.GetDataViewRetentionRequest, ...grpc.CallOption) (*datapb.GetDataViewRetentionResponse, error) {
	return nil, merr.Wrap(merr.ErrServiceUnimplemented, "GetDataViewRetention is not configured by this test")
}

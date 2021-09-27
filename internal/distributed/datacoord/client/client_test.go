package grpcdatacoordclient

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type MockDataCoordClient struct {
	err error
}

func (m *MockDataCoordClient) GetComponentStates(ctx context.Context, in *internalpb.GetComponentStatesRequest, opts ...grpc.CallOption) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{}, m.err
}

func (m *MockDataCoordClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.err
}

func (m *MockDataCoordClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.err
}

func (m *MockDataCoordClient) Flush(ctx context.Context, in *datapb.FlushRequest, opts ...grpc.CallOption) (*datapb.FlushResponse, error) {
	return &datapb.FlushResponse{}, m.err
}

func (m *MockDataCoordClient) AssignSegmentID(ctx context.Context, in *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	return &datapb.AssignSegmentIDResponse{}, m.err
}

func (m *MockDataCoordClient) GetSegmentInfo(ctx context.Context, in *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	return &datapb.GetSegmentInfoResponse{}, m.err
}

func (m *MockDataCoordClient) GetSegmentStates(ctx context.Context, in *datapb.GetSegmentStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentStatesResponse, error) {
	return &datapb.GetSegmentStatesResponse{}, m.err
}

func (m *MockDataCoordClient) GetInsertBinlogPaths(ctx context.Context, in *datapb.GetInsertBinlogPathsRequest, opts ...grpc.CallOption) (*datapb.GetInsertBinlogPathsResponse, error) {
	return &datapb.GetInsertBinlogPathsResponse{}, m.err
}

func (m *MockDataCoordClient) GetCollectionStatistics(ctx context.Context, in *datapb.GetCollectionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetCollectionStatisticsResponse, error) {
	return &datapb.GetCollectionStatisticsResponse{}, m.err
}

func (m *MockDataCoordClient) GetPartitionStatistics(ctx context.Context, in *datapb.GetPartitionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetPartitionStatisticsResponse, error) {
	return &datapb.GetPartitionStatisticsResponse{}, m.err
}

func (m *MockDataCoordClient) GetSegmentInfoChannel(ctx context.Context, in *datapb.GetSegmentInfoChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.err
}

func (m *MockDataCoordClient) SaveBinlogPaths(ctx context.Context, in *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockDataCoordClient) GetRecoveryInfo(ctx context.Context, in *datapb.GetRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponse, error) {
	return &datapb.GetRecoveryInfoResponse{}, m.err
}

func (m *MockDataCoordClient) GetFlushedSegments(ctx context.Context, in *datapb.GetFlushedSegmentsRequest, opts ...grpc.CallOption) (*datapb.GetFlushedSegmentsResponse, error) {
	return &datapb.GetFlushedSegmentsResponse{}, m.err
}

func (m *MockDataCoordClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.err
}

func Test_NewClient(t *testing.T) {
	proxy.Params.InitOnce()

	ctx := context.Background()
	client, err := NewClient(ctx, proxy.Params.MetaRootPath, proxy.Params.EtcdEndpoints)
	assert.Nil(t, err)
	assert.NotNil(t, client)

	err = client.Init()
	assert.Nil(t, err)

	err = client.Start()
	assert.Nil(t, err)

	err = client.Register()
	assert.Nil(t, err)

	checkFunc := func(retNotNil bool) {
		retCheck := func(notNil bool, ret interface{}, err error) {
			if notNil {
				assert.NotNil(t, ret)
				assert.Nil(t, err)
			} else {
				assert.Nil(t, ret)
				assert.NotNil(t, err)
			}
		}

		r1, err := client.GetComponentStates(ctx)
		retCheck(retNotNil, r1, err)

		r2, err := client.GetTimeTickChannel(ctx)
		retCheck(retNotNil, r2, err)

		r3, err := client.GetStatisticsChannel(ctx)
		retCheck(retNotNil, r3, err)

		r4, err := client.Flush(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.AssignSegmentID(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.GetSegmentInfo(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.GetSegmentStates(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.GetInsertBinlogPaths(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.GetCollectionStatistics(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.GetPartitionStatistics(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.GetSegmentInfoChannel(ctx)
		retCheck(retNotNil, r11, err)

		// r12, err := client.SaveBinlogPaths(ctx, nil)
		// retCheck(retNotNil, r12, err)

		r13, err := client.GetRecoveryInfo(ctx, nil)
		retCheck(retNotNil, r13, err)

		r14, err := client.GetFlushedSegments(ctx, nil)
		retCheck(retNotNil, r14, err)

		r15, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r15, err)
	}

	client.getGrpcClient = func() (datapb.DataCoordClient, error) {
		return &MockDataCoordClient{err: nil}, errors.New("dummy")
	}
	checkFunc(false)

	// special case since this method didn't use recall()
	ret, err := client.SaveBinlogPaths(ctx, nil)
	assert.NotNil(t, ret)
	assert.Nil(t, err)

	client.getGrpcClient = func() (datapb.DataCoordClient, error) {
		return &MockDataCoordClient{err: errors.New("dummy")}, nil
	}
	checkFunc(false)

	// special case since this method didn't use recall()
	ret, err = client.SaveBinlogPaths(ctx, nil)
	assert.NotNil(t, ret)
	assert.NotNil(t, err)

	client.getGrpcClient = func() (datapb.DataCoordClient, error) {
		return &MockDataCoordClient{err: nil}, nil
	}
	checkFunc(true)

	err = client.Stop()
	assert.Nil(t, err)
}

package grpcquerynodeclient

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type MockQueryNodeClient struct {
	err error
}

func (m *MockQueryNodeClient) GetComponentStates(ctx context.Context, in *internalpb.GetComponentStatesRequest, opts ...grpc.CallOption) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{}, m.err
}

func (m *MockQueryNodeClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.err
}
func (m *MockQueryNodeClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.err
}

func (m *MockQueryNodeClient) AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryNodeClient) RemoveQueryChannel(ctx context.Context, in *querypb.RemoveQueryChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryNodeClient) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryNodeClient) LoadSegments(ctx context.Context, in *querypb.LoadSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryNodeClient) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryNodeClient) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryNodeClient) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryNodeClient) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	return &querypb.GetSegmentInfoResponse{}, m.err
}

func (m *MockQueryNodeClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.err
}

func Test_NewClient(t *testing.T) {
	proxy.Params.InitOnce()

	ctx := context.Background()
	client, err := NewClient(ctx, "")
	assert.Nil(t, client)
	assert.NotNil(t, err)

	client, err = NewClient(ctx, "test")
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

		r4, err := client.AddQueryChannel(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.RemoveQueryChannel(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.WatchDmChannels(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.LoadSegments(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.ReleaseCollection(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.ReleasePartitions(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.ReleaseSegments(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.GetSegmentInfo(ctx, nil)
		retCheck(retNotNil, r11, err)

		r12, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r12, err)
	}

	client.getGrpcClient = func() (querypb.QueryNodeClient, error) {
		return &MockQueryNodeClient{err: nil}, errors.New("dummy")
	}
	checkFunc(false)

	client.getGrpcClient = func() (querypb.QueryNodeClient, error) {
		return &MockQueryNodeClient{err: errors.New("dummy")}, nil
	}
	checkFunc(false)

	client.getGrpcClient = func() (querypb.QueryNodeClient, error) {
		return &MockQueryNodeClient{err: nil}, nil
	}
	checkFunc(true)

	err = client.Stop()
	assert.Nil(t, err)
}

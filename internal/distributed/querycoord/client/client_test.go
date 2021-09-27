// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package grpcquerycoordclient

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

type MockQueryCoordClient struct {
	err error
}

func (m *MockQueryCoordClient) GetComponentStates(ctx context.Context, in *internalpb.GetComponentStatesRequest, opts ...grpc.CallOption) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{}, m.err
}

func (m *MockQueryCoordClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.err
}

func (m *MockQueryCoordClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.err
}

func (m *MockQueryCoordClient) ShowCollections(ctx context.Context, in *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
	return &querypb.ShowCollectionsResponse{}, m.err
}

func (m *MockQueryCoordClient) ShowPartitions(ctx context.Context, in *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error) {
	return &querypb.ShowPartitionsResponse{}, m.err
}

func (m *MockQueryCoordClient) LoadPartitions(ctx context.Context, in *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryCoordClient) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryCoordClient) LoadCollection(ctx context.Context, in *querypb.LoadCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryCoordClient) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockQueryCoordClient) CreateQueryChannel(ctx context.Context, in *querypb.CreateQueryChannelRequest, opts ...grpc.CallOption) (*querypb.CreateQueryChannelResponse, error) {
	return &querypb.CreateQueryChannelResponse{}, m.err
}

func (m *MockQueryCoordClient) GetPartitionStates(ctx context.Context, in *querypb.GetPartitionStatesRequest, opts ...grpc.CallOption) (*querypb.GetPartitionStatesResponse, error) {
	return &querypb.GetPartitionStatesResponse{}, m.err
}

func (m *MockQueryCoordClient) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	return &querypb.GetSegmentInfoResponse{}, m.err
}

func (m *MockQueryCoordClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
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

		r4, err := client.ShowCollections(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.ShowPartitions(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.LoadPartitions(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.ReleasePartitions(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.ShowCollections(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.LoadCollection(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.ReleaseCollection(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.CreateQueryChannel(ctx, nil)
		retCheck(retNotNil, r11, err)

		r12, err := client.ShowPartitions(ctx, nil)
		retCheck(retNotNil, r12, err)

		r13, err := client.GetPartitionStates(ctx, nil)
		retCheck(retNotNil, r13, err)

		r14, err := client.GetSegmentInfo(ctx, nil)
		retCheck(retNotNil, r14, err)

		r15, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r15, err)
	}

	client.getGrpcClient = func() (querypb.QueryCoordClient, error) {
		return &MockQueryCoordClient{err: nil}, errors.New("dummy")
	}
	checkFunc(false)

	client.getGrpcClient = func() (querypb.QueryCoordClient, error) {
		return &MockQueryCoordClient{err: errors.New("dummy")}, nil
	}
	checkFunc(false)

	client.getGrpcClient = func() (querypb.QueryCoordClient, error) {
		return &MockQueryCoordClient{err: nil}, nil
	}
	checkFunc(true)

	err = client.Stop()
	assert.Nil(t, err)
}

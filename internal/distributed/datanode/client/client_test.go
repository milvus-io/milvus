// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcdatanodeclient

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

type MockDataNodeClient struct {
	err error
}

func (m *MockDataNodeClient) GetComponentStates(ctx context.Context, in *internalpb.GetComponentStatesRequest, opts ...grpc.CallOption) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{}, m.err
}

func (m *MockDataNodeClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.err
}

func (m *MockDataNodeClient) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockDataNodeClient) FlushSegments(ctx context.Context, in *datapb.FlushSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
}

func (m *MockDataNodeClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.err
}

func (m *MockDataNodeClient) Compaction(ctx context.Context, req *datapb.CompactionPlan, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.err
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

		r2, err := client.GetStatisticsChannel(ctx)
		retCheck(retNotNil, r2, err)

		r3, err := client.WatchDmChannels(ctx, nil)
		retCheck(retNotNil, r3, err)

		r4, err := client.FlushSegments(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.Compaction(ctx, nil)
		retCheck(retNotNil, r6, err)
	}

	client.getGrpcClient = func() (datapb.DataNodeClient, error) {
		return &MockDataNodeClient{err: nil}, errors.New("dummy")
	}
	checkFunc(false)

	client.getGrpcClient = func() (datapb.DataNodeClient, error) {
		return &MockDataNodeClient{err: errors.New("dummy")}, nil
	}
	checkFunc(false)

	client.getGrpcClient = func() (datapb.DataNodeClient, error) {
		return &MockDataNodeClient{err: nil}, nil
	}
	checkFunc(true)

	err = client.Stop()
	assert.Nil(t, err)
}

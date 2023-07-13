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

package grpcquerynode

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type MockRootCoord struct {
	types.RootCoord
	initErr  error
	startErr error
	regErr   error
	stopErr  error
	stateErr commonpb.ErrorCode
}

func (m *MockRootCoord) Init() error {
	return m.initErr
}

func (m *MockRootCoord) Start() error {
	return m.startErr
}

func (m *MockRootCoord) Stop() error {
	return m.stopErr
}

func (m *MockRootCoord) Register() error {
	return m.regErr
}

func (m *MockRootCoord) SetEtcdClient(client *clientv3.Client) {
}

func (m *MockRootCoord) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
		Status: &commonpb.Status{ErrorCode: m.stateErr},
	}, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func TestMain(m *testing.M) {
	paramtable.Init()
	os.Exit(m.Run())
}

func Test_NewServer(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	mockQN := mocks.NewMockQueryNode(t)
	mockQN.EXPECT().Start().Return(nil).Maybe()
	mockQN.EXPECT().Stop().Return(nil).Maybe()
	mockQN.EXPECT().Register().Return(nil).Maybe()
	mockQN.EXPECT().SetEtcdClient(mock.Anything).Maybe()
	mockQN.EXPECT().SetAddress(mock.Anything).Maybe()
	mockQN.EXPECT().UpdateStateCode(mock.Anything).Maybe()
	mockQN.EXPECT().Init().Return(nil).Maybe()
	server.querynode = mockQN

	t.Run("Run", func(t *testing.T) {
		err = server.Run()
		assert.NoError(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		mockQN.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				StateCode: commonpb.StateCode_Healthy,
			}}, nil)
		req := &milvuspb.GetComponentStatesRequest{}
		states, err := server.GetComponentStates(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.StateCode_Healthy, states.State.StateCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		mockQN.EXPECT().GetStatisticsChannel(mock.Anything).Return(&milvuspb.StringResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &internalpb.GetStatisticsChannelRequest{}
		resp, err := server.GetStatisticsChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		mockQN.EXPECT().GetTimeTickChannel(mock.Anything).Return(&milvuspb.StringResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &internalpb.GetTimeTickChannelRequest{}
		resp, err := server.GetTimeTickChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("WatchDmChannels", func(t *testing.T) {
		mockQN.EXPECT().WatchDmChannels(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.WatchDmChannelsRequest{}
		resp, err := server.WatchDmChannels(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("LoadSegments", func(t *testing.T) {
		mockQN.EXPECT().LoadSegments(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.LoadSegmentsRequest{}
		resp, err := server.LoadSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleaseCollection", func(t *testing.T) {
		mockQN.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.ReleaseCollectionRequest{}
		resp, err := server.ReleaseCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("LoadPartitions", func(t *testing.T) {
		mockQN.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.LoadPartitionsRequest{}
		resp, err := server.LoadPartitions(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleasePartitions", func(t *testing.T) {
		mockQN.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.ReleasePartitionsRequest{}
		resp, err := server.ReleasePartitions(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleaseSegments", func(t *testing.T) {
		mockQN.EXPECT().ReleaseSegments(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.ReleaseSegmentsRequest{}
		resp, err := server.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetSegmentInfo", func(t *testing.T) {
		mockQN.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(&querypb.GetSegmentInfoResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &querypb.GetSegmentInfoRequest{}
		resp, err := server.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		mockQN.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(
			&milvuspb.GetMetricsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &milvuspb.GetMetricsRequest{
			Request: "",
		}
		resp, err := server.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Search", func(t *testing.T) {
		mockQN.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &querypb.SearchRequest{}
		resp, err := server.Search(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("SearchSegments", func(t *testing.T) {
		mockQN.EXPECT().SearchSegments(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &querypb.SearchRequest{}
		resp, err := server.SearchSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("Query", func(t *testing.T) {
		mockQN.EXPECT().Query(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &querypb.QueryRequest{}
		resp, err := server.Query(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("QuerySegments", func(t *testing.T) {
		mockQN.EXPECT().QuerySegments(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)
		req := &querypb.QueryRequest{}
		resp, err := server.QuerySegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("SyncReplicaSegments", func(t *testing.T) {
		mockQN.EXPECT().SyncReplicaSegments(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		req := &querypb.SyncReplicaSegmentsRequest{}
		resp, err := server.SyncReplicaSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("ShowConfigurtaions", func(t *testing.T) {
		mockQN.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(&internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		req := &internalpb.ShowConfigurationsRequest{
			Pattern: "Cache",
		}
		resp, err := server.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	err = server.Stop()
	assert.NoError(t, err)
}

func Test_Run(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	mockQN := mocks.NewMockQueryNode(t)
	mockQN.EXPECT().Start().Return(errors.New("Failed")).Maybe()
	mockQN.EXPECT().Stop().Return(errors.New("Failed")).Maybe()
	mockQN.EXPECT().Register().Return(errors.New("Failed")).Maybe()
	mockQN.EXPECT().SetEtcdClient(mock.Anything).Maybe()
	mockQN.EXPECT().SetAddress(mock.Anything).Maybe()
	mockQN.EXPECT().UpdateStateCode(mock.Anything).Maybe()
	mockQN.EXPECT().Init().Return(nil).Maybe()
	server.querynode = mockQN
	err = server.Run()
	assert.Error(t, err)

	err = server.Run()
	assert.Error(t, err)

	err = server.Stop()
	assert.Error(t, err)
}

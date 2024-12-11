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

package grpcquerycoord

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tikv"
)

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
}

func Test_NewServer(t *testing.T) {
	parameters := []string{"tikv", "etcd"}
	for _, v := range parameters {
		paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, v)
		ctx := context.Background()
		getTiKVClient = func(cfg *paramtable.TiKVConfig) (*txnkv.Client, error) {
			return tikv.SetupLocalTxn(), nil
		}
		defer func() {
			getTiKVClient = tikv.GetTiKVClient
		}()
		server, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, server)

		mdc := mocks.NewMockDataCoordClient(t)

		mrc := mocks.NewMockRootCoordClient(t)

		mqc := getQueryCoord()
		successStatus := merr.Success()

		t.Run("Run", func(t *testing.T) {
			server.queryCoord = mqc
			server.dataCoord = mdc
			server.rootCoord = mrc

			err = server.Prepare()
			assert.NoError(t, err)
			err = server.Run()
			assert.NoError(t, err)
		})

		t.Run("GetComponentStates", func(t *testing.T) {
			mqc.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
				State: &milvuspb.ComponentInfo{
					NodeID:    0,
					Role:      "MockQueryCoord",
					StateCode: commonpb.StateCode_Healthy,
				},
				Status: successStatus,
			}, nil)

			req := &milvuspb.GetComponentStatesRequest{}
			states, err := server.GetComponentStates(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.StateCode_Healthy, states.State.StateCode)
		})

		t.Run("GetStatisticsChannel", func(t *testing.T) {
			req := &internalpb.GetStatisticsChannelRequest{}
			mqc.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(
				&milvuspb.StringResponse{
					Status: successStatus,
				}, nil,
			)
			resp, err := server.GetStatisticsChannel(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("GetTimeTickChannel", func(t *testing.T) {
			req := &internalpb.GetTimeTickChannelRequest{}
			mqc.EXPECT().GetTimeTickChannel(mock.Anything, mock.Anything).Return(
				&milvuspb.StringResponse{
					Status: successStatus,
				}, nil,
			)
			resp, err := server.GetTimeTickChannel(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("ShowCollections", func(t *testing.T) {
			mqc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(
				&querypb.ShowCollectionsResponse{
					Status: successStatus,
				}, nil,
			)
			resp, err := server.ShowCollections(ctx, nil)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("LoadCollection", func(t *testing.T) {
			mqc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
			resp, err := server.LoadCollection(ctx, nil)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})

		t.Run("ReleaseCollection", func(t *testing.T) {
			mqc.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
			resp, err := server.ReleaseCollection(ctx, nil)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})

		t.Run("ShowPartitions", func(t *testing.T) {
			mqc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{Status: successStatus}, nil)
			resp, err := server.ShowPartitions(ctx, nil)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})
		t.Run("GetPartitionStates", func(t *testing.T) {
			mqc.EXPECT().GetPartitionStates(mock.Anything, mock.Anything).Return(&querypb.GetPartitionStatesResponse{Status: successStatus}, nil)
			resp, err := server.GetPartitionStates(ctx, nil)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})

		t.Run("LoadPartitions", func(t *testing.T) {
			mqc.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(successStatus, nil)
			resp, err := server.LoadPartitions(ctx, nil)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})

		t.Run("ReleasePartitions", func(t *testing.T) {
			mqc.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(successStatus, nil)
			resp, err := server.ReleasePartitions(ctx, nil)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})

		t.Run("GetTimeTickChannel", func(t *testing.T) {
			mqc.EXPECT().GetTimeTickChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{Status: successStatus}, nil)
			resp, err := server.GetTimeTickChannel(ctx, nil)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		})

		t.Run("GetSegmentInfo", func(t *testing.T) {
			req := &querypb.GetSegmentInfoRequest{}
			mqc.EXPECT().GetSegmentInfo(mock.Anything, req).Return(&querypb.GetSegmentInfoResponse{Status: successStatus}, nil)
			resp, err := server.GetSegmentInfo(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("LoadBalance", func(t *testing.T) {
			req := &querypb.LoadBalanceRequest{}
			mqc.EXPECT().LoadBalance(mock.Anything, req).Return(successStatus, nil)
			resp, err := server.LoadBalance(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		})

		t.Run("GetMetrics", func(t *testing.T) {
			req := &milvuspb.GetMetricsRequest{
				Request: "",
			}
			mqc.EXPECT().GetMetrics(mock.Anything, req).Return(&milvuspb.GetMetricsResponse{Status: successStatus}, nil)
			resp, err := server.GetMetrics(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("CheckHealth", func(t *testing.T) {
			mqc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(
				&milvuspb.CheckHealthResponse{Status: successStatus, IsHealthy: true}, nil)
			ret, err := server.CheckHealth(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, true, ret.IsHealthy)
		})

		t.Run("CreateResourceGroup", func(t *testing.T) {
			mqc.EXPECT().CreateResourceGroup(mock.Anything, mock.Anything).Return(successStatus, nil)
			resp, err := server.CreateResourceGroup(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		})

		t.Run("DropResourceGroup", func(t *testing.T) {
			mqc.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).Return(successStatus, nil)
			resp, err := server.DropResourceGroup(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		})

		t.Run("TransferNode", func(t *testing.T) {
			mqc.EXPECT().TransferNode(mock.Anything, mock.Anything).Return(successStatus, nil)
			resp, err := server.TransferNode(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		})

		t.Run("TransferReplica", func(t *testing.T) {
			mqc.EXPECT().TransferReplica(mock.Anything, mock.Anything).Return(successStatus, nil)
			resp, err := server.TransferReplica(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		})

		t.Run("ListResourceGroups", func(t *testing.T) {
			req := &milvuspb.ListResourceGroupsRequest{}
			mqc.EXPECT().ListResourceGroups(mock.Anything, req).Return(&milvuspb.ListResourceGroupsResponse{Status: successStatus}, nil)
			resp, err := server.ListResourceGroups(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("DescribeResourceGroup", func(t *testing.T) {
			mqc.EXPECT().DescribeResourceGroup(mock.Anything, mock.Anything).Return(&querypb.DescribeResourceGroupResponse{Status: successStatus}, nil)
			resp, err := server.DescribeResourceGroup(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("ListCheckers", func(t *testing.T) {
			req := &querypb.ListCheckersRequest{}
			mqc.EXPECT().ListCheckers(mock.Anything, req).Return(&querypb.ListCheckersResponse{Status: successStatus}, nil)
			resp, err := server.ListCheckers(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("ActivateChecker", func(t *testing.T) {
			req := &querypb.ActivateCheckerRequest{}
			mqc.EXPECT().ActivateChecker(mock.Anything, req).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
			resp, err := server.ActivateChecker(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		})

		t.Run("DeactivateChecker", func(t *testing.T) {
			req := &querypb.DeactivateCheckerRequest{}
			mqc.EXPECT().DeactivateChecker(mock.Anything, req).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
			resp, err := server.DeactivateChecker(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		})

		t.Run("ListQueryNode", func(t *testing.T) {
			req := &querypb.ListQueryNodeRequest{}
			mqc.EXPECT().ListQueryNode(mock.Anything, req).Return(&querypb.ListQueryNodeResponse{Status: merr.Success()}, nil)
			resp, err := server.ListQueryNode(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("GetQueryNodeDistribution", func(t *testing.T) {
			req := &querypb.GetQueryNodeDistributionRequest{}
			mqc.EXPECT().GetQueryNodeDistribution(mock.Anything, req).Return(&querypb.GetQueryNodeDistributionResponse{Status: merr.Success()}, nil)
			resp, err := server.GetQueryNodeDistribution(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		})

		t.Run("SuspendBalance", func(t *testing.T) {
			req := &querypb.SuspendBalanceRequest{}
			mqc.EXPECT().SuspendBalance(mock.Anything, req).Return(merr.Success(), nil)
			resp, err := server.SuspendBalance(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		})

		t.Run("ResumeBalance", func(t *testing.T) {
			req := &querypb.ResumeBalanceRequest{}
			mqc.EXPECT().ResumeBalance(mock.Anything, req).Return(merr.Success(), nil)
			resp, err := server.ResumeBalance(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		})

		t.Run("SuspendNode", func(t *testing.T) {
			req := &querypb.SuspendNodeRequest{}
			mqc.EXPECT().SuspendNode(mock.Anything, req).Return(merr.Success(), nil)
			resp, err := server.SuspendNode(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		})

		t.Run("ResumeNode", func(t *testing.T) {
			req := &querypb.ResumeNodeRequest{}
			mqc.EXPECT().ResumeNode(mock.Anything, req).Return(merr.Success(), nil)
			resp, err := server.ResumeNode(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		})

		t.Run("TransferSegment", func(t *testing.T) {
			req := &querypb.TransferSegmentRequest{}
			mqc.EXPECT().TransferSegment(mock.Anything, req).Return(merr.Success(), nil)
			resp, err := server.TransferSegment(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		})

		t.Run("TransferChannel", func(t *testing.T) {
			req := &querypb.TransferChannelRequest{}
			mqc.EXPECT().TransferChannel(mock.Anything, req).Return(merr.Success(), nil)
			resp, err := server.TransferChannel(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		})

		t.Run("CheckQueryNodeDistribution", func(t *testing.T) {
			req := &querypb.CheckQueryNodeDistributionRequest{}
			mqc.EXPECT().CheckQueryNodeDistribution(mock.Anything, req).Return(merr.Success(), nil)
			resp, err := server.CheckQueryNodeDistribution(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		})

		err = server.Stop()
		assert.NoError(t, err)
	}
}

// This test will no longer return error immediately.
func TestServer_Run1(t *testing.T) {
	parameters := []string{"tikv", "etcd"}
	for _, v := range parameters {
		paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, v)
		t.Skip()
		ctx := context.Background()
		getTiKVClient = func(cfg *paramtable.TiKVConfig) (*txnkv.Client, error) {
			return tikv.SetupLocalTxn(), nil
		}
		defer func() {
			getTiKVClient = tikv.GetTiKVClient
		}()
		server, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, server)

		mqc := getQueryCoord()
		mqc.EXPECT().Start().Return(errors.New("error"))
		server.queryCoord = mqc
		err = server.Run()
		assert.Error(t, err)

		err = server.Stop()
		assert.NoError(t, err)
	}
}

func getQueryCoord() *mocks.MockQueryCoord {
	mqc := &mocks.MockQueryCoord{}
	mqc.EXPECT().Init().Return(nil)
	mqc.EXPECT().SetEtcdClient(mock.Anything)
	mqc.EXPECT().SetTiKVClient(mock.Anything)
	mqc.EXPECT().SetAddress(mock.Anything)
	mqc.EXPECT().SetRootCoordClient(mock.Anything).Return(nil)
	mqc.EXPECT().SetDataCoordClient(mock.Anything).Return(nil)
	mqc.EXPECT().UpdateStateCode(mock.Anything)
	mqc.EXPECT().Register().Return(nil)
	mqc.EXPECT().Start().Return(nil)
	mqc.EXPECT().Stop().Return(nil)
	return mqc
}

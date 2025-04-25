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

package grpcdatacoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/coordinator/coordclient"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tikv"
)

func Test_NewServer(t *testing.T) {
	paramtable.Init()
	coordclient.ResetRegistration()

	ctx := context.Background()
	mockDataCoord := mocks.NewMockDataCoord(t)
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)
	server.dataCoord = mockDataCoord

	t.Run("GetComponentStates", func(t *testing.T) {
		mockDataCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{}, nil)
		states, err := server.GetComponentStates(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		mockDataCoord.EXPECT().GetTimeTickChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{}, nil)
		resp, err := server.GetTimeTickChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		mockDataCoord.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{}, nil)
		resp, err := server.GetStatisticsChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetSegmentInfo", func(t *testing.T) {
		mockDataCoord.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(&datapb.GetSegmentInfoResponse{}, nil)
		resp, err := server.GetSegmentInfo(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("Flush", func(t *testing.T) {
		mockDataCoord.EXPECT().Flush(mock.Anything, mock.Anything).Return(&datapb.FlushResponse{}, nil)
		resp, err := server.Flush(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("AssignSegmentID", func(t *testing.T) {
		mockDataCoord.EXPECT().AssignSegmentID(mock.Anything, mock.Anything).Return(&datapb.AssignSegmentIDResponse{}, nil)
		resp, err := server.AssignSegmentID(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetSegmentStates", func(t *testing.T) {
		mockDataCoord.EXPECT().GetSegmentStates(mock.Anything, mock.Anything).Return(&datapb.GetSegmentStatesResponse{}, nil)
		resp, err := server.GetSegmentStates(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetInsertBinlogPaths", func(t *testing.T) {
		mockDataCoord.EXPECT().GetInsertBinlogPaths(mock.Anything, mock.Anything).Return(&datapb.GetInsertBinlogPathsResponse{}, nil)
		resp, err := server.GetInsertBinlogPaths(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetCollectionStatistics", func(t *testing.T) {
		mockDataCoord.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(&datapb.GetCollectionStatisticsResponse{}, nil)
		resp, err := server.GetCollectionStatistics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetPartitionStatistics", func(t *testing.T) {
		mockDataCoord.EXPECT().GetPartitionStatistics(mock.Anything, mock.Anything).Return(&datapb.GetPartitionStatisticsResponse{}, nil)
		resp, err := server.GetPartitionStatistics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetSegmentInfoChannel", func(t *testing.T) {
		mockDataCoord.EXPECT().GetSegmentInfoChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{}, nil)
		resp, err := server.GetSegmentInfoChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("SaveBinlogPaths", func(t *testing.T) {
		mockDataCoord.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.SaveBinlogPaths(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetRecoveryInfo", func(t *testing.T) {
		mockDataCoord.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponse{}, nil)
		resp, err := server.GetRecoveryInfo(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetChannelRecoveryInfo", func(t *testing.T) {
		mockDataCoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).Return(&datapb.GetChannelRecoveryInfoResponse{}, nil)
		resp, err := server.GetChannelRecoveryInfo(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetFlushedSegments", func(t *testing.T) {
		mockDataCoord.EXPECT().GetFlushedSegments(mock.Anything, mock.Anything).Return(&datapb.GetFlushedSegmentsResponse{}, nil)
		resp, err := server.GetFlushedSegments(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		mockDataCoord.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(&internalpb.ShowConfigurationsResponse{}, nil)
		resp, err := server.ShowConfigurations(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		mockDataCoord.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{}, nil)
		resp, err := server.GetMetrics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("WatchChannels", func(t *testing.T) {
		mockDataCoord.EXPECT().WatchChannels(mock.Anything, mock.Anything).Return(&datapb.WatchChannelsResponse{}, nil)
		resp, err := server.WatchChannels(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetFlushState", func(t *testing.T) {
		mockDataCoord.EXPECT().GetFlushState(mock.Anything, mock.Anything).Return(&milvuspb.GetFlushStateResponse{}, nil)
		resp, err := server.GetFlushState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetFlushAllState", func(t *testing.T) {
		mockDataCoord.EXPECT().GetFlushAllState(mock.Anything, mock.Anything).Return(&milvuspb.GetFlushAllStateResponse{}, nil)
		resp, err := server.GetFlushAllState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("DropVirtualChannel", func(t *testing.T) {
		mockDataCoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{}, nil)
		resp, err := server.DropVirtualChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ManualCompaction", func(t *testing.T) {
		mockDataCoord.EXPECT().ManualCompaction(mock.Anything, mock.Anything).Return(&milvuspb.ManualCompactionResponse{}, nil)
		resp, err := server.ManualCompaction(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetCompactionState", func(t *testing.T) {
		mockDataCoord.EXPECT().GetCompactionState(mock.Anything, mock.Anything).Return(&milvuspb.GetCompactionStateResponse{}, nil)
		resp, err := server.GetCompactionState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetCompactionStateWithPlans", func(t *testing.T) {
		mockDataCoord.EXPECT().GetCompactionStateWithPlans(mock.Anything, mock.Anything).Return(&milvuspb.GetCompactionPlansResponse{}, nil)
		resp, err := server.GetCompactionStateWithPlans(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("SetSegmentState", func(t *testing.T) {
		mockDataCoord.EXPECT().SetSegmentState(mock.Anything, mock.Anything).Return(&datapb.SetSegmentStateResponse{}, nil)
		resp, err := server.SetSegmentState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("UpdateSegmentStatistics", func(t *testing.T) {
		mockDataCoord.EXPECT().UpdateSegmentStatistics(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.UpdateSegmentStatistics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("UpdateChannelCheckpoint", func(t *testing.T) {
		mockDataCoord.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.UpdateChannelCheckpoint(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("MarkSegmentsDropped", func(t *testing.T) {
		mockDataCoord.EXPECT().MarkSegmentsDropped(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.MarkSegmentsDropped(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("BroadcastAlteredCollection", func(t *testing.T) {
		mockDataCoord.EXPECT().BroadcastAlteredCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.BroadcastAlteredCollection(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("CheckHealth", func(t *testing.T) {
		mockDataCoord.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: true}, nil)
		ret, err := server.CheckHealth(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, true, ret.IsHealthy)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		mockDataCoord.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		ret, err := server.CreateIndex(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		mockDataCoord.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&indexpb.DescribeIndexResponse{}, nil)
		ret, err := server.DescribeIndex(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetIndexStatistics", func(t *testing.T) {
		mockDataCoord.EXPECT().GetIndexStatistics(mock.Anything, mock.Anything).Return(&indexpb.GetIndexStatisticsResponse{}, nil)
		ret, err := server.GetIndexStatistics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("DropIndex", func(t *testing.T) {
		mockDataCoord.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		ret, err := server.DropIndex(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		mockDataCoord.EXPECT().GetIndexState(mock.Anything, mock.Anything).Return(&indexpb.GetIndexStateResponse{}, nil)
		ret, err := server.GetIndexState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		mockDataCoord.EXPECT().GetIndexBuildProgress(mock.Anything, mock.Anything).Return(&indexpb.GetIndexBuildProgressResponse{}, nil)
		ret, err := server.GetIndexBuildProgress(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetSegmentIndexState", func(t *testing.T) {
		mockDataCoord.EXPECT().GetSegmentIndexState(mock.Anything, mock.Anything).Return(&indexpb.GetSegmentIndexStateResponse{}, nil)
		ret, err := server.GetSegmentIndexState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetIndexInfos", func(t *testing.T) {
		mockDataCoord.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(&indexpb.GetIndexInfoResponse{}, nil)
		ret, err := server.GetIndexInfos(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GcControl", func(t *testing.T) {
		mockDataCoord.EXPECT().GcControl(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil)
		ret, err := server.GcControl(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("ListIndex", func(t *testing.T) {
		mockDataCoord.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(&indexpb.ListIndexesResponse{
			Status: merr.Success(),
		}, nil)
		ret, err := server.ListIndexes(ctx, &indexpb.ListIndexesRequest{})
		assert.NoError(t, err)
		assert.True(t, merr.Ok(ret.GetStatus()))
	})

	t.Run("NotifyDropPartition", func(t *testing.T) {
		mockDataCoord.EXPECT().NotifyDropPartition(mock.Anything, mock.Anything).Return(&datapb.NotifyDropPartitionResponse{
			Status: merr.Success(),
		}, nil)
		ret, err := server.NotifyDropPartition(ctx, &datapb.NotifyDropPartitionRequest{})
		assert.NoError(t, err)
		assert.True(t, merr.Ok(ret.GetStatus()))
	})
}

func Test_Run(t *testing.T) {
	paramtable.Init()

	t.Run("test run success", func(t *testing.T) {
		parameters := []string{"tikv", "etcd"}
		for _, v := range parameters {
			coordclient.ResetRegistration()
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

			mockDataCoord := mocks.NewMockDataCoord(t)
			server.dataCoord = mockDataCoord
			mockDataCoord.EXPECT().SetEtcdClient(mock.Anything)
			mockDataCoord.EXPECT().SetAddress(mock.Anything)
			mockDataCoord.EXPECT().SetTiKVClient(mock.Anything).Maybe()

			mockDataCoord.EXPECT().Init().Return(nil)
			mockDataCoord.EXPECT().Start().Return(nil)
			mockDataCoord.EXPECT().Register().Return(nil)
			err = server.Prepare()
			assert.NoError(t, err)
			err = server.Run()
			assert.NoError(t, err)

			mockDataCoord.EXPECT().Stop().Return(nil)
			err = server.Stop()
			assert.NoError(t, err)
		}
	})

	paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, "etcd")

	t.Run("test init error", func(t *testing.T) {
		coordclient.ResetRegistration()
		ctx := context.Background()
		server, err := NewServer(ctx, nil)
		assert.NotNil(t, server)
		assert.NoError(t, err)
		mockDataCoord := mocks.NewMockDataCoord(t)
		mockDataCoord.EXPECT().SetEtcdClient(mock.Anything)
		mockDataCoord.EXPECT().SetAddress(mock.Anything)
		mockDataCoord.EXPECT().Init().Return(errors.New("error"))
		server.dataCoord = mockDataCoord

		err = server.Prepare()
		assert.NoError(t, err)
		err = server.Run()
		assert.Error(t, err)

		mockDataCoord.EXPECT().Stop().Return(nil)
		server.Stop()
	})

	t.Run("test register error", func(t *testing.T) {
		coordclient.ResetRegistration()
		ctx := context.Background()
		server, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, server)
		mockDataCoord := mocks.NewMockDataCoord(t)
		mockDataCoord.EXPECT().SetEtcdClient(mock.Anything)
		mockDataCoord.EXPECT().SetAddress(mock.Anything)
		mockDataCoord.EXPECT().Init().Return(nil)
		mockDataCoord.EXPECT().Register().Return(errors.New("error"))
		server.dataCoord = mockDataCoord

		err = server.Prepare()
		assert.NoError(t, err)
		err = server.Run()
		assert.Error(t, err)

		mockDataCoord.EXPECT().Stop().Return(nil)
		server.Stop()
	})

	t.Run("test start error", func(t *testing.T) {
		coordclient.ResetRegistration()
		ctx := context.Background()
		server, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, server)
		mockDataCoord := mocks.NewMockDataCoord(t)
		mockDataCoord.EXPECT().SetEtcdClient(mock.Anything)
		mockDataCoord.EXPECT().SetAddress(mock.Anything)
		mockDataCoord.EXPECT().Init().Return(nil)
		mockDataCoord.EXPECT().Register().Return(nil)
		mockDataCoord.EXPECT().Start().Return(errors.New("error"))
		server.dataCoord = mockDataCoord

		err = server.Prepare()
		assert.NoError(t, err)
		err = server.Run()
		assert.Error(t, err)

		mockDataCoord.EXPECT().Stop().Return(nil)
		server.Stop()
	})

	t.Run("test stop error", func(t *testing.T) {
		coordclient.ResetRegistration()
		ctx := context.Background()
		server, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, server)
		mockDataCoord := mocks.NewMockDataCoord(t)
		mockDataCoord.EXPECT().SetEtcdClient(mock.Anything)
		mockDataCoord.EXPECT().SetAddress(mock.Anything)
		mockDataCoord.EXPECT().Init().Return(nil)
		mockDataCoord.EXPECT().Register().Return(nil)
		mockDataCoord.EXPECT().Start().Return(nil)
		server.dataCoord = mockDataCoord

		err = server.Prepare()
		assert.NoError(t, err)
		err = server.Run()
		assert.NoError(t, err)

		mockDataCoord.EXPECT().Stop().Return(errors.New("error"))
		err = server.Stop()
		assert.Error(t, err)
	})
}

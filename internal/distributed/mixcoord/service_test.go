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

package grpcmixcoord

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tikv"
)

type mockMix struct {
	types.MixCoordComponent
}

func (m *mockMix) CreateDatabase(ctx context.Context, request *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockMix) DropDatabase(ctx context.Context, request *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockMix) ListDatabases(ctx context.Context, request *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil
}

func (m *mockMix) AlterDatabase(ctx context.Context, request *rootcoordpb.AlterDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockMix) RenameCollection(ctx context.Context, request *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockMix) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return &milvuspb.CheckHealthResponse{
		IsHealthy: true,
	}, nil
}

func (m *mockMix) UpdateStateCode(commonpb.StateCode) {
}

func (m *mockMix) SetAddress(address string) {
}

func (m *mockMix) SetEtcdClient(etcdClient *clientv3.Client) {
}

func (m *mockMix) SetTiKVClient(client *txnkv.Client) {
}

func (m *mockMix) SetMixCoordClient(client types.MixCoordClient) {
}

func (m *mockMix) SetProxyCreator(func(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error)) {
}

func (m *mockMix) RegisterStreamingCoordGRPCService(server *grpc.Server) {
}

func (m *mockMix) Register() error {
	return nil
}

func (m *mockMix) Init() error {
	return nil
}

func (m *mockMix) Start() error {
	return nil
}

func (m *mockMix) Stop() error {
	return errors.New("stop error")
}

func (m *mockMix) GracefulStop() {
}

func TestRun(t *testing.T) {
	paramtable.Init()
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
		rcServerConfig := &paramtable.Get().RootCoordGrpcServerCfg
		oldPort := rcServerConfig.Port.GetValue()
		paramtable.Get().Save(rcServerConfig.Port.Key, "1000000")
		svr, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		err = svr.Prepare()
		assert.Error(t, err)
		assert.EqualError(t, err, "listen tcp: address 1000000: invalid port")
		paramtable.Get().Save(rcServerConfig.Port.Key, oldPort)

		svr, err = NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, svr)
		svr.mixCoord = &mockMix{}

		paramtable.Get().Save(rcServerConfig.Port.Key, fmt.Sprintf("%d", rand.Int()%100+10010))
		etcdConfig := &paramtable.Get().EtcdCfg

		rand.Seed(time.Now().UnixNano())
		randVal := rand.Int()
		rootPath := fmt.Sprintf("/%d/test", randVal)
		rootcoord.Params.Save("etcd.rootPath", rootPath)
		// Need to reset global etcd to follow new path
		// Need to reset global etcd to follow new path
		kvfactory.CloseEtcdClient()

		etcdCli, err := etcd.GetEtcdClient(
			etcdConfig.UseEmbedEtcd.GetAsBool(),
			etcdConfig.EtcdUseSSL.GetAsBool(),
			etcdConfig.Endpoints.GetAsStrings(),
			etcdConfig.EtcdTLSCert.GetValue(),
			etcdConfig.EtcdTLSKey.GetValue(),
			etcdConfig.EtcdTLSCACert.GetValue(),
			etcdConfig.EtcdTLSMinVersion.GetValue())
		assert.NoError(t, err)
		sessKey := path.Join(rootcoord.Params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot)
		_, err = etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
		assert.NoError(t, err)
		err = svr.Prepare()
		assert.NoError(t, err)
		err = svr.Run()
		assert.NoError(t, err)

		t.Run("CheckHealth", func(t *testing.T) {
			ret, err := svr.CheckHealth(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, true, ret.IsHealthy)
		})

		t.Run("RenameCollection", func(t *testing.T) {
			_, err := svr.RenameCollection(ctx, nil)
			assert.NoError(t, err)
		})

		t.Run("CreateDatabase", func(t *testing.T) {
			ret, err := svr.CreateDatabase(ctx, nil)
			assert.Nil(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, ret.ErrorCode)
		})

		t.Run("DropDatabase", func(t *testing.T) {
			ret, err := svr.DropDatabase(ctx, nil)
			assert.Nil(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, ret.ErrorCode)
		})

		t.Run("ListDatabases", func(t *testing.T) {
			ret, err := svr.ListDatabases(ctx, nil)
			assert.Nil(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, ret.GetStatus().GetErrorCode())
		})

		t.Run("AlterDatabase", func(t *testing.T) {
			ret, err := svr.AlterDatabase(ctx, nil)
			assert.Nil(t, err)
			assert.True(t, merr.Ok(ret))
		})

		err = svr.Stop()
		assert.NoError(t, err)
	}
}

func Test_NewServer(t *testing.T) {
	paramtable.Init()
	testutil.ResetEnvironment()

	ctx := context.Background()
	mockMixCoord := mocks.NewMixCoord(t)
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)
	server.mixCoord = mockMixCoord

	t.Run("GetComponentStates", func(t *testing.T) {
		mockMixCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{}, nil)
		states, err := server.GetComponentStates(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		mockMixCoord.EXPECT().GetTimeTickChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{}, nil)
		resp, err := server.GetTimeTickChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		mockMixCoord.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{}, nil)
		resp, err := server.GetStatisticsChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetSegmentInfo", func(t *testing.T) {
		mockMixCoord.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(&datapb.GetSegmentInfoResponse{}, nil)
		resp, err := server.GetSegmentInfo(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("Flush", func(t *testing.T) {
		mockMixCoord.EXPECT().Flush(mock.Anything, mock.Anything).Return(&datapb.FlushResponse{}, nil)
		resp, err := server.Flush(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("AssignSegmentID", func(t *testing.T) {
		mockMixCoord.EXPECT().AssignSegmentID(mock.Anything, mock.Anything).Return(&datapb.AssignSegmentIDResponse{}, nil)
		resp, err := server.AssignSegmentID(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetSegmentStates", func(t *testing.T) {
		mockMixCoord.EXPECT().GetSegmentStates(mock.Anything, mock.Anything).Return(&datapb.GetSegmentStatesResponse{}, nil)
		resp, err := server.GetSegmentStates(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetInsertBinlogPaths", func(t *testing.T) {
		mockMixCoord.EXPECT().GetInsertBinlogPaths(mock.Anything, mock.Anything).Return(&datapb.GetInsertBinlogPathsResponse{}, nil)
		resp, err := server.GetInsertBinlogPaths(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetCollectionStatistics", func(t *testing.T) {
		mockMixCoord.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(&datapb.GetCollectionStatisticsResponse{}, nil)
		resp, err := server.GetCollectionStatistics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetPartitionStatistics", func(t *testing.T) {
		mockMixCoord.EXPECT().GetPartitionStatistics(mock.Anything, mock.Anything).Return(&datapb.GetPartitionStatisticsResponse{}, nil)
		resp, err := server.GetPartitionStatistics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetSegmentInfoChannel", func(t *testing.T) {
		mockMixCoord.EXPECT().GetSegmentInfoChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{}, nil)
		resp, err := server.GetSegmentInfoChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("SaveBinlogPaths", func(t *testing.T) {
		mockMixCoord.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.SaveBinlogPaths(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetRecoveryInfo", func(t *testing.T) {
		mockMixCoord.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponse{}, nil)
		resp, err := server.GetRecoveryInfo(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetChannelRecoveryInfo", func(t *testing.T) {
		mockMixCoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).Return(&datapb.GetChannelRecoveryInfoResponse{}, nil)
		resp, err := server.GetChannelRecoveryInfo(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetFlushedSegments", func(t *testing.T) {
		mockMixCoord.EXPECT().GetFlushedSegments(mock.Anything, mock.Anything).Return(&datapb.GetFlushedSegmentsResponse{}, nil)
		resp, err := server.GetFlushedSegments(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		mockMixCoord.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(&internalpb.ShowConfigurationsResponse{}, nil)
		resp, err := server.ShowConfigurations(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		mockMixCoord.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{}, nil)
		resp, err := server.GetMetrics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("WatchChannels", func(t *testing.T) {
		mockMixCoord.EXPECT().WatchChannels(mock.Anything, mock.Anything).Return(&datapb.WatchChannelsResponse{}, nil)
		resp, err := server.WatchChannels(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetFlushState", func(t *testing.T) {
		mockMixCoord.EXPECT().GetFlushState(mock.Anything, mock.Anything).Return(&milvuspb.GetFlushStateResponse{}, nil)
		resp, err := server.GetFlushState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetFlushAllState", func(t *testing.T) {
		mockMixCoord.EXPECT().GetFlushAllState(mock.Anything, mock.Anything).Return(&milvuspb.GetFlushAllStateResponse{}, nil)
		resp, err := server.GetFlushAllState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("DropVirtualChannel", func(t *testing.T) {
		mockMixCoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{}, nil)
		resp, err := server.DropVirtualChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ManualCompaction", func(t *testing.T) {
		mockMixCoord.EXPECT().ManualCompaction(mock.Anything, mock.Anything).Return(&milvuspb.ManualCompactionResponse{}, nil)
		resp, err := server.ManualCompaction(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetCompactionState", func(t *testing.T) {
		mockMixCoord.EXPECT().GetCompactionState(mock.Anything, mock.Anything).Return(&milvuspb.GetCompactionStateResponse{}, nil)
		resp, err := server.GetCompactionState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetCompactionStateWithPlans", func(t *testing.T) {
		mockMixCoord.EXPECT().GetCompactionStateWithPlans(mock.Anything, mock.Anything).Return(&milvuspb.GetCompactionPlansResponse{}, nil)
		resp, err := server.GetCompactionStateWithPlans(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("SetSegmentState", func(t *testing.T) {
		mockMixCoord.EXPECT().SetSegmentState(mock.Anything, mock.Anything).Return(&datapb.SetSegmentStateResponse{}, nil)
		resp, err := server.SetSegmentState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("UpdateSegmentStatistics", func(t *testing.T) {
		mockMixCoord.EXPECT().UpdateSegmentStatistics(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.UpdateSegmentStatistics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("UpdateChannelCheckpoint", func(t *testing.T) {
		mockMixCoord.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.UpdateChannelCheckpoint(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("MarkSegmentsDropped", func(t *testing.T) {
		mockMixCoord.EXPECT().MarkSegmentsDropped(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.MarkSegmentsDropped(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("BroadcastAlteredCollection", func(t *testing.T) {
		mockMixCoord.EXPECT().BroadcastAlteredCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.BroadcastAlteredCollection(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("CheckHealth", func(t *testing.T) {
		mockMixCoord.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: true}, nil)
		ret, err := server.CheckHealth(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, true, ret.IsHealthy)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		mockMixCoord.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		ret, err := server.CreateIndex(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		mockMixCoord.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&indexpb.DescribeIndexResponse{}, nil)
		ret, err := server.DescribeIndex(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetIndexStatistics", func(t *testing.T) {
		mockMixCoord.EXPECT().GetIndexStatistics(mock.Anything, mock.Anything).Return(&indexpb.GetIndexStatisticsResponse{}, nil)
		ret, err := server.GetIndexStatistics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("DropIndex", func(t *testing.T) {
		mockMixCoord.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		ret, err := server.DropIndex(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		mockMixCoord.EXPECT().GetIndexState(mock.Anything, mock.Anything).Return(&indexpb.GetIndexStateResponse{}, nil)
		ret, err := server.GetIndexState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		mockMixCoord.EXPECT().GetIndexBuildProgress(mock.Anything, mock.Anything).Return(&indexpb.GetIndexBuildProgressResponse{}, nil)
		ret, err := server.GetIndexBuildProgress(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetSegmentIndexState", func(t *testing.T) {
		mockMixCoord.EXPECT().GetSegmentIndexState(mock.Anything, mock.Anything).Return(&indexpb.GetSegmentIndexStateResponse{}, nil)
		ret, err := server.GetSegmentIndexState(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GetIndexInfos", func(t *testing.T) {
		mockMixCoord.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(&indexpb.GetIndexInfoResponse{}, nil)
		ret, err := server.GetIndexInfos(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("GcControl", func(t *testing.T) {
		mockMixCoord.EXPECT().GcControl(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil)
		ret, err := server.GcControl(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
	})

	t.Run("ListIndex", func(t *testing.T) {
		mockMixCoord.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(&indexpb.ListIndexesResponse{
			Status: merr.Success(),
		}, nil)
		ret, err := server.ListIndexes(ctx, &indexpb.ListIndexesRequest{})
		assert.NoError(t, err)
		assert.True(t, merr.Ok(ret.GetStatus()))
	})

	t.Run("ShowCollections", func(t *testing.T) {
		mockMixCoord.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(
			&querypb.ShowCollectionsResponse{
				Status: merr.Success(),
			}, nil,
		)
		resp, err := server.ShowLoadCollections(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("LoadCollection", func(t *testing.T) {
		mockMixCoord.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.LoadCollection(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ReleaseCollection", func(t *testing.T) {
		mockMixCoord.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.ReleaseCollection(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ShowPartitions", func(t *testing.T) {
		mockMixCoord.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{Status: merr.Success()}, nil)
		resp, err := server.ShowLoadPartitions(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})
	t.Run("GetPartitionStates", func(t *testing.T) {
		mockMixCoord.EXPECT().GetPartitionStates(mock.Anything, mock.Anything).Return(&querypb.GetPartitionStatesResponse{Status: merr.Success()}, nil)
		resp, err := server.GetPartitionStates(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("LoadPartitions", func(t *testing.T) {
		mockMixCoord.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.LoadPartitions(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ReleasePartitions", func(t *testing.T) {
		mockMixCoord.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.ReleasePartitions(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetSegmentInfo", func(t *testing.T) {
		req := &querypb.GetSegmentInfoRequest{}
		mockMixCoord.EXPECT().GetLoadSegmentInfo(mock.Anything, req).Return(&querypb.GetSegmentInfoResponse{Status: merr.Success()}, nil)
		resp, err := server.GetLoadSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("LoadBalance", func(t *testing.T) {
		req := &querypb.LoadBalanceRequest{}
		mockMixCoord.EXPECT().LoadBalance(mock.Anything, req).Return(merr.Success(), nil)
		resp, err := server.LoadBalance(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "",
		}
		mockMixCoord.EXPECT().GetMetrics(mock.Anything, req).Return(&milvuspb.GetMetricsResponse{Status: merr.Success()}, nil)
		resp, err := server.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("CheckHealth", func(t *testing.T) {
		mockMixCoord.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(
			&milvuspb.CheckHealthResponse{Status: merr.Success(), IsHealthy: true}, nil)
		ret, err := server.CheckHealth(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, true, ret.IsHealthy)
	})

	t.Run("CreateResourceGroup", func(t *testing.T) {
		mockMixCoord.EXPECT().CreateResourceGroup(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.CreateResourceGroup(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DropResourceGroup", func(t *testing.T) {
		mockMixCoord.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.DropResourceGroup(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("TransferNode", func(t *testing.T) {
		mockMixCoord.EXPECT().TransferNode(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.TransferNode(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("TransferReplica", func(t *testing.T) {
		mockMixCoord.EXPECT().TransferReplica(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		resp, err := server.TransferReplica(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ListResourceGroups", func(t *testing.T) {
		req := &milvuspb.ListResourceGroupsRequest{}
		mockMixCoord.EXPECT().ListResourceGroups(mock.Anything, req).Return(&milvuspb.ListResourceGroupsResponse{Status: merr.Success()}, nil)
		resp, err := server.ListResourceGroups(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("DescribeResourceGroup", func(t *testing.T) {
		mockMixCoord.EXPECT().DescribeResourceGroup(mock.Anything, mock.Anything).Return(&querypb.DescribeResourceGroupResponse{Status: merr.Success()}, nil)
		resp, err := server.DescribeResourceGroup(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("ListCheckers", func(t *testing.T) {
		req := &querypb.ListCheckersRequest{}
		mockMixCoord.EXPECT().ListCheckers(mock.Anything, req).Return(&querypb.ListCheckersResponse{Status: merr.Success()}, nil)
		resp, err := server.ListCheckers(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("ActivateChecker", func(t *testing.T) {
		req := &querypb.ActivateCheckerRequest{}
		mockMixCoord.EXPECT().ActivateChecker(mock.Anything, req).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		resp, err := server.ActivateChecker(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DeactivateChecker", func(t *testing.T) {
		req := &querypb.DeactivateCheckerRequest{}
		mockMixCoord.EXPECT().DeactivateChecker(mock.Anything, req).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
		resp, err := server.DeactivateChecker(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ListQueryNode", func(t *testing.T) {
		req := &querypb.ListQueryNodeRequest{}
		mockMixCoord.EXPECT().ListQueryNode(mock.Anything, req).Return(&querypb.ListQueryNodeResponse{Status: merr.Success()}, nil)
		resp, err := server.ListQueryNode(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("GetQueryNodeDistribution", func(t *testing.T) {
		req := &querypb.GetQueryNodeDistributionRequest{}
		mockMixCoord.EXPECT().GetQueryNodeDistribution(mock.Anything, req).Return(&querypb.GetQueryNodeDistributionResponse{Status: merr.Success()}, nil)
		resp, err := server.GetQueryNodeDistribution(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("SuspendBalance", func(t *testing.T) {
		req := &querypb.SuspendBalanceRequest{}
		mockMixCoord.EXPECT().SuspendBalance(mock.Anything, req).Return(merr.Success(), nil)
		resp, err := server.SuspendBalance(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("ResumeBalance", func(t *testing.T) {
		req := &querypb.ResumeBalanceRequest{}
		mockMixCoord.EXPECT().ResumeBalance(mock.Anything, req).Return(merr.Success(), nil)
		resp, err := server.ResumeBalance(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("CheckBalanceStatus", func(t *testing.T) {
		req := &querypb.CheckBalanceStatusRequest{}
		mockMixCoord.EXPECT().CheckBalanceStatus(mock.Anything, req).Return(&querypb.CheckBalanceStatusResponse{Status: merr.Success()}, nil)
		resp, err := server.CheckBalanceStatus(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("SuspendNode", func(t *testing.T) {
		req := &querypb.SuspendNodeRequest{}
		mockMixCoord.EXPECT().SuspendNode(mock.Anything, req).Return(merr.Success(), nil)
		resp, err := server.SuspendNode(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("ResumeNode", func(t *testing.T) {
		req := &querypb.ResumeNodeRequest{}
		mockMixCoord.EXPECT().ResumeNode(mock.Anything, req).Return(merr.Success(), nil)
		resp, err := server.ResumeNode(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("TransferSegment", func(t *testing.T) {
		req := &querypb.TransferSegmentRequest{}
		mockMixCoord.EXPECT().TransferSegment(mock.Anything, req).Return(merr.Success(), nil)
		resp, err := server.TransferSegment(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("TransferChannel", func(t *testing.T) {
		req := &querypb.TransferChannelRequest{}
		mockMixCoord.EXPECT().TransferChannel(mock.Anything, req).Return(merr.Success(), nil)
		resp, err := server.TransferChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("CheckQueryNodeDistribution", func(t *testing.T) {
		req := &querypb.CheckQueryNodeDistributionRequest{}
		mockMixCoord.EXPECT().CheckQueryNodeDistribution(mock.Anything, req).Return(merr.Success(), nil)
		resp, err := server.CheckQueryNodeDistribution(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

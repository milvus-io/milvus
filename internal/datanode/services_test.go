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

package datanode

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datanode/compactor"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type DataNodeServicesSuite struct {
	suite.Suite

	node          *DataNode
	storageConfig *indexpb.StorageConfig
	etcdCli       *clientv3.Client
	ctx           context.Context
	cancel        context.CancelFunc
}

func TestDataNodeServicesSuite(t *testing.T) {
	suite.Run(t, new(DataNodeServicesSuite))
}

func (s *DataNodeServicesSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	s.Require().NoError(err)
	s.etcdCli = etcdCli
}

func (s *DataNodeServicesSuite) SetupTest() {
	s.node = NewIDLEDataNodeMock(s.ctx, schemapb.DataType_Int64)
	s.node.SetEtcdClient(s.etcdCli)

	err := s.node.Init()
	s.Require().NoError(err)

	err = s.node.Start()
	s.Require().NoError(err)

	s.storageConfig = &indexpb.StorageConfig{
		Address:           paramtable.Get().MinioCfg.Address.GetValue(),
		AccessKeyID:       paramtable.Get().MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKey:   paramtable.Get().MinioCfg.SecretAccessKey.GetValue(),
		UseSSL:            paramtable.Get().MinioCfg.UseSSL.GetAsBool(),
		SslCACert:         paramtable.Get().MinioCfg.SslCACert.GetValue(),
		BucketName:        paramtable.Get().MinioCfg.BucketName.GetValue(),
		RootPath:          paramtable.Get().MinioCfg.RootPath.GetValue(),
		UseIAM:            paramtable.Get().MinioCfg.UseIAM.GetAsBool(),
		IAMEndpoint:       paramtable.Get().MinioCfg.IAMEndpoint.GetValue(),
		StorageType:       paramtable.Get().CommonCfg.StorageType.GetValue(),
		Region:            paramtable.Get().MinioCfg.Region.GetValue(),
		UseVirtualHost:    paramtable.Get().MinioCfg.UseVirtualHost.GetAsBool(),
		CloudProvider:     paramtable.Get().MinioCfg.CloudProvider.GetValue(),
		RequestTimeoutMs:  paramtable.Get().MinioCfg.RequestTimeoutMs.GetAsInt64(),
		GcpCredentialJSON: paramtable.Get().MinioCfg.GcpCredentialJSON.GetValue(),
	}

	paramtable.SetNodeID(1)
}

func (s *DataNodeServicesSuite) TearDownTest() {
	if s.node != nil {
		s.node.Stop()
		s.node = nil
	}
}

func (s *DataNodeServicesSuite) TearDownSuite() {
	s.cancel()
	err := s.etcdCli.Close()
	s.Require().NoError(err)
}

func (s *DataNodeServicesSuite) TestNotInUseAPIs() {
	s.Run("WatchDmChannels", func() {
		status, err := s.node.WatchDmChannels(s.ctx, &datapb.WatchDmChannelsRequest{})
		s.Assert().NoError(err)
		s.Assert().True(merr.Ok(status))
	})
	s.Run("GetTimeTickChannel", func() {
		_, err := s.node.GetTimeTickChannel(s.ctx, nil)
		s.Assert().NoError(err)
	})

	s.Run("GetStatisticsChannel", func() {
		_, err := s.node.GetStatisticsChannel(s.ctx, nil)
		s.Assert().NoError(err)
	})
}

func (s *DataNodeServicesSuite) TestGetComponentStates() {
	resp, err := s.node.GetComponentStates(s.ctx, nil)
	s.Assert().NoError(err)
	s.Assert().True(merr.Ok(resp.GetStatus()))
	s.Assert().Equal(common.NotRegisteredID, resp.State.NodeID)

	s.node.SetSession(&sessionutil.Session{})
	s.node.session.UpdateRegistered(true)
	resp, err = s.node.GetComponentStates(context.Background(), nil)
	s.Assert().NoError(err)
	s.Assert().True(merr.Ok(resp.GetStatus()))
}

func (s *DataNodeServicesSuite) TestGetCompactionState() {
	s.Run("success", func() {
		const (
			collection = int64(100)
			channel    = "ch-0"
		)

		mockC := compactor.NewMockCompactor(s.T())
		mockC.EXPECT().GetPlanID().Return(int64(1))
		mockC.EXPECT().GetCollection().Return(collection)
		mockC.EXPECT().GetChannelName().Return(channel)
		mockC.EXPECT().GetSlotUsage().Return(8)
		mockC.EXPECT().Complete().Return()
		mockC.EXPECT().Compact().Return(&datapb.CompactionPlanResult{
			PlanID: 1,
			State:  datapb.CompactionTaskState_completed,
		}, nil)
		s.node.compactionExecutor.Enqueue(mockC)

		mockC2 := compactor.NewMockCompactor(s.T())
		mockC2.EXPECT().GetPlanID().Return(int64(2))
		mockC2.EXPECT().GetCollection().Return(collection)
		mockC2.EXPECT().GetChannelName().Return(channel)
		mockC2.EXPECT().GetSlotUsage().Return(8)
		mockC2.EXPECT().Complete().Return()
		mockC2.EXPECT().Compact().Return(&datapb.CompactionPlanResult{
			PlanID: 2,
			State:  datapb.CompactionTaskState_failed,
		}, nil)
		s.node.compactionExecutor.Enqueue(mockC2)

		s.Eventually(func() bool {
			stat, err := s.node.GetCompactionState(s.ctx, nil)
			s.Assert().NoError(err)
			s.Assert().Equal(2, len(stat.GetResults()))
			doneCnt := 0
			failCnt := 0
			for _, res := range stat.GetResults() {
				if res.GetState() == datapb.CompactionTaskState_completed {
					doneCnt++
				}
				if res.GetState() == datapb.CompactionTaskState_failed {
					failCnt++
				}
			}
			return doneCnt == 1 && failCnt == 1
		}, 5*time.Second, 10*time.Millisecond)
	})

	s.Run("unhealthy", func() {
		node := &DataNode{lifetime: lifetime.NewLifetime(commonpb.StateCode_Abnormal)}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, _ := node.GetCompactionState(s.ctx, nil)
		s.Assert().Equal(merr.Code(merr.ErrServiceNotReady), resp.GetStatus().GetCode())
	})
}

func (s *DataNodeServicesSuite) TestCompaction() {
	dmChannelName := "by-dev-rootcoord-dml_0_100v0"

	s.Run("service_not_ready", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node := &DataNode{lifetime: lifetime.NewLifetime(commonpb.StateCode_Abnormal)}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		req := &datapb.CompactionPlan{
			PlanID:  1000,
			Channel: dmChannelName,
		}

		resp, err := node.CompactionV2(ctx, req)
		s.NoError(err)
		s.False(merr.Ok(resp))
		s.T().Logf("status=%v", resp)
	})

	s.Run("unknown CompactionType", func() {
		node := s.node
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req := &datapb.CompactionPlan{
			PlanID:  1000,
			Channel: dmChannelName,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 102, Level: datapb.SegmentLevel_L0},
				{SegmentID: 103, Level: datapb.SegmentLevel_L1},
			},
			BeginLogID:         100,
			PreAllocatedLogIDs: &datapb.IDRange{Begin: 200, End: 2000},
		}

		resp, err := node.CompactionV2(ctx, req)
		s.NoError(err)
		s.False(merr.Ok(resp))
		s.T().Logf("status=%v", resp)
	})

	s.Run("compact_clustering", func() {
		node := s.node
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req := &datapb.CompactionPlan{
			PlanID:  1000,
			Channel: dmChannelName,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 102, Level: datapb.SegmentLevel_L0},
				{SegmentID: 103, Level: datapb.SegmentLevel_L1},
			},
			Type:                   datapb.CompactionType_ClusteringCompaction,
			BeginLogID:             100,
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 100, End: 200},
			PreAllocatedLogIDs:     &datapb.IDRange{Begin: 200, End: 2000},
		}

		resp, err := node.CompactionV2(ctx, req)
		s.NoError(err)
		s.True(merr.Ok(resp))
		s.T().Logf("status=%v", resp)
	})

	s.Run("beginLogID is invalid", func() {
		node := s.node
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req := &datapb.CompactionPlan{
			PlanID:  1000,
			Channel: dmChannelName,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 102, Level: datapb.SegmentLevel_L0},
				{SegmentID: 103, Level: datapb.SegmentLevel_L1},
			},
			Type:               datapb.CompactionType_ClusteringCompaction,
			BeginLogID:         0,
			PreAllocatedLogIDs: &datapb.IDRange{Begin: 200, End: 2000},
		}

		resp, err := node.CompactionV2(ctx, req)
		s.NoError(err)
		s.False(merr.Ok(resp))
		s.T().Logf("status=%v", resp)
	})

	s.Run("pre-allocated segmentID range is invalid", func() {
		node := s.node
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req := &datapb.CompactionPlan{
			PlanID:  1000,
			Channel: dmChannelName,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 102, Level: datapb.SegmentLevel_L0},
				{SegmentID: 103, Level: datapb.SegmentLevel_L1},
			},
			Type:                   datapb.CompactionType_ClusteringCompaction,
			BeginLogID:             100,
			PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 0, End: 0},
			PreAllocatedLogIDs:     &datapb.IDRange{Begin: 200, End: 2000},
		}

		resp, err := node.CompactionV2(ctx, req)
		s.NoError(err)
		s.False(merr.Ok(resp))
		s.T().Logf("status=%v", resp)
	})
}

func (s *DataNodeServicesSuite) TestShowConfigurations() {
	pattern := "datanode.Port"
	req := &internalpb.ShowConfigurationsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		Pattern: pattern,
	}

	// test closed server
	node := &DataNode{lifetime: lifetime.NewLifetime(commonpb.StateCode_Abnormal)}
	node.SetSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}})
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	resp, err := node.ShowConfigurations(s.ctx, req)
	s.Assert().NoError(err)
	s.Assert().False(merr.Ok(resp.GetStatus()))

	node.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err = node.ShowConfigurations(s.ctx, req)
	s.Assert().NoError(err)
	s.Assert().True(merr.Ok(resp.GetStatus()))
	s.Assert().Equal(1, len(resp.Configuations))
	s.Assert().Equal("datanode.port", resp.Configuations[0].Key)
}

func (s *DataNodeServicesSuite) TestGetMetrics() {
	node := NewDataNode(context.TODO())
	node.registerMetricsRequest()
	node.SetSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}})
	// server is closed
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err := node.GetMetrics(s.ctx, &milvuspb.GetMetricsRequest{})
	s.Assert().NoError(err)
	s.Assert().False(merr.Ok(resp.GetStatus()))

	node.UpdateStateCode(commonpb.StateCode_Healthy)

	// failed to parse metric type
	invalidRequest := "invalid request"
	resp, err = node.GetMetrics(s.ctx, &milvuspb.GetMetricsRequest{
		Request: invalidRequest,
	})
	s.Assert().NoError(err)
	s.Assert().False(merr.Ok(resp.GetStatus()))

	// unsupported metric type
	unsupportedMetricType := "unsupported"
	req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
	s.Assert().NoError(err)
	resp, err = node.GetMetrics(s.ctx, req)
	s.Assert().NoError(err)
	s.Assert().False(merr.Ok(resp.GetStatus()))

	// normal case
	req, err = metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	s.Assert().NoError(err)
	resp, err = node.GetMetrics(node.ctx, req)
	s.Assert().NoError(err)
	s.Assert().True(merr.Ok(resp.GetStatus()))
	log.Info("Test DataNode.GetMetrics",
		zap.String("name", resp.ComponentName),
		zap.String("response", resp.Response))
}

func (s *DataNodeServicesSuite) TestResendSegmentStats() {
	req := &datapb.ResendSegmentStatsRequest{
		Base: &commonpb.MsgBase{},
	}

	resp, err := s.node.ResendSegmentStats(s.ctx, req)
	s.Assert().NoError(err, "empty call, no error")
	s.Assert().True(merr.Ok(resp.GetStatus()), "empty call, status shall be OK")
}

func (s *DataNodeServicesSuite) TestQuerySlot() {
	s.Run("node not healthy", func() {
		s.SetupTest()
		s.node.UpdateStateCode(commonpb.StateCode_Abnormal)

		ctx := context.Background()
		resp, err := s.node.QuerySlot(ctx, nil)
		s.NoError(err)
		s.False(merr.Ok(resp.GetStatus()))
		s.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	s.Run("normal case", func() {
		s.SetupTest()
		ctx := context.Background()
		resp, err := s.node.QuerySlot(ctx, nil)
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		s.NoError(merr.Error(resp.GetStatus()))
	})
}

func (s *DataNodeServicesSuite) TestDropCompactionPlan() {
	s.Run("node not healthy", func() {
		s.SetupTest()
		s.node.UpdateStateCode(commonpb.StateCode_Abnormal)

		ctx := context.Background()
		status, err := s.node.DropCompactionPlan(ctx, nil)
		s.NoError(err)
		s.False(merr.Ok(status))
		s.ErrorIs(merr.Error(status), merr.ErrServiceNotReady)
	})

	s.Run("normal case", func() {
		s.SetupTest()
		ctx := context.Background()
		req := &datapb.DropCompactionPlanRequest{
			PlanID: 1,
		}

		status, err := s.node.DropCompactionPlan(ctx, req)
		s.NoError(err)
		s.True(merr.Ok(status))
	})
}

func (s *DataNodeServicesSuite) TestCreateTask() {
	s.Run("create pre-import task", func() {
		preImportReq := &datapb.PreImportRequest{
			StorageConfig: compaction.CreateStorageConfig(),
		}
		payload, err := proto.Marshal(preImportReq)
		s.NoError(err)
		req := &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.PreImport,
			},
			Payload: payload,
		}
		status, err := s.node.CreateTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("create import task", func() {
		importReq := &datapb.ImportRequest{
			Schema:        &schemapb.CollectionSchema{},
			StorageConfig: compaction.CreateStorageConfig(),
		}
		payload, err := proto.Marshal(importReq)
		s.NoError(err)
		req := &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Import,
			},
			Payload: payload,
		}
		status, err := s.node.CreateTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("create compaction task", func() {
		req := &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Compaction,
			},
			Payload: []byte{},
		}
		status, err := s.node.CreateTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("create index task", func() {
		indexReq := &workerpb.CreateJobRequest{
			StorageConfig: s.storageConfig,
		}
		payload, err := proto.Marshal(indexReq)
		s.NoError(err)
		req := &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Index,
			},
			Payload: payload,
		}
		status, err := s.node.CreateTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("create stats task", func() {
		statsReq := &workerpb.CreateStatsRequest{
			StorageConfig: s.storageConfig,
		}
		payload, err := proto.Marshal(statsReq)
		s.NoError(err)
		req := &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Stats,
			},
			Payload: payload,
		}
		status, err := s.node.CreateTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("create analyze task", func() {
		req := &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Analyze,
			},
			Payload: []byte{},
		}
		status, err := s.node.CreateTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("invalid task type", func() {
		req := &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      "invalid",
			},
			Payload: []byte{},
		}
		status, err := s.node.CreateTask(s.ctx, req)
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})
}

func (s *DataNodeServicesSuite) TestQueryTask() {
	s.Run("query pre-import task", func() {
		req := &workerpb.QueryTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.PreImport,
				taskcommon.TaskIDKey:    "1",
			},
		}
		resp, err := s.node.QueryTask(s.ctx, req)
		s.NoError(err)
		s.Error(merr.Error(resp.GetStatus())) // task not found
	})

	s.Run("query import task", func() {
		req := &workerpb.QueryTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Import,
				taskcommon.TaskIDKey:    "1",
			},
		}
		resp, err := s.node.QueryTask(s.ctx, req)
		s.NoError(err)
		s.Error(merr.Error(resp.GetStatus())) // task not found
	})

	s.Run("query compaction task", func() {
		req := &workerpb.QueryTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Compaction,
				taskcommon.TaskIDKey:    "1",
			},
		}
		resp, err := s.node.QueryTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(resp, err))
	})

	s.Run("query index task", func() {
		req := &workerpb.QueryTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Index,
				taskcommon.TaskIDKey:    "1",
			},
		}
		resp, err := s.node.QueryTask(s.ctx, req)
		s.Error(merr.CheckRPCCall(resp, err))
		s.True(strings.Contains(resp.GetStatus().GetReason(), "not found"))
	})

	s.Run("query stats task", func() {
		req := &workerpb.QueryTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Stats,
				taskcommon.TaskIDKey:    "1",
			},
		}
		resp, err := s.node.QueryTask(s.ctx, req)
		s.Error(merr.CheckRPCCall(resp, err))
		s.True(strings.Contains(resp.GetStatus().GetReason(), "not found"))
	})

	s.Run("query analyze task", func() {
		req := &workerpb.QueryTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Analyze,
				taskcommon.TaskIDKey:    "1",
			},
		}
		resp, err := s.node.QueryTask(s.ctx, req)
		s.Error(merr.CheckRPCCall(resp, err))
		s.True(strings.Contains(resp.GetStatus().GetReason(), "not found"))
	})

	s.Run("invalid task type", func() {
		req := &workerpb.QueryTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      "invalid",
			},
		}
		resp, err := s.node.QueryTask(s.ctx, req)
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}

func (s *DataNodeServicesSuite) TestDropTask() {
	s.Run("drop pre-import task", func() {
		req := &workerpb.DropTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.PreImport,
				taskcommon.TaskIDKey:    "1",
			},
		}
		status, err := s.node.DropTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("drop import task", func() {
		req := &workerpb.DropTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Import,
				taskcommon.TaskIDKey:    "1",
			},
		}
		status, err := s.node.DropTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("drop compaction task", func() {
		req := &workerpb.DropTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Compaction,
				taskcommon.TaskIDKey:    "1",
			},
		}
		status, err := s.node.DropTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("drop index task", func() {
		req := &workerpb.DropTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Index,
				taskcommon.TaskIDKey:    "1",
			},
		}
		status, err := s.node.DropTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("drop stats task", func() {
		req := &workerpb.DropTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Stats,
				taskcommon.TaskIDKey:    "1",
			},
		}
		status, err := s.node.DropTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("drop analyze task", func() {
		req := &workerpb.DropTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Analyze,
				taskcommon.TaskIDKey:    "1",
			},
		}
		status, err := s.node.DropTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("invalid task type", func() {
		req := &workerpb.DropTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      "invalid",
			},
		}
		status, err := s.node.DropTask(s.ctx, req)
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})
}

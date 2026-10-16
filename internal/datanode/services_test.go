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
	"fmt"
	"io"
	"math/rand"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datanode/compactor"
	"github.com/milvus-io/milvus/internal/datanode/external"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/datanode/index"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type DataNodeServicesSuite struct {
	suite.Suite

	node          *DataNode
	storageConfig *indexpb.StorageConfig
	etcdCli       *clientv3.Client
	ctx           context.Context
	cancel        context.CancelFunc
}

type copySegmentTaskTarget struct {
	importv2.Task
}

type copySegmentStorageFactoryTarget struct {
	StorageFactory
}

type copySegmentCopierTarget struct {
	storage.CrossBucketCopier
}

func TestDataNodeServicesSuite(t *testing.T) {
	suite.Run(t, new(DataNodeServicesSuite))
}

func TestFirstExternalSourceURI(t *testing.T) {
	assert.False(t, hasExternalSourceRoot(nil))
	assert.False(t, hasExternalSourceRoot([]*datapb.CopySegmentSource{{}}))
	assert.True(t, hasExternalSourceRoot([]*datapb.CopySegmentSource{
		{},
		{SourceRootPath: "s3://foreign-bucket/foreign-root"},
	}))

	got, err := firstExternalSourceURI([]*datapb.CopySegmentSource{
		{SourceRootPath: "s3://foreign-bucket/foreign-root"},
		{SourceRootPath: "s3://foreign-bucket/foreign-root"},
	})
	if err != nil {
		t.Fatalf("firstExternalSourceURI returned error: %v", err)
	}
	if got != "s3://foreign-bucket/foreign-root" {
		t.Fatalf("firstExternalSourceURI = %q, want source root URI", got)
	}

	got, err = firstExternalSourceURI([]*datapb.CopySegmentSource{
		{SourceRootPath: "s3://foreign-bucket"},
	})
	if err != nil {
		t.Fatalf("firstExternalSourceURI rejected bucket root: %v", err)
	}
	if got != "s3://foreign-bucket" {
		t.Fatalf("firstExternalSourceURI = %q, want bucket root URI", got)
	}

	_, err = firstExternalSourceURI([]*datapb.CopySegmentSource{
		{SourceRootPath: "local-root"},
		{SourceRootPath: "s3://foreign-bucket/foreign-root"},
	})
	if err == nil {
		t.Fatalf("firstExternalSourceURI should validate the first source root")
	}
	assert.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(err))

	_, err = firstExternalSourceURI([]*datapb.CopySegmentSource{
		{SourceRootPath: "s3://foreign-bucket/root/../object"},
	})
	assert.Error(t, err)
	assert.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(err))

	_, err = firstExternalSourceURI(nil)
	assert.Error(t, err)
	assert.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(err))
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
		mockC.EXPECT().GetCompactionType().Return(datapb.CompactionType_MixCompaction)
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
		mockC2.EXPECT().GetCompactionType().Return(datapb.CompactionType_MixCompaction)
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

		jsonParams, err := compaction.GenerateJSONParams(&schemapb.CollectionSchema{})
		s.Require().NoError(err)

		req := &datapb.CompactionPlan{
			PlanID:  1000,
			Channel: dmChannelName,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 102, Level: datapb.SegmentLevel_L0},
				{SegmentID: 103, Level: datapb.SegmentLevel_L1},
			},
			BeginLogID:         100,
			PreAllocatedLogIDs: &datapb.IDRange{Begin: 200, End: 2000},
			JsonParams:         jsonParams,
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

		jsonParams, err := compaction.GenerateJSONParams(&schemapb.CollectionSchema{})
		s.Require().NoError(err)

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
			JsonParams:             jsonParams,
		}

		resp, err := node.CompactionV2(ctx, req)
		s.NoError(err)
		s.True(merr.Ok(resp))
		s.T().Logf("status=%v", resp)
	})

	s.Run("bump schema version compaction", func() {
		node := s.node
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		jsonParams, err := compaction.GenerateJSONParams(&schemapb.CollectionSchema{})
		s.Require().NoError(err)

		req := &datapb.CompactionPlan{
			PlanID:  1001,
			Channel: dmChannelName,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
				SegmentID:      102,
				Level:          datapb.SegmentLevel_L1,
				StorageVersion: storage.StorageV3,
				Manifest:       "manifest",
			}},
			Type:               datapb.CompactionType_BumpSchemaVersionCompaction,
			BeginLogID:         100,
			PreAllocatedLogIDs: &datapb.IDRange{Begin: 200, End: 2000},
			JsonParams:         jsonParams,
		}

		resp, err := node.CompactionV2(ctx, req)
		s.NoError(err)
		s.True(merr.Ok(resp))
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

		jsonParams, err := compaction.GenerateJSONParams(&schemapb.CollectionSchema{})
		s.Require().NoError(err)

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
			JsonParams:             jsonParams,
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
	mlog.Info(s.ctx, "Test DataNode.GetMetrics",
		mlog.String("name", resp.ComponentName),
		mlog.String("response", resp.Response))
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
		s.True(resp.GetCopySegmentSharedIndexes())
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

	s.Run("unknown task type is rejected before payload decoding", func() {
		req := &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      "invalid",
			},
			Payload: []byte{0xff},
		}
		status, err := s.node.CreateTask(s.ctx, req)
		s.NoError(err)
		// Task types are coordinator-assigned, so an unrecognized type is a
		// worker capability mismatch rather than invalid user input.
		s.Equal(merr.Code(merr.ErrServiceUnimplemented), status.GetCode())
		s.Contains(status.GetReason(), "unrecognized task type")
		s.ErrorIs(merr.CheckRPCCall(status, nil), merr.ErrServiceUnimplemented)
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

	s.Run("query index task with cost", func() {
		s.node.taskManager.LoadOrStoreIndexTask("cluster-0", 101, &index.IndexTaskInfo{State: commonpb.IndexState_InProgress})
		s.node.taskManager.StoreIndexTaskExecutionStart("cluster-0", 101, 100, 3)
		s.node.taskManager.StoreIndexTaskExecutionEndWithState("cluster-0", 101, 180, 80, commonpb.IndexState_Finished, "")

		req := &workerpb.QueryTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.Index,
				taskcommon.TaskIDKey:    "101",
			},
		}
		resp, err := s.node.QueryTask(s.ctx, req)
		s.NoError(merr.CheckRPCCall(resp, err))
		props := taskcommon.NewProperties(resp.GetProperties())
		// state and cost come from the same snapshot
		state, err := props.GetTaskState()
		s.NoError(err)
		s.Equal(taskcommon.State(commonpb.IndexState_Finished), state)
		s.Equal(int64(80), props.GetCostTime())
		s.Equal(int64(3), props.GetCostCPUNum())
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
		// Task types are coordinator-assigned, so an unrecognized type is a
		// worker capability mismatch rather than invalid user input.
		s.Equal(merr.Code(merr.ErrServiceUnimplemented), resp.GetStatus().GetCode())
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
		// Task types are coordinator-assigned, so an unrecognized type is a
		// worker capability mismatch rather than invalid user input.
		s.Equal(merr.Code(merr.ErrServiceUnimplemented), status.GetCode())
	})
}

func (s *DataNodeServicesSuite) TestCopySegment() {
	s.Run("unhealthy datanode", func() {
		s.node.UpdateStateCode(commonpb.StateCode_Abnormal)
		defer s.node.UpdateStateCode(commonpb.StateCode_Healthy)

		status, err := s.node.copySegment(s.ctx, &datapb.CopySegmentRequest{}, false)
		s.NoError(err)
		s.Error(merr.CheckRPCCall(status, nil))
	})

	s.Run("successful copy segment", func() {
		req := &datapb.CopySegmentRequest{
			JobID:         100,
			TaskID:        200,
			TaskSlot:      1,
			StorageConfig: s.storageConfig,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{
					CollectionId: 444,
					PartitionId:  555,
					SegmentId:    666,
				},
			},
		}

		status, err := s.node.copySegment(s.ctx, req, false)
		s.NoError(merr.CheckRPCCall(status, err))
	})

	s.Run("copy segment with invalid storage config", func() {
		req := &datapb.CopySegmentRequest{
			JobID:    100,
			TaskID:   201,
			TaskSlot: 1,
			StorageConfig: &indexpb.StorageConfig{
				BucketName: "invalid-bucket",
				Address:    "invalid-address",
			},
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{
					CollectionId: 444,
					PartitionId:  555,
					SegmentId:    666,
				},
			},
		}

		status, err := s.node.copySegment(s.ctx, req, false)
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})

	s.Run("external source resolution failure", func() {
		targetCM := &struct{ storage.ChunkManager }{}
		factory := &copySegmentStorageFactoryTarget{}
		mockFactory := mockey.Mock((*copySegmentStorageFactoryTarget).NewChunkManager).
			Return(targetCM, nil).
			Build()
		defer mockFactory.UnPatch()

		originalFactory := s.node.storageFactory
		s.node.storageFactory = factory
		defer func() { s.node.storageFactory = originalFactory }()

		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).
			Return(nil, merr.WrapErrServiceInternalMsg("resolve source storage failed")).
			Build()
		defer mockResolve.UnPatch()

		req := &datapb.CopySegmentRequest{
			StorageConfig: s.storageConfig,
			Sources: []*datapb.CopySegmentSource{
				{SourceRootPath: "s3://foreign-bucket/foreign-root"},
			},
		}
		status, err := s.node.copySegment(s.ctx, req, true)
		s.NoError(err)
		s.Equal(merr.Code(merr.ErrServiceInternal), status.GetCode())
	})
}

func (s *DataNodeServicesSuite) TestCopySegmentExternalSnapshotResolvesForeignSource() {
	targetCM := &struct{ storage.ChunkManager }{}
	sourceCM := &struct{ storage.ChunkManager }{}
	sourceStorageConfig := &indexpb.StorageConfig{
		BucketName: "foreign-bucket",
		RootPath:   "foreign-root",
	}
	resolvedCopier := &copySegmentCopierTarget{}
	mockCopy := mockey.Mock((*copySegmentCopierTarget).CopyCrossBucket).Return(nil).Build()
	defer mockCopy.UnPatch()

	var factoryConfigs []*indexpb.StorageConfig
	factory := &copySegmentStorageFactoryTarget{}
	mockFactory := mockey.Mock((*copySegmentStorageFactoryTarget).NewChunkManager).To(
		func(_ context.Context, config *indexpb.StorageConfig) (storage.ChunkManager, error) {
			factoryConfigs = append(factoryConfigs, config)
			return targetCM, nil
		},
	).Build()
	defer mockFactory.UnPatch()
	s.node.storageFactory = factory

	targetStorageConfig := &indexpb.StorageConfig{
		Address:         "localhost:9000",
		BucketName:      "target-bucket",
		RootPath:        "target-root",
		StorageType:     "remote",
		CloudProvider:   "aws",
		AccessKeyID:     "target-ak",
		SecretAccessKey: "target-sk",
	}
	req := &datapb.CopySegmentRequest{
		JobID:         100,
		TaskID:        201,
		TaskSlot:      1,
		StorageConfig: targetStorageConfig,
		ExternalSpec:  `{"extfs":{"cloud_provider":"aws"}}`,
		Sources: []*datapb.CopySegmentSource{{
			CollectionId:   111,
			PartitionId:    222,
			SegmentId:      333,
			SourceRootPath: "s3://foreign-bucket/foreign-root",
		}},
		Targets: []*datapb.CopySegmentTarget{{
			CollectionId: 444,
			PartitionId:  555,
			SegmentId:    666,
		}},
	}
	task := &copySegmentTaskTarget{}
	mockTaskID := mockey.Mock((*copySegmentTaskTarget).GetTaskID).Return(req.GetTaskID()).Build()
	defer mockTaskID.UnPatch()

	resolveCalled := false
	mResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).To(
		func(
			ctx context.Context,
			instanceCfg *objectstorage.Config,
			direction snapshotstorage.Direction,
			foreignURI string,
			externalSpec string,
		) (*snapshotstorage.ResolvedForeignStorage, error) {
			resolveCalled = true
			s.Equal(snapshotstorage.DirectionCopySource, direction)
			s.Equal("s3://foreign-bucket/foreign-root", foreignURI)
			s.Equal(req.GetExternalSpec(), externalSpec)
			s.Equal("target-bucket", instanceCfg.BucketName)
			return &snapshotstorage.ResolvedForeignStorage{
				ForeignBucket:        "foreign-bucket",
				ForeignCM:            sourceCM,
				ForeignStorageConfig: sourceStorageConfig,
				Copier:               resolvedCopier,
			}, nil
		}).Build()
	defer mResolve.UnPatch()

	newTaskCalled := false
	mNewTask := mockey.Mock(importv2.NewCopySegmentTask).To(
		func(
			parentCtx context.Context,
			copyPermits chan struct{},
			gotReq *datapb.CopySegmentRequest,
			manager importv2.TaskManager,
			gotSourceCM storage.ChunkManager,
			gotTargetCM storage.ChunkManager,
			gotSourceStorageConfig *indexpb.StorageConfig,
			gotCopier storage.CrossBucketCopier,
			sourceBucket string,
			targetBucket string,
		) importv2.Task {
			newTaskCalled = true
			s.True(proto.Equal(req, gotReq))
			s.Same(sourceCM, gotSourceCM)
			s.Same(targetCM, gotTargetCM)
			s.Same(sourceStorageConfig, gotSourceStorageConfig)
			s.True(gotCopier == resolvedCopier)
			s.Equal("foreign-bucket", sourceBucket)
			s.Equal("target-bucket", targetBucket)
			s.Same(s.node.ctx, parentCtx)
			return task
		}).Build()
	defer mNewTask.UnPatch()

	payload, err := proto.Marshal(req)
	s.NoError(err)
	status, err := s.node.CreateTask(s.ctx, &workerpb.CreateTaskRequest{
		Properties: map[string]string{
			taskcommon.TypeKey:   taskcommon.CopySegment,
			taskcommon.TaskIDKey: fmt.Sprint(req.GetTaskID()),
		},
		Payload: payload,
	})
	s.NoError(merr.CheckRPCCall(status, err))
	s.True(resolveCalled)
	s.True(newTaskCalled)
	s.Len(factoryConfigs, 1)
	s.True(proto.Equal(targetStorageConfig, factoryConfigs[0]))
}

func (s *DataNodeServicesSuite) TestCopySegmentExternalSnapshotResolvesSameBucketSourceRoot() {
	targetCM := &struct{ storage.ChunkManager }{}
	sourceCM := &struct{ storage.ChunkManager }{}
	sourceStorageConfig := &indexpb.StorageConfig{
		BucketName: "shared-bucket",
		RootPath:   "source-root",
	}
	resolvedCopier := &copySegmentCopierTarget{}
	mockCopy := mockey.Mock((*copySegmentCopierTarget).CopyCrossBucket).Return(nil).Build()
	defer mockCopy.UnPatch()

	factory := &copySegmentStorageFactoryTarget{}
	mockFactory := mockey.Mock((*copySegmentStorageFactoryTarget).NewChunkManager).
		Return(targetCM, nil).Build()
	defer mockFactory.UnPatch()
	s.node.storageFactory = factory

	targetStorageConfig := &indexpb.StorageConfig{
		Address:       "localhost:9000",
		BucketName:    "shared-bucket",
		RootPath:      "target-root",
		StorageType:   "remote",
		CloudProvider: "aws",
	}
	req := &datapb.CopySegmentRequest{
		JobID:         100,
		TaskID:        201,
		TaskSlot:      1,
		StorageConfig: targetStorageConfig,
		Sources: []*datapb.CopySegmentSource{{
			CollectionId:   111,
			PartitionId:    222,
			SegmentId:      333,
			SourceRootPath: "s3://shared-bucket/source-root",
		}},
		Targets: []*datapb.CopySegmentTarget{{
			CollectionId: 444,
			PartitionId:  555,
			SegmentId:    666,
		}},
	}
	task := &copySegmentTaskTarget{}
	mockTaskID := mockey.Mock((*copySegmentTaskTarget).GetTaskID).Return(req.GetTaskID()).Build()
	defer mockTaskID.UnPatch()

	resolveCalled := false
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).To(
		func(
			_ context.Context,
			instanceCfg *objectstorage.Config,
			direction snapshotstorage.Direction,
			foreignURI string,
			externalSpec string,
		) (*snapshotstorage.ResolvedForeignStorage, error) {
			resolveCalled = true
			s.Equal(snapshotstorage.DirectionCopySource, direction)
			s.Equal("s3://shared-bucket/source-root", foreignURI)
			s.Empty(externalSpec)
			s.Equal("shared-bucket", instanceCfg.BucketName)
			s.Equal("target-root", instanceCfg.RootPath)
			return &snapshotstorage.ResolvedForeignStorage{
				ForeignBucket:        "shared-bucket",
				ForeignCM:            sourceCM,
				ForeignStorageConfig: sourceStorageConfig,
				Copier:               resolvedCopier,
			}, nil
		}).Build()
	defer mockResolve.UnPatch()

	mockNewTask := mockey.Mock(importv2.NewCopySegmentTask).To(
		func(
			parentCtx context.Context,
			copyPermits chan struct{},
			gotReq *datapb.CopySegmentRequest,
			_ importv2.TaskManager,
			gotSourceCM storage.ChunkManager,
			gotTargetCM storage.ChunkManager,
			gotSourceStorageConfig *indexpb.StorageConfig,
			gotCopier storage.CrossBucketCopier,
			sourceBucket string,
			targetBucket string,
		) importv2.Task {
			s.True(proto.Equal(req, gotReq))
			s.Same(sourceCM, gotSourceCM)
			s.Same(targetCM, gotTargetCM)
			s.Same(sourceStorageConfig, gotSourceStorageConfig)
			s.True(gotCopier == resolvedCopier)
			s.Equal("shared-bucket", sourceBucket)
			s.Equal("shared-bucket", targetBucket)
			s.Same(s.node.ctx, parentCtx)
			return task
		}).Build()
	defer mockNewTask.UnPatch()

	payload, err := proto.Marshal(req)
	s.NoError(err)
	status, err := s.node.CreateTask(s.ctx, &workerpb.CreateTaskRequest{
		Properties: map[string]string{
			taskcommon.TypeKey:   taskcommon.ExternalCopySegment,
			taskcommon.TaskIDKey: fmt.Sprint(req.GetTaskID()),
		},
		Payload: payload,
	})
	s.NoError(merr.CheckRPCCall(status, err))
	s.True(resolveCalled)
}

func (s *DataNodeServicesSuite) TestCopySegmentExternalSnapshotUsesRawCredentialsFromExternalSpec() {
	targetCM := &struct{ storage.ChunkManager }{}

	targetStorageConfig := &indexpb.StorageConfig{
		Address:         "s3.us-west-2.amazonaws.com",
		BucketName:      "target-bucket",
		RootPath:        "target-root",
		StorageType:     "remote",
		CloudProvider:   objectstorage.CloudProviderAWS,
		Region:          "us-west-2",
		AccessKeyID:     "target-ak",
		SecretAccessKey: "target-sk",
	}
	var factoryConfigs []*indexpb.StorageConfig
	factory := &copySegmentStorageFactoryTarget{}
	mockFactory := mockey.Mock((*copySegmentStorageFactoryTarget).NewChunkManager).To(
		func(_ context.Context, config *indexpb.StorageConfig) (storage.ChunkManager, error) {
			factoryConfigs = append(factoryConfigs, config)
			return targetCM, nil
		},
	).Build()
	defer mockFactory.UnPatch()
	s.node.storageFactory = factory

	foreignSpec := `{"extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"foreign-ak","access_key_value":"foreign-sk"}}`
	req := &datapb.CopySegmentRequest{
		CopyConcurrency: 3,
		JobID:           100,
		TaskID:          202,
		TaskSlot:        1,
		StorageConfig:   targetStorageConfig,
		ExternalSpec:    foreignSpec,
		Sources: []*datapb.CopySegmentSource{{
			CollectionId:   111,
			PartitionId:    222,
			SegmentId:      333,
			SourceRootPath: "s3://foreign-bucket/foreign-root",
		}},
		Targets: []*datapb.CopySegmentTarget{{
			CollectionId: 444,
			PartitionId:  555,
			SegmentId:    666,
		}},
	}
	task := &copySegmentTaskTarget{}
	mockTaskID := mockey.Mock((*copySegmentTaskTarget).GetTaskID).Return(req.GetTaskID()).Build()
	defer mockTaskID.UnPatch()

	var remoteConfigs []objectstorage.Config
	mRemoteCM := mockey.Mock(storage.NewRemoteChunkManager).To(
		func(ctx context.Context, cfg *objectstorage.Config) (*storage.RemoteChunkManager, error) {
			_ = ctx
			remoteConfigs = append(remoteConfigs, *cfg)
			return storage.NewRemoteChunkManagerForTesting(nil, cfg.BucketName, cfg.RootPath), nil
		}).Build()
	defer mRemoteCM.UnPatch()

	var sourceStorageConfig *indexpb.StorageConfig
	mNewTask := mockey.Mock(importv2.NewCopySegmentTask).To(
		func(
			parentCtx context.Context,
			copyPermits chan struct{},
			gotReq *datapb.CopySegmentRequest,
			manager importv2.TaskManager,
			gotSourceCM storage.ChunkManager,
			gotTargetCM storage.ChunkManager,
			gotSourceStorageConfig *indexpb.StorageConfig,
			gotCopier storage.CrossBucketCopier,
			sourceBucket string,
			targetBucket string,
		) importv2.Task {
			sourceStorageConfig = gotSourceStorageConfig
			s.Equal(3, cap(copyPermits))
			s.Same(req, gotReq)
			s.NotNil(gotSourceCM)
			s.Same(targetCM, gotTargetCM)
			s.NotNil(gotCopier)
			s.Equal("foreign-bucket", sourceBucket)
			s.Equal("target-bucket", targetBucket)
			s.Same(s.node.ctx, parentCtx)
			return task
		}).Build()
	defer mNewTask.UnPatch()

	status, err := s.node.copySegment(s.ctx, req, true)
	s.NoError(merr.CheckRPCCall(status, err))
	s.Len(factoryConfigs, 1)
	s.Same(targetStorageConfig, factoryConfigs[0])
	s.Len(remoteConfigs, 2)
	s.Equal("foreign-ak", remoteConfigs[0].AccessKeyID)
	s.Equal("foreign-sk", remoteConfigs[0].SecretAccessKeyID)
	s.Equal("foreign-ak", remoteConfigs[1].AccessKeyID)
	s.Equal("foreign-sk", remoteConfigs[1].SecretAccessKeyID)
	s.NotNil(sourceStorageConfig)
	s.Equal("foreign-ak", sourceStorageConfig.GetAccessKeyID())
	s.Equal("foreign-sk", sourceStorageConfig.GetSecretAccessKey())
	s.Equal(foreignSpec, req.GetExternalSpec())
	s.NotContains(req.GetExternalSpec(), "secret_access_key")
	s.NotContains(req.GetExternalSpec(), "credential_json")
}

func (s *DataNodeServicesSuite) TestQueryCopySegment() {
	// First create a copy segment task
	createReq := &datapb.CopySegmentRequest{
		JobID:         100,
		TaskID:        300,
		TaskSlot:      1,
		StorageConfig: s.storageConfig,
		Sources: []*datapb.CopySegmentSource{
			{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			},
		},
		Targets: []*datapb.CopySegmentTarget{
			{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			},
		},
	}

	status, err := s.node.copySegment(s.ctx, createReq, false)
	s.NoError(merr.CheckRPCCall(status, err))

	s.Run("query existing task", func() {
		queryReq := &datapb.QueryCopySegmentRequest{
			TaskID: 300,
		}

		resp, err := s.node.QueryCopySegment(s.ctx, queryReq)
		s.NoError(merr.CheckRPCCall(resp.GetStatus(), err))
		s.Equal(int64(300), resp.GetTaskID())
		s.NotNil(resp.GetState())
	})

	s.Run("query non-existent task", func() {
		queryReq := &datapb.QueryCopySegmentRequest{
			TaskID: 99999,
		}

		resp, err := s.node.QueryCopySegment(s.ctx, queryReq)
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}

func (s *DataNodeServicesSuite) TestDropCopySegment() {
	// First create a copy segment task
	createReq := &datapb.CopySegmentRequest{
		JobID:         100,
		TaskID:        400,
		TaskSlot:      1,
		StorageConfig: s.storageConfig,
		Sources: []*datapb.CopySegmentSource{
			{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			},
		},
		Targets: []*datapb.CopySegmentTarget{
			{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			},
		},
	}

	status, err := s.node.copySegment(s.ctx, createReq, false)
	s.NoError(merr.CheckRPCCall(status, err))

	s.Run("drop existing task", func() {
		dropReq := &datapb.DropCopySegmentRequest{
			TaskID: 400,
			JobID:  100,
		}

		status, err := s.node.DropCopySegment(s.ctx, dropReq)
		s.NoError(merr.CheckRPCCall(status, err))

		// Verify task is dropped
		queryReq := &datapb.QueryCopySegmentRequest{
			TaskID: 400,
		}
		resp, err := s.node.QueryCopySegment(s.ctx, queryReq)
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}

func (s *DataNodeServicesSuite) TestDropCopySegment_CleanupLogic() {
	s.Run("drop failed task logs cleanup attempt", func() {
		// The test verifies that DropCopySegment checks task state
		// and calls CleanupCopiedFiles on failed CopySegmentTask

		// Note: We cannot easily mock the ChunkManager without changing DataNode internals,
		// but we can verify that the logic path is executed by checking logs
		// The unit tests in task_copy_segment_test.go thoroughly test the cleanup functionality itself

		// This test mainly verifies integration: that DropCopySegment correctly:
		// 1. Retrieves the task from the task manager
		// 2. Checks if it's a CopySegmentTask
		// 3. Checks if the state is Failed
		// 4. Calls CleanupCopiedFiles() if conditions are met

		// Create a copy task that will be in pending state
		createReq := &datapb.CopySegmentRequest{
			JobID:         200,
			TaskID:        500,
			TaskSlot:      1,
			StorageConfig: s.storageConfig,
			Sources: []*datapb.CopySegmentSource{{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
				InsertBinlogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{LogPath: "files/insert_log/111/222/333/1/file1.log", LogSize: 100},
					},
				}},
			}},
			Targets: []*datapb.CopySegmentTarget{{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			}},
		}

		status, err := s.node.copySegment(s.ctx, createReq, false)
		s.NoError(merr.CheckRPCCall(status, err))

		// Verify task exists
		queryReq := &datapb.QueryCopySegmentRequest{TaskID: 500}
		resp, err := s.node.QueryCopySegment(s.ctx, queryReq)
		s.NoError(err)
		s.NotEqual(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())

		// Drop the task (regardless of state, drop should succeed)
		dropReq := &datapb.DropCopySegmentRequest{
			TaskID: 500,
			JobID:  200,
		}

		status, err = s.node.DropCopySegment(s.ctx, dropReq)
		s.NoError(merr.CheckRPCCall(status, err))

		// Verify task is dropped
		resp, err = s.node.QueryCopySegment(s.ctx, queryReq)
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}

func (s *DataNodeServicesSuite) TestImportStateV2ToCopySegmentTaskState() {
	tests := []struct {
		name        string
		inputState  datapb.ImportTaskStateV2
		outputState datapb.CopySegmentTaskState
	}{
		{
			name:        "None to None",
			inputState:  datapb.ImportTaskStateV2_None,
			outputState: datapb.CopySegmentTaskState_CopySegmentTaskNone,
		},
		{
			name:        "Pending to Pending",
			inputState:  datapb.ImportTaskStateV2_Pending,
			outputState: datapb.CopySegmentTaskState_CopySegmentTaskPending,
		},
		{
			name:        "InProgress to InProgress",
			inputState:  datapb.ImportTaskStateV2_InProgress,
			outputState: datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		},
		{
			name:        "Completed to Completed",
			inputState:  datapb.ImportTaskStateV2_Completed,
			outputState: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		},
		{
			name:        "Failed to Failed",
			inputState:  datapb.ImportTaskStateV2_Failed,
			outputState: datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		},
		{
			name:        "Retry to Failed",
			inputState:  datapb.ImportTaskStateV2_Retry,
			outputState: datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := importStateV2ToCopySegmentTaskState(tt.inputState)
			s.Equal(tt.outputState, result)
		})
	}
}

func (s *DataNodeServicesSuite) TestCreateTaskRefreshExternalCollection() {
	s.Run("fallback cluster ID from properties", func() {
		refreshReq := &datapb.RefreshExternalCollectionTaskRequest{
			TaskID:         999,
			CollectionID:   100,
			ExternalSource: "s3:///bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
			StorageConfig:  s.storageConfig,
		}
		payload, err := proto.Marshal(refreshReq)
		s.NoError(err)

		req := &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.ClusterIDKey: "cluster-0",
				taskcommon.TypeKey:      taskcommon.RefreshExternalCollection,
				taskcommon.TaskIDKey:    "999",
			},
			Payload: payload,
		}

		status, err := s.node.CreateTask(s.ctx, req)
		s.NoError(err)
		s.True(merr.Ok(status))
		s.NotNil(s.node.externalCollectionManager.Get("cluster-0", 999))
		s.Nil(s.node.externalCollectionManager.Get("", 999))
	})

	s.Run("missing cluster ID in payload and properties", func() {
		payload, err := proto.Marshal(&datapb.RefreshExternalCollectionTaskRequest{TaskID: 1000})
		s.NoError(err)

		status, err := s.node.CreateTask(s.ctx, &workerpb.CreateTaskRequest{
			Properties: map[string]string{
				taskcommon.TypeKey:   taskcommon.RefreshExternalCollection,
				taskcommon.TaskIDKey: "1000",
			},
			Payload: payload,
		})
		s.NoError(err)
		s.Error(merr.Error(status))
	})
}

func (s *DataNodeServicesSuite) TestCreateRefreshExternalCollectionTaskReturnsUpdatedSegmentsPayload() {
	s.node.UpdateStateCode(commonpb.StateCode_Healthy)
	if s.node.externalCollectionManager != nil {
		s.node.externalCollectionManager.Close()
	}
	s.node.externalCollectionManager = external.NewExternalCollectionManager(s.ctx, 1)
	defer s.node.externalCollectionManager.Close()

	req := &datapb.RefreshExternalCollectionTaskRequest{
		ClusterID:              "cluster",
		CollectionID:           100,
		PartitionID:            1,
		TaskID:                 200,
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 1000, End: 1001},
		Schema: &schemapb.CollectionSchema{
			Version: 4,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", ExternalField: "id"},
			},
		},
	}
	task := external.NewRefreshExternalCollectionTask(s.ctx, req)
	patched := &datapb.SegmentInfo{ID: 10, CollectionID: 100, NumOfRows: 1}

	gotClusterID := make(chan string, 1)
	mockNewTask := mockey.Mock(external.NewRefreshExternalCollectionTask).
		To(func(_ context.Context, gotReq *datapb.RefreshExternalCollectionTaskRequest) *external.RefreshExternalCollectionTask {
			gotClusterID <- gotReq.GetClusterID()
			return task
		}).Build()
	defer mockNewTask.UnPatch()
	mockPre := mockey.Mock((*external.RefreshExternalCollectionTask).PreExecute).Return(nil).Build()
	defer mockPre.UnPatch()
	mockExecute := mockey.Mock((*external.RefreshExternalCollectionTask).Execute).Return(nil).Build()
	defer mockExecute.UnPatch()
	mockPost := mockey.Mock((*external.RefreshExternalCollectionTask).PostExecute).Return(nil).Build()
	defer mockPost.UnPatch()
	mockUpdated := mockey.Mock((*external.RefreshExternalCollectionTask).GetUpdatedSegments).
		Return([]*datapb.SegmentInfo{patched}).Build()
	defer mockUpdated.UnPatch()

	status, err := s.node.createRefreshExternalCollectionTask(s.ctx, req)
	s.NoError(err)
	s.True(merr.Ok(status))
	select {
	case clusterID := <-gotClusterID:
		s.Equal("cluster", clusterID)
	case <-time.After(time.Second):
		s.Fail("task constructor was not called")
	}

	s.Eventually(func() bool {
		info := s.node.externalCollectionManager.Get("cluster", 200)
		return info != nil && info.State == indexpb.JobState_JobStateFinished
	}, time.Second, 10*time.Millisecond)

	info := s.node.externalCollectionManager.Get("cluster", 200)
	s.Require().NotNil(info)
	s.Equal(indexpb.JobState_JobStateFinished, info.State)
	s.Len(info.UpdatedSegments, 1)
	s.Equal(int64(10), info.UpdatedSegments[0].GetID())
}

func (s *DataNodeServicesSuite) TestCreateTaskCopySegment() {
	tests := []struct {
		name         string
		taskID       int64
		taskType     taskcommon.Type
		expectedCode int32
	}{
		{
			name:         "local copy segment",
			taskID:       501,
			taskType:     taskcommon.CopySegment,
			expectedCode: merr.Success().GetCode(),
		},
		{
			name:         "external copy segment without source root",
			taskID:       502,
			taskType:     taskcommon.ExternalCopySegment,
			expectedCode: merr.Code(merr.ErrServiceInternal),
		},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			copyReq := &datapb.CopySegmentRequest{
				JobID:         500,
				TaskID:        test.taskID,
				TaskSlot:      1,
				StorageConfig: s.storageConfig,
				Sources: []*datapb.CopySegmentSource{
					{
						CollectionId: 111,
						PartitionId:  222,
						SegmentId:    333,
					},
				},
				Targets: []*datapb.CopySegmentTarget{
					{
						CollectionId: 444,
						PartitionId:  555,
						SegmentId:    666,
					},
				},
			}

			payload, err := proto.Marshal(copyReq)
			s.NoError(err)

			req := &workerpb.CreateTaskRequest{
				Properties: map[string]string{
					taskcommon.TypeKey:   test.taskType,
					taskcommon.TaskIDKey: fmt.Sprint(test.taskID),
				},
				Payload: payload,
			}

			status, err := s.node.CreateTask(s.ctx, req)
			s.NoError(err)
			s.Equal(test.expectedCode, status.GetCode())
			if test.expectedCode == merr.Success().GetCode() {
				s.NotNil(s.node.importTaskMgr.Get(test.taskID))
			} else {
				s.Nil(s.node.importTaskMgr.Get(test.taskID))
			}
		})
	}
}

func (s *DataNodeServicesSuite) TestQueryTaskCopySegment() {
	// First create a copy segment task
	copyReq := &datapb.CopySegmentRequest{
		JobID:         600,
		TaskID:        601,
		TaskSlot:      1,
		StorageConfig: s.storageConfig,
		Sources: []*datapb.CopySegmentSource{
			{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			},
		},
		Targets: []*datapb.CopySegmentTarget{
			{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			},
		},
	}

	payload, err := proto.Marshal(copyReq)
	s.NoError(err)

	createReq := &workerpb.CreateTaskRequest{
		Properties: map[string]string{
			taskcommon.TypeKey:   taskcommon.CopySegment,
			taskcommon.TaskIDKey: "601",
		},
		Payload: payload,
	}

	status, err := s.node.CreateTask(s.ctx, createReq)
	s.NoError(merr.CheckRPCCall(status, err))

	for _, taskType := range []taskcommon.Type{taskcommon.CopySegment, taskcommon.ExternalCopySegment} {
		s.Run(taskType, func() {
			queryReq := &workerpb.QueryTaskRequest{
				Properties: map[string]string{
					taskcommon.ClusterIDKey: "cluster-0",
					taskcommon.TypeKey:      taskType,
					taskcommon.TaskIDKey:    "601",
				},
			}

			resp, err := s.node.QueryTask(s.ctx, queryReq)
			s.NoError(merr.CheckRPCCall(resp.GetStatus(), err))
			s.NotNil(resp.GetPayload())
		})
	}
}

func (s *DataNodeServicesSuite) TestDropTaskCopySegment() {
	tests := []struct {
		taskID   int64
		taskType taskcommon.Type
	}{
		{taskID: 701, taskType: taskcommon.CopySegment},
		{taskID: 702, taskType: taskcommon.ExternalCopySegment},
	}

	for _, test := range tests {
		s.Run(test.taskType, func() {
			copyReq := &datapb.CopySegmentRequest{
				JobID:         700,
				TaskID:        test.taskID,
				TaskSlot:      1,
				StorageConfig: s.storageConfig,
				Sources: []*datapb.CopySegmentSource{
					{
						CollectionId: 111,
						PartitionId:  222,
						SegmentId:    333,
					},
				},
				Targets: []*datapb.CopySegmentTarget{
					{
						CollectionId: 444,
						PartitionId:  555,
						SegmentId:    666,
					},
				},
			}
			if test.taskType == taskcommon.ExternalCopySegment {
				copyReq.Sources[0].SourceRootPath = fmt.Sprintf(
					"s3://%s/%s",
					s.storageConfig.GetBucketName(),
					s.storageConfig.GetRootPath(),
				)
			}

			payload, err := proto.Marshal(copyReq)
			s.NoError(err)
			createReq := &workerpb.CreateTaskRequest{
				Properties: map[string]string{
					taskcommon.TypeKey:   test.taskType,
					taskcommon.TaskIDKey: fmt.Sprint(test.taskID),
				},
				Payload: payload,
			}
			status, err := s.node.CreateTask(s.ctx, createReq)
			s.NoError(merr.CheckRPCCall(status, err))

			dropReq := &workerpb.DropTaskRequest{
				Properties: map[string]string{
					taskcommon.ClusterIDKey: "cluster-0",
					taskcommon.TypeKey:      test.taskType,
					taskcommon.TaskIDKey:    fmt.Sprint(test.taskID),
				},
			}
			status, err = s.node.DropTask(s.ctx, dropReq)
			s.NoError(merr.CheckRPCCall(status, err))
		})
	}
}

func captureDataNodeLogs(t *testing.T) *mlog.TestSink {
	t.Helper()

	return mlog.CaptureGlobalLogs(t, &mlog.Config{
		Level:             "debug",
		Format:            "text",
		DisableCaller:     true,
		DisableTimestamp:  true,
		DisableStacktrace: true,
	})
}

type failingStorageFactory struct{}

func (failingStorageFactory) NewChunkManager(context.Context, *indexpb.StorageConfig) (storage.ChunkManager, error) {
	return nil, merr.WrapErrIoFailedReason("storage factory unavailable")
}

func TestChunkManagerFailureDoesNotLogStorageAccessKey(t *testing.T) {
	logs := captureDataNodeLogs(t)
	ctx := context.Background()
	node := NewDataNode(ctx)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.storageFactory = failingStorageFactory{}

	accessKey := "DATANODE_ACCESS_KEY_SENTINEL"
	storageConfig := &indexpb.StorageConfig{
		BucketName:  "audit-bucket",
		AccessKeyID: accessKey,
	}

	testCases := []struct {
		name string
		call func() (*commonpb.Status, error)
	}{
		{
			name: "legacy index job",
			call: func() (*commonpb.Status, error) {
				return node.CreateJob(ctx, &workerpb.CreateJobRequest{
					ClusterID:     "cluster",
					BuildID:       1,
					StorageConfig: storageConfig,
				})
			},
		},
		{
			name: "v2 index job",
			call: func() (*commonpb.Status, error) {
				return node.CreateJobV2(ctx, &workerpb.CreateJobV2Request{
					ClusterID: "cluster",
					TaskID:    2,
					JobType:   indexpb.JobType_JobTypeIndexJob,
					Request: &workerpb.CreateJobV2Request_IndexRequest{
						IndexRequest: &workerpb.CreateJobRequest{
							ClusterID:     "cluster",
							BuildID:       2,
							StorageConfig: storageConfig,
						},
					},
				})
			},
		},
		{
			name: "stats job",
			call: func() (*commonpb.Status, error) {
				return node.CreateJobV2(ctx, &workerpb.CreateJobV2Request{
					ClusterID: "cluster",
					TaskID:    3,
					JobType:   indexpb.JobType_JobTypeStatsJob,
					Request: &workerpb.CreateJobV2Request_StatsRequest{
						StatsRequest: &workerpb.CreateStatsRequest{
							ClusterID:     "cluster",
							TaskID:        3,
							StorageConfig: storageConfig,
						},
					},
				})
			},
		},
		{
			name: "pre-import",
			call: func() (*commonpb.Status, error) {
				return node.PreImport(ctx, &datapb.PreImportRequest{TaskID: 4, StorageConfig: storageConfig})
			},
		},
		{
			name: "import",
			call: func() (*commonpb.Status, error) {
				return node.ImportV2(ctx, &datapb.ImportRequest{TaskID: 5, StorageConfig: storageConfig})
			},
		},
		{
			name: "copy segment",
			call: func() (*commonpb.Status, error) {
				return node.copySegment(ctx, &datapb.CopySegmentRequest{TaskID: 6, StorageConfig: storageConfig}, false)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			status, err := testCase.call()
			require.NoError(t, err)
			assert.Error(t, merr.Error(status))
		})
	}

	assert.NotContains(t, logs.String(), accessKey)
	assert.Contains(t, logs.String(), "audit-bucket")
}

// The executor tests use real plan decoding and worker execution. Only object
// storage resolution/copy are replaced; no Import scheduler is started.
type snapshotWholeCopyStorage struct{}

func (*snapshotWholeCopyStorage) CopyCrossBucketAndGetSize(context.Context, string, string, string, string) (int64, error) {
	return 0, merr.ErrServiceInternal
}

func (c *snapshotWholeCopyStorage) CopyCrossBucket(ctx context.Context, sourceBucket, sourcePath, targetBucket, targetPath string) error {
	_, err := c.CopyCrossBucketAndGetSize(ctx, sourceBucket, sourcePath, targetBucket, targetPath)
	return err
}

func snapshotExportRangeRequest(t *testing.T, count int, sourceErrors ...error) (*datapb.SnapshotExportCopyRequest, storage.ChunkManager) {
	t.Helper()
	root := t.TempDir()
	sourceRoot := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(sourceRoot))
	snapshot := &snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "snapshot"}, Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "collection"}}}
	segment := &datapb.SegmentDescription{SegmentId: 1001, PartitionId: 1}
	logs := &datapb.FieldBinlog{FieldID: 1}
	for i := 0; i < count; i++ {
		file := filepath.Join(sourceRoot, "files", fmt.Sprint(i))
		require.NoError(t, cm.Write(context.Background(), file, []byte("x")))
		logs.Binlogs = append(logs.Binlogs, &datapb.Binlog{LogID: int64(i + 1), LogPath: file})
	}
	segment.Binlogs = []*datapb.FieldBinlog{logs}
	snapshot.Segments = []*datapb.SegmentDescription{segment}
	metadata, _, err := snapshotstorage.NewSnapshotWriter(cm).SaveToRootWithSize(context.Background(), snapshot, sourceRoot, datapb.SnapshotLayout_SnapshotLayoutReferenced)
	require.NoError(t, err)
	remote := &storage.RemoteChunkManager{}
	var sourceErr error
	if len(sourceErrors) > 0 {
		sourceErr = sourceErrors[0]
	}
	create := mockey.Mock(storage.NewRemoteChunkManager).Return(remote, sourceErr).Build()
	read := mockey.Mock((*storage.RemoteChunkManager).Read).To(func(_ *storage.RemoteChunkManager, ctx context.Context, key string) ([]byte, error) {
		return cm.Read(ctx, key)
	}).Build()
	size := mockey.Mock((*storage.RemoteChunkManager).Size).To(func(_ *storage.RemoteChunkManager, ctx context.Context, key string) (int64, error) {
		return cm.Size(ctx, key)
	}).Build()
	rootMock := mockey.Mock((*storage.RemoteChunkManager).RootPath).Return(sourceRoot).Build()
	t.Cleanup(func() { rootMock.UnPatch(); size.UnPatch(); read.UnPatch(); create.UnPatch() })
	req := &datapb.SnapshotExportCopyRequest{ClusterId: "cluster", JobId: 1, TaskId: 2, GenerationId: 100, SegmentEnd: 1, SnapshotId: 1, CollectionId: 100, SnapshotMetadataPath: metadata, DeadlineTime: uint64(time.Now().Add(time.Minute).UnixMilli()), SourceStorageConfig: &indexpb.StorageConfig{BucketName: "source"}, TargetUri: root}
	return req, cm
}

func TestSnapshotExportRangeTask_ContinuousExecution(t *testing.T) {
	req, cm := snapshotExportRangeRequest(t, 600)
	req.CopyConcurrency = 2
	targetStore := &snapshotWholeCopyStorage{}
	var active, maximum, copied, initializations atomic.Int64
	unblock := make(chan struct{})
	started := make(chan struct{}, 1)
	defer mockey.Mock((*snapshotWholeCopyStorage).CopyCrossBucketAndGetSize).To(func(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) (int64, error) {
		src := srcObject
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		if filepath.Base(src) == "0" {
			started <- struct{}{}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-unblock:
			}
		}
		copied.Add(1)
		return int64(1), nil
	}).Build().UnPatch()
	defer mockey.Mock(snapshotstorage.ResolveForeignStorage).To(func(context.Context, *objectstorage.Config, snapshotstorage.Direction, string, string) (*snapshotstorage.ResolvedForeignStorage, error) {
		initializations.Add(1)
		return &snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, ForeignStorageConfig: &indexpb.StorageConfig{}, Copier: targetStore, ForeignBucket: "target"}, nil
	}).Build().UnPatch()
	e := newSnapshotExportExecutor(context.Background())
	defer e.close()
	require.NoError(t, e.create(req))
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatalf("copy did not start: %s", e.query(req.ClusterId, req.TaskId).GetReason())
	}
	require.NoError(t, e.create(req))
	// No coordinator queries, checkpoints or batch acknowledgments are needed.
	require.Eventually(t, func() bool { return copied.Load() == 599 }, time.Second*3, time.Millisecond)
	require.EqualValues(t, 1, e.slots())
	require.LessOrEqual(t, maximum.Load(), int64(2))
	require.EqualValues(t, 1, initializations.Load())
	changed := proto.Clone(req).(*datapb.SnapshotExportCopyRequest)
	changed.SnapshotId++
	require.ErrorIs(t, e.create(changed), merr.ErrDataIntegrity)
	close(unblock)
	require.Eventually(t, func() bool { return e.query(req.ClusterId, 2).GetState() == datapb.ImportTaskStateV2_Completed }, time.Second, time.Millisecond)
	require.EqualValues(t, 601, e.query(req.ClusterId, 2).GetCopiedFiles())
	require.EqualValues(t, 600, e.query(req.ClusterId, 2).GetCopiedBytes())
	require.NoError(t, e.drop(context.Background(), req.ClusterId, 2))
	require.ErrorIs(t, merr.Error(e.query(req.ClusterId, 2).GetStatus()), merr.ErrImportSysFailed)
}

func TestSnapshotExportRangeTask_CancelAndShutdown(t *testing.T) {
	req, cm := snapshotExportRangeRequest(t, 2)
	req.TaskSlot = 16
	targetStore := &snapshotWholeCopyStorage{}
	entered := make(chan struct{}, 2)
	drain := make(chan struct{})
	defer mockey.Mock((*snapshotWholeCopyStorage).CopyCrossBucketAndGetSize).To(func(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) (int64, error) {
		entered <- struct{}{}
		<-ctx.Done()
		<-drain
		return 0, ctx.Err()
	}).Build().UnPatch()
	defer mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, ForeignStorageConfig: &indexpb.StorageConfig{}, Copier: targetStore, ForeignBucket: "target"}, nil).Build().UnPatch()
	e := newSnapshotExportExecutor(context.Background())
	require.NoError(t, e.create(req))
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatalf("copy did not start: %s", e.query(req.ClusterId, req.TaskId).GetReason())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, e.drop(ctx, req.ClusterId, 2), context.Canceled)
	require.EqualValues(t, 16, e.slots(), "retiring work retains its slot until copy calls return")
	closed := make(chan struct{})
	go func() { e.close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("shutdown did not join running copies")
	case <-time.After(20 * time.Millisecond):
	}
	close(drain)
	<-closed
	require.Error(t, e.create(req))
	require.Zero(t, e.slots())
}

func TestSnapshotExportRangeTask_StorageFailuresAndRPC(t *testing.T) {
	for _, scenario := range []string{"source client", "initialization", "corrupt source", "copy", "success"} {
		t.Run(scenario, func(t *testing.T) {
			var sourceErr error
			if scenario == "source client" {
				sourceErr = merr.ErrIoPermissionDenied
			}
			req, cm := snapshotExportRangeRequest(t, 1, sourceErr)
			targetStore := &snapshotWholeCopyStorage{}
			var calls atomic.Int64
			defer mockey.Mock((*snapshotWholeCopyStorage).CopyCrossBucketAndGetSize).To(func(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string) (int64, error) {
				calls.Add(1)
				if scenario == "copy" {
					return 0, merr.ErrIoTooManyRequests
				}
				return int64(1), nil
			}).Build().UnPatch()
			var resolveErr error
			if scenario == "initialization" {
				resolveErr = merr.ErrIoPermissionDenied
			}
			defer mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, ForeignStorageConfig: &indexpb.StorageConfig{}, Copier: targetStore, ForeignBucket: "target"}, resolveErr).Build().UnPatch()
			if scenario == "corrupt source" {
				require.NoError(t, cm.Write(context.Background(), req.SnapshotMetadataPath, []byte("corrupt")))
			}
			e := newSnapshotExportExecutor(context.Background())
			defer e.close()
			node := &DataNode{lifetime: lifetime.NewLifetime(commonpb.StateCode_Healthy), snapshotExportExecutor: e}
			props := taskcommon.NewProperties(nil)
			props.AppendClusterID(req.ClusterId)
			props.AppendTaskID(req.TaskId)
			props.AppendType(taskcommon.SnapshotExportCopy)
			payload, err := proto.Marshal(req)
			require.NoError(t, err)
			status, err := node.CreateTask(context.Background(), &workerpb.CreateTaskRequest{Properties: props, Payload: payload})
			require.NoError(t, merr.CheckRPCCall(status, err))
			want := datapb.ImportTaskStateV2_Failed
			if scenario == "success" {
				want = datapb.ImportTaskStateV2_Completed
			}
			require.Eventually(t, func() bool { return e.query(req.ClusterId, req.TaskId).GetState() == want }, time.Second*2, time.Millisecond)
			response, err := node.QueryTask(context.Background(), &workerpb.QueryTaskRequest{Properties: props})
			require.NoError(t, merr.CheckRPCCall(response.GetStatus(), err))
			result := &datapb.SnapshotExportCopyResponse{}
			require.NoError(t, proto.Unmarshal(response.GetPayload(), result))
			require.Equal(t, want, result.State)
			if sourceErr != nil {
				require.Contains(t, result.GetReason(), "permission")
			}
			if scenario == "source client" || scenario == "initialization" || scenario == "corrupt source" {
				require.Zero(t, calls.Load())
			}
			status, err = node.DropTask(context.Background(), &workerpb.DropTaskRequest{Properties: props})
			require.NoError(t, merr.CheckRPCCall(status, err))
			status, err = node.DropTask(context.Background(), &workerpb.DropTaskRequest{Properties: props})
			require.NoError(t, merr.CheckRPCCall(status, err))
		})
	}
}

func TestSnapshotExportRangeTask_DropBeforeCreateAndValidation(t *testing.T) {
	req, _ := snapshotExportRangeRequest(t, 1)
	e := newSnapshotExportExecutor(context.Background())
	defer e.close()
	require.NoError(t, e.drop(context.Background(), req.ClusterId, req.TaskId))
	require.NoError(t, e.create(req))
	for _, change := range []func(*datapb.SnapshotExportCopyRequest){
		func(r *datapb.SnapshotExportCopyRequest) { r.TaskId = 0 }, func(r *datapb.SnapshotExportCopyRequest) { r.SnapshotMetadataPath = "" },
		func(r *datapb.SnapshotExportCopyRequest) { r.GenerationId = 0 },
		func(r *datapb.SnapshotExportCopyRequest) {
			r.GenerationId = 1
			r.SegmentStart = -1
			r.SegmentEnd = 1
		},
		func(r *datapb.SnapshotExportCopyRequest) { r.GenerationId = 1; r.SegmentEnd = 0 }, func(r *datapb.SnapshotExportCopyRequest) { r.DeadlineTime = 1 },
	} {
		r := proto.Clone(req).(*datapb.SnapshotExportCopyRequest)
		change(r)
		require.Error(t, validateSnapshotExportRequest(r))
	}
}

func TestSnapshotExportRangeTask_ExpiryAndAdmission(t *testing.T) {
	admission := newSnapshotExportExecutor(context.Background())
	require.Error(t, admission.create(nil))
	admission.close()
	node := NewDataNode(context.Background())
	status, err := node.createSnapshotExportCopy(context.Background(), nil)
	require.Error(t, merr.CheckRPCCall(status, err))
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	status, err = node.createSnapshotExportCopy(context.Background(), nil)
	require.Error(t, merr.CheckRPCCall(status, err))
	require.Error(t, merr.Error(node.querySnapshotExportCopy("cluster", 1).Status))
	status, err = node.dropSnapshotExportCopy(context.Background(), "cluster", 1)
	require.Error(t, merr.CheckRPCCall(status, err))
	e := newSnapshotExportExecutor(context.Background())
	defer e.close()
	ctx, cancel := context.WithCancel(context.Background())
	running := &snapshotExportExecution{req: &datapb.SnapshotExportCopyRequest{DeadlineTime: 1}, cancel: cancel, done: make(chan struct{})}
	finished := &snapshotExportExecution{req: &datapb.SnapshotExportCopyRequest{DeadlineTime: 1}, cancel: func() {}, done: make(chan struct{})}
	close(finished.done)
	e.mu.Lock()
	e.tasks[snapshotExportTaskKey{taskID: 1}], e.tasks[snapshotExportTaskKey{taskID: 2}] = running, finished
	e.mu.Unlock()
	require.EqualValues(t, 1, e.slots())
	e.expire(time.Now())
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	e.mu.Lock()
	require.Contains(t, e.tasks, snapshotExportTaskKey{taskID: 1}, "an expired but draining task must remain tracked")
	require.NotContains(t, e.tasks, snapshotExportTaskKey{taskID: 2})
	e.mu.Unlock()
	close(running.done)
	e.expire(time.Now())
	e.mu.Lock()
	require.Empty(t, e.tasks)
	e.mu.Unlock()
}

func TestSnapshotExportRangeTask_ClientCleanupAndExpiryTimer(t *testing.T) {
	req, cm := snapshotExportRangeRequest(t, 0)
	defer mockey.Mock((*snapshotWholeCopyStorage).CopyCrossBucketAndGetSize).Return(int64(0), nil).Build().UnPatch()
	defer mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, ForeignStorageConfig: &indexpb.StorageConfig{}, Copier: &snapshotWholeCopyStorage{}}, nil).Build().UnPatch()
	defer mockey.Mock((*storage.RemoteChunkManager).UnderlyingObjectStorage).Return(&snapshotWholeClosingStorage{}).Build().UnPatch()
	var closed atomic.Bool
	defer mockey.Mock((*snapshotWholeClosingStorage).Close).To(func() error { closed.Store(true); return nil }).Build().UnPatch()
	ticker := time.NewTicker(time.Hour)
	ticks := make(chan time.Time, 1)
	ticker.C = ticks
	tickerCreated := make(chan struct{})
	timer := mockey.Mock(time.NewTicker).To(func(time.Duration) *time.Ticker { close(tickerCreated); return ticker }).Build()
	defer timer.UnPatch()
	e := newSnapshotExportExecutor(context.Background())
	<-tickerCreated
	timer.UnPatch()
	defer e.close()
	e.mu.Lock()
	finished := make(chan struct{})
	close(finished)
	e.tasks[snapshotExportTaskKey{taskID: 99}] = &snapshotExportExecution{req: &datapb.SnapshotExportCopyRequest{DeadlineTime: 1}, cancel: func() {}, done: finished}
	e.mu.Unlock()
	ticks <- time.Now()
	require.Eventually(t, func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.tasks[snapshotExportTaskKey{taskID: 99}] == nil
	}, time.Second, time.Millisecond)
	require.NoError(t, e.create(req))
	require.Eventually(t, func() bool { return e.query(req.ClusterId, req.TaskId).State == datapb.ImportTaskStateV2_Completed }, time.Second, time.Millisecond)
	e.mu.Lock()
	done := e.tasks[snapshotExportTaskKey{clusterID: req.ClusterId, taskID: req.TaskId}].done
	e.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("export execution did not finish client cleanup")
	}
	require.True(t, closed.Load())
}

type snapshotWholeClosingStorage struct {
	storage.ObjectStorage
	io.Closer
}

func TestSnapshotExportRangeTask_IndependentConcurrency(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int32
		want  int
	}{{"configured", 3, 6}, {"above former node cap", 64, 128}, {"legacy request", 0, 256}} {
		t.Run(tc.name, func(t *testing.T) {
			req, cm := snapshotExportRangeRequest(t, 256)
			req.CopyConcurrency = tc.limit
			if tc.limit > 0 {
				req.TaskSlot = 16
			}
			defer mockey.Mock(hardware.GetCPUNum).Return(4).Build().UnPatch()
			cfg := &Params.DataNodeCfg
			Params.Save(cfg.ImportConcurrencyPerCPUCore.Key, "8")
			defer Params.Reset(cfg.ImportConcurrencyPerCPUCore.Key)
			targetStore := &snapshotWholeCopyStorage{}
			started := make(chan struct{}, 1024)
			copy := mockey.Mock((*snapshotWholeCopyStorage).CopyCrossBucketAndGetSize).To(func(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) (int64, error) {
				started <- struct{}{}
				<-ctx.Done()
				return 0, ctx.Err()
			}).Build()
			defer copy.UnPatch()
			defer mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
				ForeignCM: cm, ForeignStorageConfig: &indexpb.StorageConfig{}, Copier: targetStore, ForeignBucket: "target",
			}, nil).Build().UnPatch()
			e := newSnapshotExportExecutor(context.Background())
			defer e.close()
			require.NoError(t, e.create(req))
			second := proto.Clone(req).(*datapb.SnapshotExportCopyRequest)
			second.JobId++
			second.TaskId++
			require.NoError(t, e.create(second))
			require.Equal(t, 2*max(req.TaskSlot, 1), e.slots())
			for range tc.want {
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("copy workers did not reach the requested per-task limit")
				}
			}
			select {
			case <-started:
				t.Fatal("tasks exceeded their individual limits")
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

func TestSnapshotCopyBudgetsIndependentForExportAndRestore(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	permits := make(chan struct{}, 1)
	exportExecutor := newSnapshotExportExecutor(ctx)
	defer exportExecutor.close()
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	sourcePath := path.Join(root, "insert_log/111/222/333/1/10001")
	targetPath := path.Join(root, "insert_log/444/555/666/1/10001")
	require.NoError(t, cm.Write(ctx, sourcePath, []byte("data")))
	factory := &copySegmentStorageFactoryTarget{}
	defer mockey.Mock((*copySegmentStorageFactoryTarget).NewChunkManager).Return(cm, nil).Build().UnPatch()
	node := &DataNode{ctx: ctx, lifetime: lifetime.NewLifetime(commonpb.StateCode_Healthy),
		storageFactory: factory, importTaskMgr: importv2.NewTaskManager(),
		snapshotExportExecutor: exportExecutor}
	req := &datapb.CopySegmentRequest{TaskID: 1, JobID: 1, TaskSlot: 1, CopyConcurrency: 1,
		StorageConfig: &indexpb.StorageConfig{StorageType: "local", RootPath: root},
		Sources: []*datapb.CopySegmentSource{{CollectionId: 111, PartitionId: 222, SegmentId: 333,
			InsertBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: sourcePath}}}},
		}},
		Targets: []*datapb.CopySegmentTarget{{CollectionId: 444, PartitionId: 555, SegmentId: 666, TargetRootPath: root}},
	}
	status, err := node.copySegment(ctx, req, false)
	require.NoError(t, merr.CheckRPCCall(status, err))
	started, release := make(chan struct{}), make(chan struct{})
	defer mockey.Mock((*snapshotWholeCopyStorage).CopyCrossBucketAndGetSize).To(func(ctx context.Context, _, _, _, _ string) (int64, error) {
		close(started)
		select {
		case <-release:
			return 4, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}).Build().UnPatch()
	exportDone := make(chan error, 1)
	go func() {
		exportDone <- snapshotstorage.CopySnapshotExportPlan(ctx,
			[]snapshotstorage.FileCopy{{SourcePath: sourcePath, DestinationPath: "export/data"}},
			"", "", &snapshotWholeCopyStorage{}, permits, nil)
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	task := node.importTaskMgr.Get(req.GetTaskID())
	futures := task.Execute()
	require.Len(t, futures, 1)
	restoreDone := make(chan error, 1)
	go func() { _, err := futures[0].Await(); restoreDone <- err }()
	select {
	case err := <-restoreDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("restore waited for an unrelated export task")
	}
	exists, err := cm.Exist(ctx, targetPath)
	require.NoError(t, err)
	require.True(t, exists)
	close(release)
	require.NoError(t, <-exportDone)
	content, err := cm.Read(ctx, targetPath)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), content)
	require.Empty(t, permits)
}

// Exercise registry isolation through the generic worker RPC boundary.
func TestSnapshotExportRangeTask_ClusterIsolation(t *testing.T) {
	req, _ := snapshotExportRangeRequest(t, 0)
	defer mockey.Mock((*snapshotExportExecutor).execute).To(func(_ *snapshotExportExecutor, ctx context.Context, _ *snapshotExportExecution) error {
		<-ctx.Done()
		return ctx.Err()
	}).Build().UnPatch()
	e := newSnapshotExportExecutor(context.Background())
	defer e.close()
	node := &DataNode{lifetime: lifetime.NewLifetime(commonpb.StateCode_Healthy), snapshotExportExecutor: e}
	properties := func(cluster string) taskcommon.Properties {
		p := taskcommon.NewProperties(nil)
		p.AppendClusterID(cluster)
		p.AppendTaskID(req.TaskId)
		p.AppendType(taskcommon.SnapshotExportCopy)
		return p
	}
	for _, cluster := range []string{"a", "b"} {
		request := proto.Clone(req).(*datapb.SnapshotExportCopyRequest)
		request.ClusterId = cluster
		payload, err := proto.Marshal(request)
		require.NoError(t, err)
		status, err := node.CreateTask(context.Background(), &workerpb.CreateTaskRequest{Properties: properties(cluster), Payload: payload})
		require.NoError(t, merr.CheckRPCCall(status, err))
	}
	query := func(cluster string) *workerpb.QueryTaskResponse {
		response, err := node.QueryTask(context.Background(), &workerpb.QueryTaskRequest{Properties: properties(cluster)})
		require.NoError(t, err)
		return response
	}
	for _, cluster := range []string{"a", "b"} {
		require.NoError(t, merr.Error(query(cluster).GetStatus()))
	}
	for _, cluster := range []string{"missing", "a", "a"} {
		status, err := node.DropTask(context.Background(), &workerpb.DropTaskRequest{Properties: properties(cluster)})
		require.NoError(t, merr.CheckRPCCall(status, err))
		require.NoError(t, merr.Error(query("b").GetStatus()))
		require.Equal(t, datapb.ImportTaskStateV2_InProgress, e.query("b", req.TaskId).GetState())
	}
	require.ErrorIs(t, merr.Error(query("a").GetStatus()), merr.ErrImportSysFailed)
	require.ErrorIs(t, merr.Error(query("missing").GetStatus()), merr.ErrImportSysFailed)
}

func TestQuerySlotExportUsage(t *testing.T) {
	paramtable.Init()
	done := make(chan struct{})
	node := &DataNode{
		lifetime:           lifetime.NewLifetime(commonpb.StateCode_Healthy),
		taskScheduler:      &index.TaskScheduler{TaskQueue: index.NewIndexBuildTaskQueue(nil)},
		compactionExecutor: compactor.NewExecutor(),
		importScheduler:    importv2.NewScheduler(importv2.NewTaskManager()),
		snapshotExportExecutor: &snapshotExportExecutor{tasks: map[snapshotExportTaskKey]*snapshotExportExecution{
			{clusterID: "test", taskID: 1}: {req: &datapb.SnapshotExportCopyRequest{TaskSlot: 2}, done: done},
		}},
	}
	for _, used := range []int64{2, 0} {
		resp, err := node.QuerySlot(context.Background(), nil)
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.Equal(t, max(index.CalculateNodeSlots()-used, 0), resp.GetAvailableSlots())
		require.Equal(t, float64(used), testutil.ToFloat64(metrics.DataNodeSlot.WithLabelValues(fmt.Sprint(node.GetNodeID()), "exportUsed")))
		if used != 0 {
			close(done)
		}
	}
}

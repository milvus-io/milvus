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
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
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
	"github.com/milvus-io/milvus/internal/datanode/resource"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/common"
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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type DataNodeServicesSuite struct {
	suite.Suite

	node          *DataNode
	storageConfig *indexpb.StorageConfig
	etcdCli       *clientv3.Client
	guardMock     *mockey.Mocker
	guard         *resource.RecordingGuard
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
	// The node's executors admit every task through the resource guard. Route
	// that at a double: the process-wide guard freezes admission from the
	// host's live memory reading, which would make these tests pass, fail or
	// hang depending on what else the machine is doing.
	//
	// Several tests below re-enter SetupTest without a matching TearDownTest,
	// and mockey panics on a second patch of a function it already holds, so
	// the patch is installed once and left in place until teardown.
	if s.guardMock == nil {
		s.guard = resource.NewRecordingGuard()
		s.guardMock = mockey.Mock(resource.GetGuard).Return(s.guard).Build()
	}

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
	if s.guardMock != nil {
		s.guardMock.UnPatch()
		s.guardMock = nil
		s.guard = nil
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
		mockC.EXPECT().GetStorageConfig().Return(s.storageConfig)
		mockC.EXPECT().GetPlan().Return(nil)
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
		mockC2.EXPECT().GetStorageConfig().Return(s.storageConfig)
		mockC2.EXPECT().GetPlan().Return(nil)
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
		JobID:         100,
		TaskID:        202,
		TaskSlot:      1,
		StorageConfig: targetStorageConfig,
		ExternalSpec:  foreignSpec,
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

func TestLegacyAvailableSlotsFoldsWorstDimension(t *testing.T) {
	paramtable.Init()

	// Memory is 90% consumed while CPU is only 10% consumed. The folded
	// scalar must follow the worse dimension so an old coordinator sees the
	// node as full early rather than late.
	snap := resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved: taskresource.Capacity{CPU: 1, Memory: 900},
	}

	assert.Equal(t, int64(12), legacyAvailableSlots(snap, 128))
}

// TestLegacyAvailableSlotsFoldsCPUDominant is the mirror of
// TestLegacyAvailableSlotsFoldsWorstDimension: CPU is 90% consumed while
// memory is only 1% consumed, so the CPU ratio alone must win the max. The
// two tests together pin both directions of the fold; without this one, a
// max-tracker that only ever looked at the memory ratio (or compared them in
// the wrong order) could still pass every existing test.
func TestLegacyAvailableSlotsFoldsCPUDominant(t *testing.T) {
	paramtable.Init()

	snap := resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved: taskresource.Capacity{CPU: 9, Memory: 10},
	}

	assert.Equal(t, int64(12), legacyAvailableSlots(snap, 128))
}

// TestLegacyAvailableSlotsDoesNotLoseASlotToFloatingPoint pins the exact
// arithmetic of the fold, which the two tests above cannot see.
//
// They both use legacyTotal=128, where the rounding error is invisible: the
// mathematically correct answer is 12.8, so 12.799999999999997 and 12.8 both
// truncate to 12. The error only surfaces when legacyTotal * (1 - utilization)
// lands exactly on an integer, i.e. when legacyTotal is a multiple of 10 for a
// 90%-consumed dimension.
//
// Folding the free fraction as `1 - reserved/total` costs two roundings: 900/1000
// is the double nearest 0.9 (which is slightly ABOVE 0.9), and subtracting that
// from 1 lands on 0.09999999999999998, slightly BELOW a tenth. 80 x that is
// 7.999999999999998, and int64() truncates towards zero -- so a node with exactly
// one tenth of its budget free advertised 7 slots instead of 8. Computing the
// free fraction directly as (total-reserved)/total is one rounding instead of
// two and lands on the double nearest 0.1, which is what this asserts.
//
// The lost slot is small but it is not noise: it is a systematic under-report,
// always in the direction of hiding capacity from DataCoord, and it is exactly
// what made TestQuerySlotReportsTheLedgersView and
// TestGetJobStatsReportsTheLedgersView fail in CI (whose legacyTotal is 80)
// while passing on hardware whose CalculateNodeSlots() is not a multiple of 10.
func TestLegacyAvailableSlotsDoesNotLoseASlotToFloatingPoint(t *testing.T) {
	paramtable.Init()

	// Exactly one tenth of the memory budget is free; CPU is nine tenths free,
	// so memory is the worse dimension and must win the fold.
	snap := resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved: taskresource.Capacity{CPU: 1, Memory: 900},
	}

	assert.Equal(t, int64(8), legacyAvailableSlots(snap, 80),
		"one tenth of 80 slots is 8, not 7")

	// The same rounding, on the CPU arm rather than the memory arm.
	cpuBound := resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved: taskresource.Capacity{CPU: 9, Memory: 10},
	}

	assert.Equal(t, int64(8), legacyAvailableSlots(cpuBound, 80),
		"one tenth of 80 slots is 8, not 7, whichever dimension is binding")
}

func TestLegacyAvailableSlotsZeroWhenFrozen(t *testing.T) {
	paramtable.Init()

	snap := resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved: taskresource.Capacity{CPU: 0, Memory: 0},
		Frozen:   true,
	}

	assert.Equal(t, int64(0), legacyAvailableSlots(snap, 128))
}

func TestLegacyAvailableSlotsNeverNegative(t *testing.T) {
	paramtable.Init()

	snap := resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved: taskresource.Capacity{CPU: 50, Memory: 5000},
	}

	assert.Equal(t, int64(0), legacyAvailableSlots(snap, 128))
}

// TestLegacyAvailableSlotsZeroWhenExclusive guards Resolution 1: an oversized
// task running alone must read as a full node even though Reserved is tiny
// relative to Total here -- the utilization formula alone would report the
// node as nearly empty (util=0.1, ~115 slots free). Node capacity is runtime
// config and can grow after the oversized task is admitted, which can pull
// its ratio back under 1; ExclusiveTaskID must be checked explicitly so the
// scalar does not depend on that race.
func TestLegacyAvailableSlotsZeroWhenExclusive(t *testing.T) {
	paramtable.Init()

	snap := resource.Snapshot{
		Total:           taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved:        taskresource.Capacity{CPU: 1, Memory: 10},
		ExclusiveTaskID: 42,
	}

	assert.Equal(t, int64(0), legacyAvailableSlots(snap, 128))
}

// availableSlots is the scalar DataCoord actually reads, and until now nothing
// exercised it end to end: the RecordingGuard's Snapshot() returned the zero
// value, so Total.CPU and Total.Memory were both 0, legacyAvailableSlots
// short-circuited, and the answer was legacyTotal -- exactly what the code this
// branch replaces produced. The fixture made old and new behavior
// indistinguishable.
func (s *DataNodeServicesSuite) TestQuerySlotReportsTheLedgersView() {
	s.SetupTest()

	legacyTotal := index.CalculateNodeSlots()
	s.Require().Greater(legacyTotal, int64(10), "setup: the fold below needs room to be visible")

	// 90% of memory committed, 10% of CPU: the worse dimension must win.
	s.guard.SetSnapshot(resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved: taskresource.Capacity{CPU: 1, Memory: 900},
	})

	resp, err := s.node.QuerySlot(context.Background(), nil)
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))

	s.Equal(int64(float64(legacyTotal)*0.1), resp.GetAvailableSlots())
	s.Less(resp.GetAvailableSlots(), legacyTotal,
		"a loaded ledger must not read as a completely free node")
}

// The index side reports the same scalar through a different RPC, so it needs
// the same guard.
func (s *DataNodeServicesSuite) TestGetJobStatsReportsTheLedgersView() {
	s.SetupTest()

	legacyTotal := index.CalculateNodeSlots()
	s.guard.SetSnapshot(resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved: taskresource.Capacity{CPU: 1, Memory: 900},
	})

	resp, err := s.node.GetJobStats(context.Background(), &workerpb.GetJobStatsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))

	s.Equal(legacyTotal, resp.GetTotalSlots())
	s.Equal(int64(float64(legacyTotal)*0.1), resp.GetAvailableSlots())
	s.Less(resp.GetAvailableSlots(), legacyTotal)
}

// A frozen node must report zero through the RPC, not merely through the
// helper. This is also the only driver of the frozen mlog.Warn branch, which
// was undrivable while the double's snapshot was hard-coded to zero.
func (s *DataNodeServicesSuite) TestQuerySlotReportsZeroWhenFrozen() {
	s.SetupTest()

	s.guard.SetSnapshot(resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 10, Memory: 1000},
		Reserved: taskresource.Capacity{CPU: 0, Memory: 0},
		Frozen:   true,
		NonTask:  4 << 30,
	})

	resp, err := s.node.QuerySlot(context.Background(), nil)
	s.NoError(err)
	s.EqualValues(0, resp.GetAvailableSlots())

	jobResp, err := s.node.GetJobStats(context.Background(), &workerpb.GetJobStatsRequest{})
	s.NoError(err)
	s.EqualValues(0, jobResp.GetAvailableSlots())
}

// The incident node from issue #52180: 16 cores, 64GiB, so
// CalculateNodeSlots reports 128 and the ledger budget is 64GiB x 0.75.
const (
	incidentLegacyTotal = int64(128)
	incidentBudget      = int64(48) << 30
)

func incidentSnapshot(reservedMemory int64, reservedCPU float64) resource.Snapshot {
	return resource.Snapshot{
		Total:    taskresource.Capacity{CPU: 16, Memory: incidentBudget},
		Reserved: taskresource.Capacity{CPU: reservedCPU, Memory: reservedMemory},
	}
}

// The scalar slot the wire still carries has to mean the same thing on both
// sides of it. This node folds its two-dimensional state into
// CalculateNodeSlots x (1 - budget utilization), so one reported slot stands
// for exactly one CalculateNodeSlots-th of the ledger budget -- and that is the
// number DataCoord must divide a byte estimate by
// (taskresource.LegacyMemoryPerSlot, memoryToSlots) and the node must multiply
// a received slot back up by (LegacySlotToRequirement).
//
// This pins the two against each other on real node shapes, because the
// derivation is algebra over CalculateNodeSlots and nothing else would notice
// if a term of it were dropped: it breaks no invariant and fails no other test,
// it just makes the coordinator dispatch the wrong amount of work forever.
func TestLegacyMemoryPerSlotMatchesCalculateNodeSlots(t *testing.T) {
	paramtable.Init()

	withNode := func(t *testing.T, cores int, memory uint64) {
		t.Helper()
		cpuMock := mockey.Mock(hardware.GetCPUNum).Return(cores).Build()
		t.Cleanup(func() { cpuMock.UnPatch() })
		memMock := mockey.Mock(hardware.GetMemoryCount).Return(memory).Build()
		t.Cleanup(func() { memMock.UnPatch() })
	}

	t.Run("memory-bound node: the whole budget is exactly legacyTotal slots", func(t *testing.T) {
		// The incident node from issue #52180.
		withNode(t, 16, uint64(64)<<30)

		legacyTotal := index.CalculateNodeSlots()
		require.EqualValues(t, incidentLegacyTotal, legacyTotal, "setup: min(16/2, 64/8) x 16")
		budget := taskresource.NodeCapacity().Memory
		require.EqualValues(t, incidentBudget, budget, "setup: 64GiB x memoryRatio 0.75")

		perSlot := taskresource.LegacyMemoryPerSlot()
		assert.EqualValues(t, int64(384)<<20, perSlot, "8GiB / 16 x 0.75")
		assert.Equal(t, budget, legacyTotal*perSlot,
			"a slot must be worth exactly the slice of the budget legacyAvailableSlots folds it out of")
	})

	t.Run("standalone: fewer slots, each worth proportionally more", func(t *testing.T) {
		withNode(t, 16, uint64(64)<<30)
		paramtable.SetRole(typeutil.StandaloneRole)
		defer paramtable.SetRole("")

		legacyTotal := index.CalculateNodeSlots()
		require.EqualValues(t, 32, legacyTotal, "setup: 128 x standaloneSlotRatio 0.25")

		assert.EqualValues(t, int64(1536)<<20, taskresource.LegacyMemoryPerSlot())
		assert.Equal(t, taskresource.NodeCapacity().Memory, legacyTotal*taskresource.LegacyMemoryPerSlot())
	})

	t.Run("CPU-bound node: the rate understates a slot, never overstates it", func(t *testing.T) {
		// 8 cores against 128GiB: the cores, not the memory, set the slot count.
		withNode(t, 8, uint64(128)<<30)

		legacyTotal := index.CalculateNodeSlots()
		require.EqualValues(t, 64, legacyTotal, "setup: min(8/2, 128/8) x 16")
		budget := taskresource.NodeCapacity().Memory

		perSlot := taskresource.LegacyMemoryPerSlot()
		assert.EqualValues(t, int64(384)<<20, perSlot, "the rate does not depend on the hardware")
		assert.Less(t, legacyTotal*perSlot, budget,
			"understating a slot makes DataCoord charge more slots and dispatch less, which is the safe direction")
	})

	t.Run("dropping the memoryRatio term would overstate every slot", func(t *testing.T) {
		withNode(t, 16, uint64(64)<<30)
		pt := paramtable.Get()
		pt.Save(pt.DataNodeCfg.ResourceMemoryRatio.Key, "0.5")
		defer pt.Reset(pt.DataNodeCfg.ResourceMemoryRatio.Key)

		// CalculateNodeSlots does not know about memoryRatio, so the whole
		// difference has to show up in the rate: half the budget, half the
		// bytes per slot, same slot count.
		require.EqualValues(t, incidentLegacyTotal, index.CalculateNodeSlots())
		assert.EqualValues(t, int64(256)<<20, taskresource.LegacyMemoryPerSlot())
		assert.Equal(t, taskresource.NodeCapacity().Memory,
			index.CalculateNodeSlots()*taskresource.LegacyMemoryPerSlot())
	})
}

// availableSlots takes whichever of the ledger and the executors' queues
// reports the node busier.
// The ledger counts only ADMITTED tasks, so a node holding a large queue of
// accepted-but-not-yet-started work has an empty ledger and would otherwise
// advertise itself completely free -- DataCoord's water-filling would keep
// choosing it until the compaction executor's channel filled and Enqueue
// blocked inside the gRPC handler.
func TestAvailableSlotsCountsQueuedWorkToo(t *testing.T) {
	paramtable.Init()

	perSlot := taskresource.LegacyMemoryPerSlot()
	idle := incidentSnapshot(0, 0)

	// Nothing admitted, nothing queued: the whole node.
	assert.Equal(t, incidentLegacyTotal, availableSlots(idle, incidentLegacyTotal, 0, 0, 0))

	// Nothing admitted, but index and import have accepted 50 of the node's
	// 128 slots. The ledger still says "free"; the answer must not.
	assert.Equal(t, int64(78), availableSlots(idle, incidentLegacyTotal, 40, 0, 10))

	// Same for a compaction backlog. These counters are in units of memory --
	// LegacyMemoryPerSlot() each, 384MiB at the defaults -- so 100 of them is
	// 37.5GiB of the 48GiB budget: ~78% full.
	queuedBytes := 100 * perSlot
	require.Equal(t, int64(37)<<30+int64(512)<<20, queuedBytes, "setup: 37.5GiB queued")
	assert.Equal(t, int64(28), availableSlots(idle, incidentLegacyTotal, 0, 100, 0))

	// The ledger stays authoritative when it is the more conservative of the
	// two: 90% of the budget committed is 12 slots, well below what a small
	// queue would allow.
	loaded := incidentSnapshot(incidentBudget/10*9, 1)
	assert.Equal(t, int64(12), availableSlots(loaded, incidentLegacyTotal, 10, 0, 0))

	// Over-subscribed queues clamp at zero rather than going negative.
	assert.Equal(t, int64(0), availableSlots(idle, incidentLegacyTotal, 100, 0, 100))

	// And a frozen node is still zero however empty the queues are.
	frozen := idle
	frozen.Frozen = true
	assert.Equal(t, int64(0), availableSlots(frozen, incidentLegacyTotal, 0, 0, 0))
}

// The queue arm must not be denominated in legacyTotal, because the compaction
// counter is not on that scale: DataCoord prices a mix compaction as
// memoryToSlots(bytes) = bytes / LegacyMemoryPerSlot(), so the counter is in
// units of memory. legacyTotal x LegacyMemoryPerSlot() equals the ledger budget
// only on a memory-bound node whose guard has taken nothing off the top; on a
// CPU-bound node CalculateNodeSlots is set by the cores, and the legacy
// denominator saturates while the budget is nearly empty (see the sub-case
// below).
func TestAvailableSlotsDoesNotCapCompactionOnTheLegacySlotScale(t *testing.T) {
	paramtable.Init()

	perSlot := taskresource.LegacyMemoryPerSlot()
	const perTask = int64(4608) << 20 // 4.5GiB
	taskSlots := perTask / perSlot
	require.Equal(t, int64(12), taskSlots, "setup: memoryToSlots prices this task at 12 slots")

	// Three of them accepted and admitted: 13.5GiB of a 48GiB budget.
	snap := incidentSnapshot(3*perTask, 3)
	got := availableSlots(snap, incidentLegacyTotal, 0, 3*taskSlots, 0)

	// The ledger is the honest constraint here (28% committed), so the queue
	// arm must not bind at all.
	ledgerOnly := legacyAvailableSlots(snap, incidentLegacyTotal)
	require.Equal(t, int64(92), ledgerOnly, "setup: 13.5GiB of 48GiB leaves ~72%")
	assert.Equal(t, ledgerOnly, got,
		"the queue arm must not report a node busier than its own ledger does")

	// Stated the way DataCoord reads it: it divides the reported availability
	// by this task's own slot count to decide how many more will fit. The
	// ledger has room for seven more; the answer must not be zero.
	assert.GreaterOrEqual(t, got/taskSlots, int64(2),
		"the node must still claim room for more of the same task")

	// The case where the two denominators visibly disagree: 8 cores and
	// 128GiB, so CalculateNodeSlots is min(4, 16) x 16 = 64 while the ledger
	// budget is 96GiB -- four times what those 64 slots are worth. A 24GiB
	// compaction backlog is 64 counter units, which on the legacy scale is the
	// whole node and against the budget is a quarter of it.
	const (
		cpuBoundLegacyTotal = int64(64)
		cpuBoundBudget      = int64(96) << 30
	)
	cpuBound := resource.Snapshot{
		Total: taskresource.Capacity{CPU: 8, Memory: cpuBoundBudget},
	}
	queued := 24 * (int64(1) << 30) / perSlot
	require.Equal(t, cpuBoundLegacyTotal, queued, "setup: the backlog is exactly legacyTotal counter units")
	assert.Equal(t, int64(48), availableSlots(cpuBound, cpuBoundLegacyTotal, 0, queued, 0),
		"a quarter of the budget must not read as a full node")
}

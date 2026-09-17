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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
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
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
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
		s.node.compactionExecutor.Enqueue(mockC, taskcommon.Resource{})

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
		s.node.compactionExecutor.Enqueue(mockC2, taskcommon.Resource{})

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
		cpuMock := mockey.Mock(hardware.GetCPUNum).Return(16).Build()
		defer cpuMock.UnPatch()
		memMock := mockey.Mock(hardware.GetMemoryCount).Return(uint64(64) << 30).Build()
		defer memMock.UnPatch()
		roleMock := mockey.Mock(paramtable.GetRole).Return(typeutil.DataNodeRole).Build()
		defer roleMock.UnPatch()

		ctx := context.Background()
		resp, err := s.node.QuerySlot(ctx, nil)
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		s.NoError(merr.Error(resp.GetStatus()))
		s.Equal(int64(16), resp.GetTotalCpu())
		s.Equal(int64(64)<<30, resp.GetTotalMemory())
		// Nothing is booked on a fresh node, so everything is available.
		s.Equal(resp.GetTotalCpu(), resp.GetAvailableCpu())
		s.Equal(resp.GetTotalMemory(), resp.GetAvailableMemory())
	})

	s.Run("booked resource is subtracted", func() {
		s.SetupTest()
		cpuMock := mockey.Mock(hardware.GetCPUNum).Return(16).Build()
		defer cpuMock.UnPatch()
		memMock := mockey.Mock(hardware.GetMemoryCount).Return(uint64(64) << 30).Build()
		defer memMock.UnPatch()
		roleMock := mockey.Mock(paramtable.GetRole).Return(typeutil.DataNodeRole).Build()
		defer roleMock.UnPatch()

		// A detached executor: nothing consumes its task channel, so the
		// booking stays put for the duration of the assertion.
		s.node.compactionExecutor = compactor.NewExecutor()
		mockC := compactor.NewMockCompactor(s.T())
		mockC.EXPECT().GetPlanID().Return(int64(9527))
		mockC.EXPECT().GetSlotUsage().Return(int64(8))
		succeed, err := s.node.compactionExecutor.Enqueue(mockC, taskcommon.Resource{CPU: 4, Memory: 8 << 30})
		s.True(succeed)
		s.NoError(err)

		resp, err := s.node.QuerySlot(context.Background(), nil)
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		s.Equal(int64(16), resp.GetTotalCpu())
		s.Equal(int64(12), resp.GetAvailableCpu())
		s.Equal(int64(64)<<30, resp.GetTotalMemory())
		s.Equal(int64(56)<<30, resp.GetAvailableMemory())
	})

	s.Run("standalone discounts the totals", func() {
		s.SetupTest()
		cpuMock := mockey.Mock(hardware.GetCPUNum).Return(16).Build()
		defer cpuMock.UnPatch()
		memMock := mockey.Mock(hardware.GetMemoryCount).Return(uint64(64) << 30).Build()
		defer memMock.UnPatch()
		roleMock := mockey.Mock(paramtable.GetRole).Return(typeutil.StandaloneRole).Build()
		defer roleMock.UnPatch()

		resp, err := s.node.QuerySlot(context.Background(), nil)
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		s.Equal(int64(4), resp.GetTotalCpu())
		s.Equal(int64(16)<<30, resp.GetTotalMemory())
	})

	s.Run("standalone floors cpu at one core", func() {
		s.SetupTest()
		cpuMock := mockey.Mock(hardware.GetCPUNum).Return(2).Build()
		defer cpuMock.UnPatch()
		memMock := mockey.Mock(hardware.GetMemoryCount).Return(uint64(8) << 30).Build()
		defer memMock.UnPatch()
		roleMock := mockey.Mock(paramtable.GetRole).Return(typeutil.StandaloneRole).Build()
		defer roleMock.UnPatch()

		resp, err := s.node.QuerySlot(context.Background(), nil)
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		// 2 * 0.25 rounds down to 0; the floor keeps a whole core offered.
		s.Equal(int64(1), resp.GetTotalCpu())
		s.Equal(int64(2)<<30, resp.GetTotalMemory())
	})

	s.Run("a zero standalone factor reports as a scalar-only worker", func() {
		s.SetupTest()
		cpuMock := mockey.Mock(hardware.GetCPUNum).Return(16).Build()
		defer cpuMock.UnPatch()
		memMock := mockey.Mock(hardware.GetMemoryCount).Return(uint64(64) << 30).Build()
		defer memMock.UnPatch()
		roleMock := mockey.Mock(paramtable.GetRole).Return(typeutil.StandaloneRole).Build()
		defer roleMock.UnPatch()

		key := paramtable.Get().DataNodeCfg.StandaloneSlotRatio.Key
		paramtable.Get().Save(key, "0")
		defer paramtable.Get().Reset(key)

		resp, err := s.node.QuerySlot(context.Background(), nil)
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		// Memory rounds away entirely, which DataCoord reads as "no ledger" and
		// routes through the scalar slot heap. CPU keeps its one-core floor.
		s.Equal(int64(0), resp.GetTotalMemory())
		s.Equal(int64(1), resp.GetTotalCpu())
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

// TestCreateTaskBooksResourceFromProperties is the end-to-end proof that the
// estimate DataCoord put in the CreateTask properties reaches the ledger the
// worker reports back in QuerySlot -- the request payload carries no such
// fields.
func (s *DataNodeServicesSuite) TestCreateTaskBooksResourceFromProperties() {
	// A scheduler that was never started has no consumer goroutine, so what
	// CreateTask books stays on the ledger for the assertion.
	original := s.node.taskScheduler
	s.node.taskScheduler = index.NewTaskScheduler(s.ctx)
	defer func() { s.node.taskScheduler = original }()

	createIndexTask := func(buildID int64, extra map[string]string) *commonpb.Status {
		properties := map[string]string{
			taskcommon.ClusterIDKey: "cluster-0",
			taskcommon.TypeKey:      taskcommon.Index,
			taskcommon.TaskIDKey:    strconv.FormatInt(buildID, 10),
		}
		for k, v := range extra {
			properties[k] = v
		}
		payload, err := proto.Marshal(&workerpb.CreateJobRequest{
			BuildID:       buildID,
			StorageConfig: s.storageConfig,
		})
		s.NoError(err)
		status, err := s.node.CreateTask(s.ctx, &workerpb.CreateTaskRequest{
			Properties: properties,
			Payload:    payload,
		})
		s.NoError(err)
		return status
	}

	s.Run("properties carry the estimate", func() {
		before := s.node.taskScheduler.TaskQueue.GetUsingResource()
		status := createIndexTask(9001, map[string]string{
			taskcommon.CPUKey:    "2",
			taskcommon.MemoryKey: strconv.FormatInt(1<<30, 10),
		})
		s.NoError(merr.CheckRPCCall(status, nil))
		s.Equal(before.Add(taskcommon.Resource{CPU: 2, Memory: 1 << 30}),
			s.node.taskScheduler.TaskQueue.GetUsingResource())
	})

	s.Run("absent properties book zero", func() {
		// A coordinator that predates the keys: the task is accepted and books
		// nothing, rather than being rejected.
		before := s.node.taskScheduler.TaskQueue.GetUsingResource()
		status := createIndexTask(9002, nil)
		s.NoError(merr.CheckRPCCall(status, nil))
		s.Equal(before, s.node.taskScheduler.TaskQueue.GetUsingResource())
	})

	s.Run("unparsable properties are rejected", func() {
		before := s.node.taskScheduler.TaskQueue.GetUsingResource()
		status := createIndexTask(9003, map[string]string{
			taskcommon.CPUKey:    "not-a-number",
			taskcommon.MemoryKey: "1024",
		})
		s.Error(merr.CheckRPCCall(status, nil))
		// Nothing was booked for a task that never started.
		s.Equal(before, s.node.taskScheduler.TaskQueue.GetUsingResource())
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

		status, err := s.node.copySegment(s.ctx, &datapb.CopySegmentRequest{}, false, taskcommon.Resource{})
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

		status, err := s.node.copySegment(s.ctx, req, false, taskcommon.Resource{})
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

		status, err := s.node.copySegment(s.ctx, req, false, taskcommon.Resource{})
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
		status, err := s.node.copySegment(s.ctx, req, true, taskcommon.Resource{})
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
			_ taskcommon.Resource,
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
			_ taskcommon.Resource,
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
			_ taskcommon.Resource,
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

	status, err := s.node.copySegment(s.ctx, req, true, taskcommon.Resource{})
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

	status, err := s.node.copySegment(s.ctx, createReq, false, taskcommon.Resource{})
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

	status, err := s.node.copySegment(s.ctx, createReq, false, taskcommon.Resource{})
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

		status, err := s.node.copySegment(s.ctx, createReq, false, taskcommon.Resource{})
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

type dataNodeLogBuffer struct {
	bytes.Buffer
}

func (*dataNodeLogBuffer) Sync() error {
	return nil
}

func captureDataNodeLogs(t *testing.T) *dataNodeLogBuffer {
	t.Helper()

	oldLogger := mlog.L()
	oldLevel := mlog.GetAtomicLevel()
	logs := &dataNodeLogBuffer{}
	logger, props, err := mlog.InitLoggerWithWriteSyncer(&mlog.Config{
		Level:             "debug",
		Format:            "text",
		DisableCaller:     true,
		DisableTimestamp:  true,
		DisableStacktrace: true,
	}, logs)
	require.NoError(t, err)
	mlog.ReplaceGlobals(logger, props)
	t.Cleanup(func() {
		mlog.ReplaceGlobals(oldLogger, &mlog.ZapProperties{Level: oldLevel})
	})
	return logs
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
				return node.copySegment(ctx, &datapb.CopySegmentRequest{TaskID: 6, StorageConfig: storageConfig}, false, taskcommon.Resource{})
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

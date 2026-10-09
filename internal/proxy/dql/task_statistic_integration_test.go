// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type StatisticTaskSuite struct {
	suite.Suite
	mixc types.MixCoordClient
	qn   *mocks.MockQueryNodeClient

	lb        shardclient.LBPolicy
	mgr       shardclient.ShardClientMgr
	metaCache *metacache.MetaCache

	collectionName string
	collectionID   int64
}

func (s *StatisticTaskSuite) SetupSuite() {
	paramtable.Init()
}

func (s *StatisticTaskSuite) SetupTest() {
	successStatus := commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	mixc := mocks.NewMockMixCoordClient(s.T())

	mixc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &successStatus,
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
				Serviceable: []bool{true, true, true},
			},
		},
	}, nil).Maybe()

	mixc.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status:       &successStatus,
		PartitionIDs: []int64{1, 2, 3},
	}, nil).Maybe()

	s.mixc = mixc
	mixc.EXPECT().Close().Return(nil).Maybe()
	s.qn = mocks.NewMockQueryNodeClient(s.T())

	s.qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	mgr := shardclient.NewMockShardClientManager(s.T())
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil).Maybe()
	mgr.EXPECT().GetShardLeaderList(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]string{"mock_qn"}, nil).Maybe()
	mgr.EXPECT().GetShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]shardclient.NodeInfo{
		{NodeID: 1, Address: "mock_qn", Serviceable: true},
	}, nil).Maybe()
	mgr.EXPECT().InvalidateShardLeaderCache(mock.Anything).Return().Maybe()
	s.mgr = mgr
	// Stub shard dispatch: Execute runs the workload directly against the mock
	// query node, bypassing shard-leader resolution against the coordinator.
	lb := shardclient.NewMockLBPolicy(s.T())
	lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(context.Background(), 1, s.qn, "mock_qn")
		}).Maybe()
	s.lb = lb

	cache := newTestCache()
	s.metaCache = cache

	s.collectionName = "test_statistics_task"
	s.loadCollection()
}

func (s *StatisticTaskSuite) loadCollection() {
	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}

	schema := constructCollectionSchemaByDataType(s.collectionName, fieldName2Types, testInt64Field, false)
	schemaInfo := mustNewSchemaInfo(schema)

	collectionID := int64(1000)
	mockTest(s.T(), (*metacache.MetaCache).GetCollectionID, collectionID, nil)
	mockTest(s.T(), (*metacache.MetaCache).GetCollectionSchema, schemaInfo, nil)
	mockTest(s.T(), (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{CollID: collectionID}, nil)
	mockTest(s.T(), (*metacache.MetaCache).GetPartitions, map[string]int64{"_default": 1}, nil)
	s.collectionID = collectionID
}

func (s *StatisticTaskSuite) TearDownSuite() {
	s.mixc.Close()
}

func (s *StatisticTaskSuite) TestStatisticTask_Timeout() {
	ctx := context.Background()
	task := s.GetStatisticsTask(ctx)

	s.NoError(task.OnEnqueue())

	// test query task with timeout
	ctx1, cancel1 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel1()
	// before preExecute
	s.Equal(typeutil.ZeroTimestamp, task.TimeoutTimestamp)
	task.ctx = ctx1
	s.NoError(task.PreExecute(ctx))
	// after preExecute
	s.Greater(task.TimeoutTimestamp, typeutil.ZeroTimestamp)
}

func (s *StatisticTaskSuite) GetStatisticsTask(ctx context.Context) *GetStatisticsTask {
	return &GetStatisticsTask{
		baseTask:       baseTask{MetaCache: s.metaCache},
		Condition:      NewTaskCondition(ctx),
		ctx:            ctx,
		collectionName: s.collectionName,
		result: &milvuspb.GetStatisticsResponse{
			Status: merr.Success(),
		},
		request: &milvuspb.GetStatisticsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: paramtable.GetNodeID(),
			},
			CollectionName: s.collectionName,
		},
		mixc:           s.mixc,
		lb:             s.lb,
		shardclientMgr: s.mgr,
	}
}

func (s *StatisticTaskSuite) TestStatisticTask_NotShardLeader() {
	ctx := context.Background()
	task := s.GetStatisticsTask(ctx)

	s.NoError(task.OnEnqueue())

	task.fromQueryNode = true
	s.qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(&internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
			Reason:    "error",
		},
	}, nil)
	s.NoError(task.PreExecute(ctx))
	s.Error(task.Execute(ctx))
	s.NoError(task.PostExecute(ctx))
}

func (s *StatisticTaskSuite) TestStatisticTask_UnexpectedError() {
	ctx := context.Background()
	task := s.GetStatisticsTask(ctx)
	s.NoError(task.OnEnqueue())

	task.fromQueryNode = true
	s.qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(&internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "error",
		},
	}, nil)
	s.NoError(task.PreExecute(ctx))
	s.Error(task.Execute(ctx))
	s.NoError(task.PostExecute(ctx))
}

func (s *StatisticTaskSuite) TestStatisticTask_Success() {
	ctx := context.Background()
	task := s.GetStatisticsTask(ctx)

	s.NoError(task.OnEnqueue())
	s.qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(nil, nil)
	s.NoError(task.PreExecute(ctx))
	task.fromQueryNode = true
	task.fromDataCoord = false
	s.NoError(task.Execute(ctx))
	s.NoError(task.PostExecute(ctx))
}

func TestStatisticTaskSuite(t *testing.T) {
	suite.Run(t, new(StatisticTaskSuite))
}

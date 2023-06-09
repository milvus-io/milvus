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
package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type StatisticTaskSuite struct {
	suite.Suite
	rc types.RootCoord
	qc types.QueryCoord
	qn *types.MockQueryNode

	lb LBPolicy

	collection string
}

func (s *StatisticTaskSuite) SetupSuite() {
	Params.Init()
}

func (s *StatisticTaskSuite) SetupTest() {
	successStatus := commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	qc := types.NewMockQueryCoord(s.T())
	qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(&successStatus, nil)

	qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &successStatus,
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
			},
		},
	}, nil).Maybe()
	qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionIDs: []int64{1, 2, 3},
	}, nil).Maybe()

	s.qc = qc
	s.rc = NewRootCoordMock()
	s.rc.Start()
	s.qn = types.NewMockQueryNode(s.T())

	s.qn.EXPECT().GetComponentStates(mock.Anything).Return(nil, nil).Maybe()
	mgr := NewMockShardClientManager(s.T())
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil).Maybe()
	mgr.EXPECT().UpdateShardLeaders(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.lb = NewLBPolicyImpl(mgr)

	err := InitMetaCache(context.Background(), s.rc, s.qc, mgr)
	s.NoError(err)

	s.collection = "test_statistics_task"
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

	schema := constructCollectionSchemaByDataType(s.collection, fieldName2Types, testInt64Field, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	ctx := context.Background()
	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			CollectionName: s.collection,
			Schema:         marshaledSchema,
			ShardsNum:      common.DefaultShardsNum,
		},
		ctx:       ctx,
		rootCoord: s.rc,
	}

	s.NoError(createColT.OnEnqueue())
	s.NoError(createColT.PreExecute(ctx))
	s.NoError(createColT.Execute(ctx))
	s.NoError(createColT.PostExecute(ctx))

	collectionID, err := globalMetaCache.GetCollectionID(ctx, s.collection)
	s.NoError(err)

	status, err := s.qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_LoadCollection,
			SourceID: paramtable.GetNodeID(),
		},
		CollectionID: collectionID,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
}

func (s *StatisticTaskSuite) TearDownSuite() {
	s.rc.Stop()
}

func (s *StatisticTaskSuite) TestStatisticTask_Timeout() {
	ctx := context.Background()
	task := s.getStatisticsTask(ctx)

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

func (s *StatisticTaskSuite) getStatisticsTask(ctx context.Context) *getStatisticsTask {
	return &getStatisticsTask{
		Condition:      NewTaskCondition(ctx),
		ctx:            ctx,
		collectionName: s.collection,
		result: &milvuspb.GetStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		},
		request: &milvuspb.GetStatisticsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: paramtable.GetNodeID(),
			},
			CollectionName: s.collection,
		},
		qc: s.qc,
		lb: s.lb,
	}
}

func (s *StatisticTaskSuite) TestStatisticTask_NotShardLeader() {
	ctx := context.Background()
	task := s.getStatisticsTask(ctx)

	s.NoError(task.OnEnqueue())

	task.fromQueryNode = true
	s.qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(&internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
			Reason:    "error",
		},
	}, nil)
	s.Error(task.Execute(ctx))
	s.NoError(task.PostExecute(ctx))
}

func (s *StatisticTaskSuite) TestStatisticTask_UnexpectedError() {
	ctx := context.Background()
	task := s.getStatisticsTask(ctx)
	s.NoError(task.OnEnqueue())

	task.fromQueryNode = true
	s.qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(&internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "error",
		},
	}, nil)
	s.Error(task.Execute(ctx))
	s.NoError(task.PostExecute(ctx))
}

func (s *StatisticTaskSuite) TestStatisticTask_Success() {
	ctx := context.Background()
	task := s.getStatisticsTask(ctx)

	s.NoError(task.OnEnqueue())
	task.fromQueryNode = true
	s.qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(nil, nil)
	s.NoError(task.Execute(ctx))
	s.NoError(task.PostExecute(ctx))
}

func TestStatisticTaskSuite(t *testing.T) {
	suite.Run(t, new(StatisticTaskSuite))
}

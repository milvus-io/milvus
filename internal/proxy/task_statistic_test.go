package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestStatisticTask_all(t *testing.T) {
	var (
		err error
		ctx = context.TODO()

		rc = NewRootCoordMock()
		qc = types.NewMockQueryCoord(t)
		qn = types.NewMockQueryNode(t)

		shardsNum      = common.DefaultShardsNum
		collectionName = t.Name() + funcutil.GenRandomStr()
	)

	successStatus := commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	qc.EXPECT().Start().Return(nil)
	qc.EXPECT().Stop().Return(nil)
	qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(&successStatus, nil)

	mockCreator := func(ctx context.Context, address string) (types.QueryNode, error) {
		return qn, nil
	}

	mgr := newShardClientMgr(withShardClientCreator(mockCreator))

	rc.Start()
	defer rc.Stop()
	qc.Start()
	defer qc.Stop()
	qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &successStatus,
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
			},
		},
	}, nil)

	err = InitMetaCache(ctx, rc, qc, mgr)
	assert.NoError(t, err)

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

	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
	}

	require.NoError(t, createColT.OnEnqueue())
	require.NoError(t, createColT.PreExecute(ctx))
	require.NoError(t, createColT.Execute(ctx))
	require.NoError(t, createColT.PostExecute(ctx))

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_LoadCollection,
			SourceID: paramtable.GetNodeID(),
		},
		CollectionID: collectionID,
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	// test begins
	task := &getStatisticsTask{
		Condition: NewTaskCondition(ctx),
		ctx:       ctx,
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
			CollectionName: collectionName,
		},
		qc:       qc,
		shardMgr: mgr,
	}

	assert.NoError(t, task.OnEnqueue())

	qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionIDs: []int64{1, 2, 3},
	}, nil)

	// test query task with timeout
	ctx1, cancel1 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel1()
	// before preExecute
	assert.Equal(t, typeutil.ZeroTimestamp, task.TimeoutTimestamp)
	task.ctx = ctx1
	assert.NoError(t, task.PreExecute(ctx))
	// after preExecute
	assert.Greater(t, task.TimeoutTimestamp, typeutil.ZeroTimestamp)

	task.ctx = ctx
	task.statisticShardPolicy = func(context.Context, *shardClientMgr, queryFunc, map[string][]nodeInfo) error {
		return fmt.Errorf("fake error")
	}
	task.fromQueryNode = true
	assert.Error(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

	task.statisticShardPolicy = func(context.Context, *shardClientMgr, queryFunc, map[string][]nodeInfo) error {
		return errInvalidShardLeaders
	}
	task.fromQueryNode = true
	assert.Error(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

	task.statisticShardPolicy = RoundRobinPolicy
	task.fromQueryNode = true
	qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(nil, errors.New("GetStatistics failed")).Times(3)
	assert.Error(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

	task.statisticShardPolicy = RoundRobinPolicy
	task.fromQueryNode = true
	qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(&internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
			Reason:    "error",
		},
	}, nil).Times(6)
	assert.Error(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

	task.statisticShardPolicy = RoundRobinPolicy
	task.fromQueryNode = true
	qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(&internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "error",
		},
	}, nil).Times(3)
	assert.Error(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

	task.statisticShardPolicy = RoundRobinPolicy
	task.fromQueryNode = true
	task.fromDataCoord = false
	qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).Return(nil, nil).Once()
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}

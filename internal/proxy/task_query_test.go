package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestQueryTask_all(t *testing.T) {
	Params.Init()

	var (
		err error
		ctx = context.TODO()

		rc = NewRootCoordMock()
		qc = NewQueryCoordMock(withValidShardLeaders())
		qn = &QueryNodeMock{}

		shardsNum      = int32(2)
		collectionName = t.Name() + funcutil.GenRandomStr()

		expr   = fmt.Sprintf("%s > 0", testInt64Field)
		hitNum = 10
	)

	mockCreator := func(ctx context.Context, address string) (types.QueryNode, error) {
		return qn, nil
	}

	mgr := newShardClientMgr(withShardClientCreator(mockCreator))

	rc.Start()
	defer rc.Stop()
	qc.Start()
	defer qc.Stop()

	err = InitMetaCache(rc, qc, mgr)
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
			SourceID: Params.ProxyCfg.GetNodeID(),
		},
		CollectionID: collectionID,
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	// test begins
	task := &queryTask{
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: Params.ProxyCfg.GetNodeID(),
			},
			CollectionID:   collectionID,
			OutputFieldsId: make([]int64, len(fieldName2Types)),
		},
		ctx: ctx,
		result: &milvuspb.QueryResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		},
		request: &milvuspb.QueryRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: Params.ProxyCfg.GetNodeID(),
			},
			CollectionName: collectionName,
			Expr:           expr,
		},
		qc: qc,

		queryShardPolicy: roundRobinPolicy,
		shardMgr:         mgr,
	}
	for i := 0; i < len(fieldName2Types); i++ {
		task.RetrieveRequest.OutputFieldsId[i] = int64(common.StartOfUserFieldID + i)
	}

	assert.NoError(t, task.OnEnqueue())

	// test query task with timeout
	ctx1, cancel1 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel1()
	// before preExecute
	assert.Equal(t, typeutil.ZeroTimestamp, task.TimeoutTimestamp)
	task.ctx = ctx1
	assert.NoError(t, task.PreExecute(ctx))
	// after preExecute
	assert.Greater(t, task.TimeoutTimestamp, typeutil.ZeroTimestamp)

	result1 := &internalpb.RetrieveResults{
		Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_RetrieveResult},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: generateInt64Array(hitNum)},
			},
		},
	}

	for fieldName, dataType := range fieldName2Types {
		result1.FieldsData = append(result1.FieldsData, generateFieldData(dataType, fieldName, hitNum))
	}

	qn.withQueryResult = result1

	task.ctx = ctx
	assert.NoError(t, task.Execute(ctx))

	assert.NoError(t, task.PostExecute(ctx))
}

func TestCheckIfLoaded(t *testing.T) {
	var err error

	Params.Init()
	var (
		rc  = NewRootCoordMock()
		qc  = NewQueryCoordMock()
		ctx = context.TODO()
	)

	err = rc.Start()
	defer rc.Stop()
	require.NoError(t, err)
	mgr := newShardClientMgr()
	err = InitMetaCache(rc, qc, mgr)
	require.NoError(t, err)

	err = qc.Start()
	defer qc.Stop()
	require.NoError(t, err)

	getQueryTask := func(t *testing.T, collName string) *queryTask {
		task := &queryTask{
			ctx: ctx,
			RetrieveRequest: &internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Retrieve,
				},
			},
			request: &milvuspb.QueryRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Retrieve,
				},
				CollectionName: collName,
			},
			qc: qc,
		}
		require.NoError(t, task.OnEnqueue())
		return task
	}

	t.Run("test checkIfLoaded error", func(t *testing.T) {
		collName := "test_checkIfLoaded_error" + funcutil.GenRandomStr()
		createColl(t, collName, rc)
		collID, err := globalMetaCache.GetCollectionID(context.TODO(), collName)
		require.NoError(t, err)
		task := getQueryTask(t, collName)
		task.collectionName = collName

		t.Run("show collection err", func(t *testing.T) {
			qc.SetShowCollectionsFunc(func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
				return nil, fmt.Errorf("mock")
			})

			loaded, err := task.checkIfLoaded(collID, []UniqueID{})
			assert.Error(t, err)
			assert.False(t, loaded)
		})

		t.Run("show collection status unexpected error", func(t *testing.T) {
			qc.SetShowCollectionsFunc(func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
				return &querypb.ShowCollectionsResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    "mock",
					},
				}, nil
			})

			loaded, err := task.checkIfLoaded(collID, []UniqueID{})
			assert.Error(t, err)
			assert.False(t, loaded)
			assert.Error(t, task.PreExecute(ctx))
			qc.ResetShowCollectionsFunc()
		})

		t.Run("show partition error", func(t *testing.T) {
			qc.SetShowPartitionsFunc(func(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
				return &querypb.ShowPartitionsResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    "mock",
					},
				}, nil
			})
			loaded, err := task.checkIfLoaded(collID, []UniqueID{1})
			assert.Error(t, err)
			assert.False(t, loaded)
		})

		t.Run("show partition status unexpected error", func(t *testing.T) {
			qc.SetShowPartitionsFunc(func(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
				return nil, fmt.Errorf("mock error")
			})
			loaded, err := task.checkIfLoaded(collID, []UniqueID{1})
			assert.Error(t, err)
			assert.False(t, loaded)
		})

		t.Run("show partitions success", func(t *testing.T) {
			qc.SetShowPartitionsFunc(func(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
				return &querypb.ShowPartitionsResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
					},
				}, nil
			})
			loaded, err := task.checkIfLoaded(collID, []UniqueID{1})
			assert.NoError(t, err)
			assert.True(t, loaded)
			qc.ResetShowPartitionsFunc()
		})

		t.Run("show collection success but not loaded", func(t *testing.T) {
			qc.SetShowCollectionsFunc(func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
				return &querypb.ShowCollectionsResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
					},
					CollectionIDs:       []UniqueID{collID},
					InMemoryPercentages: []int64{0},
				}, nil
			})

			qc.SetShowPartitionsFunc(func(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
				return nil, fmt.Errorf("mock error")
			})
			loaded, err := task.checkIfLoaded(collID, []UniqueID{})
			assert.Error(t, err)
			assert.False(t, loaded)

			qc.SetShowPartitionsFunc(func(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
				return nil, fmt.Errorf("mock error")
			})
			loaded, err = task.checkIfLoaded(collID, []UniqueID{})
			assert.Error(t, err)
			assert.False(t, loaded)

			qc.SetShowPartitionsFunc(func(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
				return &querypb.ShowPartitionsResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
					},
					PartitionIDs: []UniqueID{1},
				}, nil
			})
			loaded, err = task.checkIfLoaded(collID, []UniqueID{})
			assert.NoError(t, err)
			assert.True(t, loaded)
		})

		qc.ResetShowCollectionsFunc()
		qc.ResetShowPartitionsFunc()
	})
}

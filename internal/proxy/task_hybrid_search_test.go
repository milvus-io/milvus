package proxy

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func createCollWithMultiVecField(t *testing.T, name string, rc types.RootCoordClient) {
	schema := genCollectionSchema(name)
	marshaledSchema, err := proto.Marshal(schema)
	require.NoError(t, err)
	ctx := context.TODO()

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(context.TODO()),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			CollectionName: name,
			Schema:         marshaledSchema,
			ShardsNum:      common.DefaultShardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
	}

	require.NoError(t, createColT.OnEnqueue())
	require.NoError(t, createColT.PreExecute(ctx))
	require.NoError(t, createColT.Execute(ctx))
	require.NoError(t, createColT.PostExecute(ctx))
}

func TestHybridSearchTask_PreExecute(t *testing.T) {
	var err error

	var (
		rc  = NewRootCoordMock()
		qc  = mocks.NewMockQueryCoordClient(t)
		ctx = context.TODO()
	)

	defer rc.Close()
	require.NoError(t, err)
	mgr := newShardClientMgr()
	err = InitMetaCache(ctx, rc, qc, mgr)
	require.NoError(t, err)

	genHybridSearchTaskWithNq := func(t *testing.T, collName string, reqs []*milvuspb.SearchRequest) *hybridSearchTask {
		task := &hybridSearchTask{
			ctx:                 ctx,
			Condition:           NewTaskCondition(ctx),
			HybridSearchRequest: &internalpb.HybridSearchRequest{},
			request: &milvuspb.HybridSearchRequest{
				CollectionName: collName,
				Requests:       reqs,
			},
			qc: qc,
			tr: timerecord.NewTimeRecorder("test-hybrid-search"),
		}
		require.NoError(t, task.OnEnqueue())
		return task
	}

	t.Run("bad nq 0", func(t *testing.T) {
		collName := "test_bad_nq0_error" + funcutil.GenRandomStr()
		createCollWithMultiVecField(t, collName, rc)
		// Nq must be 1.
		task := genHybridSearchTaskWithNq(t, collName, []*milvuspb.SearchRequest{{Nq: 0}})
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("bad req num 0", func(t *testing.T) {
		collName := "test_bad_req_num0_error" + funcutil.GenRandomStr()
		createCollWithMultiVecField(t, collName, rc)
		// num of reqs must be [1, 1024].
		task := genHybridSearchTaskWithNq(t, collName, nil)
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("bad req num 1025", func(t *testing.T) {
		collName := "test_bad_req_num1025_error" + funcutil.GenRandomStr()
		createCollWithMultiVecField(t, collName, rc)
		// num of reqs must be [1, 1024].
		reqs := make([]*milvuspb.SearchRequest, 0)
		for i := 0; i <= defaultMaxSearchRequest; i++ {
			reqs = append(reqs, &milvuspb.SearchRequest{
				CollectionName: collName,
				Nq:             1,
			})
		}
		task := genHybridSearchTaskWithNq(t, collName, reqs)
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("collection not exist", func(t *testing.T) {
		collName := "test_collection_not_exist" + funcutil.GenRandomStr()
		task := genHybridSearchTaskWithNq(t, collName, []*milvuspb.SearchRequest{{Nq: 1}})
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("hybrid search with timeout", func(t *testing.T) {
		collName := "hybrid_search_with_timeout" + funcutil.GenRandomStr()
		createCollWithMultiVecField(t, collName, rc)

		task := genHybridSearchTaskWithNq(t, collName, []*milvuspb.SearchRequest{{Nq: 1}})

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		task.ctx = ctxTimeout
		task.request.OutputFields = []string{testFloatVecField}
		assert.NoError(t, task.PreExecute(ctx))
	})
}

func TestHybridSearchTask_ErrExecute(t *testing.T) {
	var (
		err error
		ctx = context.TODO()

		rc = NewRootCoordMock()
		qc = getQueryCoordClient()
		qn = getQueryNodeClient()

		collectionName = t.Name() + funcutil.GenRandomStr()
	)

	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	mgr := NewMockShardClientManager(t)
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Maybe()
	mgr.EXPECT().UpdateShardLeaders(mock.Anything, mock.Anything).Return(nil).Maybe()
	lb := NewLBPolicyImpl(mgr)

	factory := dependency.NewDefaultFactory(true)
	node, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	scheduler, err := newTaskScheduler(ctx, node.tsoAllocator, factory)
	assert.NoError(t, err)
	node.sched = scheduler
	err = node.sched.Start()
	assert.NoError(t, err)
	err = node.initRateCollector()
	assert.NoError(t, err)
	node.rootCoord = rc
	node.queryCoord = qc

	defer qc.Close()

	err = InitMetaCache(ctx, rc, qc, mgr)
	assert.NoError(t, err)

	createCollWithMultiVecField(t, collectionName, rc)

	collectionID, err := globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
	assert.NoError(t, err)

	schema, err := globalMetaCache.GetCollectionSchema(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
	assert.NoError(t, err)

	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
	qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: successStatus,
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1},
				NodeAddrs:   []string{"localhost:9000"},
			},
		},
	}, nil)
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status:              successStatus,
		CollectionIDs:       []int64{collectionID},
		InMemoryPercentages: []int64{100},
	}, nil)
	status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_LoadCollection,
			SourceID: paramtable.GetNodeID(),
		},
		CollectionID: collectionID,
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	vectorFields := typeutil.GetVectorFieldSchemas(schema.CollectionSchema)
	vectorFieldNames := make([]string, len(vectorFields))
	for i, field := range vectorFields {
		vectorFieldNames[i] = field.GetName()
	}

	// test begins
	task := &hybridSearchTask{
		Condition: NewTaskCondition(ctx),
		ctx:       ctx,
		result: &milvuspb.SearchResults{
			Status: merr.Success(),
		},
		HybridSearchRequest: &internalpb.HybridSearchRequest{},
		request: &milvuspb.HybridSearchRequest{
			CollectionName: collectionName,
			Requests: []*milvuspb.SearchRequest{
				{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Search,
						SourceID: paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					Nq:             1,
					DslType:        commonpb.DslType_BoolExprV1,
					SearchParams: []*commonpb.KeyValuePair{
						{Key: AnnsFieldKey, Value: testFloatVecField},
						{Key: TopKKey, Value: "10"},
					},
				},
				{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Search,
						SourceID: paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					Nq:             1,
					DslType:        commonpb.DslType_BoolExprV1,
					SearchParams: []*commonpb.KeyValuePair{
						{Key: AnnsFieldKey, Value: testBinaryVecField},
						{Key: TopKKey, Value: "10"},
					},
				},
			},
			OutputFields: vectorFieldNames,
		},
		qc:   qc,
		lb:   lb,
		node: node,
	}

	assert.NoError(t, task.OnEnqueue())
	task.ctx = ctx
	assert.NoError(t, task.PreExecute(ctx))

	qn.EXPECT().HybridSearch(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
	assert.Error(t, task.Execute(ctx))

	qn.ExpectedCalls = nil
	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	qn.EXPECT().HybridSearch(mock.Anything, mock.Anything).Return(&querypb.HybridSearchResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}, nil)
	assert.Error(t, task.Execute(ctx))
}

func TestHybridSearchTask_PostExecute(t *testing.T) {
	var (
		rc             = NewRootCoordMock()
		qc             = getQueryCoordClient()
		qn             = getQueryNodeClient()
		collectionName = t.Name() + funcutil.GenRandomStr()
	)

	defer rc.Close()
	mgr := NewMockShardClientManager(t)
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Maybe()
	mgr.EXPECT().UpdateShardLeaders(mock.Anything, mock.Anything).Return(nil).Maybe()
	qn.EXPECT().HybridSearch(mock.Anything, mock.Anything).Return(&querypb.HybridSearchResult{
		Base:   commonpbutil.NewMsgBase(),
		Status: merr.Success(),
	}, nil)

	t.Run("Test empty result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := InitMetaCache(ctx, rc, qc, mgr)
		assert.NoError(t, err)
		createCollWithMultiVecField(t, collectionName, rc)

		schema, err := globalMetaCache.GetCollectionSchema(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
		assert.NoError(t, err)

		rankParams := []*commonpb.KeyValuePair{
			{Key: LimitKey, Value: strconv.Itoa(3)},
			{Key: OffsetKey, Value: strconv.Itoa(2)},
		}
		qt := &hybridSearchTask{
			ctx:       ctx,
			Condition: NewTaskCondition(context.TODO()),
			qc:        nil,
			tr:        timerecord.NewTimeRecorder("search"),
			schema:    schema,
			HybridSearchRequest: &internalpb.HybridSearchRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			request: &milvuspb.HybridSearchRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Search,
				},
				CollectionName: collectionName,
				RankParams:     rankParams,
			},
			resultBuf:             typeutil.NewConcurrentSet[*querypb.HybridSearchResult](),
			multipleRecallResults: typeutil.NewConcurrentSet[*milvuspb.SearchResults](),
		}

		err = qt.PostExecute(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, qt.result.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	})
}

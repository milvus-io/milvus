package proxy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"
)

func TestSearchTask(t *testing.T) {
	ctx := context.Background()
	ctxCancel, cancel := context.WithCancel(ctx)
	qt := &searchTask{
		ctx:       ctxCancel,
		Condition: NewTaskCondition(context.TODO()),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: Params.ProxyCfg.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.SearchResults),
		query:     nil,
		chMgr:     nil,
		qc:        nil,
		tr:        timerecord.NewTimeRecorder("search"),
	}

	// no result
	go func() {
		qt.resultBuf <- []*internalpb.SearchResults{}
	}()
	err := qt.PostExecute(context.TODO())
	assert.NotNil(t, err)

	// test trace context done
	cancel()
	err = qt.PostExecute(context.TODO())
	assert.NotNil(t, err)

	// error result
	ctx = context.Background()
	qt = &searchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(context.TODO()),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: Params.ProxyCfg.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.SearchResults),
		query:     nil,
		chMgr:     nil,
		qc:        nil,
		tr:        timerecord.NewTimeRecorder("search"),
	}

	// no result
	go func() {
		result := internalpb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "test",
			},
		}
		results := make([]*internalpb.SearchResults, 1)
		results[0] = &result
		qt.resultBuf <- results
	}()
	err = qt.PostExecute(context.TODO())
	assert.NotNil(t, err)

	log.Debug("PostExecute failed" + err.Error())
	// check result SlicedBlob

	ctx = context.Background()
	qt = &searchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(context.TODO()),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: Params.ProxyCfg.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.SearchResults),
		query:     nil,
		chMgr:     nil,
		qc:        nil,
		tr:        timerecord.NewTimeRecorder("search"),
	}

	// no result
	go func() {
		result := internalpb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "test",
			},
			SlicedBlob: nil,
		}
		results := make([]*internalpb.SearchResults, 1)
		results[0] = &result
		qt.resultBuf <- results
	}()
	err = qt.PostExecute(context.TODO())
	assert.Nil(t, err)

	assert.Equal(t, qt.result.Status.ErrorCode, commonpb.ErrorCode_Success)

	// TODO, add decode result, reduce result test
}

func TestSearchTask_Channels(t *testing.T) {
	var err error

	Params.Init()

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	prefix := "TestSearchTask_Channels"
	collectionName := prefix + funcutil.GenRandomStr()
	shardsNum := int32(2)
	dbName := ""
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	task := &searchTask{
		ctx: ctx,
		query: &milvuspb.SearchRequest{
			CollectionName: collectionName,
		},
		chMgr: chMgr,
		tr:    timerecord.NewTimeRecorder("search"),
	}

	// collection not exist
	_, err = task.getVChannels()
	assert.Error(t, err)
	_, err = task.getVChannels()
	assert.Error(t, err)

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	_, err = task.getChannels()
	assert.NoError(t, err)
	_, err = task.getVChannels()
	assert.NoError(t, err)

	_ = chMgr.removeAllDMLStream()
	chMgr.dmlChannelsMgr.getChannelsFunc = func(collectionID UniqueID) (map[vChan]pChan, error) {
		return nil, errors.New("mock")
	}
	_, err = task.getChannels()
	assert.Error(t, err)
	_, err = task.getVChannels()
	assert.Error(t, err)
}

func TestSearchTask_PreExecute(t *testing.T) {
	var err error

	Params.Init()
	Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	prefix := "TestSearchTask_PreExecute"
	collectionName := prefix + funcutil.GenRandomStr()
	shardsNum := int32(2)
	dbName := ""
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	task := &searchTask{
		ctx:           ctx,
		SearchRequest: &internalpb.SearchRequest{},
		query: &milvuspb.SearchRequest{
			CollectionName: collectionName,
		},
		chMgr: chMgr,
		qc:    qc,
		tr:    timerecord.NewTimeRecorder("search"),
	}
	assert.NoError(t, task.OnEnqueue())

	// collection not exist
	assert.Error(t, task.PreExecute(ctx))

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	collectionID, _ := globalMetaCache.GetCollectionID(ctx, collectionName)

	// validateCollectionName
	task.query.CollectionName = "$"
	assert.Error(t, task.PreExecute(ctx))
	task.query.CollectionName = collectionName

	// Validate Partition
	task.query.PartitionNames = []string{"$"}
	assert.Error(t, task.PreExecute(ctx))
	task.query.PartitionNames = nil

	// mock show collections of QueryCoord
	qc.SetShowCollectionsFunc(func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
		return nil, errors.New("mock")
	})
	assert.Error(t, task.PreExecute(ctx))
	qc.SetShowCollectionsFunc(func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
		return &querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mock",
			},
		}, nil
	})
	assert.Error(t, task.PreExecute(ctx))
	qc.ResetShowCollectionsFunc()

	// collection not loaded
	assert.Error(t, task.PreExecute(ctx))
	_, _ = qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  0,
		},
		DbID:         0,
		CollectionID: collectionID,
		Schema:       nil,
	})

	// no anns field
	task.query.DslType = commonpb.DslType_BoolExprV1
	assert.Error(t, task.PreExecute(ctx))
	task.query.SearchParams = []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: floatVecField,
		},
	}

	// no topk
	assert.Error(t, task.PreExecute(ctx))
	task.query.SearchParams = []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: floatVecField,
		},
		{
			Key:   TopKKey,
			Value: "invalid",
		},
	}

	// invalid topk
	assert.Error(t, task.PreExecute(ctx))
	task.query.SearchParams = []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: floatVecField,
		},
		{
			Key:   TopKKey,
			Value: "10",
		},
	}

	// no metric type
	assert.Error(t, task.PreExecute(ctx))
	task.query.SearchParams = []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: floatVecField,
		},
		{
			Key:   TopKKey,
			Value: "10",
		},
		{
			Key:   MetricTypeKey,
			Value: distance.L2,
		},
	}

	// no search params
	assert.Error(t, task.PreExecute(ctx))
	task.query.SearchParams = []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: int64Field,
		},
		{
			Key:   TopKKey,
			Value: "10",
		},
		{
			Key:   MetricTypeKey,
			Value: distance.L2,
		},
		{
			Key:   SearchParamsKey,
			Value: `{"nprobe": 10}`,
		},
	}

	// invalid round_decimal
	assert.Error(t, task.PreExecute(ctx))
	task.query.SearchParams = []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: int64Field,
		},
		{
			Key:   TopKKey,
			Value: "10",
		},
		{
			Key:   MetricTypeKey,
			Value: distance.L2,
		},
		{
			Key:   SearchParamsKey,
			Value: `{"nprobe": 10}`,
		},
		{
			Key:   RoundDecimalKey,
			Value: "invalid",
		},
	}

	// invalid round_decimal
	assert.Error(t, task.PreExecute(ctx))
	task.query.SearchParams = []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: floatVecField,
		},
		{
			Key:   TopKKey,
			Value: "10",
		},
		{
			Key:   MetricTypeKey,
			Value: distance.L2,
		},
		{
			Key:   RoundDecimalKey,
			Value: "-1",
		},
	}

	// failed to create query plan
	assert.Error(t, task.PreExecute(ctx))
	task.query.SearchParams = []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: floatVecField,
		},
		{
			Key:   TopKKey,
			Value: "10",
		},
		{
			Key:   MetricTypeKey,
			Value: distance.L2,
		},
		{
			Key:   SearchParamsKey,
			Value: `{"nprobe": 10}`,
		},
		{
			Key:   RoundDecimalKey,
			Value: "-1",
		},
	}

	// search task with timeout
	ctx1, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	// before preExecute
	assert.Equal(t, typeutil.ZeroTimestamp, task.TimeoutTimestamp)
	task.ctx = ctx1
	assert.NoError(t, task.PreExecute(ctx))
	// after preExecute
	assert.Greater(t, task.TimeoutTimestamp, typeutil.ZeroTimestamp)

	// field not exist
	task.query.OutputFields = []string{int64Field + funcutil.GenRandomStr()}
	assert.Error(t, task.PreExecute(ctx))
	// contain vector field
	task.query.OutputFields = []string{floatVecField}
	assert.Error(t, task.PreExecute(ctx))
	task.query.OutputFields = []string{int64Field}

	// partition
	rc.showPartitionsFunc = func(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
		return nil, errors.New("mock")
	}
	assert.Error(t, task.PreExecute(ctx))
	rc.showPartitionsFunc = nil

	// TODO(dragondriver): test partition-related error
}

func TestSearchTask_Ts(t *testing.T) {
	Params.Init()

	task := &searchTask{
		SearchRequest: &internalpb.SearchRequest{
			Base: nil,
		},
		tr: timerecord.NewTimeRecorder("search"),
	}
	assert.NoError(t, task.OnEnqueue())

	ts := Timestamp(time.Now().Nanosecond())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())
}

func TestSearchTask_Execute(t *testing.T) {
	var err error

	Params.Init()
	Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	prefix := "TestSearchTask_Execute"
	collectionName := prefix + funcutil.GenRandomStr()
	shardsNum := int32(2)
	dbName := ""
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	task := &searchTask{
		ctx: ctx,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     0,
				Timestamp: uint64(time.Now().UnixNano()),
				SourceID:  0,
			},
		},
		query: &milvuspb.SearchRequest{
			CollectionName: collectionName,
		},
		result: &milvuspb.SearchResults{
			Status:  &commonpb.Status{},
			Results: nil,
		},
		chMgr: chMgr,
		qc:    qc,
		tr:    timerecord.NewTimeRecorder("search"),
	}
	assert.NoError(t, task.OnEnqueue())

	// collection not exist
	assert.Error(t, task.PreExecute(ctx))

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	assert.NoError(t, task.Execute(ctx))

	_ = chMgr.removeAllDQLStream()
	query.f = func(collectionID UniqueID) (map[vChan]pChan, error) {
		return nil, errors.New("mock")
	}
	assert.Error(t, task.Execute(ctx))
	// TODO(dragondriver): cover getDQLStream
}

func genSearchResultData(nq int64, topk int64, ids []int64, scores []float32) *schemapb.SearchResultData {
	return &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		FieldsData: nil,
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		},
		Topks: make([]int64, nq),
	}
}

func TestSearchTask_Reduce(t *testing.T) {
	const (
		nq         = 1
		topk       = 4
		metricType = "L2"
	)
	t.Run("case1", func(t *testing.T) {
		ids := []int64{1, 2, 3, 4}
		scores := []float32{-1.0, -2.0, -3.0, -4.0}
		data1 := genSearchResultData(nq, topk, ids, scores)
		data2 := genSearchResultData(nq, topk, ids, scores)
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		res, err := reduceSearchResultData(dataArray, nq, topk, metricType)
		assert.Nil(t, err)
		assert.Equal(t, ids, res.Results.Ids.GetIntId().Data)
		assert.Equal(t, []float32{1.0, 2.0, 3.0, 4.0}, res.Results.Scores)
	})
	t.Run("case2", func(t *testing.T) {
		ids1 := []int64{1, 2, 3, 4}
		scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
		ids2 := []int64{5, 1, 3, 4}
		scores2 := []float32{-1.0, -1.0, -3.0, -4.0}
		data1 := genSearchResultData(nq, topk, ids1, scores1)
		data2 := genSearchResultData(nq, topk, ids2, scores2)
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		res, err := reduceSearchResultData(dataArray, nq, topk, metricType)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int64{1, 5, 2, 3}, res.Results.Ids.GetIntId().Data)
	})
}

func TestSearchTaskWithInvalidRoundDecimal(t *testing.T) {
	var err error

	Params.Init()
	Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestSearchTask_all"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	dim := 128
	expr := fmt.Sprintf("%s > 0", testInt64Field)
	nq := 10
	topk := 10
	roundDecimal := 7
	nprobe := 10

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
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()
	status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbID:         0,
		CollectionID: collectionID,
		Schema:       nil,
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	req := constructSearchRequest(dbName, collectionName,
		expr,
		testFloatVecField,
		nq, dim, nprobe, topk, roundDecimal)

	task := &searchTask{
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			ResultChannelID:    strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			DbID:               0,
			CollectionID:       0,
			PartitionIDs:       nil,
			Dsl:                "",
			PlaceholderGroup:   nil,
			DslType:            0,
			SerializedExprPlan: nil,
			OutputFieldsId:     nil,
			TravelTimestamp:    0,
			GuaranteeTimestamp: 0,
		},
		ctx:       ctx,
		resultBuf: make(chan []*internalpb.SearchResults),
		result:    nil,
		query:     req,
		chMgr:     chMgr,
		qc:        qc,
		tr:        timerecord.NewTimeRecorder("search"),
	}

	// simple mock for query node
	// TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?

	err = chMgr.createDQLStream(collectionID)
	assert.NoError(t, err)
	stream, err := chMgr.getDQLStream(collectionID)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	consumeCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-consumeCtx.Done():
				return
			case pack, ok := <-stream.Chan():
				assert.True(t, ok)
				if pack == nil {
					continue
				}

				for _, msg := range pack.Msgs {
					_, ok := msg.(*msgstream.SearchMsg)
					assert.True(t, ok)
					// TODO(dragondriver): construct result according to the request

					constructSearchResulstData := func() *schemapb.SearchResultData {
						resultData := &schemapb.SearchResultData{
							NumQueries: int64(nq),
							TopK:       int64(topk),
							Scores:     make([]float32, nq*topk),
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: make([]int64, nq*topk),
									},
								},
							},
							Topks: make([]int64, nq),
						}

						fieldID := common.StartOfUserFieldID
						for fieldName, dataType := range fieldName2Types {
							resultData.FieldsData = append(resultData.FieldsData, generateFieldData(dataType, fieldName, int64(fieldID), nq*topk))
							fieldID++
						}

						for i := 0; i < nq; i++ {
							for j := 0; j < topk; j++ {
								offset := i*topk + j
								score := float32(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()) // increasingly
								id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
								resultData.Scores[offset] = score
								resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
							}
							resultData.Topks[i] = int64(topk)
						}

						return resultData
					}

					result1 := &internalpb.SearchResults{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_SearchResult,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  0,
						},
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						ResultChannelID:          "",
						MetricType:               distance.L2,
						NumQueries:               int64(nq),
						TopK:                     int64(topk),
						SealedSegmentIDsSearched: nil,
						ChannelIDsSearched:       nil,
						GlobalSealedSegmentIDs:   nil,
						SlicedBlob:               nil,
						SlicedNumCount:           1,
						SlicedOffset:             0,
					}
					resultData := constructSearchResulstData()
					sliceBlob, err := proto.Marshal(resultData)
					assert.NoError(t, err)
					result1.SlicedBlob = sliceBlob

					// result2.SliceBlob = nil, will be skipped in decode stage
					result2 := &internalpb.SearchResults{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_SearchResult,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  0,
						},
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						ResultChannelID:          "",
						MetricType:               distance.L2,
						NumQueries:               int64(nq),
						TopK:                     int64(topk),
						SealedSegmentIDsSearched: nil,
						ChannelIDsSearched:       nil,
						GlobalSealedSegmentIDs:   nil,
						SlicedBlob:               nil,
						SlicedNumCount:           1,
						SlicedOffset:             0,
					}

					// send search result
					task.resultBuf <- []*internalpb.SearchResults{result1, result2}
				}
			}
		}
	}()

	assert.NoError(t, task.OnEnqueue())
	assert.Error(t, task.PreExecute(ctx))

	cancel()
	wg.Wait()
}
func TestSearchTask_all(t *testing.T) {
	var err error

	Params.Init()
	Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestSearchTask_all"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	dim := 128
	expr := fmt.Sprintf("%s > 0", testInt64Field)
	nq := 10
	topk := 10
	roundDecimal := 3
	nprobe := 10

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
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()
	status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbID:         0,
		CollectionID: collectionID,
		Schema:       nil,
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	req := constructSearchRequest(dbName, collectionName,
		expr,
		testFloatVecField,
		nq, dim, nprobe, topk, roundDecimal)

	task := &searchTask{
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			ResultChannelID:    strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			DbID:               0,
			CollectionID:       0,
			PartitionIDs:       nil,
			Dsl:                "",
			PlaceholderGroup:   nil,
			DslType:            0,
			SerializedExprPlan: nil,
			OutputFieldsId:     nil,
			TravelTimestamp:    0,
			GuaranteeTimestamp: 0,
		},
		ctx:       ctx,
		resultBuf: make(chan []*internalpb.SearchResults),
		result:    nil,
		query:     req,
		chMgr:     chMgr,
		qc:        qc,
		tr:        timerecord.NewTimeRecorder("search"),
	}

	// simple mock for query node
	// TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?

	err = chMgr.createDQLStream(collectionID)
	assert.NoError(t, err)
	stream, err := chMgr.getDQLStream(collectionID)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	consumeCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-consumeCtx.Done():
				return
			case pack, ok := <-stream.Chan():
				assert.True(t, ok)
				if pack == nil {
					continue
				}

				for _, msg := range pack.Msgs {
					_, ok := msg.(*msgstream.SearchMsg)
					assert.True(t, ok)
					// TODO(dragondriver): construct result according to the request

					constructSearchResulstData := func() *schemapb.SearchResultData {
						resultData := &schemapb.SearchResultData{
							NumQueries: int64(nq),
							TopK:       int64(topk),
							Scores:     make([]float32, nq*topk),
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: make([]int64, nq*topk),
									},
								},
							},
							Topks: make([]int64, nq),
						}

						fieldID := common.StartOfUserFieldID
						for fieldName, dataType := range fieldName2Types {
							resultData.FieldsData = append(resultData.FieldsData, generateFieldData(dataType, fieldName, int64(fieldID), nq*topk))
							fieldID++
						}

						for i := 0; i < nq; i++ {
							for j := 0; j < topk; j++ {
								offset := i*topk + j
								score := float32(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()) // increasingly
								id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
								resultData.Scores[offset] = score
								resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
							}
							resultData.Topks[i] = int64(topk)
						}

						return resultData
					}

					result1 := &internalpb.SearchResults{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_SearchResult,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  0,
						},
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						ResultChannelID:          "",
						MetricType:               distance.L2,
						NumQueries:               int64(nq),
						TopK:                     int64(topk),
						SealedSegmentIDsSearched: nil,
						ChannelIDsSearched:       nil,
						GlobalSealedSegmentIDs:   nil,
						SlicedBlob:               nil,
						SlicedNumCount:           1,
						SlicedOffset:             0,
					}
					resultData := constructSearchResulstData()
					sliceBlob, err := proto.Marshal(resultData)
					assert.NoError(t, err)
					result1.SlicedBlob = sliceBlob

					// result2.SliceBlob = nil, will be skipped in decode stage
					result2 := &internalpb.SearchResults{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_SearchResult,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  0,
						},
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						ResultChannelID:          "",
						MetricType:               distance.L2,
						NumQueries:               int64(nq),
						TopK:                     int64(topk),
						SealedSegmentIDsSearched: nil,
						ChannelIDsSearched:       nil,
						GlobalSealedSegmentIDs:   nil,
						SlicedBlob:               nil,
						SlicedNumCount:           1,
						SlicedOffset:             0,
					}

					// send search result
					task.resultBuf <- []*internalpb.SearchResults{result1, result2}
				}
			}
		}
	}()

	assert.NoError(t, task.OnEnqueue())
	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

	cancel()
	wg.Wait()
}

func TestSearchTask_7803_reduce(t *testing.T) {
	var err error

	Params.Init()
	Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestSearchTask_7803_reduce"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128
	expr := fmt.Sprintf("%s > 0", int64Field)
	nq := 10
	topk := 10
	roundDecimal := 3
	nprobe := 10

	schema := constructCollectionSchema(
		int64Field,
		floatVecField,
		dim,
		collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()
	status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbID:         0,
		CollectionID: collectionID,
		Schema:       nil,
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	req := constructSearchRequest(dbName, collectionName,
		expr,
		floatVecField,
		nq, dim, nprobe, topk, roundDecimal)

	task := &searchTask{
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			ResultChannelID:    strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			DbID:               0,
			CollectionID:       0,
			PartitionIDs:       nil,
			Dsl:                "",
			PlaceholderGroup:   nil,
			DslType:            0,
			SerializedExprPlan: nil,
			OutputFieldsId:     nil,
			TravelTimestamp:    0,
			GuaranteeTimestamp: 0,
		},
		ctx:       ctx,
		resultBuf: make(chan []*internalpb.SearchResults),
		result:    nil,
		query:     req,
		chMgr:     chMgr,
		qc:        qc,
		tr:        timerecord.NewTimeRecorder("search"),
	}

	// simple mock for query node
	// TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?

	err = chMgr.createDQLStream(collectionID)
	assert.NoError(t, err)
	stream, err := chMgr.getDQLStream(collectionID)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	consumeCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-consumeCtx.Done():
				return
			case pack, ok := <-stream.Chan():
				assert.True(t, ok)
				if pack == nil {
					continue
				}

				for _, msg := range pack.Msgs {
					_, ok := msg.(*msgstream.SearchMsg)
					assert.True(t, ok)
					// TODO(dragondriver): construct result according to the request

					constructSearchResulstData := func(invalidNum int) *schemapb.SearchResultData {
						resultData := &schemapb.SearchResultData{
							NumQueries: int64(nq),
							TopK:       int64(topk),
							FieldsData: nil,
							Scores:     make([]float32, nq*topk),
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: make([]int64, nq*topk),
									},
								},
							},
							Topks: make([]int64, nq),
						}

						for i := 0; i < nq; i++ {
							for j := 0; j < topk; j++ {
								offset := i*topk + j
								if j >= invalidNum {
									resultData.Scores[offset] = minFloat32
									resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = -1
								} else {
									score := float32(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()) // increasingly
									id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
									resultData.Scores[offset] = score
									resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
								}
							}
							resultData.Topks[i] = int64(topk)
						}

						return resultData
					}

					result1 := &internalpb.SearchResults{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_SearchResult,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  0,
						},
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						ResultChannelID:          "",
						MetricType:               distance.L2,
						NumQueries:               int64(nq),
						TopK:                     int64(topk),
						SealedSegmentIDsSearched: nil,
						ChannelIDsSearched:       nil,
						GlobalSealedSegmentIDs:   nil,
						SlicedBlob:               nil,
						SlicedNumCount:           1,
						SlicedOffset:             0,
					}
					resultData := constructSearchResulstData(topk / 2)
					sliceBlob, err := proto.Marshal(resultData)
					assert.NoError(t, err)
					result1.SlicedBlob = sliceBlob

					result2 := &internalpb.SearchResults{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_SearchResult,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  0,
						},
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						ResultChannelID:          "",
						MetricType:               distance.L2,
						NumQueries:               int64(nq),
						TopK:                     int64(topk),
						SealedSegmentIDsSearched: nil,
						ChannelIDsSearched:       nil,
						GlobalSealedSegmentIDs:   nil,
						SlicedBlob:               nil,
						SlicedNumCount:           1,
						SlicedOffset:             0,
					}
					resultData2 := constructSearchResulstData(topk - topk/2)
					sliceBlob2, err := proto.Marshal(resultData2)
					assert.NoError(t, err)
					result2.SlicedBlob = sliceBlob2

					// send search result
					task.resultBuf <- []*internalpb.SearchResults{result1, result2}
				}
			}
		}
	}()

	assert.NoError(t, task.OnEnqueue())
	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

	cancel()
	wg.Wait()
}

func Test_checkSearchResultData(t *testing.T) {
	type args struct {
		data *schemapb.SearchResultData
		nq   int64
		topk int64
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			args: args{
				data: &schemapb.SearchResultData{NumQueries: 100},
				nq:   10,
			},
			wantErr: true,
		},
		{
			args: args{
				data: &schemapb.SearchResultData{NumQueries: 1, TopK: 1},
				nq:   1,
				topk: 10,
			},
			wantErr: true,
		},
		{
			args: args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{1, 2}, // != nq * topk
							},
						},
					},
				},
				nq:   1,
				topk: 1,
			},
			wantErr: true,
		},
		{
			args: args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_StrId{
							StrId: &schemapb.StringArray{
								Data: []string{"1", "2"}, // != nq * topk
							},
						},
					},
				},
				nq:   1,
				topk: 1,
			},
			wantErr: true,
		},
		{
			args: args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{1},
							},
						},
					},
					Scores: []float32{0.99, 0.98}, // != nq * topk
				},
				nq:   1,
				topk: 1,
			},
			wantErr: true,
		},
		{
			args: args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{1},
							},
						},
					},
					Scores: []float32{0.99},
				},
				nq:   1,
				topk: 1,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkSearchResultData(tt.args.data, tt.args.nq, tt.args.topk); (err != nil) != tt.wantErr {
				t.Errorf("checkSearchResultData(%v, %v, %v) error = %v, wantErr %v",
					tt.args.data, tt.args.nq, tt.args.topk, err, tt.wantErr)
			}
		})
	}
}

func Test_selectSearchResultData_int(t *testing.T) {
	type args struct {
		dataArray []*schemapb.SearchResultData
		offsets   []int64
		topk      int64
		qi        int64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			args: args{
				dataArray: []*schemapb.SearchResultData{
					{
						Ids: &schemapb.IDs{
							IdField: &schemapb.IDs_IntId{
								IntId: &schemapb.LongArray{
									Data: []int64{11, 9, 7, 5, 3, 1},
								},
							},
						},
						Scores: []float32{1.1, 0.9, 0.7, 0.5, 0.3, 0.1},
					},
					{
						Ids: &schemapb.IDs{
							IdField: &schemapb.IDs_IntId{
								IntId: &schemapb.LongArray{
									Data: []int64{12, 10, 8, 6, 4, 2},
								},
							},
						},
						Scores: []float32{1.2, 1.0, 0.8, 0.6, 0.4, 0.2},
					},
				},
				offsets: []int64{0, 1},
				topk:    2,
				qi:      0,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectSearchResultData(tt.args.dataArray, tt.args.offsets, tt.args.topk, tt.args.qi); got != tt.want {
				t.Errorf("selectSearchResultData() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_selectSearchResultData_str(t *testing.T) {
	type args struct {
		dataArray []*schemapb.SearchResultData
		offsets   []int64
		topk      int64
		qi        int64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			args: args{
				dataArray: []*schemapb.SearchResultData{
					{
						Ids: &schemapb.IDs{
							IdField: &schemapb.IDs_StrId{
								StrId: &schemapb.StringArray{
									Data: []string{"11", "9", "7", "5", "3", "1"},
								},
							},
						},
						Scores: []float32{1.1, 0.9, 0.7, 0.5, 0.3, 0.1},
					},
					{
						Ids: &schemapb.IDs{
							IdField: &schemapb.IDs_StrId{
								StrId: &schemapb.StringArray{
									Data: []string{"12", "10", "8", "6", "4", "2"},
								},
							},
						},
						Scores: []float32{1.2, 1.0, 0.8, 0.6, 0.4, 0.2},
					},
				},
				offsets: []int64{0, 1},
				topk:    2,
				qi:      1,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectSearchResultData(tt.args.dataArray, tt.args.offsets, tt.args.topk, tt.args.qi); got != tt.want {
				t.Errorf("selectSearchResultData() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_reduceSearchResultData_int(t *testing.T) {
	topk := 2
	nq := 3
	results := []*schemapb.SearchResultData{
		{
			NumQueries: int64(nq),
			TopK:       int64(topk),
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 7, 5, 3, 1},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.7, 0.5, 0.3, 0.1},
		},
		{
			NumQueries: int64(nq),
			TopK:       int64(topk),
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{12, 10, 8, 6, 4, 2},
					},
				},
			},
			Scores: []float32{1.2, 1.0, 0.8, 0.6, 0.4, 0.2},
		},
	}

	reduced, err := reduceSearchResultData(results, int64(nq), int64(topk), distance.L2)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{3, 4, 7, 8, 11, 12}, reduced.GetResults().GetIds().GetIntId().GetData())
	// hard to compare floating point value.
	// TODO: compare scores.
}

func Test_reduceSearchResultData_str(t *testing.T) {
	topk := 2
	nq := 3
	results := []*schemapb.SearchResultData{
		{
			NumQueries: int64(nq),
			TopK:       int64(topk),
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"11", "9", "7", "5", "3", "1"},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.7, 0.5, 0.3, 0.1},
		},
		{
			NumQueries: int64(nq),
			TopK:       int64(topk),
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"12", "10", "8", "6", "4", "2"},
					},
				},
			},
			Scores: []float32{1.2, 1.0, 0.8, 0.6, 0.4, 0.2},
		},
	}

	reduced, err := reduceSearchResultData(results, int64(nq), int64(topk), distance.L2)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"3", "4", "7", "8", "11", "12"}, reduced.GetResults().GetIds().GetStrId().GetData())
	// hard to compare floating point value.
	// TODO: compare scores.
}

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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestSearchTask_PostExecute(t *testing.T) {
	var err error

	var (
		rc  = NewRootCoordMock()
		qc  = mocks.NewMockQueryCoordClient(t)
		ctx = context.TODO()
	)

	defer rc.Close()
	require.NoError(t, err)
	mgr := newShardClientMgr()
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	err = InitMetaCache(ctx, rc, qc, mgr)
	require.NoError(t, err)

	getSearchTask := func(t *testing.T, collName string) *searchTask {
		task := &searchTask{
			ctx:            ctx,
			collectionName: collName,
			SearchRequest:  &internalpb.SearchRequest{},
			request: &milvuspb.SearchRequest{
				CollectionName: collName,
				Nq:             1,
				SearchParams:   getBaseSearchParams(),
			},
			qc: qc,
			tr: timerecord.NewTimeRecorder("test-search"),
		}
		require.NoError(t, task.OnEnqueue())
		return task
	}
	t.Run("Test empty result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		collName := "test_collection_empty_result" + funcutil.GenRandomStr()
		createColl(t, collName, rc)
		qt := getSearchTask(t, collName)
		err = qt.PreExecute(ctx)
		assert.NoError(t, err)

		assert.NotNil(t, qt.resultBuf)
		qt.resultBuf.Insert(&internalpb.SearchResults{})
		err := qt.PostExecute(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, qt.result.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	})
}

func createColl(t *testing.T, name string, rc types.RootCoordClient) {
	schema := constructCollectionSchema(testInt64Field, testFloatVecField, testVecDim, name)
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

func getBaseSearchParams() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: testFloatVecField,
		},
		{
			Key:   TopKKey,
			Value: "10",
		},
	}
}

func getValidSearchParams() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: testFloatVecField,
		},
		{
			Key:   TopKKey,
			Value: "10",
		},
		{
			Key:   common.MetricTypeKey,
			Value: metric.L2,
		},
		{
			Key:   SearchParamsKey,
			Value: `{"nprobe": 10}`,
		},
		{
			Key:   RoundDecimalKey,
			Value: "-1",
		},
		{
			Key:   IgnoreGrowingKey,
			Value: "false",
		},
	}
}

func resetSearchParamsValue(kvs []*commonpb.KeyValuePair, keyName string, newVal string) {
	for _, kv := range kvs {
		if kv.GetKey() == keyName {
			kv.Value = newVal
		}
	}
}

func getInvalidSearchParams(invalidName string) []*commonpb.KeyValuePair {
	kvs := getValidSearchParams()
	for _, kv := range kvs {
		if kv.GetKey() == invalidName {
			kv.Value = "invalid"
		}
	}
	return kvs
}

func TestSearchTask_PreExecute(t *testing.T) {
	var err error

	var (
		rc  = NewRootCoordMock()
		qc  = mocks.NewMockQueryCoordClient(t)
		ctx = context.TODO()
	)

	defer rc.Close()
	require.NoError(t, err)
	mgr := newShardClientMgr()
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
	err = InitMetaCache(ctx, rc, qc, mgr)
	require.NoError(t, err)

	getSearchTask := func(t *testing.T, collName string) *searchTask {
		task := &searchTask{
			ctx:            ctx,
			collectionName: collName,
			SearchRequest:  &internalpb.SearchRequest{},
			request: &milvuspb.SearchRequest{
				CollectionName: collName,
				Nq:             1,
			},
			qc: qc,
			tr: timerecord.NewTimeRecorder("test-search"),
		}
		require.NoError(t, task.OnEnqueue())
		return task
	}

	getSearchTaskWithNq := func(t *testing.T, collName string, nq int64) *searchTask {
		task := &searchTask{
			ctx:            ctx,
			collectionName: collName,
			SearchRequest:  &internalpb.SearchRequest{},
			request: &milvuspb.SearchRequest{
				CollectionName: collName,
				Nq:             nq,
			},
			qc: qc,
			tr: timerecord.NewTimeRecorder("test-search"),
		}
		require.NoError(t, task.OnEnqueue())
		return task
	}

	t.Run("bad nq 0", func(t *testing.T) {
		collName := "test_bad_nq0_error" + funcutil.GenRandomStr()
		createColl(t, collName, rc)
		// Nq must be in range [1, 16384].
		task := getSearchTaskWithNq(t, collName, 0)
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("bad nq 16385", func(t *testing.T) {
		collName := "test_bad_nq16385_error" + funcutil.GenRandomStr()
		createColl(t, collName, rc)

		// Nq must be in range [1, 16384].
		task := getSearchTaskWithNq(t, collName, 16384+1)
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("collection not exist", func(t *testing.T) {
		collName := "test_collection_not_exist" + funcutil.GenRandomStr()
		task := getSearchTask(t, collName)
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("invalid IgnoreGrowing param", func(t *testing.T) {
		collName := "test_invalid_param" + funcutil.GenRandomStr()
		createColl(t, collName, rc)

		task := getSearchTask(t, collName)
		task.request.SearchParams = getInvalidSearchParams(IgnoreGrowingKey)
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("search with timeout", func(t *testing.T) {
		collName := "search_with_timeout" + funcutil.GenRandomStr()
		createColl(t, collName, rc)

		task := getSearchTask(t, collName)
		task.request.SearchParams = getValidSearchParams()
		task.request.DslType = commonpb.DslType_BoolExprV1

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		require.Equal(t, typeutil.ZeroTimestamp, task.TimeoutTimestamp)

		task.ctx = ctxTimeout
		assert.NoError(t, task.PreExecute(ctx))
		assert.Greater(t, task.TimeoutTimestamp, typeutil.ZeroTimestamp)

		{
			task.mustUsePartitionKey = true
			err = task.PreExecute(ctx)
			assert.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			task.mustUsePartitionKey = false
		}

		// field not exist
		task.ctx = context.TODO()
		task.request.OutputFields = []string{testInt64Field + funcutil.GenRandomStr()}
		assert.Error(t, task.PreExecute(ctx))

		// contain vector field
		task.request.OutputFields = []string{testFloatVecField}
		assert.NoError(t, task.PreExecute(ctx))
	})
}

func getQueryCoord() *mocks.MockQueryCoord {
	qc := &mocks.MockQueryCoord{}
	qc.EXPECT().Start().Return(nil)
	qc.EXPECT().Stop().Return(nil)
	return qc
}

func getQueryCoordClient() *mocks.MockQueryCoordClient {
	qc := &mocks.MockQueryCoordClient{}
	qc.EXPECT().Close().Return(nil)
	return qc
}

func getQueryNode() *mocks.MockQueryNode {
	qn := &mocks.MockQueryNode{}

	return qn
}

func getQueryNodeClient() *mocks.MockQueryNodeClient {
	qn := &mocks.MockQueryNodeClient{}

	return qn
}

func TestSearchTaskV2_Execute(t *testing.T) {
	var (
		err error

		rc  = NewRootCoordMock()
		qc  = getQueryCoordClient()
		ctx = context.TODO()

		collectionName = t.Name() + funcutil.GenRandomStr()
	)

	defer rc.Close()
	mgr := newShardClientMgr()
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
	err = InitMetaCache(ctx, rc, qc, mgr)
	require.NoError(t, err)

	defer qc.Close()

	task := &searchTask{
		ctx: ctx,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: collectionName,
		},
		result: &milvuspb.SearchResults{
			Status: &commonpb.Status{},
		},
		qc: qc,
		tr: timerecord.NewTimeRecorder("search"),
	}
	require.NoError(t, task.OnEnqueue())
	createColl(t, collectionName, rc)
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

func TestSearchTask_Ts(t *testing.T) {
	task := &searchTask{
		SearchRequest: &internalpb.SearchRequest{},

		tr: timerecord.NewTimeRecorder("test-search"),
	}
	require.NoError(t, task.OnEnqueue())

	ts := Timestamp(time.Now().Nanosecond())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())
}

func TestSearchTask_Reduce(t *testing.T) {
	// const (
	//     nq         = 1
	//     topk       = 4
	//     metricType = "L2"
	// )
	// t.Run("case1", func(t *testing.T) {
	//     ids := []int64{1, 2, 3, 4}
	//     scores := []float32{-1.0, -2.0, -3.0, -4.0}
	//     data1 := genSearchResultData(nq, topk, ids, scores)
	//     data2 := genSearchResultData(nq, topk, ids, scores)
	//     dataArray := make([]*schemapb.SearchResultData, 0)
	//     dataArray = append(dataArray, data1)
	//     dataArray = append(dataArray, data2)
	//     res, err := reduceSearchResultData(dataArray, nq, topk, metricType)
	//     assert.NoError(t, err)
	//     assert.Equal(t, ids, res.Results.Ids.GetIntId().Data)
	//     assert.Equal(t, []float32{1.0, 2.0, 3.0, 4.0}, res.Results.Scores)
	// })
	// t.Run("case2", func(t *testing.T) {
	//     ids1 := []int64{1, 2, 3, 4}
	//     scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	//     ids2 := []int64{5, 1, 3, 4}
	//     scores2 := []float32{-1.0, -1.0, -3.0, -4.0}
	//     data1 := genSearchResultData(nq, topk, ids1, scores1)
	//     data2 := genSearchResultData(nq, topk, ids2, scores2)
	//     dataArray := make([]*schemapb.SearchResultData, 0)
	//     dataArray = append(dataArray, data1)
	//     dataArray = append(dataArray, data2)
	//     res, err := reduceSearchResultData(dataArray, nq, topk, metricType)
	//     assert.NoError(t, err)
	//     assert.ElementsMatch(t, []int64{1, 5, 2, 3}, res.Results.Ids.GetIntId().Data)
	// })
}

func TestSearchTaskWithInvalidRoundDecimal(t *testing.T) {
	// var err error
	//
	// Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}
	//
	// rc := NewRootCoordMock()
	// rc.Start()
	// defer rc.Stop()
	//
	// ctx := context.Background()
	//
	// err = InitMetaCache(ctx, rc)
	// assert.NoError(t, err)
	//
	// shardsNum := int32(2)
	// prefix := "TestSearchTaskV2_all"
	// collectionName := prefix + funcutil.GenRandomStr()
	//
	// dim := 128
	// expr := fmt.Sprintf("%s > 0", testInt64Field)
	// nq := 10
	// topk := 10
	// roundDecimal := 7
	// nprobe := 10
	//
	// fieldName2Types := map[string]schemapb.DataType{
	//     testBoolField:     schemapb.DataType_Bool,
	//     testInt32Field:    schemapb.DataType_Int32,
	//     testInt64Field:    schemapb.DataType_Int64,
	//     testFloatField:    schemapb.DataType_Float,
	//     testDoubleField:   schemapb.DataType_Double,
	//     testFloatVecField: schemapb.DataType_FloatVector,
	// }
	// if enableMultipleVectorFields {
	//     fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	// }
	// schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	// marshaledSchema, err := proto.Marshal(schema)
	// assert.NoError(t, err)
	//
	// createColT := &createCollectionTask{
	//     Condition: NewTaskCondition(ctx),
	//     CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
	//         Base:           nil,
	//         CollectionName: collectionName,
	//         Schema:         marshaledSchema,
	//         ShardsNum:      shardsNum,
	//     },
	//     ctx:       ctx,
	//     rootCoord: rc,
	//     result:    nil,
	//     schema:    nil,
	// }
	//
	// assert.NoError(t, createColT.OnEnqueue())
	// assert.NoError(t, createColT.PreExecute(ctx))
	// assert.NoError(t, createColT.Execute(ctx))
	// assert.NoError(t, createColT.PostExecute(ctx))
	//
	// dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	// query := newMockGetChannelsService()
	// factory := newSimpleMockMsgStreamFactory()
	//
	// collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	// assert.NoError(t, err)
	//
	// qc := NewQueryCoordMock()
	// qc.Start()
	// defer qc.Stop()
	// status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
	//     Base: &commonpb.MsgBase{
	//         MsgType:   commonpb.MsgType_LoadCollection,
	//         MsgID:     0,
	//         Timestamp: 0,
	//         SourceID:  paramtable.GetNodeID(),
	//     },
	//     DbID:         0,
	//     CollectionID: collectionID,
	//     Schema:       nil,
	// })
	// assert.NoError(t, err)
	// assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	//
	// req := constructSearchRequest("", collectionName,
	//     expr,
	//     testFloatVecField,
	//     nq, dim, nprobe, topk, roundDecimal)
	//
	// task := &searchTaskV2{
	//     Condition: NewTaskCondition(ctx),
	//     SearchRequest: &internalpb.SearchRequest{
	//         Base: &commonpb.MsgBase{
	//             MsgType:   commonpb.MsgType_Search,
	//             MsgID:     0,
	//             Timestamp: 0,
	//             SourceID:  paramtable.GetNodeID(),
	//         },
	//         ResultChannelID:    strconv.FormatInt(paramtable.GetNodeID(), 10),
	//         DbID:               0,
	//         CollectionID:       0,
	//         PartitionIDs:       nil,
	//         Dsl:                "",
	//         PlaceholderGroup:   nil,
	//         DslType:            0,
	//         SerializedExprPlan: nil,
	//         OutputFieldsId:     nil,
	//         TravelTimestamp:    0,
	//         GuaranteeTimestamp: 0,
	//     },
	//     ctx:       ctx,
	//     resultBuf: make(chan *internalpb.SearchResults, 10),
	//     result:    nil,
	//     request:   req,
	//     qc:        qc,
	//     tr:        timerecord.NewTimeRecorder("search"),
	// }
	//
	// // simple mock for query node
	// // TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?
	//
	//
	// var wg sync.WaitGroup
	// wg.Add(1)
	// consumeCtx, cancel := context.WithCancel(ctx)
	// go func() {
	//     defer wg.Done()
	//     for {
	//         select {
	//         case <-consumeCtx.Done():
	//             return
	//         case pack, ok := <-stream.Chan():
	//             assert.True(t, ok)
	//             if pack == nil {
	//                 continue
	//             }
	//
	//             for _, msg := range pack.Msgs {
	//                 _, ok := msg.(*msgstream.SearchMsg)
	//                 assert.True(t, ok)
	//                 // TODO(dragondriver): construct result according to the request
	//
	//                 constructSearchResulstData := func() *schemapb.SearchResultData {
	//                     resultData := &schemapb.SearchResultData{
	//                         NumQueries: int64(nq),
	//                         TopK:       int64(topk),
	//                         Scores:     make([]float32, nq*topk),
	//                         Ids: &schemapb.IDs{
	//                             IdField: &schemapb.IDs_IntId{
	//                                 IntId: &schemapb.LongArray{
	//                                     Data: make([]int64, nq*topk),
	//                                 },
	//                             },
	//                         },
	//                         Topks: make([]int64, nq),
	//                     }
	//
	//                     fieldID := common.StartOfUserFieldID
	//                     for fieldName, dataType := range fieldName2Types {
	//                         resultData.FieldsData = append(resultData.FieldsData, generateFieldData(dataType, fieldName, int64(fieldID), nq*topk))
	//                         fieldID++
	//                     }
	//
	//                     for i := 0; i < nq; i++ {
	//                         for j := 0; j < topk; j++ {
	//                             offset := i*topk + j
	//                             score := float32(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()) // increasingly
	//                             id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	//                             resultData.Scores[offset] = score
	//                             resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
	//                         }
	//                         resultData.Topks[i] = int64(topk)
	//                     }
	//
	//                     return resultData
	//                 }
	//
	//                 result1 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//                 resultData := constructSearchResulstData()
	//                 sliceBlob, err := proto.Marshal(resultData)
	//                 assert.NoError(t, err)
	//                 result1.SlicedBlob = sliceBlob
	//
	//                 // result2.SliceBlob = nil, will be skipped in decode stage
	//                 result2 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//
	//                 // send search result
	//                 task.resultBuf <- result1
	//                 task.resultBuf <- result2
	//             }
	//         }
	//     }
	// }()
	//
	// assert.NoError(t, task.OnEnqueue())
	// assert.Error(t, task.PreExecute(ctx))
	//
	// cancel()
	// wg.Wait()
}

func TestSearchTaskV2_all(t *testing.T) {
	// var err error
	//
	// Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}
	//
	// rc := NewRootCoordMock()
	// rc.Start()
	// defer rc.Stop()
	//
	// ctx := context.Background()
	//
	// err = InitMetaCache(ctx, rc)
	// assert.NoError(t, err)
	//
	// shardsNum := int32(2)
	// prefix := "TestSearchTaskV2_all"
	// collectionName := prefix + funcutil.GenRandomStr()
	//
	// dim := 128
	// expr := fmt.Sprintf("%s > 0", testInt64Field)
	// nq := 10
	// topk := 10
	// roundDecimal := 3
	// nprobe := 10
	//
	// fieldName2Types := map[string]schemapb.DataType{
	//     testBoolField:     schemapb.DataType_Bool,
	//     testInt32Field:    schemapb.DataType_Int32,
	//     testInt64Field:    schemapb.DataType_Int64,
	//     testFloatField:    schemapb.DataType_Float,
	//     testDoubleField:   schemapb.DataType_Double,
	//     testFloatVecField: schemapb.DataType_FloatVector,
	// }
	// if enableMultipleVectorFields {
	//     fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	// }
	//
	// schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	// marshaledSchema, err := proto.Marshal(schema)
	// assert.NoError(t, err)
	//
	// createColT := &createCollectionTask{
	//     Condition: NewTaskCondition(ctx),
	//     CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
	//         Base:           nil,
	//         CollectionName: collectionName,
	//         Schema:         marshaledSchema,
	//         ShardsNum:      shardsNum,
	//     },
	//     ctx:       ctx,
	//     rootCoord: rc,
	//     result:    nil,
	//     schema:    nil,
	// }
	//
	// assert.NoError(t, createColT.OnEnqueue())
	// assert.NoError(t, createColT.PreExecute(ctx))
	// assert.NoError(t, createColT.Execute(ctx))
	// assert.NoError(t, createColT.PostExecute(ctx))
	//
	// dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	// query := newMockGetChannelsService()
	// factory := newSimpleMockMsgStreamFactory()
	//
	// collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	// assert.NoError(t, err)
	//
	// qc := NewQueryCoordMock()
	// qc.Start()
	// defer qc.Stop()
	// status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
	//     Base: &commonpb.MsgBase{
	//         MsgType:   commonpb.MsgType_LoadCollection,
	//         MsgID:     0,
	//         Timestamp: 0,
	//         SourceID:  paramtable.GetNodeID(),
	//     },
	//     DbID:         0,
	//     CollectionID: collectionID,
	//     Schema:       nil,
	// })
	// assert.NoError(t, err)
	// assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	//
	// req := constructSearchRequest("", collectionName,
	//     expr,
	//     testFloatVecField,
	//     nq, dim, nprobe, topk, roundDecimal)
	//
	// task := &searchTaskV2{
	//     Condition: NewTaskCondition(ctx),
	//     SearchRequest: &internalpb.SearchRequest{
	//         Base: &commonpb.MsgBase{
	//             MsgType:   commonpb.MsgType_Search,
	//             MsgID:     0,
	//             Timestamp: 0,
	//             SourceID:  paramtable.GetNodeID(),
	//         },
	//         ResultChannelID:    strconv.FormatInt(paramtable.GetNodeID(), 10),
	//         DbID:               0,
	//         CollectionID:       0,
	//         PartitionIDs:       nil,
	//         Dsl:                "",
	//         PlaceholderGroup:   nil,
	//         DslType:            0,
	//         SerializedExprPlan: nil,
	//         OutputFieldsId:     nil,
	//         TravelTimestamp:    0,
	//         GuaranteeTimestamp: 0,
	//     },
	//     ctx:       ctx,
	//     resultBuf: make(chan *internalpb.SearchResults, 10),
	//     result:    nil,
	//     request:   req,
	//     qc:        qc,
	//     tr:        timerecord.NewTimeRecorder("search"),
	// }
	//
	// // simple mock for query node
	// // TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?
	//
	// var wg sync.WaitGroup
	// wg.Add(1)
	// consumeCtx, cancel := context.WithCancel(ctx)
	// go func() {
	//     defer wg.Done()
	//     for {
	//         select {
	//         case <-consumeCtx.Done():
	//             return
	//         case pack, ok := <-stream.Chan():
	//             assert.True(t, ok)
	//             if pack == nil {
	//                 continue
	//             }
	//
	//             for _, msg := range pack.Msgs {
	//                 _, ok := msg.(*msgstream.SearchMsg)
	//                 assert.True(t, ok)
	//                 // TODO(dragondriver): construct result according to the request
	//
	//                 constructSearchResulstData := func() *schemapb.SearchResultData {
	//                     resultData := &schemapb.SearchResultData{
	//                         NumQueries: int64(nq),
	//                         TopK:       int64(topk),
	//                         Scores:     make([]float32, nq*topk),
	//                         Ids: &schemapb.IDs{
	//                             IdField: &schemapb.IDs_IntId{
	//                                 IntId: &schemapb.LongArray{
	//                                     Data: make([]int64, nq*topk),
	//                                 },
	//                             },
	//                         },
	//                         Topks: make([]int64, nq),
	//                     }
	//
	//                     fieldID := common.StartOfUserFieldID
	//                     for fieldName, dataType := range fieldName2Types {
	//                         resultData.FieldsData = append(resultData.FieldsData, generateFieldData(dataType, fieldName, int64(fieldID), nq*topk))
	//                         fieldID++
	//                     }
	//
	//                     for i := 0; i < nq; i++ {
	//                         for j := 0; j < topk; j++ {
	//                             offset := i*topk + j
	//                             score := float32(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()) // increasingly
	//                             id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	//                             resultData.Scores[offset] = score
	//                             resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
	//                         }
	//                         resultData.Topks[i] = int64(topk)
	//                     }
	//
	//                     return resultData
	//                 }
	//
	//                 result1 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//                 resultData := constructSearchResulstData()
	//                 sliceBlob, err := proto.Marshal(resultData)
	//                 assert.NoError(t, err)
	//                 result1.SlicedBlob = sliceBlob
	//
	//                 // result2.SliceBlob = nil, will be skipped in decode stage
	//                 result2 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//
	//                 // send search result
	//                 task.resultBuf <- result1
	//                 task.resultBuf <- result2
	//             }
	//         }
	//     }
	// }()
	//
	// assert.NoError(t, task.OnEnqueue())
	// assert.NoError(t, task.PreExecute(ctx))
	// assert.NoError(t, task.Execute(ctx))
	// assert.NoError(t, task.PostExecute(ctx))
	//
	// cancel()
	// wg.Wait()
}

func TestSearchTaskV2_7803_reduce(t *testing.T) {
	// var err error
	//
	// Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}
	//
	// rc := NewRootCoordMock()
	// rc.Start()
	// defer rc.Stop()
	//
	// ctx := context.Background()
	//
	// err = InitMetaCache(ctx, rc)
	// assert.NoError(t, err)
	//
	// shardsNum := int32(2)
	// prefix := "TestSearchTaskV2_7803_reduce"
	// collectionName := prefix + funcutil.GenRandomStr()
	// int64Field := "int64"
	// floatVecField := "fvec"
	// dim := 128
	// expr := fmt.Sprintf("%s > 0", int64Field)
	// nq := 10
	// topk := 10
	// roundDecimal := 3
	// nprobe := 10
	//
	// schema := constructCollectionSchema(
	//     int64Field,
	//     floatVecField,
	//     dim,
	//     collectionName)
	// marshaledSchema, err := proto.Marshal(schema)
	// assert.NoError(t, err)
	//
	// createColT := &createCollectionTask{
	//     Condition: NewTaskCondition(ctx),
	//     CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
	//         Base:           nil,
	//         CollectionName: collectionName,
	//         Schema:         marshaledSchema,
	//         ShardsNum:      shardsNum,
	//     },
	//     ctx:       ctx,
	//     rootCoord: rc,
	//     result:    nil,
	//     schema:    nil,
	// }
	//
	// assert.NoError(t, createColT.OnEnqueue())
	// assert.NoError(t, createColT.PreExecute(ctx))
	// assert.NoError(t, createColT.Execute(ctx))
	// assert.NoError(t, createColT.PostExecute(ctx))
	//
	// dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	// query := newMockGetChannelsService()
	// factory := newSimpleMockMsgStreamFactory()
	//
	// collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	// assert.NoError(t, err)
	//
	// qc := NewQueryCoordMock()
	// qc.Start()
	// defer qc.Stop()
	// status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
	//     Base: &commonpb.MsgBase{
	//         MsgType:   commonpb.MsgType_LoadCollection,
	//         MsgID:     0,
	//         Timestamp: 0,
	//         SourceID:  paramtable.GetNodeID(),
	//     },
	//     DbID:         0,
	//     CollectionID: collectionID,
	//     Schema:       nil,
	// })
	// assert.NoError(t, err)
	// assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	//
	// req := constructSearchRequest("", collectionName,
	//     expr,
	//     floatVecField,
	//     nq, dim, nprobe, topk, roundDecimal)
	//
	// task := &searchTaskV2{
	//     Condition: NewTaskCondition(ctx),
	//     SearchRequest: &internalpb.SearchRequest{
	//         Base: &commonpb.MsgBase{
	//             MsgType:   commonpb.MsgType_Search,
	//             MsgID:     0,
	//             Timestamp: 0,
	//             SourceID:  paramtable.GetNodeID(),
	//         },
	//         ResultChannelID:    strconv.FormatInt(paramtable.GetNodeID(), 10),
	//         DbID:               0,
	//         CollectionID:       0,
	//         PartitionIDs:       nil,
	//         Dsl:                "",
	//         PlaceholderGroup:   nil,
	//         DslType:            0,
	//         SerializedExprPlan: nil,
	//         OutputFieldsId:     nil,
	//         TravelTimestamp:    0,
	//         GuaranteeTimestamp: 0,
	//     },
	//     ctx:       ctx,
	//     resultBuf: make(chan *internalpb.SearchResults, 10),
	//     result:    nil,
	//     request:   req,
	//     qc:        qc,
	//     tr:        timerecord.NewTimeRecorder("search"),
	// }
	//
	// // simple mock for query node
	// // TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?
	//
	// var wg sync.WaitGroup
	// wg.Add(1)
	// consumeCtx, cancel := context.WithCancel(ctx)
	// go func() {
	//     defer wg.Done()
	//     for {
	//         select {
	//         case <-consumeCtx.Done():
	//             return
	//         case pack, ok := <-stream.Chan():
	//             assert.True(t, ok)
	//             if pack == nil {
	//                 continue
	//             }
	//
	//             for _, msg := range pack.Msgs {
	//                 _, ok := msg.(*msgstream.SearchMsg)
	//                 assert.True(t, ok)
	//                 // TODO(dragondriver): construct result according to the request
	//
	//                 constructSearchResulstData := func(invalidNum int) *schemapb.SearchResultData {
	//                     resultData := &schemapb.SearchResultData{
	//                         NumQueries: int64(nq),
	//                         TopK:       int64(topk),
	//                         FieldsData: nil,
	//                         Scores:     make([]float32, nq*topk),
	//                         Ids: &schemapb.IDs{
	//                             IdField: &schemapb.IDs_IntId{
	//                                 IntId: &schemapb.LongArray{
	//                                     Data: make([]int64, nq*topk),
	//                                 },
	//                             },
	//                         },
	//                         Topks: make([]int64, nq),
	//                     }
	//
	//                     for i := 0; i < nq; i++ {
	//                         for j := 0; j < topk; j++ {
	//                             offset := i*topk + j
	//                             if j >= invalidNum {
	//                                 resultData.Scores[offset] = minFloat32
	//                                 resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = -1
	//                             } else {
	//                                 score := float32(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()) // increasingly
	//                                 id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	//                                 resultData.Scores[offset] = score
	//                                 resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
	//                             }
	//                         }
	//                         resultData.Topks[i] = int64(topk)
	//                     }
	//
	//                     return resultData
	//                 }
	//
	//                 result1 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//                 resultData := constructSearchResulstData(topk / 2)
	//                 sliceBlob, err := proto.Marshal(resultData)
	//                 assert.NoError(t, err)
	//                 result1.SlicedBlob = sliceBlob
	//
	//                 result2 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//                 resultData2 := constructSearchResulstData(topk - topk/2)
	//                 sliceBlob2, err := proto.Marshal(resultData2)
	//                 assert.NoError(t, err)
	//                 result2.SlicedBlob = sliceBlob2
	//
	//                 // send search result
	//                 task.resultBuf <- result1
	//                 task.resultBuf <- result2
	//             }
	//         }
	//     }
	// }()
	//
	// assert.NoError(t, task.OnEnqueue())
	// assert.NoError(t, task.PreExecute(ctx))
	// assert.NoError(t, task.Execute(ctx))
	// assert.NoError(t, task.PostExecute(ctx))
	//
	// cancel()
	// wg.Wait()
}

func Test_checkSearchResultData(t *testing.T) {
	type args struct {
		data *schemapb.SearchResultData
		nq   int64
		topk int64
	}
	tests := []struct {
		description string
		wantErr     bool

		args args
	}{
		{
			"data.NumQueries != nq", true,
			args{
				data: &schemapb.SearchResultData{NumQueries: 100},
				nq:   10,
			},
		},
		{
			"data.TopK != topk", true,
			args{
				data: &schemapb.SearchResultData{NumQueries: 1, TopK: 1},
				nq:   1,
				topk: 10,
			},
		},
		{
			"size of IntId != NumQueries * TopK", true,
			args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}},
					},
				},
				nq:   1,
				topk: 1,
			},
		},
		{
			"size of StrID != NumQueries * TopK", true,
			args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"1", "2"}}},
					},
				},
				nq:   1,
				topk: 1,
			},
		},
		{
			"size of score != nq * topK", true,
			args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}},
					},
					Scores: []float32{0.99, 0.98},
				},
				nq:   1,
				topk: 1,
			},
		},
		{
			"correct params", false,
			args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}},
					},
					Scores: []float32{0.99},
				},
				nq:   1,
				topk: 1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := checkSearchResultData(test.args.data, test.args.nq, test.args.topk)

			if test.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTaskSearch_selectHighestScoreIndex(t *testing.T) {
	t.Run("Integer ID", func(t *testing.T) {
		type args struct {
			subSearchResultData []*schemapb.SearchResultData
			subSearchNqOffset   [][]int64
			cursors             []int64
			topk                int64
			nq                  int64
		}
		tests := []struct {
			description string
			args        args

			expectedIdx     []int
			expectedDataIdx []int
		}{
			{
				description: "reduce 2 subSearchResultData",
				args: args{
					subSearchResultData: []*schemapb.SearchResultData{
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: []int64{11, 9, 8, 5, 3, 1},
									},
								},
							},
							Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.1},
							Topks:  []int64{2, 2, 2},
						},
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: []int64{12, 10, 7, 6, 4, 2},
									},
								},
							},
							Scores: []float32{1.2, 1.0, 0.7, 0.5, 0.4, 0.2},
							Topks:  []int64{2, 2, 2},
						},
					},
					subSearchNqOffset: [][]int64{{0, 2, 4}, {0, 2, 4}},
					cursors:           []int64{0, 0},
					topk:              2,
					nq:                3,
				},
				expectedIdx:     []int{1, 0, 1},
				expectedDataIdx: []int{0, 2, 4},
			},
		}
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				for nqNum := int64(0); nqNum < test.args.nq; nqNum++ {
					idx, dataIdx := selectHighestScoreIndex(test.args.subSearchResultData, test.args.subSearchNqOffset, test.args.cursors, nqNum)
					assert.Equal(t, test.expectedIdx[nqNum], idx)
					assert.Equal(t, test.expectedDataIdx[nqNum], int(dataIdx))
				}
			})
		}
	})

	//t.Run("Integer ID with bad score", func(t *testing.T) {
	//	type args struct {
	//		subSearchResultData []*schemapb.SearchResultData
	//		subSearchNqOffset   [][]int64
	//		cursors             []int64
	//		topk                int64
	//		nq                  int64
	//	}
	//	tests := []struct {
	//		description string
	//		args        args
	//
	//		expectedIdx     []int
	//		expectedDataIdx []int
	//	}{
	//		{
	//			description: "reduce 2 subSearchResultData",
	//			args: args{
	//				subSearchResultData: []*schemapb.SearchResultData{
	//					{
	//						Ids: &schemapb.IDs{
	//							IdField: &schemapb.IDs_IntId{
	//								IntId: &schemapb.LongArray{
	//									Data: []int64{11, 9, 8, 5, 3, 1},
	//								},
	//							},
	//						},
	//						Scores: []float32{-math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32},
	//						Topks:  []int64{2, 2, 2},
	//					},
	//					{
	//						Ids: &schemapb.IDs{
	//							IdField: &schemapb.IDs_IntId{
	//								IntId: &schemapb.LongArray{
	//									Data: []int64{12, 10, 7, 6, 4, 2},
	//								},
	//							},
	//						},
	//						Scores: []float32{-math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32, -math.MaxFloat32},
	//						Topks:  []int64{2, 2, 2},
	//					},
	//				},
	//				subSearchNqOffset: [][]int64{{0, 2, 4}, {0, 2, 4}},
	//				cursors:           []int64{0, 0},
	//				topk:              2,
	//				nq:                3,
	//			},
	//			expectedIdx:     []int{-1, -1, -1},
	//			expectedDataIdx: []int{-1, -1, -1},
	//		},
	//	}
	//	for _, test := range tests {
	//		t.Run(test.description, func(t *testing.T) {
	//			for nqNum := int64(0); nqNum < test.args.nq; nqNum++ {
	//				idx, dataIdx := selectHighestScoreIndex(test.args.subSearchResultData, test.args.subSearchNqOffset, test.args.cursors, nqNum)
	//				assert.NotEqual(t, test.expectedIdx[nqNum], idx)
	//				assert.NotEqual(t, test.expectedDataIdx[nqNum], int(dataIdx))
	//			}
	//		})
	//	}
	//})

	t.Run("String ID", func(t *testing.T) {
		type args struct {
			subSearchResultData []*schemapb.SearchResultData
			subSearchNqOffset   [][]int64
			cursors             []int64
			topk                int64
			nq                  int64
		}
		tests := []struct {
			description string
			args        args

			expectedIdx     []int
			expectedDataIdx []int
		}{
			{
				description: "reduce 2 subSearchResultData",
				args: args{
					subSearchResultData: []*schemapb.SearchResultData{
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_StrId{
									StrId: &schemapb.StringArray{
										Data: []string{"11", "9", "8", "5", "3", "1"},
									},
								},
							},
							Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.1},
							Topks:  []int64{2, 2, 2},
						},
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_StrId{
									StrId: &schemapb.StringArray{
										Data: []string{"12", "10", "7", "6", "4", "2"},
									},
								},
							},
							Scores: []float32{1.2, 1.0, 0.7, 0.5, 0.4, 0.2},
							Topks:  []int64{2, 2, 2},
						},
					},
					subSearchNqOffset: [][]int64{{0, 2, 4}, {0, 2, 4}},
					cursors:           []int64{0, 0},
					topk:              2,
					nq:                3,
				},
				expectedIdx:     []int{1, 0, 1},
				expectedDataIdx: []int{0, 2, 4},
			},
		}
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				for nqNum := int64(0); nqNum < test.args.nq; nqNum++ {
					idx, dataIdx := selectHighestScoreIndex(test.args.subSearchResultData, test.args.subSearchNqOffset, test.args.cursors, nqNum)
					assert.Equal(t, test.expectedIdx[nqNum], idx)
					assert.Equal(t, test.expectedDataIdx[nqNum], int(dataIdx))
				}
			})
		}
	})
}

func TestTaskSearch_reduceSearchResultData(t *testing.T) {
	var (
		topk int64 = 5
		nq   int64 = 2
	)

	data := [][]int64{
		{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		{20, 19, 18, 17, 16, 15, 14, 13, 12, 11},
		{30, 29, 28, 27, 26, 25, 24, 23, 22, 21},
		{40, 39, 38, 37, 36, 35, 34, 33, 32, 31},
		{50, 49, 48, 47, 46, 45, 44, 43, 42, 41},
	}

	score := [][]float32{
		{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		{20, 19, 18, 17, 16, 15, 14, 13, 12, 11},
		{30, 29, 28, 27, 26, 25, 24, 23, 22, 21},
		{40, 39, 38, 37, 36, 35, 34, 33, 32, 31},
		{50, 49, 48, 47, 46, 45, 44, 43, 42, 41},
	}

	resultScore := []float32{-50, -49, -48, -47, -46, -45, -44, -43, -42, -41}

	t.Run("Offset limit", func(t *testing.T) {
		tests := []struct {
			description string
			offset      int64
			limit       int64

			outScore []float32
			outData  []int64
		}{
			{
				"offset 0, limit 5", 0, 5,
				[]float32{-50, -49, -48, -47, -46, -45, -44, -43, -42, -41},
				[]int64{50, 49, 48, 47, 46, 45, 44, 43, 42, 41},
			},
			{
				"offset 1, limit 4", 1, 4,
				[]float32{-49, -48, -47, -46, -44, -43, -42, -41},
				[]int64{49, 48, 47, 46, 44, 43, 42, 41},
			},
			{
				"offset 2, limit 3", 2, 3,
				[]float32{-48, -47, -46, -43, -42, -41},
				[]int64{48, 47, 46, 43, 42, 41},
			},
			{
				"offset 3, limit 2", 3, 2,
				[]float32{-47, -46, -42, -41},
				[]int64{47, 46, 42, 41},
			},
			{
				"offset 4, limit 1", 4, 1,
				[]float32{-46, -41},
				[]int64{46, 41},
			},
		}

		var results []*schemapb.SearchResultData
		for i := range data {
			r := getSearchResultData(nq, topk)

			r.Ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data[i]}}
			r.Scores = score[i]
			r.Topks = []int64{5, 5}

			results = append(results, r)
		}

		queryInfo := &planpb.QueryInfo{
			GroupByFieldId: -1,
		}
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				reduced, err := reduceSearchResult(context.TODO(),
					NewReduceSearchResultInfo(results, nq, topk, metric.L2, schemapb.DataType_Int64, test.offset, queryInfo))
				assert.NoError(t, err)
				assert.Equal(t, test.outData, reduced.GetResults().GetIds().GetIntId().GetData())
				assert.Equal(t, []int64{test.limit, test.limit}, reduced.GetResults().GetTopks())
				assert.Equal(t, test.limit, reduced.GetResults().GetTopK())
				assert.InDeltaSlice(t, test.outScore, reduced.GetResults().GetScores(), 10e-8)
			})
		}

		lessThanLimitTests := []struct {
			description string
			offset      int64
			limit       int64

			outLimit int64
			outScore []float32
			outData  []int64
		}{
			{
				"offset 0, limit 6", 0, 6, 5,
				[]float32{-50, -49, -48, -47, -46, -45, -44, -43, -42, -41},
				[]int64{50, 49, 48, 47, 46, 45, 44, 43, 42, 41},
			},
			{
				"offset 1, limit 5", 1, 5, 4,
				[]float32{-49, -48, -47, -46, -44, -43, -42, -41},
				[]int64{49, 48, 47, 46, 44, 43, 42, 41},
			},
			{
				"offset 2, limit 4", 2, 4, 3,
				[]float32{-48, -47, -46, -43, -42, -41},
				[]int64{48, 47, 46, 43, 42, 41},
			},
			{
				"offset 3, limit 3", 3, 3, 2,
				[]float32{-47, -46, -42, -41},
				[]int64{47, 46, 42, 41},
			},
			{
				"offset 4, limit 2", 4, 2, 1,
				[]float32{-46, -41},
				[]int64{46, 41},
			},
			{
				"offset 5, limit 1", 5, 1, 0,
				[]float32{},
				[]int64{},
			},
		}
		for _, test := range lessThanLimitTests {
			t.Run(test.description, func(t *testing.T) {
				reduced, err := reduceSearchResult(context.TODO(), NewReduceSearchResultInfo(results, nq, topk,
					metric.L2, schemapb.DataType_Int64, test.offset, queryInfo))
				assert.NoError(t, err)
				assert.Equal(t, test.outData, reduced.GetResults().GetIds().GetIntId().GetData())
				assert.Equal(t, []int64{test.outLimit, test.outLimit}, reduced.GetResults().GetTopks())
				assert.Equal(t, test.outLimit, reduced.GetResults().GetTopK())
				assert.InDeltaSlice(t, test.outScore, reduced.GetResults().GetScores(), 10e-8)
			})
		}
	})

	t.Run("Int64 ID", func(t *testing.T) {
		resultData := []int64{50, 49, 48, 47, 46, 45, 44, 43, 42, 41}

		var results []*schemapb.SearchResultData
		for i := range data {
			r := getSearchResultData(nq, topk)

			r.Ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data[i]}}
			r.Scores = score[i]
			r.Topks = []int64{5, 5}

			results = append(results, r)
		}

		queryInfo := &planpb.QueryInfo{
			GroupByFieldId: -1,
		}

		reduced, err := reduceSearchResult(context.TODO(), NewReduceSearchResultInfo(
			results, nq, topk, metric.L2, schemapb.DataType_Int64, 0, queryInfo))

		assert.NoError(t, err)
		assert.Equal(t, resultData, reduced.GetResults().GetIds().GetIntId().GetData())
		assert.Equal(t, []int64{5, 5}, reduced.GetResults().GetTopks())
		assert.Equal(t, int64(5), reduced.GetResults().GetTopK())
		assert.InDeltaSlice(t, resultScore, reduced.GetResults().GetScores(), 10e-8)
	})

	t.Run("String ID", func(t *testing.T) {
		resultData := []string{"50", "49", "48", "47", "46", "45", "44", "43", "42", "41"}

		var results []*schemapb.SearchResultData
		for i := range data {
			r := getSearchResultData(nq, topk)

			var strData []string
			for _, d := range data[i] {
				strData = append(strData, strconv.FormatInt(d, 10))
			}
			r.Ids.IdField = &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: strData}}
			r.Scores = score[i]
			r.Topks = []int64{5, 5}

			results = append(results, r)
		}
		queryInfo := &planpb.QueryInfo{
			GroupByFieldId: -1,
		}

		reduced, err := reduceSearchResult(context.TODO(), NewReduceSearchResultInfo(results,
			nq, topk, metric.L2, schemapb.DataType_VarChar, 0, queryInfo))

		assert.NoError(t, err)
		assert.Equal(t, resultData, reduced.GetResults().GetIds().GetStrId().GetData())
		assert.Equal(t, []int64{5, 5}, reduced.GetResults().GetTopks())
		assert.Equal(t, int64(5), reduced.GetResults().GetTopK())
		assert.InDeltaSlice(t, resultScore, reduced.GetResults().GetScores(), 10e-8)
	})
}

func TestTaskSearch_reduceGroupBySearchResultData(t *testing.T) {
	var (
		nq   int64 = 2
		topK int64 = 5
	)
	ids := [][]int64{
		{1, 3, 5, 7, 9, 1, 3, 5, 7, 9},
		{2, 4, 6, 8, 10, 2, 4, 6, 8, 10},
	}
	scores := [][]float32{
		{10, 8, 6, 4, 2, 10, 8, 6, 4, 2},
		{9, 7, 5, 3, 1, 9, 7, 5, 3, 1},
	}

	groupByValuesArr := [][][]int64{
		{
			{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
			{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
		}, // result2 has completely same group_by values, no result from result2 can be selected
		{
			{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
			{6, 8, 3, 4, 5, 6, 8, 3, 4, 5},
		}, // result2 will contribute group_by values 6 and 8
	}
	expectedIDs := [][]int64{
		{1, 3, 5, 7, 9, 1, 3, 5, 7, 9},
		{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
	}
	expectedScores := [][]float32{
		{-10, -8, -6, -4, -2, -10, -8, -6, -4, -2},
		{-10, -9, -8, -7, -6, -10, -9, -8, -7, -6},
	}
	expectedGroupByValues := [][]int64{
		{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
		{1, 6, 2, 8, 3, 1, 6, 2, 8, 3},
	}

	for i, groupByValues := range groupByValuesArr {
		t.Run("Group By correctness", func(t *testing.T) {
			var results []*schemapb.SearchResultData
			for j := range ids {
				result := getSearchResultData(nq, topK)
				result.Ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids[j]}}
				result.Scores = scores[j]
				result.Topks = []int64{topK, topK}
				result.GroupByFieldValue = &schemapb.FieldData{
					Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: groupByValues[j],
								},
							},
						},
					},
				}
				results = append(results, result)
			}
			queryInfo := &planpb.QueryInfo{
				GroupByFieldId: 1,
				GroupSize:      1,
			}
			reduced, err := reduceSearchResult(context.TODO(), NewReduceSearchResultInfo(results, nq, topK, metric.L2,
				schemapb.DataType_Int64, 0, queryInfo))
			resultIDs := reduced.GetResults().GetIds().GetIntId().Data
			resultScores := reduced.GetResults().GetScores()
			resultGroupByValues := reduced.GetResults().GetGroupByFieldValue().GetScalars().GetLongData().GetData()
			assert.EqualValues(t, expectedIDs[i], resultIDs)
			assert.EqualValues(t, expectedScores[i], resultScores)
			assert.EqualValues(t, expectedGroupByValues[i], resultGroupByValues)
			assert.NoError(t, err)
		})
	}
}

func TestTaskSearch_reduceGroupBySearchResultDataWithOffset(t *testing.T) {
	var (
		nq     int64 = 1
		limit  int64 = 5
		offset int64 = 5
	)
	ids := [][]int64{
		{1, 3, 5, 7, 9},
		{2, 4, 6, 8, 10},
	}
	scores := [][]float32{
		{10, 8, 6, 4, 2},
		{9, 7, 5, 3, 1},
	}
	groupByValuesArr := [][]int64{
		{1, 3, 5, 7, 9},
		{2, 4, 6, 8, 10},
	}
	expectedIDs := []int64{6, 7, 8, 9, 10}
	expectedScores := []float32{-5, -4, -3, -2, -1}
	expectedGroupByValues := []int64{6, 7, 8, 9, 10}

	var results []*schemapb.SearchResultData
	for j := range ids {
		result := getSearchResultData(nq, limit+offset)
		result.Ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids[j]}}
		result.Scores = scores[j]
		result.Topks = []int64{limit}
		result.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: groupByValuesArr[j],
						},
					},
				},
			},
		}
		results = append(results, result)
	}

	queryInfo := &planpb.QueryInfo{
		GroupByFieldId: 1,
		GroupSize:      1,
	}
	reduced, err := reduceSearchResult(context.TODO(), NewReduceSearchResultInfo(results, nq, limit+offset, metric.L2,
		schemapb.DataType_Int64, offset, queryInfo))
	resultIDs := reduced.GetResults().GetIds().GetIntId().Data
	resultScores := reduced.GetResults().GetScores()
	resultGroupByValues := reduced.GetResults().GetGroupByFieldValue().GetScalars().GetLongData().GetData()
	assert.EqualValues(t, expectedIDs, resultIDs)
	assert.EqualValues(t, expectedScores, resultScores)
	assert.EqualValues(t, expectedGroupByValues, resultGroupByValues)
	assert.NoError(t, err)
}

func TestTaskSearch_reduceGroupBySearchWithGroupSizeMoreThanOne(t *testing.T) {
	var (
		nq   int64 = 2
		topK int64 = 5
	)
	ids := [][]int64{
		{1, 3, 5, 7, 9, 1, 3, 5, 7, 9},
		{2, 4, 6, 8, 10, 2, 4, 6, 8, 10},
	}
	scores := [][]float32{
		{10, 8, 6, 4, 2, 10, 8, 6, 4, 2},
		{9, 7, 5, 3, 1, 9, 7, 5, 3, 1},
	}

	groupByValuesArr := [][][]int64{
		{
			{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
			{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
		},
		{
			{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
			{6, 8, 3, 4, 5, 6, 8, 3, 4, 5},
		},
	}
	expectedIDs := [][]int64{
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		{1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6},
	}
	expectedScores := [][]float32{
		{-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1},
		{-10, -9, -8, -7, -6, -5, -10, -9, -8, -7, -6, -5},
	}
	expectedGroupByValues := [][]int64{
		{1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5},
		{1, 6, 2, 8, 3, 3, 1, 6, 2, 8, 3, 3},
	}

	for i, groupByValues := range groupByValuesArr {
		t.Run("Group By correctness", func(t *testing.T) {
			var results []*schemapb.SearchResultData
			for j := range ids {
				result := getSearchResultData(nq, topK)
				result.Ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids[j]}}
				result.Scores = scores[j]
				result.Topks = []int64{topK, topK}
				result.GroupByFieldValue = &schemapb.FieldData{
					Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: groupByValues[j],
								},
							},
						},
					},
				}
				results = append(results, result)
			}
			queryInfo := &planpb.QueryInfo{
				GroupByFieldId: 1,
				GroupSize:      2,
			}
			reduced, err := reduceSearchResult(context.TODO(), NewReduceSearchResultInfo(results, nq, topK, metric.L2,
				schemapb.DataType_Int64, 0, queryInfo))
			resultIDs := reduced.GetResults().GetIds().GetIntId().Data
			resultScores := reduced.GetResults().GetScores()
			resultGroupByValues := reduced.GetResults().GetGroupByFieldValue().GetScalars().GetLongData().GetData()
			assert.EqualValues(t, expectedIDs[i], resultIDs)
			assert.EqualValues(t, expectedScores[i], resultScores)
			assert.EqualValues(t, expectedGroupByValues[i], resultGroupByValues)
			assert.NoError(t, err)
		})
	}
}

func TestSearchTask_ErrExecute(t *testing.T) {
	var (
		err error
		ctx = context.TODO()

		rc = NewRootCoordMock()
		qc = getQueryCoordClient()
		qn = getQueryNodeClient()

		shardsNum      = int32(2)
		collectionName = t.Name() + funcutil.GenRandomStr()
	)

	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()

	mgr := NewMockShardClientManager(t)
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Maybe()
	mgr.EXPECT().UpdateShardLeaders(mock.Anything, mock.Anything).Return(nil).Maybe()
	lb := NewLBPolicyImpl(mgr)

	defer qc.Close()

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

	collectionID, err := globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
	assert.NoError(t, err)

	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
	qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: successStatus,
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
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

	// test begins
	task := &searchTask{
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: paramtable.GetNodeID(),
			},
			CollectionID:   collectionID,
			OutputFieldsId: make([]int64, len(fieldName2Types)),
		},
		ctx: ctx,
		result: &milvuspb.SearchResults{
			Status: merr.Success(),
		},
		request: &milvuspb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: paramtable.GetNodeID(),
			},
			CollectionName: collectionName,
			Nq:             2,
			DslType:        commonpb.DslType_BoolExprV1,
		},
		qc: qc,
		lb: lb,
	}
	for i := 0; i < len(fieldName2Types); i++ {
		task.SearchRequest.OutputFieldsId[i] = int64(common.StartOfUserFieldID + i)
	}

	assert.NoError(t, task.OnEnqueue())

	task.ctx = ctx
	if enableMultipleVectorFields {
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "multiple anns_fields exist, please specify a anns_field in search_params")
	} else {
		assert.NoError(t, task.PreExecute(ctx))
	}

	qn.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
	assert.Error(t, task.Execute(ctx))

	qn.ExpectedCalls = nil
	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	qn.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
		Status: merr.Status(merr.ErrChannelNotAvailable),
	}, nil)
	err = task.Execute(ctx)
	assert.ErrorIs(t, err, merr.ErrChannelNotAvailable)

	qn.ExpectedCalls = nil
	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	qn.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}, nil)
	assert.Error(t, task.Execute(ctx))

	qn.ExpectedCalls = nil
	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	qn.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
		Status: merr.Success(),
	}, nil)
	assert.NoError(t, task.Execute(ctx))
}

func TestTaskSearch_parseQueryInfo(t *testing.T) {
	t.Run("parseSearchInfo no error", func(t *testing.T) {
		var targetOffset int64 = 200

		normalParam := getValidSearchParams()

		noMetricTypeParams := getBaseSearchParams()
		noMetricTypeParams = append(noMetricTypeParams, &commonpb.KeyValuePair{
			Key:   SearchParamsKey,
			Value: `{"nprobe": 10}`,
		})

		noSearchParams := getBaseSearchParams()
		noSearchParams = append(noSearchParams, &commonpb.KeyValuePair{
			Key:   common.MetricTypeKey,
			Value: metric.L2,
		})

		offsetParam := getValidSearchParams()
		offsetParam = append(offsetParam, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: strconv.FormatInt(targetOffset, 10),
		})

		tests := []struct {
			description string
			validParams []*commonpb.KeyValuePair
		}{
			{"noMetricType", noMetricTypeParams},
			{"noSearchParams", noSearchParams},
			{"normal", normalParam},
			{"offsetParam", offsetParam},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				info, offset, err := parseSearchInfo(test.validParams, nil, false)
				assert.NoError(t, err)
				assert.NotNil(t, info)
				if test.description == "offsetParam" {
					assert.Equal(t, targetOffset, offset)
				}
			})
		}
	})

	t.Run("parseSearchInfo error", func(t *testing.T) {
		spNoTopk := []*commonpb.KeyValuePair{{
			Key:   AnnsFieldKey,
			Value: testFloatVecField,
		}}

		spInvalidTopk := append(spNoTopk, &commonpb.KeyValuePair{
			Key:   TopKKey,
			Value: "invalid",
		})

		spInvalidTopk65536 := append(spNoTopk, &commonpb.KeyValuePair{
			Key:   TopKKey,
			Value: "65536",
		})

		spNoMetricType := append(spNoTopk, &commonpb.KeyValuePair{
			Key:   TopKKey,
			Value: "10",
		})

		spInvalidTopkPlusOffset := append(spNoTopk, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: "65535",
		})

		spNoSearchParams := append(spNoMetricType, &commonpb.KeyValuePair{
			Key:   common.MetricTypeKey,
			Value: metric.L2,
		})

		// no roundDecimal is valid
		noRoundDecimal := append(spNoSearchParams, &commonpb.KeyValuePair{
			Key:   SearchParamsKey,
			Value: `{"nprobe": 10}`,
		})

		spInvalidRoundDecimal2 := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   RoundDecimalKey,
			Value: "1000",
		})

		spInvalidRoundDecimal := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   RoundDecimalKey,
			Value: "invalid",
		})

		spInvalidOffsetNoInt := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: "invalid",
		})

		spInvalidOffsetNegative := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: "-1",
		})

		spInvalidOffsetTooLarge := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: "16386",
		})

		tests := []struct {
			description   string
			invalidParams []*commonpb.KeyValuePair
		}{
			{"No_topk", spNoTopk},
			{"Invalid_topk", spInvalidTopk},
			{"Invalid_topk_65536", spInvalidTopk65536},
			{"Invalid_topk_plus_offset", spInvalidTopkPlusOffset},
			{"Invalid_round_decimal", spInvalidRoundDecimal},
			{"Invalid_round_decimal_1000", spInvalidRoundDecimal2},
			{"Invalid_offset_not_int", spInvalidOffsetNoInt},
			{"Invalid_offset_negative", spInvalidOffsetNegative},
			{"Invalid_offset_too_large", spInvalidOffsetTooLarge},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				info, offset, err := parseSearchInfo(test.invalidParams, nil, false)
				assert.Error(t, err)
				assert.Nil(t, info)
				assert.Zero(t, offset)

				t.Logf("err=%s", err.Error())
			})
		}
	})
	t.Run("check iterator and groupBy", func(t *testing.T) {
		normalParam := getValidSearchParams()
		normalParam = append(normalParam, &commonpb.KeyValuePair{
			Key:   IteratorField,
			Value: "True",
		})
		normalParam = append(normalParam, &commonpb.KeyValuePair{
			Key:   GroupByFieldKey,
			Value: "string_field",
		})
		fields := make([]*schemapb.FieldSchema, 0)
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: int64(101),
			Name:    "string_field",
		})
		schema := &schemapb.CollectionSchema{
			Fields: fields,
		}
		info, _, err := parseSearchInfo(normalParam, schema, false)
		assert.Nil(t, info)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
	t.Run("check range-search and groupBy", func(t *testing.T) {
		normalParam := getValidSearchParams()
		resetSearchParamsValue(normalParam, SearchParamsKey, `{"nprobe": 10, "radius":0.2}`)
		normalParam = append(normalParam, &commonpb.KeyValuePair{
			Key:   GroupByFieldKey,
			Value: "string_field",
		})
		fields := make([]*schemapb.FieldSchema, 0)
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: int64(101),
			Name:    "string_field",
		})
		schema := &schemapb.CollectionSchema{
			Fields: fields,
		}
		info, _, err := parseSearchInfo(normalParam, schema, false)
		assert.Nil(t, info)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
	t.Run("check iterator and topK", func(t *testing.T) {
		normalParam := getValidSearchParams()
		normalParam = append(normalParam, &commonpb.KeyValuePair{
			Key:   IteratorField,
			Value: "True",
		})
		resetSearchParamsValue(normalParam, TopKKey, `1024000`)
		fields := make([]*schemapb.FieldSchema, 0)
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: int64(101),
			Name:    "string_field",
		})
		schema := &schemapb.CollectionSchema{
			Fields: fields,
		}
		info, _, err := parseSearchInfo(normalParam, schema, false)
		assert.NotNil(t, info)
		assert.NoError(t, err)
		assert.Equal(t, Params.QuotaConfig.TopKLimit.GetAsInt64(), info.Topk)
	})
}

func getSearchResultData(nq, topk int64) *schemapb.SearchResultData {
	result := schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		Ids:        &schemapb.IDs{},
		Scores:     []float32{},
		Topks:      []int64{},
	}
	return &result
}

func TestSearchTask_Requery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		dim        = 128
		rows       = 5
		collection = "test-requery"

		pkField  = "pk"
		vecField = "vec"
	)

	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}

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
	node.rootCoord = mocks.NewMockRootCoordClient(t)
	node.queryCoord = mocks.NewMockQueryCoordClient(t)

	collectionName := "col"
	collectionID := UniqueID(0)
	cache := NewMockCache(t)
	collSchema := constructCollectionSchema(pkField, vecField, dim, collection)
	schema := newSchemaInfo(collSchema)
	cache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(collectionID, nil).Maybe()
	cache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schema, nil).Maybe()
	cache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{"_default": UniqueID(1)}, nil).Maybe()
	cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionBasicInfo{}, nil).Maybe()
	cache.EXPECT().GetShards(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[string][]nodeInfo{}, nil).Maybe()
	cache.EXPECT().DeprecateShardCache(mock.Anything, mock.Anything).Return().Maybe()
	globalMetaCache = cache

	t.Run("Test normal", func(t *testing.T) {
		collSchema := constructCollectionSchema(pkField, vecField, dim, collection)
		schema := newSchemaInfo(collSchema)
		qn := mocks.NewMockQueryNodeClient(t)
		qn.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, request *querypb.QueryRequest, option ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
				idFieldData := &schemapb.FieldData{
					Type:      schemapb.DataType_Int64,
					FieldName: pkField,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: ids,
								},
							},
						},
					},
				}
				idField := &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: ids,
						},
					},
				}
				if request.GetReq().GetOutputFieldsId()[0] == 100 {
					return &internalpb.RetrieveResults{
						Ids: idField,
						FieldsData: []*schemapb.FieldData{
							idFieldData,
							newFloatVectorFieldData(vecField, rows, dim),
						},
					}, nil
				}
				return &internalpb.RetrieveResults{
					Ids: idField,
					FieldsData: []*schemapb.FieldData{
						newFloatVectorFieldData(vecField, rows, dim),
						idFieldData,
					},
				}, nil
			})

		lb := NewMockLBPolicy(t)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Run(func(ctx context.Context, workload CollectionWorkLoad) {
			err = workload.exec(ctx, 0, qn, "")
			assert.NoError(t, err)
		}).Return(nil)
		lb.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything).Return()
		node.lbPolicy = lb

		resultIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		}

		outputFields := []string{pkField, vecField}
		qt := &searchTask{
			ctx: ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Search,
					SourceID: paramtable.GetNodeID(),
				},
			},
			request: &milvuspb.SearchRequest{
				CollectionName: collectionName,
				OutputFields:   outputFields,
			},
			result: &milvuspb.SearchResults{
				Results: &schemapb.SearchResultData{
					Ids: resultIDs,
				},
			},
			schema: schema,
			tr:     timerecord.NewTimeRecorder("search"),
			node:   node,
		}

		err := qt.Requery()
		assert.NoError(t, err)
		assert.Len(t, qt.result.Results.FieldsData, 2)
		for _, field := range qt.result.Results.FieldsData {
			fieldName := field.GetFieldName()
			assert.Contains(t, []string{pkField, vecField}, fieldName)
		}
	})

	t.Run("Test no primary key", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{}
		schema := newSchemaInfo(collSchema)

		node := mocks.NewMockProxy(t)

		qt := &searchTask{
			ctx: ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Search,
					SourceID: paramtable.GetNodeID(),
				},
			},
			request: &milvuspb.SearchRequest{},
			schema:  schema,
			tr:      timerecord.NewTimeRecorder("search"),
			node:    node,
		}

		err := qt.Requery()
		t.Logf("err = %s", err)
		assert.Error(t, err)
	})

	t.Run("Test requery failed", func(t *testing.T) {
		collSchema := constructCollectionSchema(pkField, vecField, dim, collection)
		schema := newSchemaInfo(collSchema)
		qn := mocks.NewMockQueryNodeClient(t)
		qn.EXPECT().Query(mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("mock err 1"))

		lb := NewMockLBPolicy(t)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Run(func(ctx context.Context, workload CollectionWorkLoad) {
			_ = workload.exec(ctx, 0, qn, "")
		}).Return(fmt.Errorf("mock err 1"))
		node.lbPolicy = lb

		qt := &searchTask{
			ctx: ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Search,
					SourceID: paramtable.GetNodeID(),
				},
			},
			request: &milvuspb.SearchRequest{
				CollectionName: collectionName,
			},
			schema: schema,
			tr:     timerecord.NewTimeRecorder("search"),
			node:   node,
		}

		err := qt.Requery()
		t.Logf("err = %s", err)
		assert.Error(t, err)
	})

	t.Run("Test postExecute with requery failed", func(t *testing.T) {
		collSchema := constructCollectionSchema(pkField, vecField, dim, collection)
		schema := newSchemaInfo(collSchema)
		qn := mocks.NewMockQueryNodeClient(t)
		qn.EXPECT().Query(mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("mock err 1"))

		lb := NewMockLBPolicy(t)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Run(func(ctx context.Context, workload CollectionWorkLoad) {
			_ = workload.exec(ctx, 0, qn, "")
		}).Return(fmt.Errorf("mock err 1"))
		node.lbPolicy = lb

		resultIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		}

		qt := &searchTask{
			ctx: ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Search,
					SourceID: paramtable.GetNodeID(),
				},
			},
			request: &milvuspb.SearchRequest{
				CollectionName: collectionName,
			},
			result: &milvuspb.SearchResults{
				Results: &schemapb.SearchResultData{
					Ids: resultIDs,
				},
			},
			requery:   true,
			schema:    schema,
			resultBuf: typeutil.NewConcurrentSet[*internalpb.SearchResults](),
			tr:        timerecord.NewTimeRecorder("search"),
			node:      node,
		}
		scores := make([]float32, rows)
		for i := range scores {
			scores[i] = float32(i)
		}
		partialResultData := &schemapb.SearchResultData{
			Ids:    resultIDs,
			Scores: scores,
		}
		bytes, err := proto.Marshal(partialResultData)
		assert.NoError(t, err)
		qt.resultBuf.Insert(&internalpb.SearchResults{
			SlicedBlob: bytes,
		})
		qt.queryInfos = []*planpb.QueryInfo{{
			GroupByFieldId: -1,
		}}
		err = qt.PostExecute(ctx)
		t.Logf("err = %s", err)
		assert.Error(t, err)
	})
}

type GetPartitionIDsSuite struct {
	suite.Suite

	mockMetaCache *MockCache
}

func (s *GetPartitionIDsSuite) SetupTest() {
	s.mockMetaCache = NewMockCache(s.T())
	globalMetaCache = s.mockMetaCache
}

func (s *GetPartitionIDsSuite) TearDownTest() {
	globalMetaCache = nil
	Params.Reset(Params.ProxyCfg.PartitionNameRegexp.Key)
}

func (s *GetPartitionIDsSuite) TestPlainPartitionNames() {
	Params.Save(Params.ProxyCfg.PartitionNameRegexp.Key, "false")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{"partition_1": 100, "partition_2": 200}, nil).Once()

	result, err := getPartitionIDs(ctx, "default_db", "test_collection", []string{"partition_1", "partition_2"})

	s.NoError(err)
	s.ElementsMatch([]int64{100, 200}, result)

	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{"partition_1": 100}, nil).Once()

	_, err = getPartitionIDs(ctx, "default_db", "test_collection", []string{"partition_1", "partition_2"})
	s.Error(err)

	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("mocked")).Once()
	_, err = getPartitionIDs(ctx, "default_db", "test_collection", []string{"partition_1", "partition_2"})
	s.Error(err)
}

func (s *GetPartitionIDsSuite) TestRegexpPartitionNames() {
	Params.Save(Params.ProxyCfg.PartitionNameRegexp.Key, "true")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{"partition_1": 100, "partition_2": 200}, nil).Once()

	result, err := getPartitionIDs(ctx, "default_db", "test_collection", []string{"partition_1", "partition_2"})

	s.NoError(err)
	s.ElementsMatch([]int64{100, 200}, result)

	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{"partition_1": 100, "partition_2": 200}, nil).Once()

	result, err = getPartitionIDs(ctx, "default_db", "test_collection", []string{"partition_.*"})

	s.NoError(err)
	s.ElementsMatch([]int64{100, 200}, result)

	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{"partition_1": 100}, nil).Once()

	_, err = getPartitionIDs(ctx, "default_db", "test_collection", []string{"partition_1", "partition_2"})
	s.Error(err)

	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("mocked")).Once()
	_, err = getPartitionIDs(ctx, "default_db", "test_collection", []string{"partition_1", "partition_2"})
	s.Error(err)
}

func TestGetPartitionIDs(t *testing.T) {
	suite.Run(t, new(GetPartitionIDsSuite))
}

func TestSearchTask_CanSkipAllocTimestamp(t *testing.T) {
	dbName := "test_query"
	collName := "test_skip_alloc_timestamp"
	collID := UniqueID(111)
	mockMetaCache := NewMockCache(t)
	globalMetaCache = mockMetaCache

	t.Run("default consistency level", func(t *testing.T) {
		st := &searchTask{
			request: &milvuspb.SearchRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: true,
			},
		}
		mockMetaCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(collID, nil)
		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionBasicInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Eventually,
			}, nil).Once()

		skip := st.CanSkipAllocTimestamp()
		assert.True(t, skip)

		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionBasicInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Bounded,
			}, nil).Once()
		skip = st.CanSkipAllocTimestamp()
		assert.True(t, skip)

		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionBasicInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Strong,
			}, nil).Once()
		skip = st.CanSkipAllocTimestamp()
		assert.False(t, skip)
	})

	t.Run("request consistency level", func(t *testing.T) {
		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionBasicInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Eventually,
			}, nil).Times(3)

		st := &searchTask{
			request: &milvuspb.SearchRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: false,
				ConsistencyLevel:      commonpb.ConsistencyLevel_Eventually,
			},
		}

		skip := st.CanSkipAllocTimestamp()
		assert.True(t, skip)

		st.request.ConsistencyLevel = commonpb.ConsistencyLevel_Bounded
		skip = st.CanSkipAllocTimestamp()
		assert.True(t, skip)

		st.request.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
		skip = st.CanSkipAllocTimestamp()
		assert.False(t, skip)
	})

	t.Run("legacy_guarantee_ts", func(t *testing.T) {
		st := &searchTask{
			request: &milvuspb.SearchRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: false,
				ConsistencyLevel:      commonpb.ConsistencyLevel_Strong,
			},
		}

		skip := st.CanSkipAllocTimestamp()
		assert.False(t, skip)

		st.request.GuaranteeTimestamp = 1 // eventually
		skip = st.CanSkipAllocTimestamp()
		assert.True(t, skip)

		st.request.GuaranteeTimestamp = 2 // bounded
		skip = st.CanSkipAllocTimestamp()
		assert.True(t, skip)
	})

	t.Run("failed", func(t *testing.T) {
		mockMetaCache.ExpectedCalls = nil
		mockMetaCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(collID, nil)
		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			nil, fmt.Errorf("mock error")).Once()

		st := &searchTask{
			request: &milvuspb.SearchRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: true,
				ConsistencyLevel:      commonpb.ConsistencyLevel_Eventually,
			},
		}

		skip := st.CanSkipAllocTimestamp()
		assert.False(t, skip)

		mockMetaCache.ExpectedCalls = nil
		mockMetaCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(collID, fmt.Errorf("mock error"))
		mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
			&collectionBasicInfo{
				collID:           collID,
				consistencyLevel: commonpb.ConsistencyLevel_Eventually,
			}, nil)

		skip = st.CanSkipAllocTimestamp()
		assert.False(t, skip)

		st2 := &searchTask{
			request: &milvuspb.SearchRequest{
				Base:                  nil,
				DbName:                dbName,
				CollectionName:        collName,
				UseDefaultConsistency: false,
				ConsistencyLevel:      commonpb.ConsistencyLevel_Eventually,
			},
		}

		skip = st2.CanSkipAllocTimestamp()
		assert.True(t, skip)
	})
}

type MaterializedViewTestSuite struct {
	suite.Suite
	mockMetaCache *MockCache

	ctx             context.Context
	cancelFunc      context.CancelFunc
	dbName          string
	colName         string
	colID           UniqueID
	fieldName2Types map[string]schemapb.DataType
}

func (s *MaterializedViewTestSuite) SetupSuite() {
	s.ctx, s.cancelFunc = context.WithCancel(context.Background())
	s.dbName = "TestMvDbName"
	s.colName = "TestMvColName"
	s.colID = UniqueID(123)
	s.fieldName2Types = map[string]schemapb.DataType{
		testInt64Field:    schemapb.DataType_Int64,
		testVarCharField:  schemapb.DataType_VarChar,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
}

func (s *MaterializedViewTestSuite) TearDownSuite() {
	s.cancelFunc()
}

func (s *MaterializedViewTestSuite) SetupTest() {
	s.mockMetaCache = NewMockCache(s.T())
	s.mockMetaCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.colID, nil)
	s.mockMetaCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&collectionBasicInfo{
			collID:                s.colID,
			partitionKeyIsolation: true,
		}, nil)
	globalMetaCache = s.mockMetaCache
}

func (s *MaterializedViewTestSuite) TearDownTest() {
	globalMetaCache = nil
}

func (s *MaterializedViewTestSuite) getSearchTask() *searchTask {
	task := &searchTask{
		ctx:            s.ctx,
		collectionName: s.colName,
		SearchRequest:  &internalpb.SearchRequest{},
		request: &milvuspb.SearchRequest{
			DbName:         dbName,
			CollectionName: s.colName,
			Nq:             1,
			SearchParams:   getBaseSearchParams(),
		},
	}
	s.NoError(task.OnEnqueue())
	return task
}

func (s *MaterializedViewTestSuite) TestMvNotEnabledWithNoPartitionKey() {
	task := s.getSearchTask()
	task.enableMaterializedView = false

	schema := constructCollectionSchemaByDataType(s.colName, s.fieldName2Types, testInt64Field, false)
	schemaInfo := newSchemaInfo(schema)
	s.mockMetaCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schemaInfo, nil)

	err := task.PreExecute(s.ctx)
	s.NoError(err)
	s.NotZero(len(task.queryInfos))
	s.Equal(false, task.queryInfos[0].MaterializedViewInvolved)
}

func (s *MaterializedViewTestSuite) TestMvNotEnabledWithPartitionKey() {
	task := s.getSearchTask()
	task.enableMaterializedView = false
	task.request.Dsl = testInt64Field + " == 1"
	schema := ConstructCollectionSchemaWithPartitionKey(s.colName, s.fieldName2Types, testInt64Field, testInt64Field, false)
	schemaInfo := newSchemaInfo(schema)
	s.mockMetaCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schemaInfo, nil)
	s.mockMetaCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).Return([]string{"partition_1", "partition_2"}, nil)
	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{"partition_1": 1, "partition_2": 2}, nil)

	err := task.PreExecute(s.ctx)
	s.NoError(err)
	s.NotZero(len(task.queryInfos))
	s.Equal(false, task.queryInfos[0].MaterializedViewInvolved)
}

func (s *MaterializedViewTestSuite) TestMvEnabledNoPartitionKey() {
	task := s.getSearchTask()
	task.enableMaterializedView = true
	schema := constructCollectionSchemaByDataType(s.colName, s.fieldName2Types, testInt64Field, false)
	schemaInfo := newSchemaInfo(schema)
	s.mockMetaCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schemaInfo, nil)

	err := task.PreExecute(s.ctx)
	s.NoError(err)
	s.NotZero(len(task.queryInfos))
	s.Equal(false, task.queryInfos[0].MaterializedViewInvolved)
}

func (s *MaterializedViewTestSuite) TestMvEnabledPartitionKeyOnInt64() {
	task := s.getSearchTask()
	task.enableMaterializedView = true
	task.request.Dsl = testInt64Field + " == 1"
	schema := ConstructCollectionSchemaWithPartitionKey(s.colName, s.fieldName2Types, testInt64Field, testInt64Field, false)
	schemaInfo := newSchemaInfo(schema)
	s.mockMetaCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schemaInfo, nil)
	s.mockMetaCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).Return([]string{"partition_1", "partition_2"}, nil)
	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{"partition_1": 1, "partition_2": 2}, nil)

	err := task.PreExecute(s.ctx)
	s.NoError(err)
	s.NotZero(len(task.queryInfos))
	s.Equal(true, task.queryInfos[0].MaterializedViewInvolved)
}

func (s *MaterializedViewTestSuite) TestMvEnabledPartitionKeyOnVarChar() {
	task := s.getSearchTask()
	task.enableMaterializedView = true
	task.request.Dsl = testVarCharField + " == \"a\""
	schema := ConstructCollectionSchemaWithPartitionKey(s.colName, s.fieldName2Types, testInt64Field, testVarCharField, false)
	schemaInfo := newSchemaInfo(schema)
	s.mockMetaCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schemaInfo, nil)
	s.mockMetaCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).Return([]string{"partition_1", "partition_2"}, nil)
	s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{"partition_1": 1, "partition_2": 2}, nil)

	err := task.PreExecute(s.ctx)
	s.NoError(err)
	s.NotZero(len(task.queryInfos))
	s.Equal(true, task.queryInfos[0].MaterializedViewInvolved)
}

func (s *MaterializedViewTestSuite) TestMvEnabledPartitionKeyOnVarCharWithIsolation() {
	isAdanceds := []bool{true, false}
	for _, isAdvanced := range isAdanceds {
		task := s.getSearchTask()
		task.enableMaterializedView = true
		task.request.Dsl = testVarCharField + " == \"a\""
		task.IsAdvanced = isAdvanced
		schema := ConstructCollectionSchemaWithPartitionKey(s.colName, s.fieldName2Types, testInt64Field, testVarCharField, false)
		schemaInfo := newSchemaInfo(schema)
		s.mockMetaCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schemaInfo, nil)
		s.mockMetaCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).Return([]string{"partition_1", "partition_2"}, nil)
		s.mockMetaCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{"partition_1": 1, "partition_2": 2}, nil)
		err := task.PreExecute(s.ctx)
		s.NoError(err)
		s.NotZero(len(task.queryInfos))
		s.Equal(true, task.queryInfos[0].MaterializedViewInvolved)
	}
}

func (s *MaterializedViewTestSuite) TestMvEnabledPartitionKeyOnVarCharWithIsolationInvalid() {
	isAdanceds := []bool{true, false}
	for _, isAdvanced := range isAdanceds {
		task := s.getSearchTask()
		task.enableMaterializedView = true
		task.IsAdvanced = isAdvanced
		task.request.Dsl = testVarCharField + " in [\"a\", \"b\"]"
		schema := ConstructCollectionSchemaWithPartitionKey(s.colName, s.fieldName2Types, testInt64Field, testVarCharField, false)
		schemaInfo := newSchemaInfo(schema)
		s.mockMetaCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schemaInfo, nil)
		s.ErrorContains(task.PreExecute(s.ctx), "partition key isolation does not support IN")
	}
}

func (s *MaterializedViewTestSuite) TestMvEnabledPartitionKeyOnVarCharWithIsolationInvalidOr() {
	isAdanceds := []bool{true, false}
	for _, isAdvanced := range isAdanceds {
		task := s.getSearchTask()
		task.enableMaterializedView = true
		task.IsAdvanced = isAdvanced
		task.request.Dsl = testVarCharField + " == \"a\" || " + testVarCharField + "  == \"b\""
		schema := ConstructCollectionSchemaWithPartitionKey(s.colName, s.fieldName2Types, testInt64Field, testVarCharField, false)
		schemaInfo := newSchemaInfo(schema)
		s.mockMetaCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schemaInfo, nil)
		s.ErrorContains(task.PreExecute(s.ctx), "partition key isolation does not support OR")
	}
}

func TestMaterializedView(t *testing.T) {
	suite.Run(t, new(MaterializedViewTestSuite))
}

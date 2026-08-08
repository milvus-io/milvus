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

package httpserver

import (
	"bytes"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func lazyRouteFieldData() []*schemapb.FieldData {
	return []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: FieldWordCount,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: []int64{10, 20}},
		}}},
	}}
}

func TestV2RowResponseRoutesUseLazyStreaming(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	})

	tests := []struct {
		name string
		path string
		body string
		mock func(*mocks.MockProxy)
	}{
		{
			name: "query",
			path: versionalV2(EntityCategory, QueryAction),
			body: `{"collectionName":"book","filter":"book_id > 0","outputFields":["word_count"]}`,
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
					Status:       commonSuccessStatus,
					OutputFields: []string{FieldWordCount},
					FieldsData:   lazyRouteFieldData(),
				}, nil).Once()
			},
		},
		{
			name: "get",
			path: versionalV2(EntityCategory, GetAction),
			body: `{"collectionName":"book","id":[1,2],"outputFields":["word_count"]}`,
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
					Status:       commonSuccessStatus,
					OutputFields: []string{FieldWordCount},
					FieldsData:   lazyRouteFieldData(),
				}, nil).Once()
			},
		},
		{
			name: "search",
			path: versionalV2(EntityCategory, SearchAction),
			body: `{"collectionName":"book","data":[[0.1,0.2]],"limit":2,"outputFields":["word_count"]}`,
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
					Status: commonSuccessStatus,
					Results: &schemapb.SearchResultData{
						TopK:         2,
						Topks:        []int64{2},
						OutputFields: []string{FieldWordCount},
						FieldsData:   lazyRouteFieldData(),
						Ids:          generateIDs(schemapb.DataType_Int64, 2),
						Scores:       []float32{0.1, 0.2},
					},
				}, nil).Once()
			},
		},
		{
			name: "hybrid search",
			path: versionalV2(EntityCategory, HybridSearchAction),
			body: `{
				"collectionName":"book",
				"search":[
					{"data":[[0.1,0.2]],"annsField":"book_intro","metricType":"L2","limit":2},
					{"data":[[0.2,0.1]],"annsField":"book_intro","metricType":"L2","limit":2}
				],
				"limit":2,
				"outputFields":["word_count"],
				"rerank":{"strategy":"rrf","params":{"k":60}}
			}`,
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().HybridSearch(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
					Status: commonSuccessStatus,
					Results: &schemapb.SearchResultData{
						TopK:         2,
						Topks:        []int64{2},
						OutputFields: []string{FieldWordCount},
						FieldsData:   lazyRouteFieldData(),
						Ids:          generateIDs(schemapb.DataType_Int64, 2),
						Scores:       []float32{0.1, 0.2},
					},
				}, nil).Once()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			test.mock(mp)
			engine := initHTTPServerV2(mp, false)

			request := httptest.NewRequest(http.MethodPost, test.path, bytes.NewBufferString(test.body))
			request.Header.Set(HTTPHeaderAllowInt64, "true")
			response := httptest.NewRecorder()
			engine.ServeHTTP(response, request)

			assert.Equal(t, http.StatusOK, response.Code)
			var body struct {
				Code  int32                    `json:"code"`
				Data  []map[string]interface{} `json:"data"`
				Topks []int64                  `json:"topks"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
			assert.Zero(t, body.Code, response.Body.String())
			require.Len(t, body.Data, 2)
			assert.EqualValues(t, 10, body.Data[0][FieldWordCount])
			assert.EqualValues(t, 20, body.Data[1][FieldWordCount])
			if test.name == "search" || test.name == "hybrid search" {
				assert.Equal(t, []int64{2}, body.Topks)
				assert.EqualValues(t, 1, body.Data[0][FieldBookID])
				assert.EqualValues(t, 0.1, body.Data[0][HTTPReturnDistance])
			}
		})
	}
}

func TestV2QueryPreservesAllNullTextRows(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	})

	mp := mocks.NewMockProxy(t)
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Once()
	mp.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status:       commonSuccessStatus,
		OutputFields: []string{"content"},
		FieldsData: []*schemapb.FieldData{{
			Type:      schemapb.DataType_Text,
			FieldName: "content",
			ValidData: []bool{false, false},
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{},
			}}},
		}},
	}, nil).Once()
	engine := initHTTPServerV2(mp, false)

	request := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, QueryAction), bytes.NewBufferString(
		`{"collectionName":"book","filter":"book_id > 0","outputFields":["content"]}`))
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	var body struct {
		Code int32                    `json:"code"`
		Data []map[string]interface{} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
	assert.Zero(t, body.Code)
	require.Len(t, body.Data, 2)
	assert.Nil(t, body.Data[0]["content"])
	assert.Nil(t, body.Data[1]["content"])
}

func TestV2SearchAcceptsEmptyRerankResult(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	})

	mp := mocks.NewMockProxy(t)
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Once()
	mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
		Status: commonSuccessStatus,
		Results: &schemapb.SearchResultData{
			NumQueries:   1,
			TopK:         10,
			Topks:        []int64{0},
			OutputFields: []string{FieldBookID},
			FieldsData: []*schemapb.FieldData{{
				Type:      schemapb.DataType_Int64,
				FieldName: FieldBookID,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{},
				}}},
			}},
			Ids:    &schemapb.IDs{},
			Scores: []float32{},
		},
	}, nil).Once()
	engine := initHTTPServerV2(mp, false)

	request := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, SearchAction), bytes.NewBufferString(
		`{"collectionName":"book","data":[[0.1,0.2]],"limit":10,"outputFields":["book_id"]}`))
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	var body struct {
		Code  int32                    `json:"code"`
		Data  []map[string]interface{} `json:"data"`
		Topks []int64                  `json:"topks"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
	assert.Zero(t, body.Code, response.Body.String())
	assert.Empty(t, body.Data)
	assert.Equal(t, []int64{0}, body.Topks)
}

func TestV2SearchRejectsInvalidRowsBeforeStreaming(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	})

	mp := mocks.NewMockProxy(t)
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Once()
	mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
		Status: commonSuccessStatus,
		Results: &schemapb.SearchResultData{
			TopK:         1,
			Topks:        []int64{1},
			OutputFields: []string{FieldWordCount},
			FieldsData: []*schemapb.FieldData{{
				Type:      schemapb.DataType_Float,
				FieldName: FieldWordCount,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{float32(math.NaN())}},
				}}},
			}},
			Ids:    generateIDs(schemapb.DataType_Int64, 1),
			Scores: []float32{0.1},
		},
	}, nil).Once()
	engine := initHTTPServerV2(mp, false)

	request := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, SearchAction), bytes.NewBufferString(
		`{"collectionName":"book","data":[[0.1,0.2]],"limit":1,"outputFields":["word_count"]}`))
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	var body ReturnErrMsg
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
	assert.Equal(t, merr.Code(merr.ErrInvalidSearchResult), body.Code)
	assert.Contains(t, body.Message, "non-finite")
	assert.NotContains(t, response.Body.String(), `"data":[`)
}

func TestV2SearchRejectsRowCountMismatchBeforeStreaming(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	})

	mp := mocks.NewMockProxy(t)
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Once()
	mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
		Status: commonSuccessStatus,
		Results: &schemapb.SearchResultData{
			TopK:         1,
			Topks:        []int64{1},
			OutputFields: []string{FieldWordCount},
			FieldsData: []*schemapb.FieldData{{
				Type:      schemapb.DataType_Int64,
				FieldName: FieldWordCount,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{},
				}}},
			}},
			Ids:    generateIDs(schemapb.DataType_Int64, 1),
			Scores: []float32{0.1},
		},
	}, nil).Once()
	engine := initHTTPServerV2(mp, false)

	request := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, SearchAction), bytes.NewBufferString(
		`{"collectionName":"book","data":[[0.1,0.2]],"limit":1,"outputFields":["word_count"]}`))
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	var body ReturnErrMsg
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
	assert.Equal(t, merr.Code(merr.ErrInvalidSearchResult), body.Code)
	assert.Contains(t, body.Message, "expected exactly 1")
	assert.NotContains(t, response.Body.String(), `"data":[`)
}

func TestV2ResultConversionFailuresRecordSystemFailureMetrics(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	})

	malformedFieldData := func() []*schemapb.FieldData {
		return []*schemapb.FieldData{{
			Type:      schemapb.DataType_Float,
			FieldName: FieldWordCount,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{Data: []float32{float32(math.NaN())}},
			}}},
		}}
	}
	malformedSearchResult := func() *schemapb.SearchResultData {
		return &schemapb.SearchResultData{
			TopK:         1,
			Topks:        []int64{1},
			OutputFields: []string{FieldWordCount},
			FieldsData:   malformedFieldData(),
			Ids:          generateIDs(schemapb.DataType_Int64, 1),
			Scores:       []float32{0.1},
		}
	}

	tests := []struct {
		name      string
		path      string
		body      string
		methodTag string
		mock      func(*mocks.MockProxy)
	}{
		{
			name:      "query",
			path:      versionalV2(EntityCategory, QueryAction),
			body:      `{"collectionName":"book","filter":"book_id > 0","outputFields":["word_count"]}`,
			methodTag: "Query",
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
					Status:       commonSuccessStatus,
					OutputFields: []string{FieldWordCount},
					FieldsData:   malformedFieldData(),
				}, nil).Once()
			},
		},
		{
			name:      "get",
			path:      versionalV2(EntityCategory, GetAction),
			body:      `{"collectionName":"book","id":[1],"outputFields":["word_count"]}`,
			methodTag: "Query",
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
					Status:       commonSuccessStatus,
					OutputFields: []string{FieldWordCount},
					FieldsData:   malformedFieldData(),
				}, nil).Once()
			},
		},
		{
			name:      "search",
			path:      versionalV2(EntityCategory, SearchAction),
			body:      `{"collectionName":"book","data":[[0.1,0.2]],"limit":1,"outputFields":["word_count"]}`,
			methodTag: "Search",
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
					Status:  commonSuccessStatus,
					Results: malformedSearchResult(),
				}, nil).Once()
			},
		},
		{
			name:      "search metadata render",
			path:      versionalV2(EntityCategory, SearchAction),
			body:      `{"collectionName":"book","data":[[0.1,0.2]],"limit":2,"outputFields":["word_count"]}`,
			methodTag: "Search",
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
					Status: commonSuccessStatus,
					Results: &schemapb.SearchResultData{
						TopK:         2,
						Topks:        []int64{2},
						OutputFields: []string{FieldWordCount},
						FieldsData:   lazyRouteFieldData(),
						Ids:          generateIDs(schemapb.DataType_Int64, 2),
						Scores:       []float32{0.1, 0.2},
						Recalls:      []float32{float32(math.NaN())},
					},
				}, nil).Once()
			},
		},
		{
			name:      "search aggregation",
			path:      versionalV2(EntityCategory, SearchAction),
			body:      `{"collectionName":"book","data":[[0.1,0.2]],"limit":1}`,
			methodTag: "Search",
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
					Status: commonSuccessStatus,
					Results: &schemapb.SearchResultData{
						AggTopks: []int64{1},
					},
				}, nil).Once()
			},
		},
		{
			name: "hybrid search",
			path: versionalV2(EntityCategory, HybridSearchAction),
			body: `{
				"collectionName":"book",
				"search":[
					{"data":[[0.1,0.2]],"annsField":"book_intro","metricType":"L2","limit":1},
					{"data":[[0.2,0.1]],"annsField":"book_intro","metricType":"L2","limit":1}
				],
				"limit":1,
				"outputFields":["word_count"],
				"rerank":{"strategy":"rrf","params":{"k":60}}
			}`,
			methodTag: "HybridSearch",
			mock: func(mp *mocks.MockProxy) {
				mp.EXPECT().HybridSearch(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
					Status:  commonSuccessStatus,
					Results: malformedSearchResult(),
				}, nil).Once()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			test.mock(mp)
			engine := initHTTPServerV2(mp, false)

			nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
			failCounter := metrics.ProxyFunctionCall.WithLabelValues(
				nodeID, test.methodTag, metrics.FailLabel, metrics.CauseSystem, DefaultDbName, DefaultCollectionName)
			successCounter := metrics.ProxyFunctionCall.WithLabelValues(
				nodeID, test.methodTag, metrics.SuccessLabel, metrics.CauseNA, DefaultDbName, DefaultCollectionName)
			failBefore := testutil.ToFloat64(failCounter)
			successBefore := testutil.ToFloat64(successCounter)

			request := httptest.NewRequest(http.MethodPost, test.path, bytes.NewBufferString(test.body))
			response := httptest.NewRecorder()
			engine.ServeHTTP(response, request)

			assert.Equal(t, http.StatusOK, response.Code)
			var responseBody ReturnErrMsg
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &responseBody), response.Body.String())
			assert.Equal(t, merr.Code(merr.ErrInvalidSearchResult), responseBody.Code)
			assert.Equal(t, failBefore+1, testutil.ToFloat64(failCounter))
			assert.Equal(t, successBefore, testutil.ToFloat64(successCounter))
		})
	}
}

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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestResponseFormatDispatch(t *testing.T) {
	paramtable.Init()
	quota := paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled
	require.NoError(t, paramtable.Get().Save(quota.Key, "false"))
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Reset(quota.Key)) })

	for _, version := range []string{"v1", "v2"} {
		actions := []string{QueryAction, GetAction, SearchAction}
		if version == "v2" {
			actions = append(actions, AdvancedSearchAction, HybridSearchAction, "aggregation")
		}
		for _, action := range actions {
			for _, format := range []string{"", responseFormatLegacy, responseFormatNamespaced, "\tlegacy \n", " namespaced\t"} {
				enabled := version == "v2" && (format == responseFormatNamespaced || format == " namespaced\t")
				if action == "aggregation" && enabled {
					continue // The unsupported combination is covered by TestNamespacedSearchAggregationRejected.
				}
				t.Run(version+"/"+action+"/"+format, func(t *testing.T) {
					var calls atomic.Int64
					var original func() resultRow
					patch := mockey.Mock(newNamespacedResultRow).Origin(&original).To(func() resultRow {
						calls.Add(1)
						return original()
					}).Build()
					defer patch.UnPatch()

					mp := mocks.NewMockProxy(t)
					schema := generateCollectionSchema(schemapb.DataType_Int64, false, true)
					mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
						CollectionName: DefaultCollectionName, Schema: schema, Status: merr.Success(),
					}, nil).Maybe()
					ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
					distance := &schemapb.FieldData{
						FieldName: "distance", Type: schemapb.DataType_VarChar,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"stored"}},
						}}},
					}
					vector := &schemapb.FieldData{
						FieldName: "vec", Type: schemapb.DataType_FloatVector,
						Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
							Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, -2}}},
						}},
					}
					query := &milvuspb.QueryResults{Status: merr.Success(), FieldsData: []*schemapb.FieldData{
						{FieldName: FieldBookID, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: ids.GetIntId()}},
						}}, distance, vector,
					}, ElementIndices: []*milvuspb.ElementIndices{{Indices: &schemapb.LongArray{Data: []int64{2}}}}}
					search := &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{
						TopK: 1, NumQueries: 1, Topks: []int64{1}, Ids: ids, Scores: []float32{0.25},
						FieldsData: []*schemapb.FieldData{distance, vector}, ElementIndices: &schemapb.LongArray{Data: []int64{2}},
					}}
					body := map[string]interface{}{"collectionName": DefaultCollectionName, "outputFields": []string{FieldBookID, "distance", "vec"}, "limit": 1}
					if format != "" {
						body["responseFormat"] = format
					}
					route := action
					switch action {
					case QueryAction:
						body["filter"] = FieldBookID + " >= 0"
						mp.EXPECT().Query(mock.Anything, mock.Anything).Return(query, nil).Once()
					case GetAction:
						body["id"] = []int64{1}
						mp.EXPECT().Query(mock.Anything, mock.Anything).Return(query, nil).Once()
					case AdvancedSearchAction, HybridSearchAction:
						body["search"] = []interface{}{map[string]interface{}{"data": [][]float32{{0.1, 0.2}}, "annsField": FieldBookIntro, "limit": 1}}
						body["rerank"] = map[string]interface{}{"strategy": "rrf", "params": map[string]interface{}{"k": 60}}
						mp.EXPECT().HybridSearch(mock.Anything, mock.Anything).Return(search, nil).Once()
					default:
						body["data"], body["vector"], body["annsField"] = [][]float32{{0.1, 0.2}}, []float32{0.1, 0.2}, FieldBookIntro
						if action == "aggregation" {
							route = SearchAction
							body["searchAggregation"] = map[string]interface{}{"fields": []string{FieldBookID}, "size": 1, "topHits": map[string]interface{}{"size": 1}}
							search.Results = &schemapb.SearchResultData{NumQueries: 1, AggTopks: []int64{1}, AggBuckets: []*schemapb.AggBucket{{
								Count: 1, Hits: []*schemapb.AggHit{{
									Pk: &schemapb.AggHit_IntPk{IntPk: 1}, Score: 0.25,
									Fields: []*schemapb.AggHitField{{FieldName: "distance", Value: &schemapb.AggHitField_StringVal{StringVal: "stored"}}},
								}},
							}}}
						}
						mp.EXPECT().Search(mock.Anything, mock.Anything).Return(search, nil).Once()
					}
					var engine http.Handler
					path := versionalV2(EntityCategory, route)
					if version == "v1" {
						engine, path = initHTTPServer(mp, false), versional("/vector/"+route)
					} else {
						engine = initHTTPServerV2(mp, false)
					}
					encoded, err := json.Marshal(body)
					require.NoError(t, err)
					req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded))
					req.Header.Set(HTTPHeaderAllowInt64, "true")
					// Rebased vector encoding must reach the shared builder in both formats.
					req.Header.Set(HTTPHeaderFloatVectorEncoding, "base64")
					response := httptest.NewRecorder()
					engine.ServeHTTP(response, req)
					require.Equal(t, http.StatusOK, response.Code)
					var decoded map[string]interface{}
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
					wantCode := float64(0)
					if version == "v1" {
						wantCode = http.StatusOK
					}
					require.Equal(t, wantCode, decoded[HTTPReturnCode], response.Body.String())
					rows := decoded[HTTPReturnData].([]interface{})
					require.Len(t, rows, 1)
					hit := rows[0].(map[string]interface{})
					if action == "aggregation" {
						hit = hit["buckets"].([]interface{})[0].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})
					}
					if action != "aggregation" {
						fields := hit
						if enabled {
							fields = hit["fields"].(map[string]interface{})
						}
						if version == "v1" {
							require.Equal(t, []interface{}{float64(1), float64(-2)}, fields["vec"])
						} else {
							require.Equal(t, "AACAPwAAAMA=", fields["vec"])
						}
					}
					if enabled {
						require.Equal(t, int64(1), calls.Load())
						require.Len(t, hit, 3)
						require.Contains(t, hit, "dynamicFields")
						require.NotContains(t, hit, "meta")
						require.Equal(t, "stored", hit["fields"].(map[string]interface{})["distance"])
						hit = hit["system"].(map[string]interface{})
					} else {
						require.Zero(t, calls.Load(), "disabled requests must not enter the namespaced renderer")
						require.NotContains(t, hit, "fields")
						require.NotContains(t, hit, "system")
					}
					if action == QueryAction || action == GetAction || action == "aggregation" {
						if !enabled {
							require.Equal(t, "stored", hit["distance"])
						}
					} else {
						require.Equal(t, 0.25, hit["distance"])
					}
					if version == "v1" || action == GetAction || action == "aggregation" {
						require.NotContains(t, hit, "offset")
					} else if action == QueryAction {
						require.Equal(t, []interface{}{float64(2)}, hit["offset"])
					} else {
						require.Equal(t, float64(2), hit["offset"])
					}
				})
			}
		}
	}
}

func TestInvalidResponseFormatRejectedBeforeRPC(t *testing.T) {
	paramtable.Init()
	for _, action := range []string{QueryAction, GetAction, SearchAction, AdvancedSearchAction, HybridSearchAction} {
		t.Run(action, func(t *testing.T) {
			mp := mocks.NewMockProxy(t)
			body := map[string]interface{}{
				"collectionName": DefaultCollectionName, "responseFormat": "unknown",
			}
			switch action {
			case QueryAction:
				body["filter"] = FieldBookID + " >= 0"
			case GetAction:
				body["id"] = []int64{1}
			case SearchAction:
				body["data"], body["annsField"] = [][]float32{{0.1, 0.2}}, FieldBookIntro
			default:
				body["search"] = []interface{}{map[string]interface{}{"data": [][]float32{{0.1, 0.2}}, "annsField": FieldBookIntro, "limit": 1}}
				body["rerank"] = map[string]interface{}{"strategy": "rrf", "params": map[string]interface{}{"k": 60}}
			}
			encoded, err := json.Marshal(body)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, action), bytes.NewReader(encoded))
			response := httptest.NewRecorder()
			initHTTPServerV2(mp, false).ServeHTTP(response, req)
			require.Equal(t, http.StatusOK, response.Code)
			var decoded map[string]interface{}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
			require.EqualValues(t, merr.Code(merr.ErrParameterInvalid), decoded[HTTPReturnCode], response.Body.String())
			require.Contains(t, decoded[HTTPReturnMessage], "responseFormat must be legacy or namespaced")
			require.NotContains(t, decoded, HTTPReturnData)
			for _, method := range []string{"DescribeCollection", "Query", "Search", "HybridSearch"} {
				mp.AssertNotCalled(t, method, mock.Anything, mock.Anything)
			}
		})
	}
}

func TestNamespacedSearchAggregationRejected(t *testing.T) {
	paramtable.Init()
	for _, format := range []string{responseFormatNamespaced, " namespaced\t"} {
		for _, tc := range []struct {
			name        string
			aggregation string
		}{
			{"without top hits", `{"fields":["brand"],"size":1}`},
			{"nested top hits", `{"fields":["brand"],"size":1,"topHits":{"size":1},"subAggregation":{"fields":["color"],"size":1,"topHits":{"size":1}}}`},
			{"empty aggregation", `{}`},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				mp := mocks.NewMockProxy(t)
				body, err := json.Marshal(map[string]interface{}{
					"collectionName":    DefaultCollectionName,
					"data":              [][]float32{{0.1, 0.2}},
					"annsField":         FieldBookIntro,
					"responseFormat":    format,
					"searchAggregation": json.RawMessage(tc.aggregation),
				})
				require.NoError(t, err)
				req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, SearchAction), bytes.NewReader(body))
				response := httptest.NewRecorder()
				initHTTPServerV2(mp, false).ServeHTTP(response, req)
				require.Equal(t, http.StatusOK, response.Code)
				var decoded map[string]interface{}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
				require.EqualValues(t, merr.Code(merr.ErrParameterInvalid), decoded[HTTPReturnCode], response.Body.String())
				require.Contains(t, decoded[HTTPReturnMessage], "responseFormat namespaced is not supported with searchAggregation")
				require.NotContains(t, decoded, HTTPReturnData)
				mp.AssertNotCalled(t, "DescribeCollection", mock.Anything, mock.Anything)
				mp.AssertNotCalled(t, "Search", mock.Anything, mock.Anything)
			})
		}
	}
}

func TestResponseFormatParsing(t *testing.T) {
	for _, tc := range []struct {
		value      string
		namespaced bool
		invalid    bool
	}{
		{"", false, false},
		{" \t\r\n", false, false},
		{"legacy", false, false},
		{"\tlegacy \n", false, false},
		{"namespaced", true, false},
		{" namespaced\t", true, false},
		{"\u2003namespaced\u2003", true, false},
		{"NAMESPACED", false, true},
		{" Namespaced ", false, true},
		{" LEGACY ", false, true},
		{"name spaced", false, true},
		{" unknown ", false, true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			namespaced, err := useNamespacedResponse(tc.value)
			if tc.invalid {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.namespaced, namespaced)
		})
	}
}

// The shared traversal must retain column order, nullable values, and
// metadata precedence for ordinary query and search results.
func TestSharedResultTraversalWriteOrder(t *testing.T) {
	paramtable.Init()
	static := &schemapb.FieldData{
		FieldName: "distance", Type: schemapb.DataType_VarChar,
		ValidData: []bool{true, false}, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"static"}}},
		}},
	}
	dynamic := jsonRespFieldData(`{"distance":"dynamic","offset":"stored","id":99}`, `{"distance":"dynamic","offset":"stored","id":99}`)
	dynamic.FieldName, dynamic.IsDynamic = "$meta", true
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}}
	for _, dynamicLast := range []bool{false, true} {
		fields := []*schemapb.FieldData{dynamic, static}
		if dynamicLast {
			fields = []*schemapb.FieldData{static, dynamic}
		}
		for _, namespaced := range []bool{false, true} {
			for _, withScores := range []bool{false, true} {
				var scores []float32
				if withScores {
					scores = []float32{0.25, 0.5}
				}
				rows, err := buildQueryRespWithFormat(0, []string{"distance", "$meta"}, fields, ids, scores, true, nil, namespaced, []string{"*"}, "")
				require.NoError(t, err)
				for i, row := range rows {
					var staticValue interface{}
					if i == 0 {
						staticValue = "static"
					}
					if namespaced {
						require.Equal(t, map[string]interface{}{"distance": staticValue, "id": int64(i + 1)}, row["fields"])
						require.Equal(t, map[string]interface{}{"distance": "dynamic", "offset": "stored", "id": json.Number("99")}, row["dynamicFields"])
						system := map[string]interface{}{}
						if withScores {
							system["distance"] = scores[i]
						}
						require.Equal(t, system, row["system"])
					} else {
						distance := staticValue
						if dynamicLast {
							distance = "dynamic"
						}
						if withScores {
							distance = scores[i]
						}
						require.Equal(t, map[string]interface{}{"distance": distance, "offset": "stored", "id": int64(i + 1)}, row)
					}
				}
				if !namespaced {
					require.NoError(t, setSearchElementOffsets(rows, &schemapb.LongArray{Data: []int64{3, 4}}))
					require.Equal(t, int64(3), rows[0]["offset"])
					require.Equal(t, int64(4), rows[1]["offset"])
				}
			}
		}
	}
}

func TestSharedResultTraversalErrorOrder(t *testing.T) {
	paramtable.Init()
	dynamic := jsonRespFieldData(`{"bad":`)
	dynamic.FieldName, dynamic.IsDynamic = "$meta", true
	fields := []*schemapb.FieldData{dynamic}
	offsets := &schemapb.LongArray{Data: []int64{0, 1}}
	_, legacy := buildQueryResp(0, []string{"$meta"}, fields, nil, nil, true, nil)
	require.Error(t, legacy)
	for _, namespaced := range []bool{false, true} {
		_, err := buildQueryRespWithMetadata(namespaced, []string{"$meta"}, fields, queryResultMetadata{searchOffsets: offsets}, true, nil, []string{"*"}, "")
		require.EqualError(t, err, legacy.Error(), "field decode errors must precede offset-count errors")
		require.Equal(t, resultErrMessage(legacy), resultErrMessage(err))
	}
}

func TestNamespacedStructArrayVectorEncoding(t *testing.T) {
	paramtable.Init()
	schema := buildElementNullableStructTestSchema(false)
	field := elementNullStructOutput([]bool{true, false, false})
	rows, err := buildQueryRespWithMetadata(true, []string{"clips"}, []*schemapb.FieldData{field},
		queryResultMetadata{}, true, schema, []string{"clips"}, "base64")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	encoded, err := json.Marshal(rows[0])
	require.NoError(t, err)
	require.JSONEq(t, `{
		"fields": {"clips": [
			{"emb": null, "lang": "en", "score": 1, "tags": ["a", null]},
			{"emb": "AAAAPwAAgD4=", "lang": null, "score": 2, "tags": null},
			{"emb": null, "lang": null, "score": 3, "tags": []}
		]},
		"dynamicFields": {}, "system": {}
	}`, string(encoded))
}

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
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func restTestFloatRow(values ...float32) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: values}}}
}

func restTestDoubleRow(values ...float64) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: values}}}
}

func restTestArrayRow(rows ...*schemapb.ScalarField) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: rows, ElementType: schemapb.DataType_Double}}}
}

func restTestScalarField(name string, dataType schemapb.DataType, row *schemapb.ScalarField) *schemapb.FieldData {
	return &schemapb.FieldData{FieldName: name, Type: dataType, Field: &schemapb.FieldData_Scalars{Scalars: row}}
}

func restTestNonFiniteFields() []*schemapb.FieldData {
	return []*schemapb.FieldData{
		restTestScalarField("f_nan", schemapb.DataType_Float, restTestFloatRow(float32(math.NaN()))),
		restTestScalarField("d_inf", schemapb.DataType_Double, restTestDoubleRow(math.Inf(1))),
		restTestScalarField("d_ninf", schemapb.DataType_Double, restTestDoubleRow(math.Inf(-1))),
	}
}

func restTestRender(t *testing.T, payload gin.H, stream, timeout bool) string {
	t.Helper()
	router := gin.New()
	var renderErr error
	var initiallyAborted bool
	var aborted bool
	handler := func(c *gin.Context) {
		initiallyAborted = c.IsAborted()
		if stream {
			HTTPReturnStream(c, http.StatusOK, payload)
		} else {
			HTTPReturn(c, http.StatusOK, payload)
		}
		if last := c.Errors.Last(); last != nil {
			renderErr = last.Err
		}
		aborted = c.IsAborted()
	}
	if timeout {
		router.GET("/result", timeoutMiddleware(handler))
	} else {
		router.GET("/result", handler)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/result", nil))
	require.NoError(t, renderErr)
	// Gin Copy initializes the timeout handler's context as aborted.
	// Rendering must preserve the state it received and produce valid JSON.
	require.Equal(t, initiallyAborted, aborted)
	require.Equal(t, http.StatusOK, w.Code)
	require.NotEmpty(t, w.Body.Bytes())
	require.True(t, json.Valid(w.Body.Bytes()), w.Body.String())
	return w.Body.String()
}

func TestRESTNonFiniteOutputRenderers(t *testing.T) {
	fields := restTestNonFiniteFields()
	fields = append(fields,
		restTestScalarField("finite", schemapb.DataType_Double, restTestDoubleRow(1.5)),
		restTestScalarField("zero", schemapb.DataType_Float, restTestFloatRow(float32(math.Copysign(0, -1)))),
		restTestScalarField("null", schemapb.DataType_Double, restTestDoubleRow(math.NaN())),
		restTestScalarField("arr", schemapb.DataType_Array, restTestArrayRow(restTestFloatRow(1.5, float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))))),
		restTestScalarField("nested", schemapb.DataType_Array, restTestArrayRow(restTestArrayRow(restTestDoubleRow(math.NaN(), 2.5), restTestDoubleRow()))),
		&schemapb.FieldData{FieldName: "vector", Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}}}},
	)
	fields[5].GetScalars().ValidData = []bool{false}
	fields = append(fields, restTestScalarField("doc", schemapb.DataType_JSON, &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{"label":"NaN","amount":9007199254740993}`)}}}}))
	for _, nativeJSON := range []string{"false", "true"} {
		params := paramtable.Get()
		params.Save(params.HTTPCfg.NativeJSONResponse.Key, nativeJSON)
		defer params.Reset(params.HTTPCfg.NativeJSONResponse.Key)
		for _, legacyArray := range []string{"false", "true"} {
			params.Save(params.HTTPCfg.LegacyArrayResponse.Key, legacyArray)
			defer params.Reset(params.HTTPCfg.LegacyArrayResponse.Key)
			rows, err := buildQueryResp(0, nil, fields, nil, nil, true, nil)
			require.NoError(t, err)
			for _, stream := range []bool{false, true} {
				for _, timeout := range []bool{false, true} {
					body := restTestRender(t, gin.H{"code": 0, "data": rows}, stream, timeout)
					require.Equal(t, "NaN", gjson.Get(body, "data.0.f_nan").String())
					require.Equal(t, "Infinity", gjson.Get(body, "data.0.d_inf").String())
					require.Equal(t, "-Infinity", gjson.Get(body, "data.0.d_ninf").String())
					require.Equal(t, gjson.Number, gjson.Get(body, "data.0.finite").Type)
					require.Equal(t, 1.5, gjson.Get(body, "data.0.finite").Float())
					require.Equal(t, gjson.Number, gjson.Get(body, "data.0.zero").Type)
					require.Equal(t, "null", gjson.Get(body, "data.0.null").Raw)
					require.Equal(t, "[1,2]", gjson.Get(body, "data.0.vector").Raw)
					if nativeJSON == "true" {
						require.Equal(t, "NaN", gjson.Get(body, "data.0.doc.label").String())
						require.Equal(t, "9007199254740993", gjson.Get(body, "data.0.doc.amount").Raw)
					} else {
						require.Equal(t, `{"label":"NaN","amount":9007199254740993}`, gjson.Get(body, "data.0.doc").String())
					}
					if legacyArray == "true" {
						require.JSONEq(t, `[1.5,"NaN","Infinity","-Infinity"]`, gjson.Get(body, "data.0.arr.Data.FloatData.data").Raw)
						require.Equal(t, "NaN", gjson.Get(body, "data.0.nested.Data.ArrayData.data.0.Data.DoubleData.data.0").String())
					} else {
						require.JSONEq(t, `[1.5,"NaN","Infinity","-Infinity"]`, gjson.Get(body, "data.0.arr").Raw)
						require.JSONEq(t, `[["NaN",2.5],[]]`, gjson.Get(body, "data.0.nested").Raw)
					}
				}
			}
		}
	}
	require.True(t, math.IsNaN(float64(fields[0].GetScalars().GetFloatData().GetData()[0])))
}

func TestRESTNonFiniteOutputStructArraysAndAggregation(t *testing.T) {
	sub := restTestScalarField("items[score]", schemapb.DataType_Array, restTestArrayRow(restTestDoubleRow(math.NaN(), math.Inf(1), math.Inf(-1), 2.5)))
	field := &schemapb.FieldData{FieldName: "items", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{sub}}}}
	rows, err := buildQueryResp(0, nil, []*schemapb.FieldData{field}, nil, nil, true, nil)
	require.NoError(t, err)
	body := restTestRender(t, gin.H{"code": 0, "data": rows}, true, true)
	require.JSONEq(t, `[{"score":"NaN"},{"score":"Infinity"},{"score":"-Infinity"},{"score":2.5}]`, gjson.Get(body, "data.0.items").Raw)

	nestedSub := restTestScalarField("items[scores]", schemapb.DataType_Array, restTestArrayRow(restTestArrayRow(restTestDoubleRow(math.NaN(), 2.5), restTestDoubleRow(math.Inf(1)))))
	field.GetStructArrays().Fields = []*schemapb.FieldData{nestedSub}
	rows, err = buildQueryResp(0, nil, []*schemapb.FieldData{field}, nil, nil, true, nil)
	require.NoError(t, err)
	body = restTestRender(t, gin.H{"code": 0, "data": rows}, true, true)
	require.JSONEq(t, `[{"scores":["NaN",2.5]},{"scores":["Infinity"]}]`, gjson.Get(body, "data.0.items").Raw)

	agg, err := buildSearchAggregationResp(&schemapb.SearchResultData{NumQueries: 1, AggTopks: []int64{1}, AggBuckets: []*schemapb.AggBucket{{
		Count: 1, Metrics: map[string]*schemapb.MetricValue{"min": {Value: &schemapb.MetricValue_DoubleVal{DoubleVal: math.NaN()}}, "max": {Value: &schemapb.MetricValue_DoubleVal{DoubleVal: math.Inf(1)}}},
		Hits: []*schemapb.AggHit{{Pk: &schemapb.AggHit_IntPk{IntPk: 1}, Score: 0.5, Fields: []*schemapb.AggHitField{{FieldName: "f", Value: &schemapb.AggHitField_FloatVal{FloatVal: float32(math.NaN())}}, {FieldName: "d", Value: &schemapb.AggHitField_DoubleVal{DoubleVal: math.Inf(-1)}}}}},
	}}}, true, nil)
	require.NoError(t, err)
	body = restTestRender(t, gin.H{"code": 0, "data": agg}, true, true)
	require.Equal(t, "NaN", gjson.Get(body, "data.0.buckets.0.metrics.min").String())
	require.Equal(t, "Infinity", gjson.Get(body, "data.0.buckets.0.metrics.max").String())
	require.Equal(t, "NaN", gjson.Get(body, "data.0.buckets.0.hits.0.f").String())
	require.Equal(t, "-Infinity", gjson.Get(body, "data.0.buckets.0.hits.0.d").String())
}

func TestRESTNonFiniteOutputHandlers(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer params.Reset(params.QuotaConfig.QuotaAndLimitsEnabled.Key)
	for _, version := range []string{"v1", "v2"} {
		for _, action := range []string{QueryAction, GetAction, SearchAction} {
			t.Run(version+"/"+action, func(t *testing.T) {
				mp := mocks.NewMockProxy(t)
				schema := generateCollectionSchema(schemapb.DataType_Int64, false, true)
				schema.Fields = append(schema.Fields,
					&schemapb.FieldSchema{Name: "f_nan", FieldID: 200, DataType: schemapb.DataType_Float},
					&schemapb.FieldSchema{Name: "d_inf", FieldID: 201, DataType: schemapb.DataType_Double},
					&schemapb.FieldSchema{Name: "d_ninf", FieldID: 202, DataType: schemapb.DataType_Double})
				mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{Status: merr.Success(), Schema: schema, CollectionName: DefaultCollectionName}, nil).Maybe()
				if action == SearchAction {
					mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{TopK: 1, NumQueries: 1, Topks: []int64{1}, FieldsData: restTestNonFiniteFields(), OutputFields: []string{"f_nan", "d_inf", "d_ninf"}, Ids: generateIDs(schemapb.DataType_Int64, 1), Scores: []float32{0.5}}}, nil).Once()
				} else {
					mp.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{Status: merr.Success(), FieldsData: restTestNonFiniteFields(), OutputFields: []string{"f_nan", "d_inf", "d_ninf"}}, nil).Once()
				}
				path, request := "", `{"collectionName":"book","filter":"book_id > 0","outputFields":["f_nan","d_inf","d_ninf"]}`
				switch action {
				case GetAction:
					request = `{"collectionName":"book","id":[1],"outputFields":["f_nan","d_inf","d_ninf"]}`
				case SearchAction:
					request = `{"collectionName":"book","data":[[0.1,0.2]],"vector":[0.1,0.2],"limit":1,"outputFields":["f_nan","d_inf","d_ninf"]}`
				}
				var router *gin.Engine
				if version == "v2" {
					router, path = initHTTPServerV2(mp, false), versionalV2(EntityCategory, action)
				} else {
					router = initHTTPServer(mp, false)
					switch action {
					case QueryAction:
						path = versional(VectorQueryPath)
					case GetAction:
						path = versional(VectorGetPath)
					case SearchAction:
						path = versional(VectorSearchPath)
					}
				}
				w := httptest.NewRecorder()
				router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(request)))
				require.Equal(t, http.StatusOK, w.Code)
				require.True(t, json.Valid(w.Body.Bytes()), w.Body.String())
				require.Equal(t, "NaN", gjson.Get(w.Body.String(), "data.0.f_nan").String(), w.Body.String())
				require.Equal(t, "Infinity", gjson.Get(w.Body.String(), "data.0.d_inf").String())
				require.Equal(t, "-Infinity", gjson.Get(w.Body.String(), "data.0.d_ninf").String())
			})
		}
	}
}

func TestRESTNonFiniteOutputLegacyShape(t *testing.T) {
	floatField := restTestScalarField("f", schemapb.DataType_Float, restTestFloatRow(float32(math.NaN()), 1.5))
	floatField.FieldId, floatField.ValidData = 9007199254740993, []bool{true, false}
	floatField.GetScalars().ValidData = []bool{true, false}
	for _, result := range []any{
		&milvuspb.QueryResults{FieldsData: []*schemapb.FieldData{floatField}, CollectionName: "book", SessionTs: 9007199254740993},
		&milvuspb.SearchResults{Results: &schemapb.SearchResultData{NumQueries: 1, TopK: 2, FieldsData: []*schemapb.FieldData{floatField}, Scores: []float32{1, 2}}},
	} {
		router := gin.New()
		router.POST("/result", wrapHandler(func(c *gin.Context) (any, error) { return result, nil }))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/result", nil))
		require.Equal(t, http.StatusOK, w.Code)
		require.True(t, json.Valid(w.Body.Bytes()), w.Body.String())
		path := "fields_data.0"
		if _, search := result.(*milvuspb.SearchResults); search {
			path = "results." + path
			require.False(t, gjson.Get(w.Body.String(), "results.group_by_field_value").Exists())
			require.False(t, gjson.Get(w.Body.String(), "results.group_by_field_values").Exists())
		}
		require.Equal(t, "9007199254740993", gjson.Get(w.Body.String(), path+".field_id").Raw)
		require.Equal(t, "10", gjson.Get(w.Body.String(), path+".type").Raw)
		require.Equal(t, "[true,false]", gjson.Get(w.Body.String(), path+".valid_data").Raw)
		require.Equal(t, "[true,false]", gjson.Get(w.Body.String(), path+".Field.Scalars.valid_data").Raw)
		require.JSONEq(t, `["NaN",1.5]`, gjson.Get(w.Body.String(), path+".Field.Scalars.Data.FloatData.data").Raw)
	}
	finite := &milvuspb.QueryResults{FieldsData: []*schemapb.FieldData{restTestScalarField("f", schemapb.DataType_Float, restTestFloatRow(1.5, float32(math.Copysign(0, -1))))}}
	require.Same(t, finite, restLegacyResponse(finite))
	before, err := json.Marshal(finite)
	require.NoError(t, err)
	after, err := json.Marshal(restLegacyResponse(finite))
	require.NoError(t, err)
	require.Equal(t, string(before), string(after))
	emptyFields := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{GroupByFieldValue: floatField}}
	body, err := json.Marshal(restLegacyResponse(emptyFields))
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(body, "results.fields_data").Exists())
	require.False(t, gjson.GetBytes(body, "results.group_by_field_values").Exists())
}

func TestRESTNonFiniteOutputDefaultsAndFiniteStorage(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		field := &schemapb.FieldSchema{Name: "f", DataType: schemapb.DataType_Double, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_DoubleData{DoubleData: value}}}
		body := restTestRender(t, gin.H{"code": 0, "data": printFieldsV2([]*schemapb.FieldSchema{field})}, false, false)
		text := gjson.Get(body, "data.0.defaultValue.Data.DoubleData").String()
		parsed, err := cast.ToFloat64E(text)
		require.NoError(t, err)
		require.Equal(t, math.IsNaN(value), math.IsNaN(parsed))
		if !math.IsNaN(value) {
			require.Equal(t, value, parsed)
		}
		response := &milvuspb.DescribeCollectionResponse{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}}
		router := gin.New()
		router.GET("/collection", wrapHandler(func(c *gin.Context) (any, error) { return response, nil }))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/collection", nil))
		require.Equal(t, http.StatusOK, w.Code)
		require.True(t, json.Valid(w.Body.Bytes()), w.Body.String())
		require.Equal(t, text, gjson.Get(w.Body.String(), "schema.fields.0.default_value.Data.DoubleData").String())
	}
	finite := []float32{1.5, float32(math.Copysign(0, -1))}
	converted, changed := restFloatSlice(finite)
	require.False(t, changed)
	got := converted.([]float32)
	require.Same(t, &finite[0], &got[0])
	require.True(t, math.Signbit(float64(got[1])))
}

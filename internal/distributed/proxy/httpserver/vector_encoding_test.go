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
	"context"
	"encoding"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBase64FloatVectorAllocationBudget(t *testing.T) {
	values := make([]float32, 768)
	var vectorEncodingBuffer []byte
	vector := floatVectorEncoding("base64").floatVector(values, &vectorEncodingBuffer).(encoding.TextMarshaler)
	allocs := testing.AllocsPerRun(100, func() {
		text, err := vector.MarshalText()
		if err != nil || len(text) != 4096 {
			t.Fatal("invalid encoded vector", err)
		}
	})
	require.Zero(t, allocs, "encoding another vector must not allocate another temporary byte buffer")
}

func TestBase64FloatVectorWireFormat(t *testing.T) {
	for _, marshal := range []func(any) ([]byte, error){json.Marshal, sonic.ConfigStd.Marshal} {
		for _, tc := range []struct {
			input []float32
			want  string
		}{
			{nil, `null`},
			{[]float32{}, `""`},
			{[]float32{1}, `"AACAPw=="`},
			{[]float32{1, -2}, `"AACAPwAAAMA="`},
			{[]float32{1, -2, 0}, `"AACAPwAAAMAAAAAA"`},
		} {
			var vectorEncodingBuffer []byte
			encoded, err := marshal(floatVectorEncoding("base64").floatVector(tc.input, &vectorEncodingBuffer))
			require.NoError(t, err)
			require.Equal(t, tc.want, string(encoded))
		}
	}
}

func TestBase64FloatVectorPreservesFiniteBits(t *testing.T) {
	values := []float32{
		0, math.Float32frombits(0x80000000), math.SmallestNonzeroFloat32,
		-math.SmallestNonzeroFloat32, math.MaxFloat32, -math.MaxFloat32, 0.1,
	}
	var vectorEncodingBuffer []byte
	encoded, err := sonic.ConfigStd.Marshal(floatVectorEncoding("base64").floatVector(values, &vectorEncodingBuffer))
	require.NoError(t, err)
	var text string
	require.NoError(t, json.Unmarshal(encoded, &text))
	decoded, err := base64.StdEncoding.DecodeString(text)
	require.NoError(t, err)
	require.Len(t, decoded, 4*len(values))
	for i, value := range values {
		require.Equal(t, math.Float32bits(value), binary.LittleEndian.Uint32(decoded[i*4:]))
	}
}

func TestBase64FloatVectorRejectsNonfinite(t *testing.T) {
	for _, value := range []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))} {
		var vectorEncodingBuffer []byte
		_, err := sonic.ConfigStd.Marshal(floatVectorEncoding("base64").floatVector([]float32{1, value}, &vectorEncodingBuffer))
		require.Error(t, err, "Base64 must not silently make an invalid vector result encodable")
	}
}

func TestFloatVectorEncodingValues(t *testing.T) {
	encoding, err := parseFloatVectorEncoding(nil)
	require.NoError(t, err)
	require.Empty(t, encoding)
	for _, tc := range []struct{ input, want string }{
		{"", ""}, {"array", "array"}, {"base64", "base64"}, {" \tbase64\t ", "base64"}, {"\t ", ""},
	} {
		header := http.Header{}
		header.Set(HTTPHeaderFloatVectorEncoding, tc.input)
		encoding, err := parseFloatVectorEncoding(header)
		require.NoError(t, err)
		require.Equal(t, floatVectorEncoding(tc.want), encoding)
	}
}

func TestBase64FloatVectorMixedDimensions(t *testing.T) {
	for _, batchRows := range []int{1, 2, 256} {
		t.Run(fmt.Sprint(batchRows), func(t *testing.T) {
			t.Parallel()
			var vectorEncodingBuffer []byte
			vectors := [][]float32{
				nil,
				{},
				{1, -2},
				make([]float32, 768),
				{math.Float32frombits(0x80000000)},
				{3},
			}
			for i := range vectors[3] {
				vectors[3][i] = float32(i+batchRows) / 7919
			}
			rows := make([]map[string]interface{}, len(vectors))
			want := make([]map[string]interface{}, len(vectors))
			for i, values := range vectors {
				rows[i] = map[string]interface{}{
					"vector": floatVectorEncoding("base64").floatVector(values, &vectorEncodingBuffer),
					"nested": []map[string]interface{}{{"vector": floatVectorEncoding("base64").floatVector([]float32{float32(i)}, &vectorEncodingBuffer)}},
					"text":   "<>&",
				}
				var encoded any
				if values != nil {
					raw := make([]byte, len(values)*4)
					for j, value := range values {
						binary.LittleEndian.PutUint32(raw[j*4:], math.Float32bits(value))
					}
					encoded = base64.StdEncoding.EncodeToString(raw)
				}
				var raw [4]byte
				binary.LittleEndian.PutUint32(raw[:], math.Float32bits(float32(i)))
				want[i] = map[string]interface{}{
					"vector": encoded,
					"nested": []map[string]interface{}{{"vector": base64.StdEncoding.EncodeToString(raw[:])}},
					"text":   "<>&",
				}
			}
			result := map[string]interface{}{"code": 0, "data": rows}
			expected, err := json.Marshal(map[string]interface{}{"code": 0, "data": want})
			require.NoError(t, err)
			for repeat := 0; repeat < 3; repeat++ {
				for _, marshal := range []func(any) ([]byte, error){json.Marshal, sonic.ConfigStd.Marshal} {
					actual, err := marshal(result)
					require.NoError(t, err)
					require.Equal(t, string(expected), string(actual))
				}
				var actual bytes.Buffer
				require.NoError(t, writeDQLJSON(context.Background(), &actual, result, batchRows))
				require.Equal(t, string(expected)+"\n", actual.String())
			}
		})
	}
}

// Exercise binding, projection, the buffered/streamed boundary and the real
// Sonic renderer. Only the remote Proxy RPC is replaced.
func TestFloatVectorEncodingQuery(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key
	require.NoError(t, paramtable.Get().Save(key, "false"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	for _, count := range []int{1, 256, 257} {
		for _, encoding := range []string{"", "array", "base64"} {
			t.Run(fmt.Sprintf("%d/%s", count, encoding), func(t *testing.T) {
				mp := mocks.NewMockProxy(t)
				mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
					Status: &StatusSuccess, CollectionName: DefaultCollectionName,
					Schema: generateCollectionSchema(schemapb.DataType_Int64, false, true),
				}, nil)
				values := make([]float32, 0, count*2)
				elementIndices := make([]*milvuspb.ElementIndices, count)
				for i := 0; i < count; i++ {
					values = append(values, float32(i+1), -float32(i+2))
					elementIndices[i] = &milvuspb.ElementIndices{Indices: &schemapb.LongArray{Data: []int64{int64(i), int64(i + 1)}}}
				}
				mp.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
					Status: &StatusSuccess, OutputFields: []string{"vec"},
					ElementIndices: elementIndices,
					FieldsData: []*schemapb.FieldData{{
						Type: schemapb.DataType_FloatVector, FieldName: "vec",
						Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
							Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values}},
						}},
					}},
				}, nil)
				// Body properties from the unpublished version must not select the encoding.
				body := `{"collectionName":"book","filter":"id > 0","outputVectorEncoding":"base64","floatVectorEncoding":"base64"}`
				response := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/query", strings.NewReader(body))
				if encoding != "" {
					request.Header.Set(strings.ToLower(HTTPHeaderFloatVectorEncoding), encoding)
				}
				initHTTPServerV2(mp, false).ServeHTTP(response, request)
				require.Equal(t, http.StatusOK, response.Code)
				result := gjson.Parse(response.Body.String())
				require.EqualValues(t, 0, result.Get("code").Int(), response.Body.String())
				require.Len(t, result.Get("data").Array(), count)
				for i, row := range result.Get("data").Array() {
					require.Equal(t, fmt.Sprintf("[%d,%d]", i, i+1), row.Get(HTTPReturnOffset).Raw)
					if encoding == "base64" {
						want := base64.StdEncoding.EncodeToString(typeutil.Float32ArrayToBytes(values[i*2 : i*2+2]))
						require.Equal(t, want, row.Get("vec").String())
					} else {
						require.Equal(t, fmt.Sprintf("[%d,%d]", i+1, -i-2), row.Get("vec").Raw)
					}
				}
			})
		}
	}
}

func TestFloatVectorEncodingOtherDQLRoutes(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key
	require.NoError(t, paramtable.Get().Save(key, "false"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	for _, action := range []string{GetAction, SearchAction, AdvancedSearchAction, HybridSearchAction} {
		for _, encoding := range []string{"", "array", "base64"} {
			t.Run(fmt.Sprintf("%s/%s", action, encoding), func(t *testing.T) {
				mp := mocks.NewMockProxy(t)
				mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
					Status: &StatusSuccess, Schema: generateCollectionSchema(schemapb.DataType_Int64, false, true),
				}, nil).Once()
				fields := []*schemapb.FieldData{{
					Type: schemapb.DataType_FloatVector, FieldName: "vec",
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
						Dim:  2,
						Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, -2}}},
					}},
				}}
				body := gin.H{"collectionName": "book", "outputFields": []string{"vec"}}
				if action == GetAction {
					body["id"] = []int64{1}
					mp.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
						Status: &StatusSuccess, OutputFields: []string{"vec"}, FieldsData: fields,
					}, nil).Once()
				} else {
					result := &milvuspb.SearchResults{Status: &StatusSuccess, Results: &schemapb.SearchResultData{
						NumQueries: 1, TopK: 1, Topks: []int64{1}, OutputFields: []string{"vec"},
						FieldsData: fields, Scores: []float32{0.5},
						Ids:            &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
						ElementIndices: &schemapb.LongArray{Data: []int64{3}},
					}}
					if action == SearchAction {
						body["data"] = [][]float32{{0.1, 0.2}}
						mp.EXPECT().Search(mock.Anything, mock.Anything).Return(result, nil).Once()
					} else {
						body["search"] = []gin.H{{"data": [][]float32{{0.1, 0.2}}, "annsField": FieldBookIntro, "limit": 1}}
						body["rerank"] = gin.H{"strategy": "rrf", "params": gin.H{"k": 60}}
						mp.EXPECT().HybridSearch(mock.Anything, mock.Anything).Return(result, nil).Once()
					}
				}
				payload, err := json.Marshal(body)
				require.NoError(t, err)
				response := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, action), bytes.NewReader(payload))
				if encoding != "" {
					request.Header.Set(HTTPHeaderFloatVectorEncoding, encoding)
				}
				initHTTPServerV2(mp, false).ServeHTTP(response, request)
				result := gjson.Parse(response.Body.String())
				require.EqualValues(t, 0, result.Get("code").Int(), response.Body.String())
				if encoding == "base64" {
					require.Equal(t, `"AACAPwAAAMA="`, result.Get("data.0.vec").Raw)
				} else {
					require.Equal(t, `[1,-2]`, result.Get("data.0.vec").Raw)
				}
				if action != GetAction {
					require.Equal(t, 0.5, result.Get("data.0.distance").Float())
					require.Equal(t, "1", result.Get("data.0."+FieldBookID).String())
					require.EqualValues(t, 3, result.Get("data.0."+HTTPReturnOffset).Int())
				}
			})
		}
	}
}

func TestFloatVectorEncodingInvalidBeforeRPC(t *testing.T) {
	paramtable.Init()
	for _, action := range []string{QueryAction, GetAction, SearchAction, AdvancedSearchAction, HybridSearchAction} {
		for _, values := range [][]string{{"gzip"}, {"BASE64"}, {"null"}, {"123"}, {"array,base64"}, {"array", "base64"}, {"base64", "base64"}} {
			t.Run(action+"/"+strings.Join(values, "_"), func(t *testing.T) {
				// No Proxy calls, including schema lookup, may precede validation.
				mp := mocks.NewMockProxy(t)
				response := httptest.NewRecorder()
				body := `{"collectionName":"book","id":[1],"data":[[1,2]],"search":[{"data":[[1,2]],"annsField":"vec","limit":1}],"rerank":{"strategy":"rrf","params":{"k":60}}}`
				request := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, action), strings.NewReader(body))
				request.Header[http.CanonicalHeaderKey(HTTPHeaderFloatVectorEncoding)] = values
				initHTTPServerV2(mp, false).ServeHTTP(response, request)
				require.Equal(t, http.StatusOK, response.Code)
				require.EqualValues(t, merr.Code(merr.ErrParameterInvalid), gjson.Get(response.Body.String(), "code").Int(), response.Body.String())
				require.Contains(t, gjson.Get(response.Body.String(), "message").String(), HTTPHeaderFloatVectorEncoding)
			})
		}
	}
}

func TestFloatVectorEncodingCORS(t *testing.T) {
	paramtable.Init()
	router := gin.New()
	router.Use(RequestHandlerFunc)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodOptions, "/v2/vectordb/entities/query", nil)
	request.Header.Set("Origin", "https://example.com")
	request.Header.Set("Access-Control-Request-Method", http.MethodPost)
	request.Header.Set("Access-Control-Request-Headers", strings.ToLower(HTTPHeaderFloatVectorEncoding))
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)
	allowed := strings.ToLower(response.Header().Get("Access-Control-Allow-Headers"))
	require.Contains(t, strings.Split(allowed, ", "), strings.ToLower(HTTPHeaderFloatVectorEncoding))
}

func TestFloatVectorEncodingProjection(t *testing.T) {
	paramtable.Init()
	values := []float32{1, -2}
	vector := &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldName: "vec",
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  2,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values}},
		}},
	}
	typeutil.SetFieldDataValidData(vector, []bool{false, true})
	fields := []*schemapb.FieldData{vector, {
		Type: schemapb.DataType_JSON, FieldName: "$meta", IsDynamic: true,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
			JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{"numbers":[1,-2],"text":"<>&"}`), []byte(`{"numbers":[1,-2],"text":"<>&"}`)}},
		}}},
	}, {
		Type: schemapb.DataType_BinaryVector, FieldName: "binary",
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  8,
			Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2}},
		}},
	}}
	rows, err := buildQueryRespWithEncoding(0, []string{"vec", "numbers", "text", "binary"}, fields, nil, nil, false, nil, "base64")
	require.NoError(t, err)
	require.Nil(t, rows[0]["vec"])
	view := rows[1]["vec"].(base64FloatVector)
	require.Same(t, &values[0], &view.values[0], "projection must retain a view, not allocate full-result bytes")
	var out bytes.Buffer
	require.NoError(t, writeDQLJSON(context.Background(), &out, map[string]interface{}{"data": rows}, 1))
	require.Equal(t, `"AACAPwAAAMA="`, gjson.Get(out.String(), "data.1.vec").Raw)
	require.Equal(t, `[1,-2]`, gjson.Get(out.String(), "data.1.numbers").Raw)
	require.Equal(t, `"Ag=="`, gjson.Get(out.String(), "data.1.binary").Raw)
	require.Contains(t, out.String(), `\u003c\u003e\u0026`)

	// V1 and callers without an option retain numeric arrays.
	legacy, err := buildQueryResp(0, []string{"vec"}, []*schemapb.FieldData{vector}, nil, nil, false, nil)
	require.NoError(t, err)
	require.Equal(t, []float32{1, -2}, legacy[1]["vec"])
}

func TestFloatVectorEncodingStructArray(t *testing.T) {
	paramtable.Init()
	schema := buildStructArrayTestSchema()
	row, err := parseStructArrayRow(`[{"sub_int":1,"sub_vec":[1,-2,0,0]}]`, schema.GetStructArrayFields()[0], false)
	require.NoError(t, err)
	field, err := buildStructArrayFieldData(schema.GetStructArrayFields()[0], []structArrayRow{row})
	require.NoError(t, err)
	rows, err := buildQueryRespWithEncoding(0, []string{field.GetFieldName()}, []*schemapb.FieldData{field}, nil, nil, true, schema, "base64")
	require.NoError(t, err)
	nested := rows[0][field.GetFieldName()].([]map[string]interface{})
	encoded, err := sonic.ConfigStd.Marshal(nested)
	require.NoError(t, err)
	require.JSONEq(t, `[{"sub_int":1,"sub_vec":"AACAPwAAAMAAAAAAAAAAAA=="}]`, string(encoded))
}

func TestFloatVectorEncodingStructElementNull(t *testing.T) {
	paramtable.Init()
	schema := buildElementNullableStructTestSchema(false)
	// Server output contains compact vector payloads with null elements before
	// and after the valid vector, plus nullable scalar and nested array fields.
	field := elementNullStructOutput([]bool{true, false, false})
	rows, err := buildQueryRespWithEncoding(0, []string{"clips"}, []*schemapb.FieldData{field}, nil, nil, true, schema, "base64")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	encoded, err := sonic.ConfigStd.Marshal(rows[0]["clips"])
	require.NoError(t, err)
	require.JSONEq(t, `[
		{"emb": null, "lang": "en", "score": 1, "tags": ["a", null]},
		{"emb": "AAAAPwAAgD4=", "lang": null, "score": 2, "tags": null},
		{"emb": null, "lang": null, "score": 3, "tags": []}
	]`, string(encoded))
}

func TestFloatVectorEncodingFailure(t *testing.T) {
	setQueryTestBudget(t, "5000")
	for _, h2 := range []bool{false, true} {
		for _, count := range []int{1, 257} {
			t.Run(fmt.Sprintf("h2=%t/rows=%d", h2, count), func(t *testing.T) {
				s := queryStreamServer(t, func(c *gin.Context) {
					var vectorEncodingBuffer []byte
					rows := make([]map[string]interface{}, count)
					for i := range rows {
						rows[i] = map[string]interface{}{"vec": floatVectorEncoding("base64").floatVector([]float32{1, -2}, &vectorEncodingBuffer)}
					}
					rows[count-1]["vec"] = floatVectorEncoding("base64").floatVector([]float32{float32(math.NaN())}, &vectorEncodingBuffer)
					HTTPReturnStream(c, 200, gin.H{"code": 0, "data": rows})
				}, h2)
				resp, err := s.Client().Post(s.URL+"/query", "application/json", nil)
				if count == 1 {
					require.NoError(t, err)
					defer resp.Body.Close()
					body, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
					require.JSONEq(t, `{"code":500}`, string(body))
				} else {
					if err == nil {
						_, err = io.ReadAll(resp.Body)
						resp.Body.Close()
					}
					require.Error(t, err, "late Base64 encoding failure must abort the response")
				}
			})
		}
	}
}

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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type unknownFieldsTestMetric struct {
	Op        string `json:"op"`
	FieldName string `json:"fieldName"`
}

type unknownFieldsTestRequest struct {
	Name       string                              `json:"name"`
	Search     []unknownFieldsTestRequest          `json:"search"`
	Metrics    map[string]*unknownFieldsTestMetric `json:"metrics"`
	Data       []map[string]any                    `json:"data"`
	Params     map[string]any                      `json:"params"`
	ExprParams map[string]json.RawMessage          `json:"exprParams"`
	Properties map[string]string                   `json:"properties"`
	Ignored    string                              `json:"-"`
}

func TestV2UnknownFieldsPolicies(t *testing.T) {
	for _, policy := range []string{"ignore", "warn", "strict"} {
		t.Run(policy, func(t *testing.T) {
			body := []byte(`{"name":"ok","groupParams":{"secret":"not-in-diagnostic"},"other":1}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Set(gin.BodyBytesKey, body)
			c.Header("Access-Control-Expose-Headers", "Existing")
			err := checkV2UnknownFields(c, &unknownFieldsTestRequest{}, policy)
			if policy == "strict" {
				require.Error(t, err)
				require.EqualValues(t, 1100, merr.Code(err))
				require.Contains(t, err.Error(), `"/groupParams"`)
				require.NotContains(t, err.Error(), "not-in-diagnostic")
			} else {
				require.NoError(t, err)
			}
			if policy == "warn" {
				require.Contains(t, recorder.Header().Get("X-Milvus-Warning"), `"/groupParams"`)
				require.NotContains(t, recorder.Header().Get("X-Milvus-Warning"), "not-in-diagnostic")
				require.Contains(t, strings.Join(recorder.Header().Values("Access-Control-Expose-Headers"), ","), "X-Milvus-Warning")
			} else {
				require.Empty(t, recorder.Header().Get("X-Milvus-Warning"))
			}
			require.Contains(t, recorder.Header().Get("Access-Control-Expose-Headers"), "Existing")
			require.Empty(t, recorder.Body.String(), "checking must not replace the business response")
			require.Equal(t, string(body), string(c.MustGet(gin.BodyBytesKey).([]byte)))
		})
	}
}

func TestV2UnknownFieldsBoundaries(t *testing.T) {
	for _, tc := range []struct{ name, body, path string }{
		{"known", `{"name":"ok"}`, ""},
		{"case alias", `{"NAME":"ok"}`, ""},
		{"duplicate known", `{"name":"first","name":"last"}`, ""},
		{"nested array", `{"search":[{"name":"ok"},{"typo":1}]}`, "/search/1/typo"},
		{"struct map", `{"metrics":{"user/name~":{"oops":1}}}`, "/metrics/user~1name~0/oops"},
		{"struct map valid", `{"metrics":{"custom":{"op":"sum","fieldName":"price"}}}`, ""},
		{"duplicate structure", `{"metrics":{"x":{"typo":1}},"metrics":{"x":{"op":"sum"}}}`, "/metrics/x/typo"},
		{"null structures", `{"search":null,"metrics":{"x":null}}`, ""},
		{"open data", `{"data":[{"custom":{"nested":{"typo":1}},"id":9223372036854775807}]}`, ""},
		{"open parameters", `{"params":{"arbitrary":{"typo":1}},"exprParams":{"x":{"unknown":true}},"properties":{"arbitrary":"value"}}`, ""},
		{"ignored field", `{"Ignored":"not an API field"}`, "/Ignored"},
		{"empty field", `{"":1}`, "/"},
		{"escaped field", `{"na\u006de":"ok"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req unknownFieldsTestRequest
			require.NoError(t, json.Unmarshal([]byte(tc.body), &req))
			before, err := json.Marshal(req)
			require.NoError(t, err)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Set(gin.BodyBytesKey, []byte(tc.body))
			err = checkV2UnknownFields(c, &req, "strict")
			if tc.path == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), strconv.Quote(tc.path))
			}
			after, err := json.Marshal(req)
			require.NoError(t, err)
			require.Equal(t, before, after, "checking must not mutate bound values")
		})
	}
}

func TestV2UnknownFieldsSafeDiagnostics(t *testing.T) {
	for _, name := range []string{"line\r\nX-Injected: yes", strings.Repeat("界", 1000)} {
		body, err := json.Marshal(map[string]string{name: "private-value"})
		require.NoError(t, err)
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Set(gin.BodyBytesKey, body)
		require.NoError(t, checkV2UnknownFields(c, &unknownFieldsTestRequest{}, "warn"))
		warning := recorder.Header().Get("X-Milvus-Warning")
		require.NotEmpty(t, warning)
		require.Less(t, len(warning), 2048)
		require.NotContains(t, warning, "private-value")
		for _, r := range warning {
			require.True(t, r >= 32 && r < 127)
		}
	}
}

func TestV2UnknownFieldsIgnoreFastPath(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	require.NoError(t, checkV2UnknownFields(c, nil, "ignore"), "ignore must not need or inspect a body")
	for _, policy := range []string{"strict", "warn", "invalid"} {
		err := checkV2UnknownFields(c, &unknownFieldsTestRequest{}, policy)
		require.Error(t, err)
		require.NotEqualValues(t, 1100, merr.Code(err), "missing internal state is not an input error")
	}
}

func TestV2UnknownFieldsMatchesJSONFieldSelection(t *testing.T) {
	type Plain struct {
		Name  string
		Count int
	}
	type Tagged struct {
		Value string `json:"Count"`
	}
	for _, tc := range []struct {
		name   string
		newReq func() any
		body   string
	}{
		{"plain", func() any { return &Plain{} }, `{"Name":"ok"}`},
		{"folded", func() any { return &Plain{} }, `{"naME":"ok"}`},
		{"unicode fold", func() any { return &struct{ Key string }{} }, `{"Key":"ok"}`},
		{"tagged", func() any { return &Tagged{} }, `{"Count":"ok"}`},
		{"private ignored", func() any { return &struct{ private string }{} }, `{"private":"no"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder := json.NewDecoder(strings.NewReader(tc.body))
			decoder.DisallowUnknownFields()
			wantErr := decoder.Decode(tc.newReq())
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Set(gin.BodyBytesKey, []byte(tc.body))
			gotErr := checkV2UnknownFields(c, tc.newReq(), "strict")
			require.Equal(t, wantErr != nil, gotErr != nil, "field-name acceptance must match encoding/json")
		})
	}
}

func TestV2UnknownFieldsRecursiveOpenMap(t *testing.T) {
	type recursive map[string]recursive
	require.False(t, fixedJSONShape(reflect.TypeOf(recursive{})), "recursive open containers must not hang type inspection")
}

func TestV2UnknownFieldsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
	c.Set(gin.BodyBytesKey, []byte(`{"data":[{"anything":"value"}]}`))
	require.ErrorIs(t, checkV2UnknownFields(c, &unknownFieldsTestRequest{}, "strict"), context.Canceled)
}

type unknownFieldsTestAggregation struct {
	Sub    *unknownFieldsTestAggregation `json:"subAggregation"`
	Fields []string                      `json:"fields"`
}

func TestV2UnknownFieldsDeepStructure(t *testing.T) {
	body := strings.Repeat(`{"subAggregation":`, 1000) + `{"fields":["a"],"typo":true}` + strings.Repeat("}", 1000)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set(gin.BodyBytesKey, []byte(body))
	require.Error(t, checkV2UnknownFields(c, &unknownFieldsTestAggregation{}, "strict"))
	// Opaque numeric values must never be converted by the key-only scanner.
	c.Set(gin.BodyBytesKey, []byte(`{"exprParams":{"v":0.1e1000},"data":[{"x":0.1e1000}]}`))
	require.NoError(t, checkV2UnknownFields(c, &unknownFieldsTestRequest{}, "strict"))
}

func BenchmarkV2UnknownFieldsDeepAggregation(b *testing.B) {
	for _, depth := range []int{1, 100, 1000} {
		body := []byte(strings.Repeat(`{"subAggregation":`, depth) + `{"fields":["` + strings.Repeat("x", 1<<20) + `"]}` + strings.Repeat("}", depth))
		b.Run(strconv.Itoa(depth), func(b *testing.B) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Set(gin.BodyBytesKey, body)
			req := &unknownFieldsTestAggregation{}
			require.NoError(b, checkV2UnknownFields(c, req, "strict"))
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := checkV2UnknownFields(c, req, "strict"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkV2UnknownFieldsEntityData(b *testing.B) {
	for _, rows := range []int{1, 10000} {
		body := []byte(`{"data":[` + strings.TrimSuffix(strings.Repeat(`{"id":9223372036854775807,"json":{"arbitrary":"value"}},`, rows), ",") + `],"name":"c"}`)
		for _, policy := range []string{"ignore", "strict"} {
			b.Run(strconv.Itoa(rows)+"/"+policy, func(b *testing.B) {
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Set(gin.BodyBytesKey, body)
				req := &unknownFieldsTestRequest{}
				require.NoError(b, checkV2UnknownFields(c, req, policy))
				b.ReportAllocs()
				b.SetBytes(int64(len(body)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := checkV2UnknownFields(c, req, policy); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

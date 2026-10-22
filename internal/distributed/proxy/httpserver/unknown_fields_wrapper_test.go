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
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	ginjson "github.com/gin-gonic/gin/codec/json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestV2UnknownFieldsCounter(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.V2UnknownFieldPolicy.Key
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	registry := prometheus.NewRegistry()
	metrics.RegisterProxy(registry)
	counts := func() map[string]float64 {
		families, err := registry.Gather()
		require.NoError(t, err)
		result := map[string]float64{}
		for _, family := range families {
			if family.GetName() != "milvus_proxy_restful_unknown_field_requests_total" {
				continue
			}
			for _, metric := range family.Metric {
				labels := map[string]string{}
				for _, label := range metric.Label {
					labels[label.GetName()] = label.GetValue()
				}
				require.Len(t, labels, 3, "only node_id, function_name and policy are labels")
				require.Contains(t, labels, "node_id")
				require.Contains(t, labels, "function_name")
				require.Contains(t, labels, "policy")
				if labels["function_name"] == "Search" {
					result[labels["policy"]] += metric.GetCounter().GetValue()
				}
			}
		}
		return result
	}
	server := gin.New()
	server.Use(func(c *gin.Context) { c.Set(ContextUsername, "root") })
	path := versionalV2(EntityCategory, SearchAction)
	server.POST(path, wrapperPost(func() any { return &SearchReqV2{} },
		func(_ context.Context, c *gin.Context, _ any, _ string) (any, error) {
			c.JSON(http.StatusOK, gin.H{"code": 0})
			return nil, nil
		}))
	for _, policy := range []string{"ignore", "warn", "strict"} {
		require.NoError(t, paramtable.Get().Save(key, policy))
		for _, tc := range []struct {
			name    string
			body    string
			unknown bool
		}{
			{"known", `{"collectionName":"c","searchParams":{"custom":1}}`, false},
			{"unknown", `{"collectionName":"c","typo":1,"secondTypo":2}`, true},
			{"binding error", `{"collectionName":"c","limit":"bad","typo":1}`, false},
		} {
			t.Run(policy+"/"+tc.name, func(t *testing.T) {
				before := counts()
				w := postJSONBody(server, path, tc.body)
				require.Equal(t, http.StatusOK, w.Code)
				after := counts()
				for _, observedPolicy := range []string{"ignore", "warn", "strict"} {
					want := float64(0)
					if tc.unknown && policy != "ignore" && observedPolicy == policy {
						want = 1
					}
					require.Equal(t, want, after[observedPolicy]-before[observedPolicy], "count requests, not unknown keys; policy=%s", observedPolicy)
				}
			})
		}
	}
}

// Exercise the real binder -> policy -> business boundary, including the two
// decoders that a global Gin DisallowUnknownFields flag would not fully cover.
func TestV2UnknownFieldsWrapper(t *testing.T) {
	t.Logf("Gin JSON backend: %s", ginjson.Package)
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.V2UnknownFieldPolicy.Key
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	tests := []struct {
		name   string
		newReq newReqFunc
		body   string
		path   string
		check  func(*testing.T, any)
	}{
		{"empty API", func() any { return &EmptyReq{} }, `{"typo":1}`, "/typo", nil},
		{"collection", func() any { return &CollectionNameReq{} }, `{"collectionName":"c","typo":1}`, "/typo", nil},
		{"search", func() any { return &SearchReqV2{} }, `{"collectionName":"c","groupParams":{"groupSize":2}}`, "/groupParams", nil},
		{"hybrid nested", func() any { return &HybridSearchReq{} }, `{"collectionName":"c","search":[{"data":[[1,2]],"typo":1}]}`, "/search/0/typo", nil},
		{"function chain", func() any { return &HybridSearchReq{} }, `{"collectionName":"c","groupSize":2,"functionChains":[{"ops":[{"typo":1}]}]}`, "/functionChains/0/ops/0/typo", nil},
		{"aggregation map", func() any { return &SearchReqV2{} }, `{"collectionName":"c","searchAggregation":{"metrics":{"price.avg":{"op":"avg","typo":1}}}}`, "/searchAggregation/metrics/price.avg/typo", nil},
		{"insert envelope", func() any { return &CollectionDataReq{} }, `{"collectionName":"c","data":[{"id":9223372036854775807,"json":{"anything":1}}],"typo":1}`, "/typo", nil},
		{"upsert field ops", func() any { return &CollectionDataReq{} }, `{"collectionName":"c","data":[{"id":1}],"fieldOps":[{"fieldName":"json","op":"merge","typo":1}]}`, "/fieldOps/0/typo", nil},
		{"import custom decoder", func() any { return &ImportReq{} }, `{"collectionName":"c","files":[["a.json"]],"options":{"custom":null},"typo":1}`, "/typo", nil},
		{"insert data untouched", func() any { return &CollectionDataReq{} }, `{"collectionName":"c","data":[{"id":9223372036854775807,"dynamic":{"anything":true}}]}`, "", func(t *testing.T, req any) {
			require.Nil(t, req.(*CollectionDataReq).Data, "metadata-only binding must not materialize data")
		}},
		{"import null options", func() any { return &ImportReq{} }, `{"collectionName":"c","files":[["a.json"]],"options":{"custom":null,"auto_commit":"true"}}`, "", func(t *testing.T, req any) {
			require.Equal(t, "", req.(*ImportReq).Options["custom"])
			require.Equal(t, "true", req.(*ImportReq).Options["auto_commit"])
		}},
		{"function chains and groups", func() any { return &HybridSearchReq{} }, `{"collectionName":"c","groupingField":"g","groupSize":2,"strictGroupSize":true,"functionChains":[{"name":"n","stage":"rerank","ops":[{"op":"expr","params":{"custom":{"unknown":1}},"expr":{"name":"x","args":[{"literal":{"arbitrary":1}}]}}]}]}`, "", func(t *testing.T, req any) {
			r := req.(*HybridSearchReq)
			require.EqualValues(t, 2, *r.GroupSize)
			require.True(t, *r.StrictGroupSize)
			require.Len(t, r.FunctionChains, 1)
		}},
		{"compatibility params", func() any { return &SearchReqV2{} }, `{"COLLECTIONNAME":"c","params":{"custom":2},"searchParams":{"custom":{"arbitrary":true}},"exprParams":{"v":{"anything":1}},"functionChains":null}`, "", nil},
	}
	for _, tc := range tests {
		for _, policy := range []string{"ignore", "warn", "strict"} {
			t.Run(tc.name+"/"+policy, func(t *testing.T) {
				require.NoError(t, paramtable.Get().Save(key, policy))
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(http.MethodPost, "/policy-test", strings.NewReader(tc.body))
				c.Request.Header.Set("Content-Type", "application/json")
				c.Set(ContextUsername, "root")
				called := false
				wrapperPost(tc.newReq, func(_ context.Context, c *gin.Context, req any, _ string) (any, error) {
					called = true
					require.Equal(t, tc.body, string(c.MustGet(gin.BodyBytesKey).([]byte)))
					if tc.check != nil {
						tc.check(t, req)
					}
					c.JSON(http.StatusOK, gin.H{"code": 0, "data": "unchanged"})
					return nil, nil
				})(c)
				reject := policy == "strict" && tc.path != ""
				require.Equal(t, !reject, called, "strict must reject before business execution")
				require.Equal(t, http.StatusOK, w.Code)
				if reject {
					var resp ReturnErrMsg
					require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
					require.EqualValues(t, 1100, resp.Code)
					require.Contains(t, resp.Message, tc.path)
				} else {
					require.JSONEq(t, `{"code":0,"data":"unchanged"}`, w.Body.String())
				}
				if policy == "warn" && tc.path != "" {
					require.Contains(t, w.Header().Get("X-Milvus-Warning"), tc.path)
				} else {
					require.Empty(t, w.Header().Get("X-Milvus-Warning"))
				}
			})
		}
	}
}

func TestV2UnknownFieldsRequestPolicyOverridesServerDefault(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.V2UnknownFieldPolicy.Key
	t.Cleanup(func() { paramtable.Get().Reset(key) })

	const body = `{"collectionName":"c","typo":1}`
	for _, tc := range []struct {
		name         string
		serverPolicy string
		query        string
		called       bool
		warning      bool
		invalid      bool
	}{
		{"default ignore", "ignore", "", true, false, false},
		{"default strict", "strict", "", false, false, false},
		{"strict overridden by ignore", "strict", "?unknownFieldPolicy=ignore", true, false, false},
		{"strict overridden by warn", "strict", "?unknownFieldPolicy=warn", true, true, false},
		{"ignore overridden by strict", "ignore", "?unknownFieldPolicy=strict", false, false, false},
		{"warn overridden by strict", "warn", "?unknownFieldPolicy=StRiCt", false, false, false},
		{"invalid value", "ignore", "?unknownFieldPolicy=other", false, false, true},
		{"empty value", "ignore", "?unknownFieldPolicy=", false, false, true},
		{"ambiguous duplicate", "ignore", "?unknownFieldPolicy=ignore&unknownFieldPolicy=strict", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, paramtable.Get().Save(key, tc.serverPolicy))
			called := false
			server := gin.New()
			server.Use(func(c *gin.Context) { c.Set(ContextUsername, "root") })
			server.POST("/v2/vectordb/entities/search", wrapperPost(func() any { return &SearchReqV2{} },
				func(_ context.Context, c *gin.Context, _ any, _ string) (any, error) {
					called = true
					c.JSON(http.StatusOK, gin.H{"code": 0})
					return nil, nil
				}))
			w := postJSONBody(server, "/v2/vectordb/entities/search"+tc.query, body)
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, tc.called, called)
			if tc.called {
				require.JSONEq(t, `{"code":0}`, w.Body.String())
			} else {
				var resp ReturnErrMsg
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
				require.EqualValues(t, 1100, resp.Code)
				if tc.invalid {
					require.Contains(t, resp.Message, "unknownFieldPolicy")
				} else {
					require.Contains(t, resp.Message, "/typo")
				}
			}
			if tc.warning {
				require.Contains(t, w.Header().Get("X-Milvus-Warning"), "/typo")
			} else {
				require.Empty(t, w.Header().Get("X-Milvus-Warning"))
			}
		})
	}
}

func TestV2UnknownFieldsRequestOverrideForImportSDKBodies(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.V2UnknownFieldPolicy.Key
	require.NoError(t, paramtable.Get().Save(key, "strict"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })

	for _, tc := range []struct {
		name   string
		newReq newReqFunc
		body   string
		path   string
	}{
		{"create", func() any { return &ImportReq{} }, `{"collectionName":"c","files":[["a.json"]],"objectUrl":"","clusterId":"","projectId":"","accessKey":""}`, "/objectUrl"},
		{"get progress", func() any { return &JobIDReq{} }, `{"jobId":"1","clusterId":"","projectId":"","regionId":""}`, "/clusterId"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			server := gin.New()
			server.Use(func(c *gin.Context) { c.Set(ContextUsername, "root") })
			server.POST("/v2/vectordb/jobs/import/test", wrapperPost(tc.newReq,
				func(_ context.Context, c *gin.Context, _ any, _ string) (any, error) {
					called = true
					c.JSON(http.StatusOK, gin.H{"code": 0})
					return nil, nil
				}))

			w := postJSONBody(server, "/v2/vectordb/jobs/import/test", tc.body)
			var rejected ReturnErrMsg
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rejected))
			require.EqualValues(t, 1100, rejected.Code)
			require.Contains(t, rejected.Message, tc.path)
			require.False(t, called, "strict rejection must precede import business execution")

			w = postJSONBody(server, "/v2/vectordb/jobs/import/test?unknownFieldPolicy=ignore", tc.body)
			require.JSONEq(t, `{"code":0}`, w.Body.String())
			require.True(t, called)
		})
	}
}

func TestV2UnknownFieldsBindingErrorsWin(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.V2UnknownFieldPolicy.Key
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	for _, tc := range []struct {
		name   string
		newReq newReqFunc
		body   string
		code   int32
	}{
		{"required", func() any { return &SearchReqV2{} }, `{"typo":1}`, merr.Code(merr.ErrMissingRequiredParameters)},
		{"type", func() any { return &SearchReqV2{} }, `{"collectionName":"c","limit":"bad","typo":1}`, merr.Code(merr.ErrIncorrectParameterFormat)},
		{"syntax", func() any { return &SearchReqV2{} }, `{"collectionName":"c","typo":}`, merr.Code(merr.ErrIncorrectParameterFormat)},
		{"missing data", func() any { return &CollectionDataReq{} }, `{"collectionName":"c","typo":1}`, merr.Code(merr.ErrMissingRequiredParameters)},
		{"invalid data", func() any { return &CollectionDataReq{} }, `{"collectionName":"c","data":{},"typo":1}`, merr.Code(merr.ErrIncorrectParameterFormat)},
		{"custom decoder error", func() any { return &ImportReq{} }, `{"collectionName":"c","files":[["x"]],"options":{"auto_commit":null},"typo":1}`, merr.Code(merr.ErrIncorrectParameterFormat)},
	} {
		for _, policy := range []string{"ignore", "warn", "strict"} {
			t.Run(tc.name+"/"+policy, func(t *testing.T) {
				require.NoError(t, paramtable.Get().Save(key, policy))
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(http.MethodPost, "/policy-test", strings.NewReader(tc.body))
				c.Set(ContextUsername, "root")
				wrapperPost(tc.newReq, func(context.Context, *gin.Context, any, string) (any, error) {
					t.Fatal("invalid binding must never reach business execution")
					return nil, nil
				})(c)
				var resp ReturnErrMsg
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
				require.Equal(t, tc.code, resp.Code)
				require.NotContains(t, resp.Message, "unknown request field")
				require.Empty(t, w.Header().Get("X-Milvus-Warning"))
			})
		}
	}
}

func TestV2UnknownFieldsDoesNotChangeOtherBinders(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set(gin.BodyBytesKey, []byte(`{"typo":1}`))
	require.Error(t, checkV2UnknownFields(c, &EmptyReq{}, "strict"))
	other, _ := gin.CreateTestContext(httptest.NewRecorder())
	other.Request = httptest.NewRequest(http.MethodPost, "/other", strings.NewReader(`{"typo":1}`))
	require.NoError(t, other.ShouldBindBodyWith(&EmptyReq{}, binding.JSON), "v1/non-v2 JSON binding must keep ignoring unknown fields")
}

func TestV2UnknownFieldsAllRegisteredRoutes(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.V2UnknownFieldPolicy.Key
	require.NoError(t, paramtable.Get().Save(key, "strict"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	server := initHTTPServerV2(nil, false)
	// A superset of binding-required fields lets every registered endpoint
	// reach the common gate. __unknown is deliberately first in wire order.
	// No proxy is supplied: any accidental business execution is a test failure.
	const fixture = `{
		"__unknown":true,
		"name":"n","path":"p","dbName":"default","collectionName":"c",
		"newCollectionName":"new","sourceCollectionName":"src","targetCollectionName":"dst",
		"snapshotName":"s","snapshotMetadataURI":"s3://b/s","targetS3Path":"s3://b/",
		"pinId":"1","jobId":"1","partitionName":"p","partitionNames":["p"],
		"fieldName":"f","functionName":"f","indexName":"i","aliasName":"a",
		"data":[{"id":1}],"id":1,"filter":"id > 0","files":[["a.json"]],
		"userName":"u","password":"p","roleName":"r","privilege":"p",
		"privilegeGroupName":"p","objectType":"Collection","objectName":"c",
		"indexParams":[{"fieldName":"f"}],
		"schema":{"fieldName":"f","dataType":"Int64"},
		"outputField":{"fieldName":"f","dataType":"Int64"},
		"function":{"name":"f","type":"BM25","inputFieldNames":["a"],"outputFieldNames":["b"]},
		"resource_groups":{"r":{"requests":{"node_num":1},"limits":{"node_num":1}}},
		"sourceRgName":"a","targetRgName":"b","replicaNum":1,"text":["a"]
	}`
	routes := server.Routes()
	require.NotEmpty(t, routes)
	for _, route := range routes {
		t.Run(route.Path, func(t *testing.T) {
			body := fixture
			switch route.Path {
			case versionalV2(ExternalCollectionJobCategory, DescribeAction), versionalV2(CollectionCategory, CompactionStateAction):
				body = strings.Replace(body, `"jobId":"1"`, `"jobId":1`, 1)
			case versionalV2(CollectionCategory, AddFunctionFieldAction):
				body = strings.Replace(body, `"indexParams":[{"fieldName":"f"}]`, `"indexParams":{"fieldName":"f"}`, 1)
			}
			w := postJSONBody(server, route.Path, body)
			require.Equal(t, http.StatusOK, w.Code)
			var resp ReturnErrMsg
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.EqualValues(t, 1100, resp.Code, resp.Message)
			require.Contains(t, resp.Message, "/__unknown")
		})
	}
	t.Logf("Registered REST v2 routes: %d", len(routes))
}

func TestV2UnknownFieldsWarningThroughTimeout(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.V2UnknownFieldPolicy.Key
	require.NoError(t, paramtable.Get().Save(key, "warn"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	server := gin.New()
	server.Use(func(c *gin.Context) {
		c.Set(ContextUsername, "root")
	})
	server.POST("/warning-test", timeoutMiddleware(wrapperPost(func() any { return &EmptyReq{} },
		func(_ context.Context, c *gin.Context, _ any, _ string) (any, error) {
			c.JSON(http.StatusOK, gin.H{"code": 0})
			return nil, nil
		})))
	w := postJSONBody(server, "/warning-test", `{"typo":true}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"code":0}`, w.Body.String())
	require.Contains(t, w.Header().Get("X-Milvus-Warning"), "/typo")
	require.Contains(t, w.Header().Get("Access-Control-Expose-Headers"), "X-Milvus-Warning")
}

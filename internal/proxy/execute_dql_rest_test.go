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

package proxy_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const executeDQLRESTPath = "/v2/vectordb/entities/execute_dql"

const readySearch = `{"search":{"collectionName":"ready","data":[[1,0]]}}`

type executeDQLRESTResponse struct {
	Code    int32
	Message string
	Data    []struct {
		Code    int32
		Message string
		Search  *struct {
			Code  int32
			Data  []map[string]any
			Topks []int64
		}
	}
}

func newExecuteDQLRESTHandler(p types.ProxyComponent, middleware ...gin.HandlerFunc) http.Handler {
	router := gin.New()
	router.Use(middleware...)
	group := router.Group("/v2/vectordb", func(c *gin.Context) {
		c.Set(httpserver.ContextUsername, "root")
	})
	httpserver.NewHandlersV2(p).RegisterRoutesToV2(group)
	return httpserver.DQLStreamingHTTPHandler(router)
}

type executeDQLHTTPProxy struct {
	*proxy.Proxy
	cache  proxy.Cache
	search func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)
}

func (p *executeDQLHTTPProxy) GetMetaCache() proxy.Cache { return p.cache }

func (p *executeDQLHTTPProxy) Search(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return p.search(ctx, req)
}

// The HTTP fixture has one Proxy; these methods stand in for the coordinator's
// fan-out while keeping the real local registry and cancellation handlers.
func (p *executeDQLHTTPProxy) ListRunningRequests(ctx context.Context, req *milvuspb.ListRunningRequestsRequest) (*milvuspb.ListRunningRequestsResponse, error) {
	return p.ListLocalRunningRequests(ctx, req)
}

func (p *executeDQLHTTPProxy) CancelRequests(ctx context.Context, req *milvuspb.CancelRequestsRequest) (*milvuspb.CancelRequestsResponse, error) {
	return p.CancelLocalRequests(ctx, req)
}

type executeDQLHTTPCache struct {
	proxy.Cache
	schema   *metacache.SchemaInfo
	onSchema func(context.Context, string)
}

func (m *executeDQLHTTPCache) GetCollectionSchema(ctx context.Context, _, collection string) (*metacache.SchemaInfo, error) {
	if m.onSchema != nil {
		m.onSchema(ctx, collection)
	}
	return m.schema, nil
}

func newExecuteDQLHTTPProxy(t *testing.T, onSchema func(context.Context, string), search func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)) *executeDQLHTTPProxy {
	t.Helper()
	params := paramtable.Get()
	for key, value := range map[string]string{
		params.QuotaConfig.QuotaAndLimitsEnabled.Key: "false",
		params.CommonCfg.AuthorizationEnabled.Key:    "false",
	} {
		require.NoError(t, params.Save(key, value))
		t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
	}
	schema, err := metacache.NewSchemaInfo(&schemapb.CollectionSchema{
		Name: "ready",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}},
			},
		},
	})
	require.NoError(t, err)
	return &executeDQLHTTPProxy{
		Proxy:  proxy.NewExecuteDQLTestProxy(t, 1, 1),
		cache:  &executeDQLHTTPCache{schema: schema, onSchema: onSchema},
		search: search,
	}
}

func successfulSearchResult() *milvuspb.SearchResults {
	return &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{
		TopK: 1, NumQueries: 1, Topks: []int64{1}, Scores: []float32{0.5},
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{9007199254740993}}}},
	}}
}

func recordExecuteDQL(t *testing.T, handler http.Handler, payload io.Reader, requestTimeout string) (int, executeDQLRESTResponse) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, executeDQLRESTPath, payload)
	if requestTimeout != "" {
		request.Header.Set(mhttp.HTTPHeaderRequestTimeout, requestTimeout)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var body executeDQLRESTResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	return response.Code, body
}

func TestExecuteDQLRESTRejectsBatchLimits(t *testing.T) {
	result := successfulSearchResult()
	var prepared, executed int
	p := newExecuteDQLHTTPProxy(t, func(context.Context, string) { prepared++ }, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		executed++
		return result, nil
	})
	handler := newExecuteDQLRESTHandler(p)
	post := func(payload, setting string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, executeDQLRESTPath, strings.NewReader(payload))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		require.Equal(t, http.StatusOK, response.Code)
		var body map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.JSONEq(t, "1102", string(body["code"]))
		require.NotContains(t, body, "data")
		require.Contains(t, string(body["message"]), setting)
	}
	// Reject the size before trying to parse the unrecognized last item.
	post(`{"requests":[`+strings.Repeat(readySearch+",", 256)+`null]}`, "maxBatchSize")
	require.Zero(t, prepared)
	require.Zero(t, executed)

	size := proto.Size(&milvuspb.ExecuteDQLResponse{Status: merr.Success(), Results: []*milvuspb.DQLResult{
		{Status: merr.Success(), Result: &milvuspb.DQLResult_Search{Search: result}},
	}})
	params := paramtable.Get()
	key := params.ProxyCfg.ExecuteDQLMaxResultBytes.Key
	require.NoError(t, params.Save(key, strconv.Itoa(size-1)))
	t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
	post(`{"requests":[`+readySearch+`]}`, "maxResultBytes")
	require.Equal(t, 1, prepared)
	require.Equal(t, 1, executed)
}

func TestExecuteDQLRESTPromotesSearchRenderingErrorsAndPanics(t *testing.T) {
	p := newExecuteDQLHTTPProxy(t, nil, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		switch req.GetCollectionName() {
		case "malformed":
			return &milvuspb.SearchResults{Status: merr.Success()}, nil
		case "panics":
			panic("Search panicked")
		default:
			return successfulSearchResult(), nil
		}
	})
	request := httptest.NewRequest(http.MethodPost, executeDQLRESTPath, strings.NewReader(`{"requests":[
		{"search":{"collectionName":"malformed","data":[[1,0]]}},
		{"search":{"collectionName":"panics","data":[[1,0]]}},
		`+readySearch+`]}`))
	response := httptest.NewRecorder()
	newExecuteDQLRESTHandler(p).ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	var body struct {
		Code int32
		Data []map[string]json.RawMessage
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Zero(t, body.Code)
	require.Len(t, body.Data, 3)
	for i, err := range []error{merr.ErrInvalidSearchResult, merr.ErrServiceInternal} {
		item := body.Data[i]
		require.JSONEq(t, strconv.Itoa(int(merr.Code(err))), string(item["code"]))
		require.NotEmpty(t, item["message"])
		require.NotContains(t, item, "search")
	}
	require.JSONEq(t, "0", string(body.Data[2]["code"]))
	require.Contains(t, body.Data[2], "search")
}

func TestExecuteDQLRESTWritesSearchAccessLogs(t *testing.T) {
	const filename = "execute-dql-access.log"
	logDir := t.TempDir()
	params := paramtable.Get()
	cfg := &params.ProxyCfg.AccessLog
	for _, setting := range []struct{ key, value string }{
		{cfg.LocalPath.Key, logDir},
		{cfg.Filename.Key, filename},
		{cfg.Enable.Key, "true"},
	} {
		require.NoError(t, params.Save(setting.key, setting.value))
		t.Cleanup(func() { require.NoError(t, params.Reset(setting.key)) })
	}
	accesslog.InitAccessLogger(params)

	hookutil.InitOnceHook()
	hookutil.SetTestHook(executeDQLMetricsHook{
		before: func(ctx context.Context, req any) (context.Context, error) {
			if req.(*milvuspb.SearchRequest).GetCollectionName() == "blocked" {
				return ctx, merr.ErrPrivilegeNotPermitted
			}
			return ctx, nil
		},
	})
	t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}) })
	p := newExecuteDQLHTTPProxy(t, nil, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		if req.GetDbName() == "catalog" {
			accesslog.SetActualConsistencyLevel(ctx, commonpb.ConsistencyLevel_Bounded)
		} else {
			accesslog.SetActualConsistencyLevel(ctx, commonpb.ConsistencyLevel_Eventually)
		}
		return successfulSearchResult(), nil
	})
	handler := newExecuteDQLRESTHandler(p, accesslog.AccessLogMiddleware, httpserver.LoggerHandlerFunc())
	payload := `{"requests":[
		{"search":{"dbName":"catalog","collectionName":"papers","data":[[1,0]]}},
		{"search":{"dbName":"archive","collectionName":"reports","data":[[1,0],[0,1]]}},
		{"search":{"dbName":"catalog","collectionName":"blocked","data":[[1,0],[0,1],[1,1]]}}
	]}`
	status, body := recordExecuteDQL(t, handler, strings.NewReader(payload), "")
	require.Equal(t, http.StatusOK, status)
	require.Zero(t, body.Code)
	require.Len(t, body.Data, 3)
	require.Zero(t, body.Data[0].Code)
	require.NotNil(t, body.Data[0].Search)
	require.Zero(t, body.Data[0].Search.Code)
	require.Zero(t, body.Data[1].Code)
	require.NotNil(t, body.Data[1].Search)
	require.Zero(t, body.Data[1].Search.Code)
	require.Equal(t, merr.Code(merr.ErrParameterInvalid), body.Data[2].Code)
	require.NotEmpty(t, body.Data[2].Message)
	require.Nil(t, body.Data[2].Search)

	data, err := os.ReadFile(filepath.Join(logDir, filename))
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 4)
	require.Contains(t, lines[3], executeDQLRESTPath+" [status: Successful]")
	require.NotContains(t, lines[3], "[database:")
	for _, tc := range []struct {
		db, collection, nq, consistency, outcome string
	}{
		{"catalog", "papers", "1", "Bounded", "Successful"},
		{"archive", "reports", "2", "Eventually", "Successful"},
		{"catalog", "blocked", "3", "", "GrpcInvalidArgument"},
	} {
		var entry string
		for _, line := range lines[:3] {
			if strings.Contains(line, "[collection: "+tc.collection+"]") {
				entry = line
				break
			}
		}
		require.Contains(t, entry, " Search [status: "+tc.outcome+"]")
		require.Contains(t, entry, "[database: "+tc.db+"]")
		require.Contains(t, entry, "[nq: "+tc.nq+"]")
		require.Contains(t, entry, "[timeCost:")
		if tc.consistency != "" {
			require.Contains(t, entry, "[consistency_level: "+tc.consistency+"]")
		}
	}
}

func TestExecuteDQLRESTExplicitZeroBudget(t *testing.T) {
	server := httptest.NewServer(newExecuteDQLRESTHandler(proxy.NewExecuteDQLTestProxy(t, 1, 1)))
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Post(server.URL+executeDQLRESTPath, "application/json", strings.NewReader(
		`{"requests":[{"search":{}},{"search":{}}],"timeoutMs":0}`))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var body executeDQLRESTResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Zero(t, body.Code)
	require.Len(t, body.Data, 2)
	for _, item := range body.Data {
		require.Equal(t, merr.TimeoutCode, item.Code)
		require.NotEmpty(t, item.Message)
		require.Nil(t, item.Search)
	}
}

func TestExecuteDQLRESTListAndCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stopped := make(chan error, 2)
		p := newExecuteDQLHTTPProxy(t, nil, func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			<-ctx.Done()
			stopped <- context.Cause(ctx)
			return nil, ctx.Err()
		})
		handler := newExecuteDQLRESTHandler(p)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		request := httptest.NewRequestWithContext(ctx, http.MethodPost, executeDQLRESTPath,
			strings.NewReader(`{"maxConcurrency":2,"requests":[`+readySearch+`,`+readySearch+`,`+readySearch+`]}`))
		response := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			handler.ServeHTTP(response, request)
		}()
		synctest.Wait()

		list := httptest.NewRecorder()
		handler.ServeHTTP(list, httptest.NewRequest(http.MethodPost, "/v2/vectordb/requests/running/list", strings.NewReader(`{"user":"root"}`)))
		require.Equal(t, http.StatusOK, list.Code)
		var listed struct {
			Code int32
			Data struct {
				Requests []struct {
					RequestID string `json:"requestId"`
					Type      string
					User      string
				}
			}
		}
		require.NoError(t, json.Unmarshal(list.Body.Bytes(), &listed))
		require.Zero(t, listed.Code)
		require.Len(t, listed.Data.Requests, 1)
		batch := listed.Data.Requests[0]
		require.Equal(t, "ExecuteDQL", batch.Type)
		require.Equal(t, "root", batch.User)
		body, err := json.Marshal(map[string]any{"requestIds": []string{batch.RequestID}, "reason": "stop the batch"})
		require.NoError(t, err)
		canceled := httptest.NewRecorder()
		handler.ServeHTTP(canceled, httptest.NewRequest(http.MethodPost, "/v2/vectordb/requests/running/cancel", bytes.NewReader(body)))
		require.Equal(t, http.StatusOK, canceled.Code)
		var cancelResponse struct {
			Code int32
			Data struct {
				Canceled []struct {
					RequestID string `json:"requestId"`
				}
				NotFound     []string `json:"notFound"`
				Undetermined []string
			}
		}
		require.NoError(t, json.Unmarshal(canceled.Body.Bytes(), &cancelResponse))
		require.Zero(t, cancelResponse.Code)
		require.Len(t, cancelResponse.Data.Canceled, 1)
		require.Equal(t, batch.RequestID, cancelResponse.Data.Canceled[0].RequestID)
		require.Empty(t, cancelResponse.Data.NotFound)
		require.Empty(t, cancelResponse.Data.Undetermined)
		<-done
		require.Equal(t, http.StatusOK, response.Code)
		var result executeDQLRESTResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		require.Equal(t, merr.RequestCanceledCode, result.Code)
		require.Contains(t, result.Message, "stop the batch")
		require.Empty(t, result.Data)
		require.NoError(t, ctx.Err())
		synctest.Wait()
		require.Len(t, stopped, 2, "only the two dispatched items should run")
		for range 2 {
			require.ErrorIs(t, <-stopped, merr.ErrRequestCanceled)
		}
	})
}

func TestExecuteDQLRESTDeliversPartialResultsAfterExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releasePrepare := sync.OnceFunc(func() { close(release) })
		defer releasePrepare()
		canceled := make(chan error, 1)
		p := newExecuteDQLHTTPProxy(t, func(ctx context.Context, collection string) {
			switch collection {
			case "ready":
			case "preparing":
				<-ctx.Done()
				canceled <- ctx.Err()
				<-release
			default:
				t.Error("prepared an undispatched search")
			}
		}, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			if req.GetCollectionName() != "ready" {
				t.Error("executed a search after its preparation timed out")
			}
			return successfulSearchResult(), nil
		})
		payload := `{"maxConcurrency":1,"timeoutMs":100,"requests":[` + readySearch + `,
			{"search":{}},
			{"search":{"collectionName":"preparing","data":[[1,0]]}},
			{"search":{"collectionName":"undispatched","data":[[1,0]]}}]}`
		status, body := recordExecuteDQL(t, newExecuteDQLRESTHandler(p), strings.NewReader(payload), "")
		require.Equal(t, http.StatusOK, status)
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 4)
		require.Zero(t, body.Data[0].Code)
		require.NotNil(t, body.Data[0].Search)
		require.Zero(t, body.Data[0].Search.Code)
		require.Equal(t, []int64{1}, body.Data[0].Search.Topks)
		require.Len(t, body.Data[0].Search.Data, 1)
		require.Equal(t, "9007199254740993", body.Data[0].Search.Data[0]["id"])
		require.Equal(t, merr.Code(merr.ErrMissingRequiredParameters), body.Data[1].Code)
		require.NotEmpty(t, body.Data[1].Message)
		require.Nil(t, body.Data[1].Search)
		require.Equal(t, merr.TimeoutCode, body.Data[2].Code)
		require.NotEmpty(t, body.Data[2].Message)
		require.Nil(t, body.Data[2].Search)
		require.Equal(t, merr.TimeoutCode, body.Data[3].Code)
		require.NotEmpty(t, body.Data[3].Message)
		require.Nil(t, body.Data[3].Search)
		require.ErrorIs(t, <-canceled, context.DeadlineExceeded)

		releasePrepare()
		synctest.Wait()
	})
}

type slowExecuteDQLReader struct {
	io.Reader
	delay time.Duration
}

func (r slowExecuteDQLReader) Read(p []byte) (int, error) {
	time.Sleep(r.delay)
	return r.Reader.Read(p)
}

func TestExecuteDQLRESTBudgetStartsBeforeDecode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		timeout := &paramtable.Get().HTTPCfg.RequestTimeoutMs
		previous := timeout.SwapTempValue("5")
		defer timeout.SwapTempValue(previous)
		p := newExecuteDQLHTTPProxy(t, func(context.Context, string) {
			t.Error("prepared a search after the budget expired during decoding")
		}, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			return successfulSearchResult(), nil
		})
		handler := newExecuteDQLRESTHandler(p)
		for _, tc := range []struct {
			timeout, header string
			readDelay       time.Duration
			wantHTTP        int
		}{
			{readDelay: 10 * time.Millisecond, wantHTTP: http.StatusOK},
			{timeout: `"timeoutMs":5,`, readDelay: 10 * time.Millisecond, wantHTTP: http.StatusOK},
			{header: "1", readDelay: 2 * time.Second, wantHTTP: http.StatusOK},
			{timeout: `"timeoutMs":5000,`, header: "1", readDelay: 2 * time.Second, wantHTTP: http.StatusRequestTimeout},
		} {
			payload := `{` + tc.timeout + `"requests":[` + readySearch + `]}`
			status, body := recordExecuteDQL(t, handler, slowExecuteDQLReader{strings.NewReader(payload), tc.readDelay}, tc.header)
			require.Equal(t, tc.wantHTTP, status)
			if status == http.StatusRequestTimeout {
				require.Equal(t, merr.TimeoutCode, body.Code)
				require.Empty(t, body.Data)
				continue
			}
			require.Zero(t, body.Code)
			require.Len(t, body.Data, 1)
			require.Equal(t, merr.TimeoutCode, body.Data[0].Code)
			require.NotEmpty(t, body.Data[0].Message)
			require.Nil(t, body.Data[0].Search)
		}
	})
}

func TestExecuteDQLRESTReturnsAsSoonAsAllItemsFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExecuteDQLHTTPProxy(t, nil, func(_ context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			time.Sleep(100 * time.Millisecond)
			return successfulSearchResult(), nil
		})
		handler := newExecuteDQLRESTHandler(p)
		for _, payload := range []string{
			`{"requests":[` + readySearch + `]}`,
			`{"timeoutMs":10000,"requests":[` + readySearch + `]}`,
		} {
			start := time.Now()
			status, body := recordExecuteDQL(t, handler, strings.NewReader(payload), "")
			require.Equal(t, http.StatusOK, status)
			require.Zero(t, body.Code)
			require.Len(t, body.Data, 1)
			require.Zero(t, body.Data[0].Code)
			require.NotNil(t, body.Data[0].Search)
			require.Zero(t, body.Data[0].Search.Code)
			require.Equal(t, 100*time.Millisecond, time.Since(start))
		}
	})
}

func TestExecuteDQLRESTDefaultBudgetAndOverrides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		timeout := &paramtable.Get().HTTPCfg.RequestTimeoutMs
		previous := timeout.SwapTempValue("50")
		defer timeout.SwapTempValue(previous)
		p := newExecuteDQLHTTPProxy(t, nil, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			if req.GetCollectionName() == "slow" {
				select {
				case <-time.After(200 * time.Millisecond):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return successfulSearchResult(), nil
		})
		handler := newExecuteDQLRESTHandler(p)
		for _, tc := range []struct {
			name, timeoutField, header string
			wantCode                   int32
			wantElapsed                time.Duration
		}{
			{"configured execution budget", "", "", merr.TimeoutCode, 50 * time.Millisecond},
			{"body overrides default", `"timeoutMs":500,`, "", 0, 200 * time.Millisecond},
			{"header alone", "", "1", 0, 200 * time.Millisecond},
			{"body expires before header", `"timeoutMs":100,`, "1", merr.TimeoutCode, 100 * time.Millisecond},
		} {
			payload := `{` + tc.timeoutField + `"maxConcurrency":1,"requests":[` + readySearch +
				`,{"search":{"collectionName":"slow","data":[[1,0]]}},` + readySearch + `]}`
			start := time.Now()
			status, body := recordExecuteDQL(t, handler, strings.NewReader(payload), tc.header)
			require.Equal(t, tc.wantElapsed, time.Since(start), tc.name)
			require.Equal(t, http.StatusOK, status, tc.name)
			require.Zero(t, body.Code, tc.name)
			require.Len(t, body.Data, 3, tc.name)
			require.Zero(t, body.Data[0].Code, tc.name)
			require.NotNil(t, body.Data[0].Search, tc.name)
			require.Zero(t, body.Data[0].Search.Code, tc.name)
			require.Len(t, body.Data[0].Search.Data, 1, tc.name)
			for _, item := range body.Data[1:] {
				require.Equal(t, tc.wantCode, item.Code, tc.name)
				if tc.wantCode == 0 {
					require.NotNil(t, item.Search, tc.name)
					require.Zero(t, item.Search.Code, tc.name)
				} else {
					require.NotEmpty(t, item.Message, tc.name)
					require.Nil(t, item.Search, tc.name)
				}
			}
		}
	})
}

func TestExecuteDQLRESTTransportDeadlineEndsCall(t *testing.T) {
	for _, tc := range []struct {
		name         string
		timeoutField string
		wantHTTP     int
	}{
		{name: "header before body", timeoutField: `"timeoutMs":5000,`, wantHTTP: http.StatusRequestTimeout},
		{name: "header only", wantHTTP: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				canceled := make(chan error, 1)
				p := newExecuteDQLHTTPProxy(t, nil, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					if req.GetCollectionName() == "slow" {
						<-ctx.Done()
						canceled <- ctx.Err()
					}
					return successfulSearchResult(), nil
				})
				payload := `{` + tc.timeoutField + `"maxConcurrency":1,"requests":[` + readySearch +
					`,{"search":{"collectionName":"slow","data":[[1,0]]}},` + readySearch + `]}`
				start := time.Now()
				status, body := recordExecuteDQL(t, newExecuteDQLRESTHandler(p), strings.NewReader(payload), "1")
				require.Equal(t, time.Second, time.Since(start))
				require.Equal(t, tc.wantHTTP, status)
				if status == http.StatusRequestTimeout {
					require.Equal(t, merr.TimeoutCode, body.Code)
					require.Empty(t, body.Data)
				} else {
					require.Zero(t, body.Code)
					require.Len(t, body.Data, 3)
					require.Zero(t, body.Data[0].Code)
					require.NotNil(t, body.Data[0].Search)
					require.Zero(t, body.Data[0].Search.Code)
					require.NotEmpty(t, body.Data[0].Search.Data)
					require.Equal(t, merr.TimeoutCode, body.Data[1].Code)
					require.NotEmpty(t, body.Data[1].Message)
					require.Nil(t, body.Data[1].Search)
					require.Equal(t, merr.TimeoutCode, body.Data[2].Code)
					require.NotEmpty(t, body.Data[2].Message)
					require.Nil(t, body.Data[2].Search)
				}
				require.ErrorIs(t, <-canceled, context.DeadlineExceeded)
			})
		})
	}
}

func TestExecuteDQLRESTHeaderTimeoutValues(t *testing.T) {
	p := newExecuteDQLHTTPProxy(t, func(context.Context, string) {
		t.Error("prepared an item without an execution budget or with an invalid header")
	}, nil)
	handler := newExecuteDQLRESTHandler(p)
	for _, tc := range []struct {
		name, header, timeout string
		status                int
		code                  int32
	}{
		{name: "zero execution budget", header: "0", status: http.StatusOK},
		{name: "zero transport deadline", header: "0", timeout: `"timeoutMs":5000,`, status: http.StatusRequestTimeout, code: merr.TimeoutCode},
		{name: "negative", header: "-1", status: http.StatusOK, code: merr.Code(merr.ErrParameterInvalid)},
		{name: "invalid", header: "1.5", status: http.StatusOK, code: merr.Code(merr.ErrParameterInvalid)},
		{name: "empty", status: http.StatusOK, code: merr.Code(merr.ErrParameterInvalid)},
		{name: "duration overflow", header: "9223372037", status: http.StatusOK, code: merr.Code(merr.ErrParameterInvalid)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := `{` + tc.timeout + `"requests":[` + readySearch + `,` + readySearch + `]}`
			req := httptest.NewRequest(http.MethodPost, executeDQLRESTPath, strings.NewReader(payload))
			req.Header.Set(mhttp.HTTPHeaderRequestTimeout, tc.header)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			require.Equal(t, tc.status, response.Code)
			var body executeDQLRESTResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, tc.code, body.Code)
			if tc.code != 0 {
				require.Empty(t, body.Data)
				return
			}
			require.Len(t, body.Data, 2)
			for _, item := range body.Data {
				require.Equal(t, merr.TimeoutCode, item.Code)
				require.NotEmpty(t, item.Message)
				require.Nil(t, item.Search)
			}
		})
	}
}

func TestExecuteDQLRESTBudgetStartsBeforeAuthentication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExecuteDQLHTTPProxy(t, nil, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			t.Error("Search started after the batch execution deadline")
			return successfulSearchResult(), nil
		})
		handler := newExecuteDQLRESTHandler(p, func(c *gin.Context) {
			time.Sleep(20 * time.Millisecond)
			c.Next()
		})
		payload := `{"timeoutMs":10,"requests":[` + readySearch + `]}`
		status, body := recordExecuteDQL(t, handler, strings.NewReader(payload), "")
		require.Equal(t, http.StatusOK, status)
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 1)
		require.Equal(t, merr.TimeoutCode, body.Data[0].Code)
		require.NotEmpty(t, body.Data[0].Message)
		require.Nil(t, body.Data[0].Search)
	})
}

func TestRESTSearchReportsOriginalVectorErrorCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExecuteDQLHTTPProxy(t, nil, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			t.Error("Search ran after placeholder generation failed")
			return successfulSearchResult(), nil
		})
		nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
		rejected := metrics.ProxyFunctionCall.WithLabelValues(nodeID, "Search", metrics.RejectedLabel, metrics.CauseSystem, "default", "ready", "")
		before := testutil.ToFloat64(rejected)
		req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", strings.NewReader(
			`{"collectionName":"ready","annsField":"absent","data":[[1,0]]}`))
		response := httptest.NewRecorder()
		newExecuteDQLRESTHandler(p).ServeHTTP(response, req)
		var body struct {
			Code    int32
			Message string
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Equal(t, merr.Code(merr.ErrIncorrectParameterFormat), body.Code)
		require.Equal(t, merr.ErrIncorrectParameterFormat.Error()+", error: "+
			merr.WrapErrFieldNotFound("absent", "cannot find a vector field").Error(), body.Message)
		require.Equal(t, before+1, testutil.ToFloat64(rejected))
	})
}

type blockedExecuteDQLResponseWriter struct {
	*httptest.ResponseRecorder
	interrupted chan struct{}
}

func (w blockedExecuteDQLResponseWriter) SetWriteDeadline(time.Time) error {
	close(w.interrupted)
	return nil
}

func (w blockedExecuteDQLResponseWriter) FlushError() error {
	<-w.interrupted
	return context.DeadlineExceeded
}

func TestExecuteDQLRESTTransportDeadlineInterruptsFlush(t *testing.T) {
	for _, test := range []struct {
		name, payload string
		code          int32
	}{
		{"valid batch", `{"requests":[{"search":{}}],"timeoutMs":0}`, 0},
		{"invalid batch", `{"requests":[],"timeoutMs":100}`, merr.Code(merr.ErrParameterMissing)},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				handler := newExecuteDQLRESTHandler(proxy.NewExecuteDQLTestProxy(t, 1, 1))
				req := httptest.NewRequest(http.MethodPost, executeDQLRESTPath, strings.NewReader(test.payload))
				req.Header.Set(mhttp.HTTPHeaderRequestTimeout, "1")
				response := blockedExecuteDQLResponseWriter{httptest.NewRecorder(), make(chan struct{})}
				require.PanicsWithValue(t, http.ErrAbortHandler, func() {
					handler.ServeHTTP(response, req)
				})
				require.Equal(t, http.StatusOK, response.Code)
				require.Contains(t, response.Header().Get("Content-Type"), "application/json")
				var body executeDQLRESTResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
				require.Equal(t, test.code, body.Code)
			})
		})
	}
}

type shortExecuteDQLResponseWriter struct{ http.ResponseWriter }

func (w shortExecuteDQLResponseWriter) Write(p []byte) (int, error) {
	return w.ResponseWriter.Write(p[:min(6, len(p))])
}

func TestExecuteDQLRESTShortWriteFailsTransport(t *testing.T) {
	for _, test := range []struct{ name, payload, requestTimeout string }{
		{"valid batch", `{"requests":[{"search":{}}],"timeoutMs":0}`, ""},
		{"invalid batch", `{"requests":[]}`, "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newExecuteDQLRESTHandler(proxy.NewExecuteDQLTestProxy(t, 1, 1))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(shortExecuteDQLResponseWriter{w}, r)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+executeDQLRESTPath, strings.NewReader(test.payload))
			require.NoError(t, err)
			if test.requestTimeout != "" {
				req.Header.Set(mhttp.HTTPHeaderRequestTimeout, test.requestTimeout)
			}
			response, err := server.Client().Do(req)
			if err == nil {
				defer response.Body.Close()
				_, err = io.ReadAll(response.Body)
			}
			require.Error(t, err)
		})
	}
}

type executeDQLQuotaProxy struct {
	*proxy.Proxy
	cache   proxy.Cache
	search  proxy.ExecuteDQLExecute
	limiter types.Limiter
}

func (p *executeDQLQuotaProxy) GetMetaCache() proxy.Cache { return p.cache }

func (p *executeDQLQuotaProxy) GetRateLimiter() (types.Limiter, error) { return p.limiter, nil }

func (p *executeDQLQuotaProxy) Search(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	if p.search != nil {
		return p.search(ctx, req)
	}
	return p.Proxy.Search(ctx, req)
}

type executeDQLQuotaCache struct{ proxy.Cache }

func (executeDQLQuotaCache) GetDatabaseInfo(_ context.Context, name string) (*metacache.DatabaseInfo, error) {
	ids := map[string]int64{"catalog_a": 10, "catalog_b": 20}
	return &metacache.DatabaseInfo{DBID: ids[name]}, nil
}

func (executeDQLQuotaCache) GetCollectionID(_ context.Context, db, name string) (int64, error) {
	ids := map[string]int64{"catalog_a/ready": 11, "catalog_b/ready": 21}
	return ids[db+"/"+name], nil
}

func (c executeDQLQuotaCache) GetPartitionInfo(ctx context.Context, db, collection, partition string) (*metacache.PartitionInfo, error) {
	partitions, err := c.GetPartitions(ctx, db, collection)
	if err != nil {
		return nil, err
	}
	id, ok := partitions[partition]
	if !ok {
		return nil, merr.WrapErrPartitionNotFound(partition)
	}
	return &metacache.PartitionInfo{PartitionID: id}, nil
}

func (executeDQLQuotaCache) GetPartitions(_ context.Context, db, collection string) (map[string]int64, error) {
	partitions := map[string]map[string]int64{
		"catalog_a/ready": {"east": 12, "south": 13},
		"catalog_b/ready": {"west": 22, "south": 23},
	}
	return partitions[db+"/"+collection], nil
}

type executeDQLQuotaCall struct {
	dbID       int64
	partitions map[int64][]int64
	rateType   internalpb.RateType
	nq         int
}

type executeDQLQuotaLimiter struct {
	types.Limiter
	mu        sync.Mutex
	calls     []executeDQLQuotaCall
	denyFirst bool
}

func (l *executeDQLQuotaLimiter) Check(dbID int64, parts map[int64][]int64, rateType internalpb.RateType, nq int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, executeDQLQuotaCall{dbID, parts, rateType, nq})
	if l.denyFirst && len(l.calls) == 1 {
		return merr.WrapErrServiceRateLimit(10)
	}
	return nil
}

func (l *executeDQLQuotaLimiter) snapshot() []executeDQLQuotaCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]executeDQLQuotaCall(nil), l.calls...)
}

func TestExecuteDQLRESTRetryChargesEachAttemptToItsTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metrics.ProxyFunctionCall.Reset()
		t.Cleanup(metrics.ProxyFunctionCall.Reset)
		var searched []string
		p := newExecuteDQLHTTPProxy(t, nil, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			searched = append(searched, req.GetDbName())
			return successfulSearchResult(), nil
		})
		params := paramtable.Get()
		require.NoError(t, params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, "true"))
		regexpKey := params.ProxyCfg.PartitionNameRegexp.Key
		require.NoError(t, params.Save(regexpKey, "true"))
		t.Cleanup(func() { require.NoError(t, params.Reset(regexpKey)) })
		p.cache = executeDQLQuotaCache{p.cache}
		limiter := &executeDQLQuotaLimiter{denyFirst: true}
		handler := newExecuteDQLRESTHandler(&executeDQLQuotaProxy{Proxy: p.Proxy, cache: p.cache, search: p.Search, limiter: limiter})
		payload := `{"maxConcurrency":1,"requests":[
			{"search":{"dbName":"catalog_a","collectionName":"ready","partitionNames":["e.*"],"data":[[1,0],[0,1]]}},
			{"search":{"dbName":"catalog_b","collectionName":"ready","partitionNames":["w.*"],"ids":[10,20,30]}}]}`
		status, body := recordExecuteDQL(t, handler, strings.NewReader(payload), "")
		require.Equal(t, http.StatusOK, status)
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 2)
		for _, item := range body.Data {
			require.Zero(t, item.Code)
			require.NotNil(t, item.Search)
			require.Zero(t, item.Search.Code)
		}
		first := executeDQLQuotaCall{10, map[int64][]int64{11: {12}}, internalpb.RateType_DQLSearch, 2}
		second := executeDQLQuotaCall{20, map[int64][]int64{21: {22}}, internalpb.RateType_DQLSearch, 3}
		require.Equal(t, []executeDQLQuotaCall{first, first, second}, limiter.snapshot())
		require.Equal(t, []string{"catalog_a", "catalog_b"}, searched)
		for _, expected := range []struct {
			method, db, collection, outcome, cause string
			count                                  float64
		}{
			{"Search", "catalog_a", "ready", metrics.TotalLabel, metrics.CauseNA, 2},
			{"Search", "catalog_a", "ready", metrics.RejectedLabel, metrics.CauseSystem, 1},
			{"Search", "catalog_a", "ready", metrics.SuccessLabel, metrics.CauseNA, 1},
			{"Search", "catalog_b", "ready", metrics.TotalLabel, metrics.CauseNA, 1},
			{"Search", "catalog_b", "ready", metrics.SuccessLabel, metrics.CauseNA, 1},
			{"ExecuteDQL", "", "", metrics.TotalLabel, metrics.CauseNA, 1},
			{"ExecuteDQL", "", "", metrics.SuccessLabel, metrics.CauseNA, 1},
		} {
			require.Equal(t, expected.count, testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(
				paramtable.GetStringNodeID(), expected.method, expected.outcome, expected.cause, expected.db, expected.collection, "")), "%+v", expected)
		}
		require.Equal(t, 7, testutil.CollectAndCount(metrics.ProxyFunctionCall))
	})
}

type executeDQLMetricsHook struct {
	hookutil.DefaultHook
	before func(context.Context, any) (context.Context, error)
	after  func(context.Context) error
}

func (h executeDQLMetricsHook) Before(ctx context.Context, req any, _ string) (context.Context, error) {
	return h.before(ctx, req)
}

func (h executeDQLMetricsHook) After(ctx context.Context, _ any, _ error, _ string) error {
	if h.after != nil {
		return h.after(ctx)
	}
	return nil
}

func TestExecuteDQLRESTRequestMetricsIsolateConcurrentAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metrics.ProxyFunctionCall.Reset()
		t.Cleanup(metrics.ProxyFunctionCall.Reset)
		hookutil.InitOnceHook()
		t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}) })
		var mu sync.Mutex
		attempts := make(map[string]int)
		hookutil.SetTestHook(executeDQLMetricsHook{before: func(ctx context.Context, req any) (context.Context, error) {
			db := req.(*milvuspb.SearchRequest).GetDbName()
			mu.Lock()
			attempts[db]++
			attempt := attempts[db]
			mu.Unlock()
			// The retry is unscoped: it must not inherit the first attempt's RG.
			if attempt == 1 {
				ctx = extension.WithQueryResourceGroup(ctx, "rg-"+db)
			}
			return ctx, nil
		}})
		entered := make(chan string, 2)
		release := make(chan struct{})
		releaseSearches := sync.OnceFunc(func() { close(release) })
		defer releaseSearches()
		p := newExecuteDQLHTTPProxy(t, nil, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			db := req.GetDbName()
			group := extension.QueryResourceGroupFromContext(ctx)
			if group != "" {
				entered <- group
				<-release
				assert.Equal(t, "rg-"+db, extension.QueryResourceGroupFromContext(ctx))
			}
			// Target metrics belong to the request at entry, even if execution
			// rewrites the private attempt's request.
			req.DbName, req.CollectionName = "rewritten", "rewritten"
			if db == "catalog_a" && group != "" {
				return &milvuspb.SearchResults{Status: merr.Status(merr.ErrServiceRateLimit)}, nil
			}
			return successfulSearchResult(), nil
		})
		handler := newExecuteDQLRESTHandler(p)
		request := httptest.NewRequest(http.MethodPost, executeDQLRESTPath, strings.NewReader(`{"maxConcurrency":2,"requests":[
			{"search":{"collectionName":"ready","data":[[1,0]]}},
			{"search":{"dbName":"catalog_b","collectionName":"ready","data":[[1,0]]}}]}`))
		request.Header.Set(httpserver.HTTPHeaderDBName, "catalog_a")
		response := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			handler.ServeHTTP(response, request)
		}()
		require.ElementsMatch(t, []string{"rg-catalog_a", "rg-catalog_b"}, []string{<-entered, <-entered})
		releaseSearches()
		<-done
		require.Equal(t, http.StatusOK, response.Code)
		var body executeDQLRESTResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 2)
		for _, item := range body.Data {
			require.Zero(t, item.Code)
			require.NotNil(t, item.Search)
			require.Zero(t, item.Search.Code)
		}
		require.Equal(t, map[string]int{"catalog_a": 2, "catalog_b": 1}, attempts)
		for _, expected := range []struct{ method, db, collection, group, outcome string }{
			{"Search", "catalog_a", "ready", "rg-catalog_a", metrics.RetryLabel},
			{"Search", "catalog_a", "ready", "", metrics.SuccessLabel},
			{"Search", "catalog_b", "ready", "rg-catalog_b", metrics.SuccessLabel},
			{"ExecuteDQL", "", "", "", metrics.SuccessLabel},
		} {
			for _, outcome := range []string{metrics.TotalLabel, expected.outcome} {
				require.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(
					paramtable.GetStringNodeID(), expected.method, outcome, metrics.CauseNA, expected.db, expected.collection, expected.group)), "%+v", expected)
			}
		}
		require.Equal(t, 8, testutil.CollectAndCount(metrics.ProxyFunctionCall), "no batch RG leakage or duplicate attempt counts")
	})
}

func TestExecuteDQLRESTRequestMetricsRecordRefusals(t *testing.T) {
	for _, phase := range []string{"before", "after", "execution"} {
		t.Run(phase, func(t *testing.T) {
			metrics.ProxyFunctionCall.Reset()
			t.Cleanup(metrics.ProxyFunctionCall.Reset)
			hookutil.InitOnceHook()
			t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}) })
			hookutil.SetTestHook(executeDQLMetricsHook{
				before: func(ctx context.Context, _ any) (context.Context, error) {
					if phase == "before" {
						return ctx, merr.ErrPrivilegeNotPermitted
					}
					return extension.WithQueryResourceGroup(ctx, "rg-a"), nil
				},
				after: func(context.Context) error {
					if phase == "after" {
						return merr.ErrPrivilegeNotPermitted
					}
					return nil
				},
			})
			p := newExecuteDQLHTTPProxy(t, nil, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				assert.NotEqual(t, "before", phase)
				return &milvuspb.SearchResults{Status: merr.Status(merr.ErrParameterInvalid)}, nil
			})
			_, body := recordExecuteDQL(t, newExecuteDQLRESTHandler(p), strings.NewReader(`{"requests":[`+readySearch+`]}`), "")
			require.Zero(t, body.Code)
			require.Len(t, body.Data, 1)
			require.NotZero(t, body.Data[0].Code)
			require.Nil(t, body.Data[0].Search)
			group, outcome := "rg-a", metrics.RejectedLabel
			switch phase {
			case "before":
				group = ""
			case "execution":
				outcome = metrics.FailLabel
			}
			for _, labels := range [][2]string{{metrics.TotalLabel, metrics.CauseNA}, {outcome, metrics.CauseUser}} {
				require.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(
					paramtable.GetStringNodeID(), "Search", labels[0], labels[1], "default", "ready", group)))
			}
			for _, label := range []string{metrics.TotalLabel, metrics.SuccessLabel} {
				require.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(
					paramtable.GetStringNodeID(), "ExecuteDQL", label, metrics.CauseNA, "", "", "")))
			}
			require.Equal(t, 4, testutil.CollectAndCount(metrics.ProxyFunctionCall))
		})
	}
}

type executeDQLTestShardPolicy struct {
	shardclient.LBPolicy
	node types.QueryNodeClient
}

func (p executeDQLTestShardPolicy) Execute(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
	return workload.Exec(ctx, 1, p.node, "channel")
}

func (executeDQLTestShardPolicy) UpdateCostMetrics(int64, *internalpb.CostAggregation) {}

type executeDQLTestQueryNode struct {
	types.QueryNodeClient
	searched chan *querypb.SearchRequest
}

func (p executeDQLTestQueryNode) Search(_ context.Context, req *querypb.SearchRequest, _ ...grpc.CallOption) (*internalpb.SearchResults, error) {
	p.searched <- req
	return &internalpb.SearchResults{
		Status:     merr.Success(),
		MetricType: "L2",
		ResultData: &schemapb.SearchResultData{
			NumQueries: 2, TopK: 1, Topks: []int64{1, 1}, Scores: []float32{-0.5, -0.6},
			Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{17, 18}}}},
		},
	}, nil
}

type pendingExecuteDQLTestQuery struct {
	taskmodel.BaseTask
	*taskmodel.TaskCondition
	id  int64
	ts  uint64
	run func(context.Context) error
}

func (t *pendingExecuteDQLTestQuery) TraceCtx() context.Context      { return t.Ctx() }
func (*pendingExecuteDQLTestQuery) Name() string                     { return taskmodel.QueryTaskName }
func (*pendingExecuteDQLTestQuery) Type() commonpb.MsgType           { return commonpb.MsgType_Retrieve }
func (t *pendingExecuteDQLTestQuery) ID() int64                      { return t.id }
func (t *pendingExecuteDQLTestQuery) SetID(id int64)                 { t.id = id }
func (t *pendingExecuteDQLTestQuery) BeginTs() uint64                { return t.ts }
func (t *pendingExecuteDQLTestQuery) EndTs() uint64                  { return t.ts }
func (t *pendingExecuteDQLTestQuery) SetTs(ts uint64)                { t.ts = ts }
func (*pendingExecuteDQLTestQuery) OnEnqueue() error                 { return nil }
func (*pendingExecuteDQLTestQuery) PreExecute(context.Context) error { return nil }
func (t *pendingExecuteDQLTestQuery) Execute(ctx context.Context) error {
	if t.run != nil {
		return t.run(ctx)
	}
	return nil
}
func (*pendingExecuteDQLTestQuery) PostExecute(context.Context) error { return nil }

func TestExecuteDQLRESTRetriesFullDQLQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := paramtable.Get()
		for key, value := range map[string]string{
			params.QuotaConfig.QuotaAndLimitsEnabled.Key: "true",
			params.CommonCfg.AuthorizationEnabled.Key:    "false",
			params.ProxyCfg.MaxTaskNum.Key:               "1",
			params.ProxyCfg.EnableAutoLoad.Key:           "false",
			params.HTTPCfg.DQLAdmissionEnabled.Key:       "true",
		} {
			require.NoError(t, params.Save(key, value))
			t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
		}
		const db, collection = "catalog", "ready"
		cache := proxy.InitEmptyMetaCacheForTest()
		cache.SeedDBInfoForTest(db, &metacache.DatabaseInfo{DBID: 10})
		info := cache.SeedCollectionForTest(db, collection, 11)
		info.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
		schema, err := metacache.NewSchemaInfo(&schemapb.CollectionSchema{
			Name: collection,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{
					FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}},
				},
			},
		})
		require.NoError(t, err)
		info.Schema = schema
		cache.SeedPartitionCacheForTest(11, []*metacache.PartitionInfo{{Name: "east", PartitionID: 12}}, false)

		p := proxy.NewExecuteDQLTestProxy(t, 1, 1)
		searched := make(chan *querypb.SearchRequest, 2)
		sched := proxy.SetExecuteDQLSearchDependenciesForTest(p, cache, executeDQLTestShardPolicy{node: executeDQLTestQueryNode{searched: searched}})
		started := make(chan struct{})
		release := make(chan struct{})
		releaseQueries := sync.OnceFunc(func() { close(release) })
		defer releaseQueries()
		running := &pendingExecuteDQLTestQuery{
			TaskCondition: taskmodel.NewTaskCondition(t.Context()),
			run: func(ctx context.Context) error {
				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}
		require.NoError(t, sched.DqQueue.Enqueue(running))
		<-started
		dispatching := &pendingExecuteDQLTestQuery{TaskCondition: taskmodel.NewTaskCondition(t.Context())}
		require.NoError(t, sched.DqQueue.Enqueue(dispatching))
		synctest.Wait() // The DQL dispatcher is now waiting for its occupied worker.
		queued := &pendingExecuteDQLTestQuery{TaskCondition: taskmodel.NewTaskCondition(t.Context())}
		require.NoError(t, sched.DqQueue.Enqueue(queued))
		require.True(t, p.IsDQLQueueFull())
		require.NoError(t, p.CheckExecuteDQLAdmission())

		limiter := &executeDQLQuotaLimiter{}
		handler := newExecuteDQLRESTHandler(&executeDQLQuotaProxy{Proxy: p, cache: cache, limiter: limiter})
		payload := `{"timeoutMs":1000,"requests":[{"search":{"dbName":"catalog","collectionName":"ready","partitionNames":["east"],"limit":1,"data":[[1,0],[0,1]]}}]}`
		req := httptest.NewRequest(http.MethodPost, executeDQLRESTPath, strings.NewReader(payload))
		response := httptest.NewRecorder()
		done := make(chan struct{})
		start := time.Now()
		go func() {
			defer close(done)
			handler.ServeHTTP(response, req)
		}()

		// Before the retry timer can advance, the first attempt has tried to
		// enqueue in the full queue and reached its backoff.
		synctest.Wait()
		charge := executeDQLQuotaCall{10, map[int64][]int64{11: {12}}, internalpb.RateType_DQLSearch, 2}
		require.Equal(t, []executeDQLQuotaCall{charge}, limiter.snapshot())
		require.Empty(t, searched)
		require.Empty(t, done)

		releaseQueries()
		synctest.Wait()
		require.False(t, p.IsDQLQueueFull())
		<-done
		for _, task := range []*pendingExecuteDQLTestQuery{running, dispatching, queued} {
			require.NoError(t, task.WaitToFinish())
		}
		require.Equal(t, 200*time.Millisecond, time.Since(start))
		require.Equal(t, []executeDQLQuotaCall{charge, charge}, limiter.snapshot())
		require.Len(t, searched, 1)
		search := (<-searched).GetReq()
		require.Equal(t, int64(11), search.GetCollectionID())
		require.Equal(t, []int64{12}, search.GetPartitionIDs())
		require.Equal(t, int64(2), search.GetNq())

		require.Equal(t, http.StatusOK, response.Code)
		var body executeDQLRESTResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 1)
		require.Zero(t, body.Data[0].Code)
		require.NotNil(t, body.Data[0].Search)
		item := body.Data[0].Search
		require.Zero(t, item.Code, response.Body.String())
		require.Equal(t, []int64{1, 1}, item.Topks)
		require.Len(t, item.Data, 2)
		require.Equal(t, "17", item.Data[0]["id"])
		require.Equal(t, "18", item.Data[1]["id"])
	})
}

func TestRESTSearchKeepsOrdinaryPartitionQuotaLookup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var searched []int64
		p := newExecuteDQLHTTPProxy(t, nil, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			searched = append(searched, req.GetNq())
			return successfulSearchResult(), nil
		})
		params := paramtable.Get()
		require.NoError(t, params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, "true"))
		regexpKey := params.ProxyCfg.PartitionNameRegexp.Key
		require.NoError(t, params.Save(regexpKey, "true"))
		t.Cleanup(func() { require.NoError(t, params.Reset(regexpKey)) })
		p.cache = executeDQLQuotaCache{p.cache}
		limiter := &executeDQLQuotaLimiter{}
		handler := newExecuteDQLRESTHandler(&executeDQLQuotaProxy{Proxy: p.Proxy, cache: p.cache, search: p.Search, limiter: limiter})
		search := func(partition string) int32 {
			payload := `{"dbName":"catalog_a","collectionName":"ready","partitionNames":["` + partition + `"],"data":[[1,0],[0,1]]}`
			req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", strings.NewReader(payload))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			require.Equal(t, http.StatusOK, response.Code)
			var body struct{ Code int32 }
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			return body.Code
		}
		require.Zero(t, search("e.*"), "an exact quota lookup miss lets ordinary Search resolve the partition pattern")
		require.Empty(t, limiter.snapshot())
		require.Zero(t, search("east"))
		require.Equal(t, []executeDQLQuotaCall{
			{dbID: 10, partitions: map[int64][]int64{11: {12}}, rateType: internalpb.RateType_DQLSearch, nq: 2},
		}, limiter.snapshot())
		require.Equal(t, []int64{0, 0}, searched)
	})
}

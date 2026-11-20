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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy"
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

const multiSearchRESTPath = "/v2/vectordb/entities/multi_search"

const readySearch = `{"search":{"collectionName":"ready","data":[[1,0]]}}`

type multiSearchRESTResponse struct {
	Code    int32
	Message string
	Data    []struct {
		Search struct {
			Code  int32
			Data  []map[string]any
			Topks []int64
		}
	}
}

func newMultiSearchRESTHandler(p types.ProxyComponent, middleware ...gin.HandlerFunc) http.Handler {
	router := gin.New()
	router.Use(middleware...)
	group := router.Group("/v2/vectordb", func(c *gin.Context) {
		c.Set(httpserver.ContextUsername, "root")
	})
	httpserver.NewHandlersV2(p).RegisterRoutesToV2(group)
	return httpserver.DQLStreamingHTTPHandler(router)
}

type multiSearchHTTPProxy struct {
	*proxy.Proxy
	cache  proxy.Cache
	search func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)
}

func (p *multiSearchHTTPProxy) GetMetaCache() proxy.Cache { return p.cache }

func (p *multiSearchHTTPProxy) Search(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return p.search(ctx, req)
}

// The HTTP fixture has one Proxy; these methods stand in for the coordinator's
// fan-out while keeping the real local registry and cancellation handlers.
func (p *multiSearchHTTPProxy) ListRunningRequests(ctx context.Context, req *milvuspb.ListRunningRequestsRequest) (*milvuspb.ListRunningRequestsResponse, error) {
	return p.ListLocalRunningRequests(ctx, req)
}

func (p *multiSearchHTTPProxy) CancelRequests(ctx context.Context, req *milvuspb.CancelRequestsRequest) (*milvuspb.CancelRequestsResponse, error) {
	return p.CancelLocalRequests(ctx, req)
}

type multiSearchHTTPCache struct {
	proxy.Cache
	schema   *metacache.SchemaInfo
	onSchema func(context.Context, string)
}

func (m *multiSearchHTTPCache) GetCollectionSchema(ctx context.Context, _, collection string) (*metacache.SchemaInfo, error) {
	if m.onSchema != nil {
		m.onSchema(ctx, collection)
	}
	return m.schema, nil
}

func newMultiSearchHTTPProxy(t *testing.T, onSchema func(context.Context, string), search func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)) *multiSearchHTTPProxy {
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
	return &multiSearchHTTPProxy{
		Proxy:  proxy.NewMultiSearchTestProxy(t, 1, 1),
		cache:  &multiSearchHTTPCache{schema: schema, onSchema: onSchema},
		search: search,
	}
}

func successfulSearchResult() *milvuspb.SearchResults {
	return &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{
		TopK: 1, NumQueries: 1, Topks: []int64{1}, Scores: []float32{0.5},
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{9007199254740993}}}},
	}}
}

func recordMultiSearch(t *testing.T, handler http.Handler, payload io.Reader, requestTimeout string) (int, multiSearchRESTResponse) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, multiSearchRESTPath, payload)
	if requestTimeout != "" {
		request.Header.Set(mhttp.HTTPHeaderRequestTimeout, requestTimeout)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var body multiSearchRESTResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	return response.Code, body
}

func TestMultiSearchRESTExplicitZeroBudget(t *testing.T) {
	server := httptest.NewServer(newMultiSearchRESTHandler(proxy.NewMultiSearchTestProxy(t, 1, 1)))
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Post(server.URL+multiSearchRESTPath, "application/json", strings.NewReader(
		`{"requests":[{"search":{}},{"search":{}}],"timeoutMs":0}`))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var body multiSearchRESTResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Zero(t, body.Code)
	require.Len(t, body.Data, 2)
	for _, item := range body.Data {
		require.Equal(t, merr.TimeoutCode, item.Search.Code)
	}
}

func TestMultiSearchRESTListAndCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stopped := make(chan error, 2)
		p := newMultiSearchHTTPProxy(t, nil, func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			<-ctx.Done()
			stopped <- context.Cause(ctx)
			return nil, ctx.Err()
		})
		handler := newMultiSearchRESTHandler(p)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		request := httptest.NewRequestWithContext(ctx, http.MethodPost, multiSearchRESTPath,
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
		require.Equal(t, "MultiSearch", batch.Type)
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
		var result multiSearchRESTResponse
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

func TestMultiSearchRESTDeliversPartialResultsAfterExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releasePrepare := sync.OnceFunc(func() { close(release) })
		defer releasePrepare()
		canceled := make(chan error, 1)
		p := newMultiSearchHTTPProxy(t, func(ctx context.Context, collection string) {
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
		status, body := recordMultiSearch(t, newMultiSearchRESTHandler(p), strings.NewReader(payload), "")
		require.Equal(t, http.StatusOK, status)
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 4)
		require.Zero(t, body.Data[0].Search.Code)
		require.Equal(t, []int64{1}, body.Data[0].Search.Topks)
		require.Len(t, body.Data[0].Search.Data, 1)
		require.Equal(t, "9007199254740993", body.Data[0].Search.Data[0]["id"])
		require.Equal(t, merr.Code(merr.ErrMissingRequiredParameters), body.Data[1].Search.Code)
		require.Equal(t, merr.TimeoutCode, body.Data[2].Search.Code)
		require.Empty(t, body.Data[2].Search.Data)
		require.Equal(t, merr.TimeoutCode, body.Data[3].Search.Code)
		require.ErrorIs(t, <-canceled, context.DeadlineExceeded)

		releasePrepare()
		synctest.Wait()
	})
}

type slowMultiSearchReader struct{ io.Reader }

func (r slowMultiSearchReader) Read(p []byte) (int, error) {
	time.Sleep(10 * time.Millisecond)
	return r.Reader.Read(p)
}

func TestMultiSearchRESTBudgetStartsBeforeDecode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		timeout := &paramtable.Get().HTTPCfg.RequestTimeoutMs
		previous := timeout.SwapTempValue("5")
		defer timeout.SwapTempValue(previous)
		p := newMultiSearchHTTPProxy(t, func(context.Context, string) {
			t.Error("prepared a search after the budget expired during decoding")
		}, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			return successfulSearchResult(), nil
		})
		handler := newMultiSearchRESTHandler(p)
		for _, payload := range []string{
			`{"requests":[` + readySearch + `]}`,
			`{"timeoutMs":5,"requests":[` + readySearch + `]}`,
		} {
			status, body := recordMultiSearch(t, handler, slowMultiSearchReader{strings.NewReader(payload)}, "")
			require.Equal(t, http.StatusOK, status)
			require.Zero(t, body.Code)
			require.Len(t, body.Data, 1)
			require.Equal(t, merr.TimeoutCode, body.Data[0].Search.Code)
		}
	})
}

func TestMultiSearchRESTReturnsAsSoonAsAllItemsFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newMultiSearchHTTPProxy(t, nil, func(_ context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			time.Sleep(100 * time.Millisecond)
			return successfulSearchResult(), nil
		})
		handler := newMultiSearchRESTHandler(p)
		for _, payload := range []string{
			`{"requests":[` + readySearch + `]}`,
			`{"timeoutMs":10000,"requests":[` + readySearch + `]}`,
		} {
			start := time.Now()
			status, body := recordMultiSearch(t, handler, strings.NewReader(payload), "")
			require.Equal(t, http.StatusOK, status)
			require.Zero(t, body.Code)
			require.Len(t, body.Data, 1)
			require.Zero(t, body.Data[0].Search.Code)
			require.Equal(t, 100*time.Millisecond, time.Since(start))
		}
	})
}

func TestMultiSearchRESTDefaultBudgetAndOverrides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		timeout := &paramtable.Get().HTTPCfg.RequestTimeoutMs
		previous := timeout.SwapTempValue("50")
		defer timeout.SwapTempValue(previous)
		p := newMultiSearchHTTPProxy(t, nil, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			if req.GetCollectionName() == "slow" {
				select {
				case <-time.After(200 * time.Millisecond):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return successfulSearchResult(), nil
		})
		handler := newMultiSearchRESTHandler(p)
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
			status, body := recordMultiSearch(t, handler, strings.NewReader(payload), tc.header)
			require.Equal(t, tc.wantElapsed, time.Since(start), tc.name)
			require.Equal(t, http.StatusOK, status, tc.name)
			require.Zero(t, body.Code, tc.name)
			require.Len(t, body.Data, 3, tc.name)
			require.Zero(t, body.Data[0].Search.Code, tc.name)
			require.Len(t, body.Data[0].Search.Data, 1, tc.name)
			require.Equal(t, tc.wantCode, body.Data[1].Search.Code, tc.name)
			require.Equal(t, tc.wantCode, body.Data[2].Search.Code, tc.name)
		}
	})
}

func TestMultiSearchRESTTransportDeadlineEndsCall(t *testing.T) {
	for _, tc := range []struct {
		name         string
		timeoutField string
	}{
		{name: "header before body", timeoutField: `"timeoutMs":5000,`},
		{name: "header only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				canceled := make(chan error, 1)
				p := newMultiSearchHTTPProxy(t, nil, func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
					<-ctx.Done()
					canceled <- ctx.Err()
					return successfulSearchResult(), nil
				})
				payload := `{` + tc.timeoutField + `"requests":[` + readySearch + `]}`
				start := time.Now()
				status, body := recordMultiSearch(t, newMultiSearchRESTHandler(p), strings.NewReader(payload), "1")
				require.Equal(t, time.Second, time.Since(start))
				require.Equal(t, http.StatusRequestTimeout, status)
				require.Equal(t, merr.TimeoutCode, body.Code)
				require.Empty(t, body.Data)
				require.ErrorIs(t, <-canceled, context.DeadlineExceeded)
			})
		})
	}
}

func TestMultiSearchRESTBudgetStartsBeforeAuthentication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newMultiSearchHTTPProxy(t, nil, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			t.Error("Search started after the batch execution deadline")
			return successfulSearchResult(), nil
		})
		handler := newMultiSearchRESTHandler(p, func(c *gin.Context) {
			time.Sleep(20 * time.Millisecond)
			c.Next()
		})
		payload := `{"timeoutMs":10,"requests":[` + readySearch + `]}`
		status, body := recordMultiSearch(t, handler, strings.NewReader(payload), "")
		require.Equal(t, http.StatusOK, status)
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 1)
		require.Equal(t, merr.TimeoutCode, body.Data[0].Search.Code)
	})
}

func TestRESTSearchReportsOriginalVectorErrorCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newMultiSearchHTTPProxy(t, nil, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			t.Error("Search ran after placeholder generation failed")
			return successfulSearchResult(), nil
		})
		nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
		rejected := metrics.ProxyFunctionCall.WithLabelValues(nodeID, "Search", metrics.RejectedLabel, metrics.CauseSystem, "default", "ready", "")
		before := testutil.ToFloat64(rejected)
		req := httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", strings.NewReader(
			`{"collectionName":"ready","annsField":"absent","data":[[1,0]]}`))
		response := httptest.NewRecorder()
		newMultiSearchRESTHandler(p).ServeHTTP(response, req)
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

type blockedMultiSearchResponseWriter struct {
	*httptest.ResponseRecorder
	interrupted chan struct{}
}

func (w blockedMultiSearchResponseWriter) SetWriteDeadline(time.Time) error {
	close(w.interrupted)
	return nil
}

func (w blockedMultiSearchResponseWriter) FlushError() error {
	<-w.interrupted
	return context.DeadlineExceeded
}

func TestMultiSearchRESTTransportDeadlineInterruptsFlush(t *testing.T) {
	for _, test := range []struct {
		name, payload string
		capacity      int
		status        int
		code          int32
	}{
		{"valid batch", `{"requests":[{"search":{}}],"timeoutMs":0}`, 1, http.StatusOK, 0},
		{"decode failure", "{", 1, http.StatusOK, merr.Code(merr.ErrIncorrectParameterFormat)},
		{"invalid batch", `{"requests":[]}`, 1, http.StatusOK, merr.Code(merr.ErrParameterMissing)},
		{"admission rejection", "{", 0, http.StatusTooManyRequests, merr.Code(merr.ErrServiceTooManyRequests)},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				handler := newMultiSearchRESTHandler(proxy.NewMultiSearchTestProxy(t, test.capacity, 1))
				req := httptest.NewRequest(http.MethodPost, multiSearchRESTPath, strings.NewReader(test.payload))
				req.Header.Set(mhttp.HTTPHeaderRequestTimeout, "1")
				response := blockedMultiSearchResponseWriter{httptest.NewRecorder(), make(chan struct{})}
				require.PanicsWithValue(t, http.ErrAbortHandler, func() {
					handler.ServeHTTP(response, req)
				})
				require.Equal(t, test.status, response.Code)
				require.Contains(t, response.Header().Get("Content-Type"), "application/json")
				if test.status == http.StatusTooManyRequests {
					require.Equal(t, "1", response.Header().Get("Retry-After"))
					require.Equal(t, "Retry-After", response.Header().Get("Access-Control-Expose-Headers"))
				}
				var body multiSearchRESTResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
				require.Equal(t, test.code, body.Code)
			})
		})
	}
}

type shortMultiSearchResponseWriter struct{ http.ResponseWriter }

func (w shortMultiSearchResponseWriter) Write(p []byte) (int, error) {
	return w.ResponseWriter.Write(p[:min(6, len(p))])
}

func TestMultiSearchRESTShortWriteFailsTransport(t *testing.T) {
	for _, test := range []struct{ name, payload, requestTimeout string }{
		{"valid batch", `{"requests":[{"search":{}}],"timeoutMs":0}`, ""},
		{"invalid batch", `{"requests":[]}`, "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newMultiSearchRESTHandler(proxy.NewMultiSearchTestProxy(t, 1, 1))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(shortMultiSearchResponseWriter{w}, r)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+multiSearchRESTPath, strings.NewReader(test.payload))
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

type multiSearchQuotaProxy struct {
	*proxy.Proxy
	cache   proxy.Cache
	search  proxy.MultiSearchExecute
	limiter types.Limiter
}

func (p *multiSearchQuotaProxy) GetMetaCache() proxy.Cache { return p.cache }

func (p *multiSearchQuotaProxy) GetRateLimiter() (types.Limiter, error) { return p.limiter, nil }

func (p *multiSearchQuotaProxy) Search(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	if p.search != nil {
		return p.search(ctx, req)
	}
	return p.Proxy.Search(ctx, req)
}

type multiSearchQuotaCache struct{ proxy.Cache }

func (multiSearchQuotaCache) GetDatabaseInfo(_ context.Context, name string) (*metacache.DatabaseInfo, error) {
	ids := map[string]int64{"catalog_a": 10, "catalog_b": 20}
	return &metacache.DatabaseInfo{DBID: ids[name]}, nil
}

func (multiSearchQuotaCache) GetCollectionID(_ context.Context, db, name string) (int64, error) {
	ids := map[string]int64{"catalog_a/ready": 11, "catalog_b/ready": 21}
	return ids[db+"/"+name], nil
}

func (c multiSearchQuotaCache) GetPartitionInfo(ctx context.Context, db, collection, partition string) (*metacache.PartitionInfo, error) {
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

func (multiSearchQuotaCache) GetPartitions(_ context.Context, db, collection string) (map[string]int64, error) {
	partitions := map[string]map[string]int64{
		"catalog_a/ready": {"east": 12, "south": 13},
		"catalog_b/ready": {"west": 22, "south": 23},
	}
	return partitions[db+"/"+collection], nil
}

type multiSearchQuotaCall struct {
	dbID       int64
	partitions map[int64][]int64
	rateType   internalpb.RateType
	nq         int
}

type multiSearchQuotaLimiter struct {
	types.Limiter
	mu        sync.Mutex
	calls     []multiSearchQuotaCall
	denyFirst bool
}

func (l *multiSearchQuotaLimiter) Check(dbID int64, parts map[int64][]int64, rateType internalpb.RateType, nq int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, multiSearchQuotaCall{dbID, parts, rateType, nq})
	if l.denyFirst && len(l.calls) == 1 {
		return merr.WrapErrServiceRateLimit(10)
	}
	return nil
}

func (l *multiSearchQuotaLimiter) snapshot() []multiSearchQuotaCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]multiSearchQuotaCall(nil), l.calls...)
}

func TestMultiSearchRESTRetryChargesEachAttemptToItsTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metrics.ProxyFunctionCall.Reset()
		t.Cleanup(metrics.ProxyFunctionCall.Reset)
		var searched []string
		p := newMultiSearchHTTPProxy(t, nil, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			searched = append(searched, req.GetDbName())
			return successfulSearchResult(), nil
		})
		params := paramtable.Get()
		require.NoError(t, params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, "true"))
		regexpKey := params.ProxyCfg.PartitionNameRegexp.Key
		require.NoError(t, params.Save(regexpKey, "true"))
		t.Cleanup(func() { require.NoError(t, params.Reset(regexpKey)) })
		p.cache = multiSearchQuotaCache{p.cache}
		limiter := &multiSearchQuotaLimiter{denyFirst: true}
		handler := newMultiSearchRESTHandler(&multiSearchQuotaProxy{Proxy: p.Proxy, cache: p.cache, search: p.Search, limiter: limiter})
		payload := `{"maxConcurrency":1,"requests":[
			{"search":{"dbName":"catalog_a","collectionName":"ready","partitionNames":["e.*"],"data":[[1,0],[0,1]]}},
			{"search":{"dbName":"catalog_b","collectionName":"ready","partitionNames":["w.*"],"ids":[10,20,30]}}]}`
		status, body := recordMultiSearch(t, handler, strings.NewReader(payload), "")
		require.Equal(t, http.StatusOK, status)
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 2)
		for _, item := range body.Data {
			require.Zero(t, item.Search.Code)
		}
		first := multiSearchQuotaCall{10, map[int64][]int64{11: {12}}, internalpb.RateType_DQLSearch, 2}
		second := multiSearchQuotaCall{20, map[int64][]int64{21: {22}}, internalpb.RateType_DQLSearch, 3}
		require.Equal(t, []multiSearchQuotaCall{first, first, second}, limiter.snapshot())
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
			{"MultiSearch", "", "", metrics.TotalLabel, metrics.CauseNA, 1},
			{"MultiSearch", "", "", metrics.SuccessLabel, metrics.CauseNA, 1},
		} {
			require.Equal(t, expected.count, testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(
				paramtable.GetStringNodeID(), expected.method, expected.outcome, expected.cause, expected.db, expected.collection, "")), "%+v", expected)
		}
		require.Equal(t, 7, testutil.CollectAndCount(metrics.ProxyFunctionCall))
	})
}

type multiSearchMetricsHook struct {
	hookutil.DefaultHook
	before func(context.Context, any) (context.Context, error)
	after  func(context.Context) error
}

func (h multiSearchMetricsHook) Before(ctx context.Context, req any, _ string) (context.Context, error) {
	return h.before(ctx, req)
}

func (h multiSearchMetricsHook) After(ctx context.Context, _ any, _ error, _ string) error {
	if h.after != nil {
		return h.after(ctx)
	}
	return nil
}

func TestMultiSearchRESTRequestMetricsIsolateConcurrentAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metrics.ProxyFunctionCall.Reset()
		t.Cleanup(metrics.ProxyFunctionCall.Reset)
		hookutil.InitOnceHook()
		t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}) })
		var mu sync.Mutex
		attempts := make(map[string]int)
		hookutil.SetTestHook(multiSearchMetricsHook{before: func(ctx context.Context, req any) (context.Context, error) {
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
		p := newMultiSearchHTTPProxy(t, nil, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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
		handler := newMultiSearchRESTHandler(p)
		request := httptest.NewRequest(http.MethodPost, multiSearchRESTPath, strings.NewReader(`{"maxConcurrency":2,"requests":[
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
		var body multiSearchRESTResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 2)
		for _, item := range body.Data {
			require.Zero(t, item.Search.Code)
		}
		require.Equal(t, map[string]int{"catalog_a": 2, "catalog_b": 1}, attempts)
		for _, expected := range []struct{ method, db, collection, group, outcome string }{
			{"Search", "catalog_a", "ready", "rg-catalog_a", metrics.RetryLabel},
			{"Search", "catalog_a", "ready", "", metrics.SuccessLabel},
			{"Search", "catalog_b", "ready", "rg-catalog_b", metrics.SuccessLabel},
			{"MultiSearch", "", "", "", metrics.SuccessLabel},
		} {
			for _, outcome := range []string{metrics.TotalLabel, expected.outcome} {
				require.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(
					paramtable.GetStringNodeID(), expected.method, outcome, metrics.CauseNA, expected.db, expected.collection, expected.group)), "%+v", expected)
			}
		}
		require.Equal(t, 8, testutil.CollectAndCount(metrics.ProxyFunctionCall), "no batch RG leakage or duplicate attempt counts")
	})
}

func TestMultiSearchRESTRequestMetricsRecordRefusals(t *testing.T) {
	for _, phase := range []string{"before", "after", "execution"} {
		t.Run(phase, func(t *testing.T) {
			metrics.ProxyFunctionCall.Reset()
			t.Cleanup(metrics.ProxyFunctionCall.Reset)
			hookutil.InitOnceHook()
			t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}) })
			hookutil.SetTestHook(multiSearchMetricsHook{
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
			p := newMultiSearchHTTPProxy(t, nil, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				assert.NotEqual(t, "before", phase)
				return &milvuspb.SearchResults{Status: merr.Status(merr.ErrParameterInvalid)}, nil
			})
			_, body := recordMultiSearch(t, newMultiSearchRESTHandler(p), strings.NewReader(`{"requests":[`+readySearch+`]}`), "")
			require.Zero(t, body.Code)
			require.Len(t, body.Data, 1)
			require.NotZero(t, body.Data[0].Search.Code)
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
					paramtable.GetStringNodeID(), "MultiSearch", label, metrics.CauseNA, "", "", "")))
			}
			require.Equal(t, 4, testutil.CollectAndCount(metrics.ProxyFunctionCall))
		})
	}
}

type multiSearchTestShardPolicy struct {
	shardclient.LBPolicy
	node types.QueryNodeClient
}

func (p multiSearchTestShardPolicy) Execute(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
	return workload.Exec(ctx, 1, p.node, "channel")
}

func (multiSearchTestShardPolicy) UpdateCostMetrics(int64, *internalpb.CostAggregation) {}

type multiSearchTestQueryNode struct {
	types.QueryNodeClient
	searched chan *querypb.SearchRequest
}

func (p multiSearchTestQueryNode) Search(_ context.Context, req *querypb.SearchRequest, _ ...grpc.CallOption) (*internalpb.SearchResults, error) {
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

type pendingMultiSearchTestQuery struct {
	taskmodel.BaseTask
	*taskmodel.TaskCondition
	id  int64
	ts  uint64
	run func(context.Context) error
}

func (t *pendingMultiSearchTestQuery) TraceCtx() context.Context      { return t.Ctx() }
func (*pendingMultiSearchTestQuery) Name() string                     { return taskmodel.QueryTaskName }
func (*pendingMultiSearchTestQuery) Type() commonpb.MsgType           { return commonpb.MsgType_Retrieve }
func (t *pendingMultiSearchTestQuery) ID() int64                      { return t.id }
func (t *pendingMultiSearchTestQuery) SetID(id int64)                 { t.id = id }
func (t *pendingMultiSearchTestQuery) BeginTs() uint64                { return t.ts }
func (t *pendingMultiSearchTestQuery) EndTs() uint64                  { return t.ts }
func (t *pendingMultiSearchTestQuery) SetTs(ts uint64)                { t.ts = ts }
func (*pendingMultiSearchTestQuery) OnEnqueue() error                 { return nil }
func (*pendingMultiSearchTestQuery) PreExecute(context.Context) error { return nil }
func (t *pendingMultiSearchTestQuery) Execute(ctx context.Context) error {
	if t.run != nil {
		return t.run(ctx)
	}
	return nil
}
func (*pendingMultiSearchTestQuery) PostExecute(context.Context) error { return nil }

func TestMultiSearchRESTRetriesFullDQLQueue(t *testing.T) {
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

		p := proxy.NewMultiSearchTestProxy(t, 1, 1)
		searched := make(chan *querypb.SearchRequest, 2)
		sched := proxy.SetMultiSearchSearchDependenciesForTest(p, cache, multiSearchTestShardPolicy{node: multiSearchTestQueryNode{searched: searched}})
		started := make(chan struct{})
		release := make(chan struct{})
		releaseQueries := sync.OnceFunc(func() { close(release) })
		defer releaseQueries()
		running := &pendingMultiSearchTestQuery{
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
		dispatching := &pendingMultiSearchTestQuery{TaskCondition: taskmodel.NewTaskCondition(t.Context())}
		require.NoError(t, sched.DqQueue.Enqueue(dispatching))
		synctest.Wait() // The DQL dispatcher is now waiting for its occupied worker.
		queued := &pendingMultiSearchTestQuery{TaskCondition: taskmodel.NewTaskCondition(t.Context())}
		require.NoError(t, sched.DqQueue.Enqueue(queued))
		require.True(t, p.IsDQLQueueFull())
		require.NoError(t, p.CheckMultiSearchAdmission())

		limiter := &multiSearchQuotaLimiter{}
		handler := newMultiSearchRESTHandler(&multiSearchQuotaProxy{Proxy: p, cache: cache, limiter: limiter})
		payload := `{"timeoutMs":1000,"requests":[{"search":{"dbName":"catalog","collectionName":"ready","partitionNames":["east"],"limit":1,"data":[[1,0],[0,1]]}}]}`
		req := httptest.NewRequest(http.MethodPost, multiSearchRESTPath, strings.NewReader(payload))
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
		charge := multiSearchQuotaCall{10, map[int64][]int64{11: {12}}, internalpb.RateType_DQLSearch, 2}
		require.Equal(t, []multiSearchQuotaCall{charge}, limiter.snapshot())
		require.Empty(t, searched)
		require.Empty(t, done)

		releaseQueries()
		synctest.Wait()
		require.False(t, p.IsDQLQueueFull())
		<-done
		for _, task := range []*pendingMultiSearchTestQuery{running, dispatching, queued} {
			require.NoError(t, task.WaitToFinish())
		}
		require.Equal(t, 200*time.Millisecond, time.Since(start))
		require.Equal(t, []multiSearchQuotaCall{charge, charge}, limiter.snapshot())
		require.Len(t, searched, 1)
		search := (<-searched).GetReq()
		require.Equal(t, int64(11), search.GetCollectionID())
		require.Equal(t, []int64{12}, search.GetPartitionIDs())
		require.Equal(t, int64(2), search.GetNq())

		require.Equal(t, http.StatusOK, response.Code)
		var body multiSearchRESTResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 1)
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
		p := newMultiSearchHTTPProxy(t, nil, func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			searched = append(searched, req.GetNq())
			return successfulSearchResult(), nil
		})
		params := paramtable.Get()
		require.NoError(t, params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, "true"))
		regexpKey := params.ProxyCfg.PartitionNameRegexp.Key
		require.NoError(t, params.Save(regexpKey, "true"))
		t.Cleanup(func() { require.NoError(t, params.Reset(regexpKey)) })
		p.cache = multiSearchQuotaCache{p.cache}
		limiter := &multiSearchQuotaLimiter{}
		handler := newMultiSearchRESTHandler(&multiSearchQuotaProxy{Proxy: p.Proxy, cache: p.cache, search: p.Search, limiter: limiter})
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
		require.Equal(t, merr.Code(merr.ErrHTTPRateLimit), search("e.*"))
		require.Empty(t, limiter.snapshot())
		require.Zero(t, search("east"))
		require.Equal(t, []multiSearchQuotaCall{
			{dbID: 10, partitions: map[int64][]int64{11: {12}}, rateType: internalpb.RateType_DQLSearch, nq: 2},
		}, limiter.snapshot())
		require.Equal(t, []int64{0}, searched)
	})
}

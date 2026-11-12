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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const multiSearchRESTPath = "/v2/vectordb/entities/multi_search"

const readySearch = `{"search":{"collectionName":"ready","data":[[1,0]]}}`

type multiSearchRESTResponse struct {
	Code int32
	Data []struct {
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
		params.HTTPCfg.RequestTimeoutMs.Key:          "1",
	} {
		require.NoError(t, params.Save(key, value))
		t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
	}
	schema, err := metacache.NewSchemaInfo(&schemapb.CollectionSchema{
		Name: "ready",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
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

func TestMultiSearchRESTDeliversPartialResultsAfterExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releasePrepare := sync.OnceFunc(func() { close(release) })
		defer releasePrepare()
		cancelled := make(chan error, 1)
		p := newMultiSearchHTTPProxy(t, func(ctx context.Context, collection string) {
			switch collection {
			case "ready":
			case "preparing":
				<-ctx.Done()
				cancelled <- ctx.Err()
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
		require.ErrorIs(t, <-cancelled, context.DeadlineExceeded)

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
		p := newMultiSearchHTTPProxy(t, func(context.Context, string) {
			t.Error("prepared a search after the budget expired during decoding")
		}, nil)
		payload := slowMultiSearchReader{strings.NewReader(`{"timeoutMs":5,"requests":[` + readySearch + `]}`)}
		status, body := recordMultiSearch(t, newMultiSearchRESTHandler(p), payload, "")
		require.Equal(t, http.StatusOK, status)
		require.Zero(t, body.Code)
		require.Len(t, body.Data, 1)
		require.Equal(t, merr.TimeoutCode, body.Data[0].Search.Code)
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

func TestMultiSearchRESTTransportDeadlineEndsCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cancelled := make(chan error, 1)
		p := newMultiSearchHTTPProxy(t, nil, func(ctx context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			<-ctx.Done()
			cancelled <- ctx.Err()
			return successfulSearchResult(), nil
		})
		payload := `{"timeoutMs":5000,"requests":[` + readySearch + `]}`
		status, body := recordMultiSearch(t, newMultiSearchRESTHandler(p), strings.NewReader(payload), "1")
		require.Equal(t, http.StatusRequestTimeout, status)
		require.Equal(t, merr.TimeoutCode, body.Code)
		require.Empty(t, body.Data)
		require.ErrorIs(t, <-cancelled, context.DeadlineExceeded)
	})
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
		rejected := metrics.ProxyFunctionCall.WithLabelValues(nodeID, "Search", metrics.RejectedLabel, metrics.CauseSystem, "default", "ready")
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
	synctest.Test(t, func(t *testing.T) {
		handler := newMultiSearchRESTHandler(proxy.NewMultiSearchTestProxy(t, 1, 1))
		req := httptest.NewRequest(http.MethodPost, multiSearchRESTPath, strings.NewReader(
			`{"requests":[{"search":{}}],"timeoutMs":0}`))
		req.Header.Set(mhttp.HTTPHeaderRequestTimeout, "1")
		response := blockedMultiSearchResponseWriter{httptest.NewRecorder(), make(chan struct{})}
		require.PanicsWithValue(t, http.ErrAbortHandler, func() {
			handler.ServeHTTP(response, req)
		})
		require.Equal(t, http.StatusOK, response.Code)
	})
}

type shortMultiSearchResponseWriter struct{ http.ResponseWriter }

func (w shortMultiSearchResponseWriter) Write(p []byte) (int, error) {
	return w.ResponseWriter.Write(p[:min(6, len(p))])
}

func TestMultiSearchRESTShortWriteFailsTransport(t *testing.T) {
	handler := newMultiSearchRESTHandler(proxy.NewMultiSearchTestProxy(t, 1, 1))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(shortMultiSearchResponseWriter{w}, r)
	}))
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Post(server.URL+multiSearchRESTPath, "application/json", strings.NewReader(
		`{"requests":[{"search":{}}],"timeoutMs":0}`))
	if err == nil {
		defer response.Body.Close()
		_, err = io.ReadAll(response.Body)
	}
	require.Error(t, err)
}

type multiSearchQuotaProxy struct {
	*multiSearchHTTPProxy
	limiter types.Limiter
}

func (p *multiSearchQuotaProxy) GetRateLimiter() (types.Limiter, error) { return p.limiter, nil }

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
	calls []multiSearchQuotaCall
}

func (l *multiSearchQuotaLimiter) Check(dbID int64, parts map[int64][]int64, rateType internalpb.RateType, nq int) error {
	l.calls = append(l.calls, multiSearchQuotaCall{dbID, parts, rateType, nq})
	if len(l.calls) == 1 {
		return merr.WrapErrServiceRateLimit(10)
	}
	return nil
}

func TestMultiSearchRESTRetryChargesEachAttemptToItsTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		limiter := &multiSearchQuotaLimiter{}
		handler := newMultiSearchRESTHandler(&multiSearchQuotaProxy{multiSearchHTTPProxy: p, limiter: limiter})
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
		require.Equal(t, []multiSearchQuotaCall{first, first, second}, limiter.calls)
		require.Equal(t, []string{"catalog_a", "catalog_b"}, searched)
	})
}

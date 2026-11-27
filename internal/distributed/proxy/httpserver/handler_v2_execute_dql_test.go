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
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type executeDQLAdmissionProxy struct {
	mockProxyComponent
	precheckErr error
	enqueueErr  error
	results     []*milvuspb.DQLResult
}

func (p *executeDQLAdmissionProxy) CheckExecuteDQLAdmission() error { return p.precheckErr }

func (p *executeDQLAdmissionProxy) RunExecuteDQL(context.Context, proxy.ExecuteDQLOptions, []proxy.ExecuteDQLPrepare, proxy.ExecuteDQLExecute) ([]*milvuspb.DQLResult, error) {
	return p.results, p.enqueueErr
}

func TestExecuteDQLRejectsBeforeDecode(t *testing.T) {
	server := initHTTPServerV2(&executeDQLAdmissionProxy{precheckErr: merr.ErrServiceTooManyRequests}, false)
	response := postJSONBody(server, versionalV2(EntityCategory, ExecuteDQLAction), "{")
	require.Equal(t, http.StatusTooManyRequests, response.Code)
	require.Equal(t, "1", response.Header().Get("Retry-After"))
	var body ReturnErrMsg
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, merr.Code(merr.ErrServiceTooManyRequests), body.Code)
}

func TestExecuteDQLRejectsAtEnqueue(t *testing.T) {
	server := initHTTPServerV2(&executeDQLAdmissionProxy{enqueueErr: merr.ErrServiceTooManyRequests}, false)
	response := postJSONBody(server, versionalV2(EntityCategory, ExecuteDQLAction), `{"requests":[{"search":{}}]}`)
	require.Equal(t, http.StatusTooManyRequests, response.Code)
	var body ReturnErrMsg
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, merr.Code(merr.ErrServiceTooManyRequests), body.Code)
}

type blockedExecuteDQLJSON struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (v blockedExecuteDQLJSON) MarshalJSON() ([]byte, error) {
	close(v.started)
	<-v.release
	return []byte("null"), nil
}

func TestExecuteDQLTransportDeadlineInterruptsEncoding(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		releaseEncoding := sync.OnceFunc(func() { close(release) })
		defer releaseEncoding()
		respond := newDQLResponseMiddleware(dqlExplicitTransportTimeout)
		router := gin.New()
		router.POST("/", func(c *gin.Context) {
			state := c.Request.Context().Value(dqlStreamKey{}).(*dqlStreamState)
			respond(c, func(c *gin.Context) {
				HTTPReturnStream(c, http.StatusOK, gin.H{
					HTTPReturnCode: int32(0),
					HTTPReturnData: blockedExecuteDQLJSON{started, release},
				})
			}, state.receivedAt.Add(time.Second))
		})
		recorder := httptest.NewRecorder()
		start := time.Now()
		DQLStreamingHTTPHandler(router).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/", nil))
		require.Equal(t, time.Second, time.Since(start))
		select {
		case <-started:
		default:
			t.Fatal("response encoding did not start")
		}
		require.Equal(t, http.StatusRequestTimeout, recorder.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
		require.EqualValues(t, merr.TimeoutCode, body[HTTPReturnCode])
		require.NotContains(t, body, HTTPReturnData)
		releaseEncoding()
		synctest.Wait()
	})
}

type executeDQLRESTProxy struct {
	executeDQLAdmissionProxy
	cache   proxy.Cache
	limiter types.Limiter
	search  func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)
}

func (*executeDQLRESTProxy) IsDQLQueueFull() bool { return true }

func (p *executeDQLRESTProxy) GetMetaCache() proxy.Cache { return p.cache }

func (p *executeDQLRESTProxy) GetRateLimiter() (types.Limiter, error) { return p.limiter, nil }

func (p *executeDQLRESTProxy) RunExecuteDQL(ctx context.Context, _ proxy.ExecuteDQLOptions, items []proxy.ExecuteDQLPrepare, execute proxy.ExecuteDQLExecute) ([]*milvuspb.DQLResult, error) {
	results := make([]*milvuspb.DQLResult, len(items))
	for i, prepare := range items {
		req, err := prepare(ctx)
		var search *milvuspb.SearchResults
		if err == nil {
			search, err = execute(ctx, req)
		}
		if err != nil {
			// The Proxy converts unary permission failures to item statuses.
			if grpcStatus := status.Convert(err); grpcStatus.Code() == codes.PermissionDenied {
				err = merr.WrapErrPrivilegeNotPermitted("%s", grpcStatus.Message())
			}
			results[i] = &milvuspb.DQLResult{Status: merr.Status(err)}
			continue
		}
		results[i] = &milvuspb.DQLResult{Status: search.GetStatus()}
		if merr.Ok(search.GetStatus()) {
			results[i].Result = &milvuspb.DQLResult_Search{Search: search}
		}
	}
	return results, nil
}

func (p *executeDQLRESTProxy) Search(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return p.search(ctx, req)
}

type executeDQLRESTCache struct {
	proxy.Cache
	schema *metacache.SchemaInfo
}

func (m *executeDQLRESTCache) GetCollectionSchema(_ context.Context, db, collection string) (*metacache.SchemaInfo, error) {
	if db == DefaultDbName && (collection == "papers" || collection == "guidelines") {
		return m.schema, nil
	}
	return nil, merr.WrapErrCollectionNotFound(collection)
}

func (*executeDQLRESTCache) GetDatabaseInfo(context.Context, string) (*metacache.DatabaseInfo, error) {
	return &metacache.DatabaseInfo{DBID: 4}, nil
}

func (*executeDQLRESTCache) GetCollectionID(_ context.Context, _, collection string) (int64, error) {
	if collection == "guidelines" {
		return 5, nil
	}
	return 6, nil
}

func (*executeDQLRESTCache) GetPartitionInfo(context.Context, string, string, string) (*metacache.PartitionInfo, error) {
	return &metacache.PartitionInfo{PartitionID: 7}, nil
}

type executeDQLRESTLimiter struct {
	types.Limiter
	check func(int64, map[int64][]int64, internalpb.RateType, int) error
}

func (m executeDQLRESTLimiter) Check(db int64, collections map[int64][]int64, kind internalpb.RateType, n int) error {
	return m.check(db, collections, kind, n)
}

type executeDQLRESTResponse struct {
	Code int32
	Data []struct {
		Code    int32
		Message string
		Search  *struct {
			Code    int32
			Message string
			Data    []map[string]any
			Topks   []int64
		}
	}
}

func TestExecuteDQLRendersOrderedSearchResults(t *testing.T) {
	params := paramtable.Get()
	quota := params.QuotaConfig.QuotaAndLimitsEnabled.Key
	require.NoError(t, params.Save(quota, "true"))
	t.Cleanup(func() { require.NoError(t, params.Reset(quota)) })

	schema, err := metacache.NewSchemaInfo(&schemapb.CollectionSchema{
		Name: "papers",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}},
			},
		},
	})
	require.NoError(t, err)
	checked := make([]int, 0, 2)
	mock := &executeDQLRESTProxy{
		cache: &executeDQLRESTCache{schema: schema},
		limiter: executeDQLRESTLimiter{check: func(db int64, collections map[int64][]int64, kind internalpb.RateType, nq int) error {
			require.Equal(t, int64(4), db)
			require.Equal(t, internalpb.RateType_DQLSearch, kind)
			checked = append(checked, nq)
			if partitions, ok := collections[6]; ok {
				require.Equal(t, []int64{7}, partitions)
				return nil
			}
			require.Contains(t, collections, int64(5))
			return merr.ErrServiceRateLimit
		}},
		search: func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			require.Equal(t, DefaultDbName, req.GetDbName())
			require.Equal(t, "root", proxy.GetCurUserFromContextOrDefault(ctx))
			require.Equal(t, "papers", req.GetCollectionName())
			require.Equal(t, "id > 0", req.GetDsl())
			topK, err := funcutil.GetAttrByKeyFromRepeatedKV("topk", req.GetSearchParams())
			require.NoError(t, err)
			require.Equal(t, "100", topK)
			return &milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					TopK: 1, NumQueries: 2, Topks: []int64{1, 1}, Scores: []float32{0.1, 0.2},
					Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{Data: []int64{9007199254740993, 9}},
					}},
				},
			}, nil
		},
	}
	server := initHTTPServerV2(mock, false)
	recorder := recordActions(t)
	response := postJSONBody(server, versionalV2(EntityCategory, ExecuteDQLAction), `{"requests":[
		{"search":{}},
		{"search":{"collectionName":"papers","filter":"id > 0","partitionNames":["test"],"data":[[1,0],[0,1]]}},
		{"search":{"collectionName":"guidelines","ids":[7]}},
		{"search":{"collectionName":"missing","data":[[1,0]]}}
	]}`)
	require.Equal(t, http.StatusOK, response.Code)
	var body executeDQLRESTResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Zero(t, body.Code)
	require.Len(t, body.Data, 4)
	require.Equal(t, merr.Code(merr.ErrMissingRequiredParameters), body.Data[0].Code)
	require.NotEmpty(t, body.Data[0].Message)
	require.Nil(t, body.Data[0].Search)
	require.Zero(t, body.Data[1].Code)
	require.NotNil(t, body.Data[1].Search)
	require.Zero(t, body.Data[1].Search.Code)
	require.Equal(t, []int64{1, 1}, body.Data[1].Search.Topks)
	require.Len(t, body.Data[1].Search.Data, 2)
	require.Equal(t, "9007199254740993", body.Data[1].Search.Data[0]["id"])
	require.Equal(t, "9", body.Data[1].Search.Data[1]["id"])
	require.Equal(t, merr.Code(merr.ErrServiceRateLimit), body.Data[2].Code)
	require.NotEmpty(t, body.Data[2].Message)
	require.Nil(t, body.Data[2].Search)
	require.Empty(t, recorder.authorizeActions(), "quota refusal must not emit an authorization report")
	require.Equal(t, merr.Code(merr.ErrCollectionNotFound), body.Data[3].Code)
	require.NotEmpty(t, body.Data[3].Message)
	require.Nil(t, body.Data[3].Search)
	require.Equal(t, []int{2, 1}, checked)
}

func TestExecuteDQLSearchWithoutVectorField(t *testing.T) {
	schema, err := metacache.NewSchemaInfo(&schemapb.CollectionSchema{
		Name:   "papers",
		Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
	})
	require.NoError(t, err)
	server := initHTTPServerV2(&executeDQLRESTProxy{cache: &executeDQLRESTCache{schema: schema}}, false)
	response := postJSONBody(server, versionalV2(EntityCategory, ExecuteDQLAction), `{"requests":[
		{"search":{"collectionName":"papers","data":[[1,0]]}},
		{"search":{"collectionName":"papers","ids":[1]}}
	]}`)
	require.Equal(t, http.StatusOK, response.Code)
	var body executeDQLRESTResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Zero(t, body.Code)
	require.Len(t, body.Data, 2)
	for _, item := range body.Data {
		require.Equal(t, merr.Code(merr.ErrParameterInvalid), item.Code)
		require.Contains(t, item.Message, "collection papers has no vector field, search is not supported")
		require.Nil(t, item.Search)
	}
}

func TestExecuteDQLRoutesAndAuthorizesEachDatabase(t *testing.T) {
	metrics.ProxyFunctionCall.Reset()
	t.Cleanup(metrics.ProxyFunctionCall.Reset)
	params := paramtable.Get()
	for _, key := range []string{
		params.CommonCfg.AuthorizationEnabled.Key,
		params.ProxyCfg.ResolveAliasForPrivilege.Key,
		params.QuotaConfig.QuotaAndLimitsEnabled.Key,
	} {
		require.NoError(t, params.Save(key, "true"))
		t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
	}

	const username = "execute_dql_reader"
	cache := proxy.InitEmptyMetaCacheForTest()
	policy := privilege.GetPrivilegeCache()
	for _, target := range []struct {
		db, collection, dim, role  string
		dbID, collectionID, partID int64
	}{
		{DefaultDbName, "papers", "2", "default_reader", 11, 101, 201},
		{"catalog", "catalog_papers", "3", "catalog_reader", 12, 102, 202},
		{"archive", "papers", "2", "", 13, 103, 203},
	} {
		schema, err := metacache.NewSchemaInfo(&schemapb.CollectionSchema{
			Name: target.collection,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{
					FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: target.dim}},
				},
			},
		})
		require.NoError(t, err)
		cache.SeedDBInfoForTest(target.db, &metacache.DatabaseInfo{DBID: target.dbID})
		collection := cache.SeedCollectionForTest(target.db, target.collection, target.collectionID, "current")
		collection.Schema = schema
		cache.SeedPartitionCacheForTest(target.collectionID, []*metacache.PartitionInfo{
			{Name: "ready", PartitionID: target.partID},
		}, false)
		if target.role != "" {
			require.NoError(t, policy.RefreshPolicyInfo(typeutil.CacheOp{
				OpType: typeutil.CacheGrantPrivilege,
				OpKey:  funcutil.PolicyForPrivilege(target.role, commonpb.ObjectType_Collection.String(), target.collection, commonpb.ObjectPrivilege_PrivilegeSearch.String(), target.db),
			}))
			require.NoError(t, policy.RefreshPolicyInfo(typeutil.CacheOp{
				OpType: typeutil.CacheAddUserToRole,
				OpKey:  funcutil.EncodeUserRoleCache(username, target.role),
			}))
		}
	}

	span := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}})
	type quotaCharge struct {
		db          int64
		collections map[int64][]int64
		nq          int
	}
	var charges []quotaCharge
	server := newRESTV2ServerForRBACTest(t, &executeDQLRESTProxy{
		cache: cache,
		limiter: executeDQLRESTLimiter{check: func(db int64, collections map[int64][]int64, kind internalpb.RateType, nq int) error {
			require.Equal(t, internalpb.RateType_DQLSearch, kind)
			charges = append(charges, quotaCharge{db, collections, nq})
			return nil
		}},
		search: func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			require.Equal(t, []string{req.GetDbName()}, metadata.ValueFromIncomingContext(ctx, util.HeaderDBName))
			require.Equal(t, []string{"batch"}, metadata.ValueFromIncomingContext(ctx, "request-marker"))
			require.Equal(t, span.TraceID(), trace.SpanContextFromContext(ctx).TraceID())
			require.Equal(t, username, proxy.GetCurUserFromContextOrDefault(ctx))
			require.ElementsMatch(t, []string{"default_reader", "catalog_reader", util.RolePublic}, ctx.Value(proxy.RBACRoleContextKey))
			id := map[string]int64{DefaultDbName: 101, "catalog": 102}[req.GetDbName()]
			require.NotZero(t, id)
			topks := make([]int64, req.GetNq())
			topks[0] = 1
			return &milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					TopK: 1, NumQueries: req.GetNq(), Topks: topks, Scores: []float32{1},
					Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{id}}}},
				},
			}, nil
		},
	})
	recorder := recordActions(t)
	post := func(headerDB, bodyDB, items string) executeDQLRESTResponse {
		t.Helper()
		parent := metadata.NewIncomingContext(trace.ContextWithSpanContext(t.Context(), span), metadata.Pairs(
			util.HeaderDBName, "upstream_db", "request-marker", "batch",
		))
		dbField := ""
		if bodyDB != "" {
			dbField = `"dbName":"` + bodyDB + `",`
		}
		req := httptest.NewRequestWithContext(parent, http.MethodPost, versionalV2(EntityCategory, ExecuteDQLAction), strings.NewReader(`{`+dbField+`"requests":[`+items+`]}`))
		req.SetBasicAuth(username, "pwd")
		if headerDB != "" {
			req.Header.Set(HTTPHeaderDBName, headerDB)
		}
		response := httptest.NewRecorder()
		server.ServeHTTP(response, req)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var body executeDQLRESTResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Zero(t, body.Code, response.Body.String())
		require.Equal(t, []string{"upstream_db"}, metadata.ValueFromIncomingContext(parent, util.HeaderDBName))
		return body
	}

	body := post("catalog", "", `
		{"search":{"dbName":"default","collectionName":"current","partitionNames":["ready"],"data":[[1,0],[0,1]]}},
		{"search":{"collectionName":"current","partitionNames":["ready"],"data":[[1,0,0]]}},
		{"search":{"dbName":"","collectionName":"current","partitionNames":["ready"],"data":[[1,0,0]]}},
		{"search":{"dbName":"archive","collectionName":"papers","data":[[1,0]]}},
		{"search":{"dbName":"archive","collectionName":"current","data":[[1,0]]}}`)
	require.Len(t, body.Data, 5)
	for i, id := range []string{"101", "102", "102"} {
		require.Zero(t, body.Data[i].Search.Code, body.Data[i].Search.Message)
		require.Len(t, body.Data[i].Search.Data, 1)
		require.Equal(t, id, body.Data[i].Search.Data[0]["id"])
	}
	for _, item := range body.Data[3:] {
		require.Equal(t, merr.Code(merr.ErrPrivilegeNotPermitted), item.Code, item.Message)
		require.Contains(t, item.Message, "`archive` database")
		require.Nil(t, item.Search)
	}
	reports := recorder.authorizeActions()
	require.Len(t, reports, 2)
	for _, report := range reports {
		caller, ok := report.ctx.Value(hook.GinParamsKey).(*gin.Context)
		require.True(t, ok)
		require.Equal(t, username, caller.GetString(ContextUsername))
		require.Equal(t, versionalV2(EntityCategory, ExecuteDQLAction), caller.Request.URL.Path)
		require.Equal(t, milvuspb.MilvusService_Search_FullMethodName, report.fullMethod)
	}
	require.NotSame(t, reports[0].ctx.Value(hook.GinParamsKey), reports[1].ctx.Value(hook.GinParamsKey))
	require.ElementsMatch(t, []quotaCharge{
		{11, map[int64][]int64{101: {201}}, 2},
		{12, map[int64][]int64{102: {202}}, 1},
		{12, map[int64][]int64{102: {202}}, 1},
	}, charges)

	body = post("unused_header_database", "unused_body_database", `
		{"search":{"dbName":"default","collectionName":"current","data":[[1,0]]}},
		{"search":{"dbName":"catalog","collectionName":"current","data":[[1,0,0]]}}`)
	require.Len(t, body.Data, 2)
	require.Zero(t, body.Data[0].Search.Code, body.Data[0].Search.Message)
	require.Zero(t, body.Data[1].Search.Code, body.Data[1].Search.Message)
	body = post("", "", `{"search":{"collectionName":"current","data":[[1,0]]}}`)
	require.Len(t, body.Data, 1)
	require.Zero(t, body.Data[0].Search.Code, body.Data[0].Search.Message)
	body = post("", "catalog", `{"search":{"collectionName":"current","data":[[1,0,0]]}}`)
	require.Len(t, body.Data, 1)
	require.Equal(t, "102", body.Data[0].Search.Data[0]["id"])
	body = post("default", "catalog", `
		{"search":{"collectionName":"current","data":[[1,0,0]]}},
		{"search":{"dbName":"default","collectionName":"current","data":[[1,0]]}}`)
	require.Len(t, body.Data, 2)
	require.Equal(t, "102", body.Data[0].Search.Data[0]["id"])
	require.Equal(t, "101", body.Data[1].Search.Data[0]["id"])
	for _, expected := range []struct {
		method, db, collection, outcome, cause string
		count                                  float64
	}{
		{"Search", "default", "current", metrics.SuccessLabel, metrics.CauseNA, 4},
		{"Search", "catalog", "current", metrics.SuccessLabel, metrics.CauseNA, 5},
		{"Search", "archive", "papers", metrics.RejectedLabel, metrics.CauseUser, 1},
		{"Search", "archive", "current", metrics.RejectedLabel, metrics.CauseUser, 1},
		{"ExecuteDQL", "", "", metrics.SuccessLabel, metrics.CauseNA, 5},
	} {
		for _, labels := range [][2]string{{metrics.TotalLabel, metrics.CauseNA}, {expected.outcome, expected.cause}} {
			require.Equal(t, expected.count, testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(
				paramtable.GetStringNodeID(), expected.method, labels[0], labels[1], expected.db, expected.collection, "")), "%+v", expected)
		}
	}
	require.Equal(t, 10, testutil.CollectAndCount(metrics.ProxyFunctionCall))
}

func TestExecuteDQLStructuralFailureStopsWholeBatch(t *testing.T) {
	mock := &executeDQLRESTProxy{}
	path := versionalV2(EntityCategory, ExecuteDQLAction)
	server := initHTTPServerV2(mock, false)
	for _, request := range []string{
		`{"requests":[]}`,
		`{"requests":[{"search":{"collectionName":"papers","data":[[1,0]]}},{"query":{}}]}`,
		`{"requests":[{}]}`,
		`{"requests":[{"search":null}]}`,
		`{"requests":[{"search":{}}]}invalid trailing bytes`,
		`{"requests":[{"search":{}}],"timeoutMs":-1}`,
		`{"requests":[{"search":{}}],"timeoutMs":9223372036855}`,
		`{"requests":[{"search":{}}],"timeoutMs":9223372036854775808}`,
		`{"requests":[{"search":{}}],"timeoutMs":0.5}`,
	} {
		response := postJSONBody(server, path, request)
		require.Equal(t, http.StatusOK, response.Code, request)
		var body executeDQLRESTResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.NotZero(t, body.Code, request)
		require.Empty(t, body.Data, request)
	}

	// A recognized Search whose required fields are missing fails at its position.
	response := postJSONBody(server, path, `{"requests":[{"search":{}},{"search":{"collectionName":0}}]}`)
	var body executeDQLRESTResponse
	require.Equal(t, http.StatusOK, response.Code)
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Zero(t, body.Code)
	require.Len(t, body.Data, 2)
	require.Equal(t, merr.Code(merr.ErrMissingRequiredParameters), body.Data[0].Code)
	require.NotEmpty(t, body.Data[0].Message)
	require.Nil(t, body.Data[0].Search)
	require.Equal(t, merr.Code(merr.ErrIncorrectParameterFormat), body.Data[1].Code)
	require.NotEmpty(t, body.Data[1].Message)
	require.Nil(t, body.Data[1].Search)

	unauthenticated := initHTTPServerV2(mock, true)
	response = postJSONBody(unauthenticated, path, `{"requests":[{"search":{"collectionName":"papers","data":[[1,0]]}}]}`)
	require.Equal(t, http.StatusUnauthorized, response.Code)
}

func TestExecuteDQLRejectsMissingSearchResult(t *testing.T) {
	proxy := &executeDQLAdmissionProxy{results: []*milvuspb.DQLResult{{Status: merr.Success()}}}
	server := initHTTPServerV2(proxy, false)
	response := postJSONBody(server, versionalV2(EntityCategory, ExecuteDQLAction), `{"requests":[{"search":{}}]}`)
	require.Equal(t, http.StatusOK, response.Code)
	var body struct {
		Code int32
		Data []map[string]json.RawMessage
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Zero(t, body.Code)
	require.Len(t, body.Data, 1)
	require.JSONEq(t, strconv.Itoa(int(merr.Code(merr.ErrServiceInternal))), string(body.Data[0]["code"]))
	require.Contains(t, string(body.Data[0]["message"]), "without a Search result")
	require.NotContains(t, body.Data[0], "search")
}

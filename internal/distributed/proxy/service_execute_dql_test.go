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

package grpcproxy

import (
	"context"
	"crypto/rand"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type executeDQLTestProxy struct {
	types.ProxyComponent
	cache       proxy.Cache
	limiter     types.Limiter
	search      proxy.ExecuteDQLExecute
	run         func(context.Context, proxy.ExecuteDQLOptions, []proxy.ExecuteDQLPrepare, proxy.ExecuteDQLExecute) ([]*milvuspb.DQLResult, error)
	batchCalls  atomic.Int32
	searchCalls atomic.Int32
}

func (p *executeDQLTestProxy) GetMetaCache() proxy.Cache {
	return p.cache
}

func (p *executeDQLTestProxy) GetRateLimiter() (types.Limiter, error) {
	return p.limiter, nil
}

func (p *executeDQLTestProxy) Search(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	p.searchCalls.Add(1)
	if p.search != nil {
		return p.search(ctx, req)
	}
	return &milvuspb.SearchResults{Status: merr.Success()}, nil
}

func (p *executeDQLTestProxy) RunExecuteDQL(ctx context.Context, options proxy.ExecuteDQLOptions, items []proxy.ExecuteDQLPrepare, execute proxy.ExecuteDQLExecute) ([]*milvuspb.DQLResult, error) {
	p.batchCalls.Add(1)
	if p.run == nil {
		return nil, merr.WrapErrServiceInternalMsg("unexpected batch admission")
	}
	return p.run(ctx, options, items, execute)
}

type executeDQLTestLimiter struct {
	types.Limiter
	check func(int64, map[int64][]int64, internalpb.RateType, int) error
}

func (l executeDQLTestLimiter) Check(dbID int64, collections map[int64][]int64, kind internalpb.RateType, nq int) error {
	return l.check(dbID, collections, kind, nq)
}

type executeDQLTestHook struct {
	hookutil.DefaultHook
	apiKey      string
	username    string
	authCalls   chan time.Time
	searchCalls chan string
	before      func(any)
}

func (h *executeDQLTestHook) VerifyAPIKey(key string) (string, error) {
	h.authCalls <- time.Now()
	if key != h.apiKey {
		return "", merr.ErrNeedAuthenticate
	}
	return h.username, nil
}

func (h *executeDQLTestHook) Before(ctx context.Context, req any, method string) (context.Context, error) {
	h.searchCalls <- method
	if h.before != nil {
		h.before(req)
	}
	return ctx, nil
}

func newExecuteDQLTestClient(t *testing.T, server *grpc.Server) milvuspb.MilvusServiceClient {
	t.Helper()
	accesslog.InitAccessLogger(paramtable.Get())
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return milvuspb.NewMilvusServiceClient(conn)
}

func startExecuteDQLTestProxy(t *testing.T, component types.ProxyComponent) milvuspb.MilvusServiceClient {
	t.Helper()
	server := &Server{
		ctx:             t.Context(),
		proxy:           component,
		listenerManager: &listenerManager{portShareMode: true},
	}
	started := make(chan error, 1)
	server.wg.Add(1)
	server.startExternalGrpc(started)
	require.NoError(t, <-started)
	return newExecuteDQLTestClient(t, server.grpcExternalServer)
}

func executeDQLSearchRequest(search *milvuspb.SearchRequest) *milvuspb.DQLRequest {
	return &milvuspb.DQLRequest{Request: &milvuspb.DQLRequest_Search{Search: search}}
}

func executeDQLSearchRequests(searches ...*milvuspb.SearchRequest) []*milvuspb.DQLRequest {
	requests := make([]*milvuspb.DQLRequest, len(searches))
	for i, search := range searches {
		requests[i] = executeDQLSearchRequest(search)
	}
	return requests
}

func TestExecuteDQLGRPCRejectsInvalidBatch(t *testing.T) {
	proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
	t.Cleanup(func() { proxy.Params.Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key) })
	component := &executeDQLTestProxy{cache: proxy.InitEmptyMetaCacheForTest()}
	client := startExecuteDQLTestProxy(t, component)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	response, err := client.ExecuteDQL(ctx, &milvuspb.ExecuteDQLRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrParameterMissing)
	require.Empty(t, response.GetResults())

	response, err = client.ExecuteDQL(ctx, &milvuspb.ExecuteDQLRequest{
		Requests:  executeDQLSearchRequests(&milvuspb.SearchRequest{CollectionName: "papers"}),
		TimeoutMs: proto.Int64(-1),
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrParameterInvalid)
	require.Empty(t, response.GetResults())
	future := &milvuspb.DQLRequest{}
	require.NoError(t, proto.Unmarshal([]byte{0x12, 0x00}, future)) // An unknown length-delimited operation in field 2.
	valid := executeDQLSearchRequest(&milvuspb.SearchRequest{CollectionName: "papers"})
	for _, tc := range []struct {
		name string
		item *milvuspb.DQLRequest
	}{
		{"nil", nil},
		{"unset", &milvuspb.DQLRequest{}},
		{"unknown", future},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &milvuspb.ExecuteDQLRequest{Requests: []*milvuspb.DQLRequest{valid, tc.item}}
			// Exercise both a nil pointer at the server boundary and the wire
			// form, where gRPC decodes nil repeated messages as empty messages.
			server := &Server{proxy: component, executeDQLExecute: component.Search}
			for _, execute := range []func(context.Context, *milvuspb.ExecuteDQLRequest) (*milvuspb.ExecuteDQLResponse, error){
				server.ExecuteDQL,
				func(ctx context.Context, req *milvuspb.ExecuteDQLRequest) (*milvuspb.ExecuteDQLResponse, error) {
					return client.ExecuteDQL(ctx, req)
				},
			} {
				response, err := execute(ctx, req)
				require.NoError(t, err)
				require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrParameterInvalid)
				require.Contains(t, response.GetStatus().GetReason(), "requests[1]")
				require.Empty(t, response.GetResults())
			}
		})
	}
	require.Zero(t, component.batchCalls.Load())
	require.Zero(t, component.searchCalls.Load())
}

func TestExecuteDQLGRPCRejectsBatchLimits(t *testing.T) {
	params := paramtable.Get()
	for _, key := range []string{params.CommonCfg.AuthorizationEnabled.Key, params.QuotaConfig.QuotaAndLimitsEnabled.Key} {
		require.NoError(t, params.Save(key, "false"))
		t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
	}
	cache := proxy.InitEmptyMetaCacheForTest()
	cache.SeedDBInfoForTest(util.DefaultDBName, &metacache.DatabaseInfo{DBID: 1})
	cache.SeedCollectionForTest(util.DefaultDBName, "papers", 2)
	runner := proxy.NewExecuteDQLTestProxy(t, 1, 1)
	result := &milvuspb.SearchResults{Status: merr.Success(), CollectionName: strings.Repeat("x", 128)}
	component := &executeDQLTestProxy{
		cache:   cache,
		run:     runner.RunExecuteDQL,
		limiter: executeDQLTestLimiter{check: func(int64, map[int64][]int64, internalpb.RateType, int) error { return nil }},
		search: func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			return result, nil
		},
	}
	client := startExecuteDQLTestProxy(t, component)
	request := &milvuspb.SearchRequest{
		CollectionName: "papers",
		SearchInput: &milvuspb.SearchRequest_Ids{
			Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		},
	}
	requests := make([]*milvuspb.DQLRequest, 257)
	for i := range requests {
		requests[i] = executeDQLSearchRequest(request)
	}
	checkLimit := func(requests []*milvuspb.DQLRequest, setting string) {
		t.Helper()
		response, err := client.ExecuteDQL(t.Context(), &milvuspb.ExecuteDQLRequest{Requests: requests})
		require.NoError(t, err)
		require.EqualValues(t, 1102, response.GetStatus().GetCode())
		require.False(t, response.GetStatus().GetRetriable())
		require.Equal(t, "true", response.GetStatus().GetExtraInfo()[merr.InputErrorFlagKey])
		require.Contains(t, response.GetStatus().GetReason(), setting)
		require.Empty(t, response.GetResults())
	}
	checkLimit(requests, "maxBatchSize")
	require.Zero(t, component.batchCalls.Load())
	require.Zero(t, component.searchCalls.Load())

	size := proto.Size(&milvuspb.ExecuteDQLResponse{Status: merr.Success(), Results: []*milvuspb.DQLResult{
		{Status: merr.Success(), Result: &milvuspb.DQLResult_Search{Search: result}},
	}})
	key := params.ProxyCfg.ExecuteDQLMaxResultBytes.Key
	require.NoError(t, params.Save(key, strconv.Itoa(size-1)))
	t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
	checkLimit(requests[:1], "maxResultBytes")
	require.EqualValues(t, 1, component.searchCalls.Load())
}

func TestExecuteDQLGRPCRejectsInvalidIdentity(t *testing.T) {
	proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
	t.Cleanup(func() { proxy.Params.Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key) })
	component := &executeDQLTestProxy{cache: proxy.InitEmptyMetaCacheForTest()}
	client := startExecuteDQLTestProxy(t, component)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(util.HeaderAuthorize, "!"))

	response, err := client.ExecuteDQL(ctx, &milvuspb.ExecuteDQLRequest{
		Requests: executeDQLSearchRequests(&milvuspb.SearchRequest{CollectionName: "papers"}),
	})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Nil(t, response)
	require.Zero(t, component.batchCalls.Load())
	require.Zero(t, component.searchCalls.Load())
}

func TestExecuteDQLGRPCSearchEntry(t *testing.T) {
	require.NoError(t, proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	t.Cleanup(func() { require.NoError(t, proxy.Params.Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)) })
	regexpKey := proxy.Params.ProxyCfg.PartitionNameRegexp.Key
	require.NoError(t, proxy.Params.Save(regexpKey, "true"))
	t.Cleanup(func() { require.NoError(t, proxy.Params.Reset(regexpKey)) })
	quotaKey := proxy.Params.QuotaConfig.QuotaAndLimitsEnabled.Key
	require.NoError(t, proxy.Params.Save(quotaKey, "true"))
	t.Cleanup(func() { require.NoError(t, proxy.Params.Reset(quotaKey)) })
	const username = "grpc_execute_dql_reader"
	const role = "grpc_execute_dql_role"
	const collection = "papers"
	apiKey := rand.Text()
	cache := proxy.InitEmptyMetaCacheForTest()
	policy := privilege.GetPrivilegeCache()
	require.NoError(t, policy.RefreshPolicyInfo(typeutil.CacheOp{
		OpType: typeutil.CacheAddUserToRole,
		OpKey:  funcutil.EncodeUserRoleCache(username, role),
	}))
	for i, db := range []string{"header_db", "explicit_db", "denied_db"} {
		cache.SeedDBInfoForTest(db, &metacache.DatabaseInfo{DBID: int64(i + 11)})
		cache.SeedCollectionForTest(db, collection, int64(i+101))
		cache.SeedPartitionCacheForTest(int64(i+101), []*metacache.PartitionInfo{
			{Name: "part_a", PartitionID: int64(i + 1001)},
			{Name: "other", PartitionID: int64(i + 2001)},
		}, false)
		if db != "denied_db" {
			require.NoError(t, policy.RefreshPolicyInfo(typeutil.CacheOp{
				OpType: typeutil.CacheGrantPrivilege,
				OpKey:  funcutil.PolicyForPrivilege(role, commonpb.ObjectType_Collection.String(), collection, commonpb.ObjectPrivilege_PrivilegeSearch.String(), db),
			}))
		}
	}
	previousHook := hookutil.GetHook()
	hook := &executeDQLTestHook{apiKey: apiKey, username: username, authCalls: make(chan time.Time, 8), searchCalls: make(chan string, 8)}
	hookutil.SetTestHook(hook)
	t.Cleanup(func() { hookutil.SetTestHook(previousHook) })

	type charge struct {
		db          int64
		collections map[int64][]int64
		nq          int
	}
	charges := make(chan charge, 4)
	searchAccess := make(chan any, 4)
	batchOptions := make(chan proxy.ExecuteDQLOptions, 1)
	component := &executeDQLTestProxy{
		cache: cache,
		limiter: executeDQLTestLimiter{check: func(db int64, collections map[int64][]int64, kind internalpb.RateType, nq int) error {
			assert.Equal(t, internalpb.RateType_DQLSearch, kind)
			charges <- charge{db, collections, nq}
			return nil
		}},
		search: func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			assert.Equal(t, []string{req.GetDbName()}, metadata.ValueFromIncomingContext(ctx, util.HeaderDBName))
			assert.Equal(t, []string{apiKey}, metadata.ValueFromIncomingContext(ctx, util.HeaderToken))
			assert.Equal(t, username, proxy.GetCurUserFromContextOrDefault(ctx))
			assert.ElementsMatch(t, []string{role, util.RolePublic}, ctx.Value(proxy.RBACRoleContextKey))
			searchAccess <- ctx.Value(accesslog.AccessKey{})
			return &milvuspb.SearchResults{
				Status: merr.Success(), CollectionName: req.GetCollectionName(), SessionTs: 1234,
				Results: &schemapb.SearchResultData{NumQueries: req.GetNq(), Topks: make([]int64, req.GetNq())},
			}, nil
		},
		run: func(ctx context.Context, options proxy.ExecuteDQLOptions, items []proxy.ExecuteDQLPrepare, execute proxy.ExecuteDQLExecute) ([]*milvuspb.DQLResult, error) {
			batchOptions <- options
			results := make([]*milvuspb.DQLResult, len(items))
			for i, prepare := range items {
				itemCtx := metrics.WithIndependentRPCStats(ctx)
				request, err := prepare(itemCtx)
				if err != nil {
					results[i] = &milvuspb.DQLResult{Status: merr.Status(err)}
					continue
				}
				result, err := execute(itemCtx, request)
				if err != nil {
					if request.GetDbName() != "denied_db" || status.Code(err) != codes.PermissionDenied {
						return nil, err
					}
					results[i] = &milvuspb.DQLResult{Status: merr.Status(merr.ErrPrivilegeNotPermitted)}
					continue
				}
				results[i] = &milvuspb.DQLResult{Status: result.GetStatus()}
				if merr.Ok(result.GetStatus()) {
					results[i].Result = &milvuspb.DQLResult_Search{Search: result}
				}
			}
			assert.Equal(t, []string{"header_db"}, metadata.ValueFromIncomingContext(ctx, util.HeaderDBName))
			return results, nil
		},
	}
	client := startExecuteDQLTestProxy(t, component)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode(apiKey), util.HeaderDBName, "header_db"))
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{{0, 0, 0, 0}, {0, 0, 0, 0}},
	}}})
	require.NoError(t, err)
	vectorInput := &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder}
	const budget = 500 * time.Millisecond
	batchMetric := metrics.ProxyFunctionCall.WithLabelValues(
		paramtable.GetStringNodeID(), "ExecuteDQL", metrics.SuccessLabel, metrics.CauseNA, "", "", "",
	)
	beforeBatchMetric := testutil.ToFloat64(batchMetric)
	beforeCall := time.Now()
	response, err := client.ExecuteDQL(ctx, &milvuspb.ExecuteDQLRequest{
		MaxConcurrency: 2,
		TimeoutMs:      proto.Int64(budget.Milliseconds()),
		Requests: executeDQLSearchRequests([]*milvuspb.SearchRequest{
			{CollectionName: collection, PartitionNames: []string{"part_.*"}, SearchInput: vectorInput},
			{DbName: "explicit_db", CollectionName: collection, PartitionNames: []string{"part_.*"}, SearchInput: &milvuspb.SearchRequest_Ids{
				Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
			}},
			{DbName: "denied_db", CollectionName: collection, Nq: 2, SearchInput: vectorInput},
			{CollectionName: collection, Nq: 2, SearchInput: vectorInput, SubReqs: []*milvuspb.SubSearchRequest{{Nq: 2}}},
			{CollectionName: collection, Nq: 1, SearchInput: vectorInput},
			{CollectionName: collection, Nq: -1, SearchInput: vectorInput},
			{CollectionName: collection, Nq: 1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: []byte{0xff}}},
			{CollectionName: collection, SearchInput: &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{}}},
			{CollectionName: collection, PartitionNames: []string{"["}, Nq: 2, SearchInput: vectorInput},
			{CollectionName: collection, Nq: 2, SearchInput: vectorInput},
			{},
			nil,
		}...),
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(response.GetStatus()))
	require.Len(t, response.GetResults(), 12)
	require.Equal(t, beforeBatchMetric+1, testutil.ToFloat64(batchMetric))
	for i, nq := range map[int]int64{0: 2, 1: 3, 9: 2} {
		result := response.GetResults()[i]
		require.NotNil(t, result.GetStatus())
		require.NoError(t, merr.Error(result.GetStatus()))
		require.NotNil(t, result.GetSearch().GetStatus())
		require.NoError(t, merr.Error(result.GetSearch().GetStatus()))
		require.Equal(t, collection, result.GetSearch().GetCollectionName())
		require.Equal(t, uint64(1234), result.GetSearch().GetSessionTs())
		require.Equal(t, nq, result.GetSearch().GetResults().GetNumQueries())
		require.Len(t, result.GetSearch().GetResults().GetTopks(), int(nq))
	}
	require.ErrorIs(t, merr.Error(response.GetResults()[2].GetStatus()), merr.ErrPrivilegeNotPermitted)
	require.Nil(t, response.GetResults()[2].GetResult())
	for _, i := range []int{3, 4, 5, 6, 7, 8, 10, 11} {
		require.ErrorIs(t, merr.Error(response.GetResults()[i].GetStatus()), merr.ErrParameterInvalid, "item %d", i)
		require.Nil(t, response.GetResults()[i].GetResult())
	}
	require.Len(t, charges, 3)
	require.Equal(t, charge{11, map[int64][]int64{101: {1001}}, 2}, <-charges)
	require.Equal(t, charge{12, map[int64][]int64{102: {1002}}, 3}, <-charges)
	require.Equal(t, charge{11, map[int64][]int64{101: {}}, 2}, <-charges)
	require.Len(t, searchAccess, 3)
	firstAccess, secondAccess := <-searchAccess, <-searchAccess
	require.NotNil(t, firstAccess)
	require.NotNil(t, secondAccess)
	require.NotSame(t, firstAccess, secondAccess)
	require.Len(t, hook.searchCalls, 4)
	for range 4 {
		require.Equal(t, milvuspb.MilvusService_Search_FullMethodName, <-hook.searchCalls)
	}
	require.Len(t, hook.authCalls, 1)
	require.Len(t, batchOptions, 1)
	options := <-batchOptions
	require.Equal(t, uint32(2), options.MaxConcurrency)
	require.False(t, options.ExecutionDeadline.Add(-budget).Before(beforeCall))
	require.False(t, options.ExecutionDeadline.Add(-budget).After(<-hook.authCalls))
}

func TestExecuteDQLGRPCUnimplementedOnOldService(t *testing.T) {
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "milvus.proto.milvus.MilvusService",
		HandlerType: (*milvuspb.MilvusServiceServer)(nil),
	}, &milvuspb.UnimplementedMilvusServiceServer{})
	client := newExecuteDQLTestClient(t, server)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	response, err := client.ExecuteDQL(ctx, &milvuspb.ExecuteDQLRequest{
		Requests: executeDQLSearchRequests(&milvuspb.SearchRequest{CollectionName: "papers"}),
	})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Nil(t, response)
}

func TestExecuteDQLGRPCPreservesSearchHookFields(t *testing.T) {
	key := proxy.Params.CommonCfg.AuthorizationEnabled.Key
	require.NoError(t, proxy.Params.Save(key, "false"))
	t.Cleanup(func() { require.NoError(t, proxy.Params.Reset(key)) })
	type hookFields struct {
		principal string
		skip      bool
	}
	seen := make(chan hookFields, 2)
	previousHook := hookutil.GetHook()
	hookutil.SetTestHook(&executeDQLTestHook{
		searchCalls: make(chan string, 2),
		before: func(req any) {
			if search, ok := req.(*milvuspb.SearchRequest); ok {
				seen <- hookFields{search.GetRlsPrincipal(), search.GetSkipRls()}
			}
		},
	})
	t.Cleanup(func() { hookutil.SetTestHook(previousHook) })

	runner := proxy.NewExecuteDQLTestProxy(t, 1, 1)
	cache := proxy.InitEmptyMetaCacheForTest()
	cache.SeedDBInfoForTest(util.DefaultDBName, &metacache.DatabaseInfo{DBID: 1})
	cache.SeedCollectionForTest(util.DefaultDBName, "papers", 2)
	client := startExecuteDQLTestProxy(t, &executeDQLTestProxy{
		cache: cache,
		limiter: executeDQLTestLimiter{check: func(int64, map[int64][]int64, internalpb.RateType, int) error {
			return nil
		}},
		run: runner.RunExecuteDQL,
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ids := &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}}
	response, err := client.ExecuteDQL(ctx, &milvuspb.ExecuteDQLRequest{
		MaxConcurrency: 1,
		Requests: executeDQLSearchRequests([]*milvuspb.SearchRequest{
			{CollectionName: "papers", SearchInput: ids, RlsPrincipal: "paper-reader"},
			{CollectionName: "papers", SearchInput: ids, RlsPrincipal: "paper-owner", SkipRls: true},
		}...),
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(response.GetStatus()))
	require.Len(t, response.GetResults(), 2)
	for _, result := range response.GetResults() {
		require.NotNil(t, result.GetStatus())
		require.NoError(t, merr.Error(result.GetStatus()))
		require.NotNil(t, result.GetSearch().GetStatus())
		require.NoError(t, merr.Error(result.GetSearch().GetStatus()))
	}
	require.Len(t, seen, 2)
	require.Equal(t, hookFields{"paper-reader", false}, <-seen)
	require.Equal(t, hookFields{"paper-owner", true}, <-seen)
}

func TestExecuteDQLGRPCDeliversPartialResultsAfterExecutionDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, key := range []string{
			proxy.Params.CommonCfg.AuthorizationEnabled.Key,
			proxy.Params.QuotaConfig.QuotaAndLimitsEnabled.Key,
		} {
			require.NoError(t, proxy.Params.Save(key, "false"))
			t.Cleanup(func() { require.NoError(t, proxy.Params.Reset(key)) })
		}
		runner := proxy.NewExecuteDQLTestProxy(t, 1, 1)
		cache := proxy.InitEmptyMetaCacheForTest()
		cache.SeedDBInfoForTest(util.DefaultDBName, &metacache.DatabaseInfo{DBID: 1})
		for i, collection := range []string{"ready", "panic", "active", "undispatched"} {
			cache.SeedCollectionForTest(util.DefaultDBName, collection, int64(i+2))
		}
		started := make(chan string, 3)
		deadlines := make(chan time.Time, 3)
		canceled := make(chan error, 1)
		batchDeadlines := make(chan [2]time.Time, 1)
		client := startExecuteDQLTestProxy(t, &executeDQLTestProxy{
			cache: cache,
			limiter: executeDQLTestLimiter{check: func(int64, map[int64][]int64, internalpb.RateType, int) error {
				return nil
			}},
			run: func(ctx context.Context, options proxy.ExecuteDQLOptions, items []proxy.ExecuteDQLPrepare, execute proxy.ExecuteDQLExecute) ([]*milvuspb.DQLResult, error) {
				parentDeadline, _ := ctx.Deadline()
				batchDeadlines <- [2]time.Time{parentDeadline, options.ExecutionDeadline}
				return runner.RunExecuteDQL(ctx, options, items, execute)
			},
			search: func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				started <- req.GetCollectionName()
				deadline, _ := ctx.Deadline()
				deadlines <- deadline
				if req.GetCollectionName() == "panic" {
					panic("Search panicked")
				}
				if req.GetCollectionName() == "active" {
					<-ctx.Done()
					canceled <- ctx.Err()
					return nil, ctx.Err()
				}
				return &milvuspb.SearchResults{
					Status: merr.Success(), CollectionName: req.GetCollectionName(), SessionTs: 42,
					Results: &schemapb.SearchResultData{NumQueries: 1, Topks: []int64{0}},
				}, nil
			},
		})
		parent, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		input := &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}}
		budget := 250 * time.Millisecond
		response, err := client.ExecuteDQL(parent, &milvuspb.ExecuteDQLRequest{
			MaxConcurrency: 1,
			TimeoutMs:      proto.Int64(budget.Milliseconds()),
			Requests: executeDQLSearchRequests([]*milvuspb.SearchRequest{
				{CollectionName: "ready", SearchInput: input},
				{CollectionName: "panic", SearchInput: input},
				{CollectionName: "active", SearchInput: input},
				{CollectionName: "undispatched", SearchInput: input},
			}...),
		})
		require.NoError(t, err)
		require.NoError(t, parent.Err())
		require.NoError(t, merr.Error(response.GetStatus()))
		require.Len(t, response.GetResults(), 4)
		ready := response.GetResults()[0]
		require.NotNil(t, ready.GetStatus())
		require.NoError(t, merr.Error(ready.GetStatus()))
		require.NotNil(t, ready.GetSearch().GetStatus())
		require.NoError(t, merr.Error(ready.GetSearch().GetStatus()))
		require.Equal(t, "ready", ready.GetSearch().GetCollectionName())
		require.Equal(t, uint64(42), ready.GetSearch().GetSessionTs())
		require.Equal(t, []int64{0}, ready.GetSearch().GetResults().GetTopks())
		panicked := response.GetResults()[1]
		require.ErrorIs(t, merr.Error(panicked.GetStatus()), merr.ErrServiceInternal)
		require.Nil(t, panicked.GetResult())
		for _, item := range response.GetResults()[2:] {
			require.Equal(t, merr.TimeoutCode, item.GetStatus().GetCode())
			require.Nil(t, item.GetResult())
		}
		synctest.Wait()
		require.Len(t, started, 3)
		require.Equal(t, "ready", <-started)
		require.Equal(t, "panic", <-started)
		require.Equal(t, "active", <-started)
		require.Len(t, canceled, 1)
		require.ErrorIs(t, <-canceled, context.DeadlineExceeded)
		require.Len(t, batchDeadlines, 1)
		pair := <-batchDeadlines
		require.True(t, pair[0].After(pair[1]))
		require.Len(t, deadlines, 3)
		for range 3 {
			require.Equal(t, pair[1], <-deadlines)
		}
	})
}

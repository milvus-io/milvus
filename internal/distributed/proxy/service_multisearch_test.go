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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"
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

type multiSearchTestProxy struct {
	types.ProxyComponent
	cache       proxy.Cache
	limiter     types.Limiter
	search      proxy.MultiSearchExecute
	run         func(context.Context, proxy.MultiSearchOptions, []proxy.MultiSearchPrepare, proxy.MultiSearchExecute) ([]*milvuspb.SearchResults, error)
	batchCalls  atomic.Int32
	searchCalls atomic.Int32
}

func (p *multiSearchTestProxy) GetMetaCache() proxy.Cache {
	return p.cache
}

func (p *multiSearchTestProxy) GetRateLimiter() (types.Limiter, error) {
	return p.limiter, nil
}

func (p *multiSearchTestProxy) Search(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	p.searchCalls.Add(1)
	if p.search != nil {
		return p.search(ctx, req)
	}
	return &milvuspb.SearchResults{Status: merr.Success()}, nil
}

func (p *multiSearchTestProxy) RunMultiSearch(ctx context.Context, options proxy.MultiSearchOptions, items []proxy.MultiSearchPrepare, execute proxy.MultiSearchExecute) ([]*milvuspb.SearchResults, error) {
	p.batchCalls.Add(1)
	if p.run == nil {
		return nil, merr.WrapErrServiceInternalMsg("unexpected batch admission")
	}
	return p.run(ctx, options, items, execute)
}

type multiSearchTestLimiter struct {
	types.Limiter
	check func(int64, map[int64][]int64, internalpb.RateType, int) error
}

func (l multiSearchTestLimiter) Check(dbID int64, collections map[int64][]int64, kind internalpb.RateType, nq int) error {
	return l.check(dbID, collections, kind, nq)
}

type multiSearchTestHook struct {
	hookutil.DefaultHook
	apiKey      string
	username    string
	authCalls   chan time.Time
	searchCalls chan string
}

func (h *multiSearchTestHook) VerifyAPIKey(key string) (string, error) {
	h.authCalls <- time.Now()
	if key != h.apiKey {
		return "", merr.ErrNeedAuthenticate
	}
	return h.username, nil
}

func (h *multiSearchTestHook) Before(ctx context.Context, _ any, method string) (context.Context, error) {
	h.searchCalls <- method
	return ctx, nil
}

func newMultiSearchTestClient(t *testing.T, server *grpc.Server) milvuspb.MilvusServiceClient {
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

func startMultiSearchTestProxy(t *testing.T, component types.ProxyComponent) milvuspb.MilvusServiceClient {
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
	return newMultiSearchTestClient(t, server.grpcExternalServer)
}

func grpcSearchItem(request *milvuspb.SearchRequest) *milvuspb.MultiSearchSubRequest {
	return &milvuspb.MultiSearchSubRequest{Request: &milvuspb.MultiSearchSubRequest_Search{Search: request}}
}

func TestMultiSearchGRPCRejectsInvalidBatch(t *testing.T) {
	proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
	t.Cleanup(func() { proxy.Params.Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key) })
	component := &multiSearchTestProxy{cache: proxy.InitEmptyMetaCacheForTest()}
	client := startMultiSearchTestProxy(t, component)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	response, err := client.MultiSearch(ctx, &milvuspb.MultiSearchRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrParameterMissing)
	require.Empty(t, response.GetResults())

	futureOperation := &milvuspb.MultiSearchSubRequest{}
	futureOperation.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), nil))
	response, err = client.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
		Requests: []*milvuspb.MultiSearchSubRequest{
			grpcSearchItem(&milvuspb.SearchRequest{CollectionName: "papers"}),
			futureOperation,
		},
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrParameterInvalid)
	require.Empty(t, response.GetResults())

	response, err = client.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
		Requests:  []*milvuspb.MultiSearchSubRequest{grpcSearchItem(&milvuspb.SearchRequest{CollectionName: "papers"})},
		TimeoutMs: proto.Int64(-1),
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrParameterInvalid)
	require.Empty(t, response.GetResults())
	require.Zero(t, component.batchCalls.Load())
	require.Zero(t, component.searchCalls.Load())
}

func TestMultiSearchGRPCRejectsInvalidIdentity(t *testing.T) {
	proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
	t.Cleanup(func() { proxy.Params.Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key) })
	component := &multiSearchTestProxy{cache: proxy.InitEmptyMetaCacheForTest()}
	client := startMultiSearchTestProxy(t, component)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(util.HeaderAuthorize, "!"))

	response, err := client.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
		Requests: []*milvuspb.MultiSearchSubRequest{grpcSearchItem(&milvuspb.SearchRequest{CollectionName: "papers"})},
	})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Nil(t, response)
	require.Zero(t, component.batchCalls.Load())
	require.Zero(t, component.searchCalls.Load())
}

func TestMultiSearchGRPCSearchEntry(t *testing.T) {
	require.NoError(t, proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	t.Cleanup(func() { require.NoError(t, proxy.Params.Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)) })
	regexpKey := proxy.Params.ProxyCfg.PartitionNameRegexp.Key
	require.NoError(t, proxy.Params.Save(regexpKey, "true"))
	t.Cleanup(func() { require.NoError(t, proxy.Params.Reset(regexpKey)) })
	quotaKey := proxy.Params.QuotaConfig.QuotaAndLimitsEnabled.Key
	require.NoError(t, proxy.Params.Save(quotaKey, "true"))
	t.Cleanup(func() { require.NoError(t, proxy.Params.Reset(quotaKey)) })
	const username = "grpc_multisearch_reader"
	const role = "grpc_multisearch_role"
	const apiKey = "grpc-multisearch-test"
	const collection = "papers"
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
	hook := &multiSearchTestHook{apiKey: apiKey, username: username, authCalls: make(chan time.Time, 8), searchCalls: make(chan string, 8)}
	hookutil.SetTestHook(hook)
	t.Cleanup(func() { hookutil.SetTestHook(previousHook) })

	type charge struct {
		db          int64
		collections map[int64][]int64
		nq          int
	}
	charges := make(chan charge, 4)
	searchAccess := make(chan any, 4)
	batchOptions := make(chan proxy.MultiSearchOptions, 1)
	component := &multiSearchTestProxy{
		cache: cache,
		limiter: multiSearchTestLimiter{check: func(db int64, collections map[int64][]int64, kind internalpb.RateType, nq int) error {
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
		run: func(ctx context.Context, options proxy.MultiSearchOptions, items []proxy.MultiSearchPrepare, execute proxy.MultiSearchExecute) ([]*milvuspb.SearchResults, error) {
			batchOptions <- options
			results := make([]*milvuspb.SearchResults, len(items))
			for i, prepare := range items {
				itemCtx := metrics.WithIndependentRPCStats(ctx)
				request, err := prepare(itemCtx)
				if err != nil {
					results[i] = &milvuspb.SearchResults{Status: merr.Status(err)}
					continue
				}
				result, err := execute(itemCtx, request)
				if err != nil {
					if request.GetDbName() != "denied_db" || status.Code(err) != codes.PermissionDenied {
						return nil, err
					}
					result = &milvuspb.SearchResults{Status: merr.Status(merr.ErrPrivilegeNotPermitted)}
				}
				results[i] = result
			}
			assert.Equal(t, []string{"header_db"}, metadata.ValueFromIncomingContext(ctx, util.HeaderDBName))
			return results, nil
		},
	}
	client := startMultiSearchTestProxy(t, component)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode(apiKey), util.HeaderDBName, "header_db"))
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{{0, 0, 0, 0}, {0, 0, 0, 0}},
	}}})
	require.NoError(t, err)
	vectorInput := &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder}
	const budget = 500 * time.Millisecond
	beforeCall := time.Now()
	response, err := client.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
		MaxConcurrency: 2,
		TimeoutMs:      proto.Int64(budget.Milliseconds()),
		Requests: []*milvuspb.MultiSearchSubRequest{
			grpcSearchItem(&milvuspb.SearchRequest{CollectionName: collection, PartitionNames: []string{"part_.*"}, SearchInput: vectorInput}),
			grpcSearchItem(&milvuspb.SearchRequest{DbName: "explicit_db", CollectionName: collection, PartitionNames: []string{"part_.*"}, SearchInput: &milvuspb.SearchRequest_Ids{
				Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
			}}),
			grpcSearchItem(&milvuspb.SearchRequest{DbName: "denied_db", CollectionName: collection, Nq: 2, SearchInput: vectorInput}),
			grpcSearchItem(&milvuspb.SearchRequest{CollectionName: collection, Nq: 2, SearchInput: vectorInput, SubReqs: []*milvuspb.SubSearchRequest{{Nq: 2}}}),
			grpcSearchItem(&milvuspb.SearchRequest{CollectionName: collection, Nq: 1, SearchInput: vectorInput}),
			grpcSearchItem(&milvuspb.SearchRequest{CollectionName: collection, Nq: -1, SearchInput: vectorInput}),
			grpcSearchItem(&milvuspb.SearchRequest{CollectionName: collection, Nq: 1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: []byte{0xff}}}),
			grpcSearchItem(&milvuspb.SearchRequest{CollectionName: collection, SearchInput: &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{}}}),
			grpcSearchItem(&milvuspb.SearchRequest{CollectionName: collection, PartitionNames: []string{"["}, Nq: 2, SearchInput: vectorInput}),
			grpcSearchItem(&milvuspb.SearchRequest{CollectionName: collection, Nq: 2, SearchInput: vectorInput}),
		},
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(response.GetStatus()))
	require.Len(t, response.GetResults(), 10)
	for i, nq := range map[int]int64{0: 2, 1: 3, 9: 2} {
		result := response.GetResults()[i].GetSearch()
		require.NoError(t, merr.Error(result.GetStatus()))
		require.Equal(t, collection, result.GetCollectionName())
		require.Equal(t, uint64(1234), result.GetSessionTs())
		require.Equal(t, nq, result.GetResults().GetNumQueries())
		require.Len(t, result.GetResults().GetTopks(), int(nq))
	}
	require.ErrorIs(t, merr.Error(response.GetResults()[2].GetSearch().GetStatus()), merr.ErrPrivilegeNotPermitted)
	require.Nil(t, response.GetResults()[2].GetSearch().GetResults())
	for i := 3; i <= 8; i++ {
		require.ErrorIs(t, merr.Error(response.GetResults()[i].GetSearch().GetStatus()), merr.ErrParameterInvalid, "item %d", i)
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

func TestMultiSearchGRPCUnimplementedOnOldService(t *testing.T) {
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "milvus.proto.milvus.MilvusService",
		HandlerType: (*milvuspb.MilvusServiceServer)(nil),
	}, &milvuspb.UnimplementedMilvusServiceServer{})
	client := newMultiSearchTestClient(t, server)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	response, err := client.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
		Requests: []*milvuspb.MultiSearchSubRequest{grpcSearchItem(&milvuspb.SearchRequest{CollectionName: "papers"})},
	})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Nil(t, response)
}

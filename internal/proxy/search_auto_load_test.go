// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

func TestEnsureCollectionReadyUsesOneRPC(t *testing.T) {
	enableAutoLoad(t)
	for _, want := range []error{nil, merr.ErrIndexNotFound, merr.ErrServiceUnavailable, context.DeadlineExceeded} {
		name := "ready"
		if want != nil {
			name = want.Error()
		}
		t.Run(name, func(t *testing.T) {
			cache := mockSearchCollectionMeta(t, 100, []string{"v0", "v1"})
			coord := mocks.NewMockMixCoordClient(t)
			coord.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).RunAndReturn(
				func(_ context.Context, req *querypb.EnsureCollectionReadyRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
					require.EqualValues(t, 100, req.GetCollectionID())
					require.Equal(t, []string{"v0", "v1"}, req.GetExpectedVchannels())
					return merr.Status(want), nil
				}).Once()
			node := &Proxy{metaCache: cache, mixCoord: coord}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			err := node.ensureCollectionReady(context.Background(), "db", "collection")
			if want == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, merr.Code(want), merr.Code(err))
			}
			// Any GetLoadState, DescribeIndex, AllocTimestamp or LoadCollection call
			// would fail because the coordinator has no expectation for it.
		})
	}
}

func TestEnsureCollectionReadySkipsDisabledAndUnhealthy(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	require.ErrorIs(t, node.ensureCollectionReady(context.Background(), "db", "collection"), merr.ErrServiceNotReady)
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableAutoLoad.Key, "false"))
	t.Cleanup(func() { require.NoError(t, Params.Reset(Params.ProxyCfg.EnableAutoLoad.Key)) })
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	require.NoError(t, node.ensureCollectionReady(context.Background(), "db", "collection"))
}

func TestEnsureCollectionReadyCallerCancellation(t *testing.T) {
	enableAutoLoad(t)
	coord := mocks.NewMockMixCoordClient(t)
	entered := make(chan struct{})
	coord.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ *querypb.EnsureCollectionReadyRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()
	node := &Proxy{mixCoord: coord, metaCache: mockSearchCollectionMeta(t, 100, []string{"v0"})}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- node.ensureCollectionReady(ctx, "db", "collection") }()
	awaitAutoLoadSchedulerResult(t, entered)
	cancel()
	require.ErrorIs(t, awaitAutoLoadSchedulerResult(t, done), context.Canceled)
}

func TestEnsureCollectionReadyRequiresQueryPrivilegeOnly(t *testing.T) {
	enableAutoLoad(t)
	for _, key := range []string{Params.CommonCfg.AuthorizationEnabled.Key, Params.ProxyCfg.ResolveAliasForPrivilege.Key} {
		require.NoError(t, Params.Save(key, "true"))
		t.Cleanup(func() { require.NoError(t, Params.Reset(key)) })
	}
	coord := &MockMixCoordClientInterface{}
	coord.listPolicy = func(context.Context, *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status: merr.Success(),
			PolicyInfos: []string{
				funcutil.PolicyForPrivilege("reader", commonpb.ObjectType_Collection.String(), "collection", commonpb.ObjectPrivilege_PrivilegeSearch.String(), "db"),
				funcutil.PolicyForPrivilege("reader", commonpb.ObjectType_Collection.String(), "collection", commonpb.ObjectPrivilege_PrivilegeQuery.String(), "db"),
			},
			UserRoles: []string{
				funcutil.EncodeUserRoleCache("search_user", "reader"),
			},
		}, nil
	}
	cache := mustInitMetaCacheForTest(context.Background(), coord).(*MetaCache)
	cache.SeedCollectionForTest("db", "collection", 100, "alias").VChannels = []string{"v0"}
	var readinessCalls int
	mix := &MixCoordMock{EnsureCollectionReadyFunc: func(_ context.Context, req *querypb.EnsureCollectionReadyRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
		readinessCalls++
		require.EqualValues(t, 100, req.GetCollectionID())
		require.Equal(t, []string{"v0"}, req.GetExpectedVchannels())
		return merr.Success(), nil
	}}
	node := &Proxy{metaCache: cache, mixCoord: mix}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	interceptor := UnaryServerInterceptor(PrivilegeInterceptorWithMetaCache(node.GetMetaCache))
	ctx := GetContext(context.Background(), "search_user:pwd")
	for _, req := range []any{
		&milvuspb.SearchRequest{DbName: "db", CollectionName: "alias"},
		&milvuspb.QueryRequest{DbName: "db", CollectionName: "alias"},
	} {
		_, err := interceptor(ctx, req, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			return nil, node.ensureCollectionReady(ctx, "db", "alias")
		})
		require.NoError(t, err)
		_, err = interceptor(GetContext(context.Background(), "ungranted_user:pwd"), req, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) {
			t.Fatal("a request without query privilege reached the handler")
			return nil, nil
		})
		require.Equal(t, codes.PermissionDenied, grpcstatus.Code(err))
	}
	// The query user's policy grants no explicit LoadCollection permission.
	_, err := interceptor(ctx, &milvuspb.LoadCollectionRequest{DbName: "db", CollectionName: "alias"}, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) {
		t.Fatal("a request without Load privilege reached the handler")
		return nil, nil
	})
	require.Equal(t, codes.PermissionDenied, grpcstatus.Code(err))
	require.Equal(t, 2, readinessCalls)
}

func TestSearchStopsBeforeExecutionWhenAutoLoadFails(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	readinessCalls := 0
	searchCalls := 0
	readinessMock := mockey.Mock((*Proxy).ensureCollectionReady).To(
		func(_ *Proxy, _ context.Context, dbName, collectionName string) error {
			readinessCalls++
			require.Equal(t, "db", dbName)
			require.Equal(t, "collection", collectionName)
			return merr.ErrIndexNotFound
		}).Build()
	searchMock := mockey.Mock((*Proxy).search).To(
		func(_ *Proxy, _ context.Context, _ *milvuspb.SearchRequest, _, _ bool, _ *searchRLSSnapshot) (*milvuspb.SearchResults, bool, bool, bool, error) {
			searchCalls++
			return &milvuspb.SearchResults{Status: merr.Success()}, false, false, false, nil
		}).Build()
	defer readinessMock.UnPatch()
	defer searchMock.UnPatch()

	response, err := node.Search(context.Background(), &milvuspb.SearchRequest{
		DbName:         "db",
		CollectionName: "collection",
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrIndexNotFound)
	require.Equal(t, 1, readinessCalls)
	require.Equal(t, 0, searchCalls)
}

func TestHybridSearchStopsBeforeExecutionWhenAutoLoadFails(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	readinessCalls := 0
	hybridSearchCalls := 0
	readinessMock := mockey.Mock((*Proxy).ensureCollectionReady).To(
		func(_ *Proxy, _ context.Context, dbName, collectionName string) error {
			readinessCalls++
			require.Equal(t, "db", dbName)
			require.Equal(t, "collection", collectionName)
			return merr.ErrIndexNotFound
		}).Build()
	hybridSearchMock := mockey.Mock((*Proxy).hybridSearch).To(
		func(_ *Proxy, _ context.Context, _ *milvuspb.HybridSearchRequest, _ bool, _ *searchRLSSnapshot) (*milvuspb.SearchResults, bool, bool, error) {
			hybridSearchCalls++
			return &milvuspb.SearchResults{Status: merr.Success()}, false, false, nil
		}).Build()
	defer readinessMock.UnPatch()
	defer hybridSearchMock.UnPatch()

	response, err := node.HybridSearch(context.Background(), &milvuspb.HybridSearchRequest{
		DbName:         "db",
		CollectionName: "collection",
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrIndexNotFound)
	require.Equal(t, 1, readinessCalls)
	require.Equal(t, 0, hybridSearchCalls)
}

func TestQueryStopsBeforeExecutionWhenAutoLoadFails(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{}
	previousRateCol := rateCol
	require.NoError(t, node.initRateCollector())
	t.Cleanup(func() { rateCol = previousRateCol })
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	readinessCalls := 0
	queryCalls := 0
	readinessMock := mockey.Mock((*Proxy).ensureCollectionReady).To(
		func(_ *Proxy, _ context.Context, dbName, collectionName string) error {
			readinessCalls++
			require.Equal(t, "db", dbName)
			require.Equal(t, "collection", collectionName)
			return merr.ErrIndexNotFound
		}).Build()
	queryMock := mockey.Mock((*Proxy).query).To(
		func(_ *Proxy, _ context.Context, _ *queryTask, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			queryCalls++
			return &milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil
		}).Build()
	defer readinessMock.UnPatch()
	defer queryMock.UnPatch()

	response, err := node.Query(context.Background(), &milvuspb.QueryRequest{
		DbName:         "db",
		CollectionName: "collection",
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrIndexNotFound)
	require.Equal(t, 1, readinessCalls)
	require.Equal(t, 0, queryCalls)
}

func TestQueryExecutesWhenCollectionIsReady(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{}
	previousRateCol := rateCol
	t.Cleanup(func() { rateCol = previousRateCol })
	require.NoError(t, node.initRateCollector())
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	readinessCalls := 0
	queryCalls := 0
	readinessMock := mockey.Mock((*Proxy).ensureCollectionReady).To(
		func(_ *Proxy, _ context.Context, dbName, collectionName string) error {
			readinessCalls++
			require.Equal(t, "db", dbName)
			require.Equal(t, "collection", collectionName)
			return nil
		}).Build()
	queryMock := mockey.Mock((*Proxy).query).To(
		func(_ *Proxy, _ context.Context, _ *queryTask, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			queryCalls++
			return &milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil
		}).Build()
	defer readinessMock.UnPatch()
	defer queryMock.UnPatch()

	response, err := node.Query(context.Background(), &milvuspb.QueryRequest{
		DbName:         "db",
		CollectionName: "collection",
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(response.GetStatus()))
	require.Equal(t, 1, readinessCalls)
	require.Equal(t, 1, queryCalls)
}

func mockSearchCollectionMeta(t *testing.T, collectionID int64, vchannels []string) *MetaCache {
	metaCache := &MetaCache{}
	getCollectionInfoMock := mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
		CollID:    collectionID,
		VChannels: vchannels,
	}, nil).Build()
	t.Cleanup(func() {
		getCollectionInfoMock.UnPatch()
	})
	return metaCache
}

func enableAutoLoad(t *testing.T) {
	t.Helper()
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableAutoLoad.Key, "true"))
	t.Cleanup(func() { require.NoError(t, Params.Reset(Params.ProxyCfg.EnableAutoLoad.Key)) })
}

// Use a real scheduler with metadata and coordinator test doubles.
func newAutoLoadSchedulingProxy(t *testing.T) (*Proxy, *mocks.MockMixCoordClient) {
	t.Helper()
	enableAutoLoad(t)
	for _, setting := range []struct {
		key, previous, value string
	}{
		{Params.ProxyCfg.MaxTaskNum.Key, Params.ProxyCfg.MaxTaskNum.GetValue(), "1"},
		{Params.ProxyCfg.DDLConcurrency.Key, Params.ProxyCfg.DDLConcurrency.GetValue(), "1"},
		{Params.CommonCfg.AuthorizationEnabled.Key, Params.CommonCfg.AuthorizationEnabled.GetValue(), "false"},
	} {
		require.NoError(t, Params.Save(setting.key, setting.value))
		t.Cleanup(func() { require.NoError(t, Params.Save(setting.key, setting.previous)) })
	}

	schema, err := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
			},
		},
	})
	require.NoError(t, err)
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, "db", "collection").Return(int64(100), nil).Maybe()
	cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "collection", int64(0)).Return(&collectionInfo{
		CollID: 100, VChannels: []string{"v0"}, Schema: schema,
	}, nil).Maybe()
	cache.EXPECT().GetCollectionSchema(mock.Anything, "db", "collection").Return(schema, nil).Maybe()

	coordinator := mocks.NewMockMixCoordClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	allocator, err := newTimestampAllocator(newMockTimestampAllocatorInterface(), 0)
	require.NoError(t, err)
	sched, err := scheduler.NewTaskScheduler(ctx, allocator)
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		sched.Close()
	})
	node := &Proxy{
		ctx: ctx, sched: sched, metaCache: cache, mixCoord: coordinator, tsoAllocator: allocator,
	}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	return node, coordinator
}

type autoLoadSchedulerTask struct {
	*dqlFullMockTask
	execute func(context.Context) error
}

func newAutoLoadSchedulerBase(ctx context.Context) *dqlFullMockTask {
	return &dqlFullMockTask{TaskCondition: taskmodel.NewTaskCondition(ctx), name: taskmodel.SearchTaskName}
}

func (t *autoLoadSchedulerTask) TraceCtx() context.Context {
	return t.Ctx()
}

func (t *autoLoadSchedulerTask) Execute(ctx context.Context) error {
	return t.execute(ctx)
}

func (t *autoLoadSchedulerTask) IsSubTask() bool {
	return false
}

func awaitAutoLoadSchedulerResult[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scheduler progress")
		var zero T
		return zero
	}
}

func TestAutoLoadBypassesSaturatedTaskQueues(t *testing.T) {
	node, coordinator := newAutoLoadSchedulingProxy(t)
	loaded := make(chan *querypb.EnsureCollectionReadyRequest, 1)
	coordinator.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, request *querypb.EnsureCollectionReadyRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
			loaded <- request
			return merr.Success(), nil
		}).Once()
	require.NoError(t, node.sched.Start())

	for _, queue := range []scheduler.TaskQueue{node.sched.DdQueue, node.sched.DqQueue} {
		started := make(chan struct{})
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		blocker := &autoLoadSchedulerTask{
			dqlFullMockTask: newAutoLoadSchedulerBase(node.ctx),
			execute: func(ctx context.Context) error {
				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}
		require.NoError(t, queue.Enqueue(blocker))
		awaitAutoLoadSchedulerResult(t, started)
		// Occupy the dispatcher in Submit, then fill the pending queue too.
		require.NoError(t, queue.Enqueue(newAutoLoadSchedulerBase(node.ctx)))
		require.Eventually(t, func() bool { return queue.FrontUnissuedTask() == nil }, time.Second, time.Millisecond)
		require.NoError(t, queue.Enqueue(newAutoLoadSchedulerBase(node.ctx)))
	}

	require.NoError(t, node.ensureCollectionReady(node.ctx, "db", "collection"))
	request := awaitAutoLoadSchedulerResult(t, loaded)
	require.Equal(t, int64(100), request.GetCollectionID())
	require.Equal(t, []string{"v0"}, request.GetExpectedVchannels())

	// The explicit API must still respect the saturated DDL queue.
	status, err := node.LoadCollection(node.ctx, &milvuspb.LoadCollectionRequest{DbName: "db", CollectionName: "collection"})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrServiceTooManyRequests)
}

type dqlRetryServer struct {
	viewpb.UnimplementedQueryPlanServiceServer
	viewpb.UnimplementedViewQueryServiceServer
	loaded    atomic.Bool
	injected  atomic.Bool
	plans     atomic.Int32
	searches  atomic.Int32
	queries   atomic.Int32
	failPhase string
}

func (s *dqlRetryServer) failure(phase string) error {
	if s.failPhase == phase && s.injected.CompareAndSwap(false, true) {
		s.loaded.Store(false)
		return viewerror.NewGRPCStatusFromViewError(viewerror.NewViewInvalidated("collection released during %s", phase)).Err()
	}
	if !s.loaded.Load() {
		return viewerror.NewGRPCStatusFromViewError(viewerror.NewViewNotFound("collection is released")).Err()
	}
	return nil
}

func (s *dqlRetryServer) GetQueryPlan(_ context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	s.plans.Add(1)
	if err := s.failure("plan"); err != nil {
		return nil, err
	}
	plan := &viewpb.QueryPlan{
		ShardId: &viewpb.ShardID{Vchannel: req.GetShardId().GetVchannel(), ReplicaId: 1},
		Version: &viewpb.QueryViewVersion{},
		Mvcc:    &viewpb.QueryPlanMVCC{},
		WorkNodes: []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{
			QueryNode: &viewpb.QueryWorkNode{NodeId: 11},
		}}},
	}
	if search := req.GetLegacySearchRequest(); search != nil {
		plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: search}
	} else {
		plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: req.GetLegacyRetrieveRequest()}
	}
	return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
}

func (s *dqlRetryServer) SearchOnView(context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	s.searches.Add(1)
	if err := s.failure("search"); err != nil {
		return nil, err
	}
	return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
}

func (s *dqlRetryServer) QueryOnView(context.Context, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	s.queries.Add(1)
	if err := s.failure("query"); err != nil {
		return nil, err
	}
	return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
}

type dqlRetryPlanClient struct {
	queryclient.QueryPlanClient
	client viewpb.QueryPlanServiceClient
}

func (c *dqlRetryPlanClient) GetQueryPlan(ctx context.Context, _ qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	resp, err := c.client.GetQueryPlan(ctx, req)
	return resp, viewerror.ConvertViewError("GetQueryPlan", err)
}

type dqlRetryServiceClient struct {
	queryclient.ViewQueryServiceClient
	client viewpb.ViewQueryServiceClient
}

func (c *dqlRetryServiceClient) SearchOnView(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	resp, err := c.client.SearchOnView(ctx, req)
	return resp, viewerror.ConvertViewError("SearchOnView", err)
}

func (c *dqlRetryServiceClient) QueryOnView(ctx context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	resp, err := c.client.QueryOnView(ctx, req)
	return resp, viewerror.ConvertViewError("QueryOnView", err)
}

type dqlRetryClient struct {
	queryclient.Client
	server *dqlRetryServer
	checks atomic.Int32
}

func (c *dqlRetryClient) ResolveVChannels(context.Context, int64) ([]string, error) {
	return []string{"v0"}, nil
}

func newDQLRetryClient(t *testing.T, phase string) *dqlRetryClient {
	t.Helper()
	server := &dqlRetryServer{failPhase: phase}
	server.loaded.Store(true)
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	viewpb.RegisterQueryPlanServiceServer(grpcServer, server)
	viewpb.RegisterViewQueryServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///dql-retry", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := &dqlRetryClient{server: server}
	client.Client = queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{},
		&dqlRetryPlanClient{client: viewpb.NewQueryPlanServiceClient(conn)},
		&dqlRetryServiceClient{client: viewpb.NewViewQueryServiceClient(conn)}, client)
	return client
}

// Use the real public Proxy entry, scheduler, task Execute, legacy query client,
// shard retries, gRPC error transport, EnsureCollectionReady RPC and full DQL retries.
// Parsing/reducing results and coordinator storage are replaced by test doubles.
func TestDQLAutoLoadRetryThroughTaskAndGRPC(t *testing.T) {
	for _, test := range []struct{ method, phase string }{
		{"Search", "plan"},
		{"Search", "search"},
		{"HybridSearch", "plan"},
		{"HybridSearch", "search"},
		{"Query", "query"},
		{"Requery", "query"},
		{"SearchByPK", "query"},
		{"SearchByPK", "search"},
		{"Search", "wait"},
	} {
		t.Run(test.method+"/"+test.phase, func(t *testing.T) {
			node, coordinator := newAutoLoadSchedulingProxy(t)
			oldRateCol := rateCol
			require.NoError(t, node.initRateCollector())
			t.Cleanup(func() { rateCol = oldRateCol })
			client := newDQLRetryClient(t, test.phase)
			node.viewQueryClient = client
			if test.phase == "wait" {
				client.server.loaded.Store(false)
			}
			var loads atomic.Int32
			coordinator.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).RunAndReturn(
				func(_ context.Context, req *querypb.EnsureCollectionReadyRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
					require.EqualValues(t, 100, req.GetCollectionID())
					client.checks.Add(1)
					if !client.server.loaded.Load() {
						loads.Add(1)
						client.server.loaded.Store(true)
					}
					if client.server.failure("wait") != nil {
						return merr.Status(merr.WrapErrCollectionNotLoaded(100)), nil
					}
					return merr.Success(), nil
				})

			if test.method == "Requery" {
				// Internal requery tasks use the scheduler's subtask path; their
				// parent search is still occupying a DQL worker.
				subtask := mockey.Mock((*queryTask).IsSubTask).Return(true).Build()
				t.Cleanup(func() { subtask.UnPatch() })
			}
			var searchTasks, queryTasks atomic.Int32
			searchPre := mockey.Mock((*searchTask).PreExecute).To(func(st *searchTask, _ context.Context) error {
				searchTasks.Add(1)
				st.CollectionID = 100
				st.Nq = 1
				return nil
			}).Build()
			t.Cleanup(func() { searchPre.UnPatch() })
			queryPre := mockey.Mock((*queryTask).PreExecute).To(func(qt *queryTask, _ context.Context) error {
				queryTasks.Add(1)
				qt.CollectionID = 100
				return nil
			}).Build()
			t.Cleanup(func() { queryPre.UnPatch() })
			var queryResult *milvuspb.QueryResults
			queryResultMock := mockey.Mock((*queryTask).Result).To(func(*queryTask) *milvuspb.QueryResults { return queryResult }).Build()
			t.Cleanup(func() { queryResultMock.UnPatch() })
			queryPost := mockey.Mock((*queryTask).PostExecute).To(func(qt *queryTask, _ context.Context) error {
				queryResult = &milvuspb.QueryResults{Status: merr.Success()}
				if test.method == "SearchByPK" {
					queryResult.FieldsData = []*schemapb.FieldData{
						{FieldId: 100, FieldName: "id", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}},
						{FieldId: 101, FieldName: "vector", Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}}},
					}
				}
				return nil
			}).Build()
			t.Cleanup(func() { queryPost.UnPatch() })
			var searchResult *milvuspb.SearchResults
			searchResultMock := mockey.Mock((*searchTask).Result).To(func(*searchTask) *milvuspb.SearchResults { return searchResult }).Build()
			t.Cleanup(func() { searchResultMock.UnPatch() })
			searchPost := mockey.Mock((*searchTask).PostExecute).To(func(st *searchTask, ctx context.Context) error {
				if test.method == "Requery" {
					// The requery pipeline now lives in dql. Exercise its TaskNode
					// query-execution boundary without reaching private operator fields.
					qt := NewQueryTask(ctx, node, &milvuspb.QueryRequest{DbName: "db", CollectionName: "collection"}, nil,
						&internalpb.RetrieveRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Retrieve}, QueryLabel: metrics.ReQueryLabel}, node.GetMetaCache(), false)
					_, _, err := node.ExecuteQuery(ctx, qt, trace.SpanFromContext(ctx))
					if err != nil {
						return err
					}
				}
				searchResult = &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{NumQueries: 1}}
				return nil
			}).Build()
			t.Cleanup(func() { searchPost.UnPatch() })
			require.NoError(t, node.sched.Start())

			var status *commonpb.Status
			switch test.method {
			case "HybridSearch":
				resp, err := node.HybridSearch(node.ctx, &milvuspb.HybridSearchRequest{DbName: "db", CollectionName: "collection"})
				require.NoError(t, err)
				status = resp.GetStatus()
			case "Query":
				resp, err := node.Query(node.ctx, &milvuspb.QueryRequest{DbName: "db", CollectionName: "collection"})
				require.NoError(t, err)
				status = resp.GetStatus()
			default:
				req := &milvuspb.SearchRequest{DbName: "db", CollectionName: "collection", Nq: 1}
				if test.method == "SearchByPK" {
					req.SearchInput = &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}}
				}
				resp, err := node.Search(node.ctx, req)
				require.NoError(t, err)
				status = resp.GetStatus()
			}
			require.NoError(t, merr.Error(status))
			require.True(t, client.server.injected.Load())
			require.EqualValues(t, 2, client.checks.Load())
			if test.phase == "wait" {
				require.EqualValues(t, 2, loads.Load())
				require.EqualValues(t, 1, searchTasks.Load())
			} else {
				require.EqualValues(t, 1, loads.Load())
				if test.method == "SearchByPK" && test.phase == "search" {
					// qv recreates the PK lookup from the original IDs on each search attempt.
					require.EqualValues(t, 2, queryTasks.Load())
					require.EqualValues(t, 2, searchTasks.Load())
				} else if test.method == "Query" || test.method == "SearchByPK" {
					require.EqualValues(t, 2, queryTasks.Load())
				} else {
					require.EqualValues(t, 2, searchTasks.Load())
				}
			}
		})
	}
}

func TestDQLAutoLoadRetryEnsuresOnEveryAttempt(t *testing.T) {
	enableAutoLoad(t)
	coord := mocks.NewMockMixCoordClient(t)
	coord.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).Return(merr.Success(), nil).Twice()
	node := &Proxy{metaCache: mockSearchCollectionMeta(t, 100, []string{"v0"}), mixCoord: coord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	calls := 0
	err := node.retryDQL(context.Background(), "db", "collection", func(context.Context) (bool, error) {
		calls++
		if calls == 1 {
			return false, viewerror.NewViewInvalidated("view replaced by balance")
		}
		return false, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestDQLAutoLoadRetryStopsOnPermanentErrorsAndCancellation(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{}
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()
	for _, want := range []error{
		merr.ErrCollectionNotFound,
		merr.ErrParameterInvalid,
		viewerror.NewOnShutdownError("shutdown"),
		merr.WrapErrAsInputError(merr.ErrCollectionNotLoaded),
	} {
		calls := 0
		err := node.retryDQL(context.Background(), "db", "collection", func(context.Context) (bool, error) {
			calls++
			return false, want
		})
		require.ErrorIs(t, err, want)
		require.Equal(t, 1, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := node.retryDQL(ctx, "db", "collection", func(context.Context) (bool, error) {
		calls++
		cancel()
		return false, viewerror.NewViewNotFound("released")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestDQLAutoLoadRetryDisabled(t *testing.T) {
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableAutoLoad.Key, "false"))
	t.Cleanup(func() { require.NoError(t, Params.Reset(Params.ProxyCfg.EnableAutoLoad.Key)) })
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	calls := 0
	want := viewerror.NewViewInvalidated("released")
	err := node.retryDQL(context.Background(), "db", "collection", func(context.Context) (bool, error) {
		calls++
		return false, want
	})
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, calls)
}

func TestDQLAutoLoadRetryQueryCreatesFreshTask(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	oldRateCol := rateCol
	require.NoError(t, node.initRateCollector())
	defer func() { rateCol = oldRateCol }()
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()
	request := &milvuspb.QueryRequest{DbName: "db", CollectionName: "collection", Expr: "id > 0", GuaranteeTimestamp: 1234}
	var tasks []*queryTask
	query := mockey.Mock((*Proxy).query).To(func(_ *Proxy, _ context.Context, qt *queryTask, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
		tasks = append(tasks, qt)
		require.Same(t, request, qt.Request())
		if len(tasks) == 1 {
			require.Equal(t, "id > 0", qt.Request().Expr)
		} else {
			require.Equal(t, "changed", qt.Request().Expr)
		}
		require.EqualValues(t, 1234, qt.Request().GuaranteeTimestamp)
		qt.Request().Expr = "changed"
		if len(tasks) == 1 {
			err := viewerror.NewViewInvalidated("released")
			return &milvuspb.QueryResults{Status: merr.Status(err)}, segcore.StorageCost{}, err
		}
		return &milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil
	}).Build()
	defer query.UnPatch()
	resp, err := node.Query(context.Background(), request)
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Len(t, tasks, 2)
	require.NotSame(t, tasks[0], tasks[1])
	require.NotSame(t, tasks[0].Condition, tasks[1].Condition)
	require.Equal(t, "changed", request.Expr)
}

func TestDQLAutoLoadRetryHonorsDeadline(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{}
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	calls := 0
	err := node.retryDQL(ctx, "db", "collection", func(ctx context.Context) (bool, error) {
		calls++
		<-ctx.Done()
		return false, viewerror.NewViewInvalidated("released while request timed out")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, calls)
}

func TestDQLAutoLoadRetryPreservesLatestReadinessTimeout(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{}
	readinessCalls := 0
	ready := mockey.Mock((*Proxy).ensureCollectionReady).To(
		func(_ *Proxy, ctx context.Context, _, _ string) error {
			readinessCalls++
			if readinessCalls == 1 {
				return nil
			}
			childCtx, cancel := context.WithTimeout(ctx, time.Nanosecond)
			defer cancel()
			<-childCtx.Done()
			return context.Cause(childCtx)
		}).Build()
	defer ready.UnPatch()

	parentCtx := context.Background()
	executeCalls := 0
	err := node.retryDQL(parentCtx, "db", "collection", func(context.Context) (bool, error) {
		executeCalls++
		return false, viewerror.NewViewNotFound("collection released")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, parentCtx.Err())
	require.Equal(t, 2, readinessCalls)
	require.Equal(t, 1, executeCalls)
}

func TestDQLAutoLoadRetryPreservesSearchFallbackAndRecall(t *testing.T) {
	for _, mode := range []string{"fallback", "recall"} {
		t.Run(mode, func(t *testing.T) {
			enableAutoLoad(t)
			key := Params.AutoIndexConfig.EnableResultLimitCheck.Key
			previous := Params.AutoIndexConfig.EnableResultLimitCheck.GetValue()
			require.NoError(t, Params.Save(key, "true"))
			t.Cleanup(func() { require.NoError(t, Params.Save(key, previous)) })
			node := &Proxy{}
			checks := 0
			ready := mockey.Mock((*Proxy).ensureCollectionReady).To(func(*Proxy, context.Context, string, string) error {
				checks++
				return nil
			}).Build()
			defer ready.UnPatch()
			calls := 0
			search := mockey.Mock((*Proxy).search).To(func(_ *Proxy, _ context.Context, _ *milvuspb.SearchRequest, optimized, recall bool, _ *searchRLSSnapshot) (*milvuspb.SearchResults, bool, bool, bool, error) {
				calls++
				if calls == 1 {
					require.True(t, optimized)
					return &milvuspb.SearchResults{Status: merr.Success()}, mode == "fallback", mode == "fallback", mode == "recall", nil
				}
				if calls == 2 {
					require.False(t, optimized)
					require.Equal(t, mode == "recall", recall)
					err := viewerror.NewViewInvalidated("released during extra search")
					return &milvuspb.SearchResults{Status: merr.Status(err)}, false, false, false, err
				}
				return &milvuspb.SearchResults{Status: merr.Success()}, false, false, false, nil
			}).Build()
			defer search.UnPatch()
			resp, err := node.Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "collection"})
			require.NoError(t, err)
			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Equal(t, 3, calls)
			require.Equal(t, 2, checks)
		})
	}
}

func TestDQLAutoLoadRetryPreservesInconsistentRequery(t *testing.T) {
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableAutoLoad.Key, "false"))
	t.Cleanup(func() { require.NoError(t, Params.Reset(Params.ProxyCfg.EnableAutoLoad.Key)) })
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	calls := 0
	search := mockey.Mock((*Proxy).hybridSearch).To(func(*Proxy, context.Context, *milvuspb.HybridSearchRequest, bool, *searchRLSSnapshot) (*milvuspb.SearchResults, bool, bool, error) {
		calls++
		if calls == 1 {
			return &milvuspb.SearchResults{Status: merr.Status(merr.ErrInconsistentRequery)}, false, false, nil
		}
		return &milvuspb.SearchResults{Status: merr.Success()}, false, false, nil
	}).Build()
	defer search.UnPatch()
	resp, err := node.HybridSearch(context.Background(), &milvuspb.HybridSearchRequest{DbName: "db", CollectionName: "collection"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, 2, calls)
}

func TestDQLQueryRetryCountsRequestOnce(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	previous := rateCol
	require.NoError(t, node.initRateCollector())
	t.Cleanup(func() { rateCol = previous })
	counter := metrics.ProxyReceivedNQ.WithLabelValues(paramtable.GetStringNodeID(), metrics.QueryLabel, "db", "retry_count")
	before := testutil.ToFloat64(counter)
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()
	calls := 0
	query := mockey.Mock((*Proxy).query).To(func(*Proxy, context.Context, *queryTask, trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
		calls++
		if calls <= 2 {
			err := viewerror.NewViewInvalidated("retry query")
			return &milvuspb.QueryResults{Status: merr.Status(err)}, segcore.StorageCost{}, err
		}
		return &milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil
	}).Build()
	defer query.UnPatch()
	resp, err := node.Query(context.Background(), &milvuspb.QueryRequest{DbName: "db", CollectionName: "retry_count"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, 3, calls)
	require.Equal(t, before+1, testutil.ToFloat64(counter))
	rate, err := rateCol.Rate(internalpb.RateType_DQLQuery.String(), ratelimitutil.DefaultWindow)
	require.NoError(t, err)
	require.InDelta(t, 1/float64(ratelimitutil.DefaultWindow/ratelimitutil.DefaultGranularity), rate, 1e-9)
}

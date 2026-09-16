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

package proxy

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/reqregistry"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func kv(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

func TestRequestInfoExtraction(t *testing.T) {
	t.Run("search", func(t *testing.T) {
		info := searchRequestInfo(&milvuspb.SearchRequest{
			DbName: "db", CollectionName: "c", Nq: 7, Dsl: "a > 1",
			SearchParams: []*commonpb.KeyValuePair{kv("metric_type", "L2"), kv("topk", "10")},
		})
		assert.Equal(t, reqregistry.TypeSearch, info.Type)
		assert.Equal(t, "db", info.DBName)
		assert.Equal(t, "c", info.CollectionName)
		assert.Equal(t, int64(7), info.NQ)
		assert.Equal(t, int64(10), info.TopK)
		assert.Equal(t, "a > 1", info.Expr)

		// limit is accepted as an alias of topk; a malformed value yields 0
		assert.Equal(t, int64(5), searchRequestInfo(&milvuspb.SearchRequest{SearchParams: []*commonpb.KeyValuePair{kv("limit", "5")}}).TopK)
		assert.Equal(t, int64(0), searchRequestInfo(&milvuspb.SearchRequest{SearchParams: []*commonpb.KeyValuePair{kv("topk", "ten")}}).TopK)
	})

	t.Run("hybrid search", func(t *testing.T) {
		info := hybridSearchRequestInfo(&milvuspb.HybridSearchRequest{
			DbName: "db", CollectionName: "c",
			Requests: []*milvuspb.SearchRequest{
				{Nq: 2, Dsl: "x", SearchParams: []*commonpb.KeyValuePair{kv("topk", "20")}},
				{Nq: 3, Dsl: "y", SearchParams: []*commonpb.KeyValuePair{kv("topk", "30")}},
			},
			RankParams: []*commonpb.KeyValuePair{kv("limit", "15")},
		})
		assert.Equal(t, reqregistry.TypeHybridSearch, info.Type)
		assert.Equal(t, int64(5), info.NQ, "nq is summed over the sub-requests")
		assert.Equal(t, int64(15), info.TopK, "the final limit wins over the sub-request topk")
		assert.Equal(t, "x", info.Expr)

		// without a rank limit the largest sub-request topk is reported
		assert.Equal(t, int64(30), hybridSearchRequestInfo(&milvuspb.HybridSearchRequest{
			Requests: []*milvuspb.SearchRequest{
				{SearchParams: []*commonpb.KeyValuePair{kv("topk", "20")}},
				{SearchParams: []*commonpb.KeyValuePair{kv("topk", "30")}},
			},
		}).TopK)
	})

	t.Run("query", func(t *testing.T) {
		info := queryRequestInfo(&milvuspb.QueryRequest{
			DbName: "db", CollectionName: "c", Expr: "id in [1,2]",
			QueryParams: []*commonpb.KeyValuePair{kv("limit", "100")},
		})
		assert.Equal(t, reqregistry.TypeQuery, info.Type)
		assert.Equal(t, int64(0), info.NQ)
		assert.Equal(t, int64(100), info.TopK)
		assert.Equal(t, "id in [1,2]", info.Expr)
	})
}

func TestClientAddrFromContext(t *testing.T) {
	assert.Equal(t, "", clientAddrFromContext(context.Background()))
	addr := &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 12345}
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: addr})
	assert.Equal(t, "10.0.0.1:12345", clientAddrFromContext(ctx))
}

func TestStatusIfCancelled(t *testing.T) {
	ok := merr.Success()
	assert.Same(t, ok, statusIfCancelled(context.Background(), ok), "a live ctx leaves the status alone")

	// a client-side cancellation is not an operator cancel
	clientCtx, clientCancel := context.WithCancel(context.Background())
	clientCancel()
	timeout := merr.Status(context.Canceled)
	assert.Same(t, timeout, statusIfCancelled(clientCtx, timeout))

	// an operator cancel replaces whatever status the inner layers produced
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(merr.WrapErrRequestCancelled("root", "heavy"))
	status := statusIfCancelled(ctx, merr.Status(context.Canceled))
	assert.ErrorIs(t, merr.Error(status), merr.ErrRequestCancelled)
	assert.False(t, status.GetRetriable())
}

func TestRegisterAndCancelRequestOnProxy(t *testing.T) {
	cache := NewMockCache(t)
	cache.EXPECT().AllocID(mock.Anything).Return(int64(4242), nil)
	node := &Proxy{requests: reqregistry.New()}
	node.metaCache = cache

	parent := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 1}})
	ctx, entry := node.registerRequest(parent, searchRequestInfo(&milvuspb.SearchRequest{DbName: "db", CollectionName: "c", Nq: 1}))
	require.NotNil(t, entry)
	assert.Same(t, entry, reqregistry.FromContext(ctx))
	assert.Equal(t, int64(4242), entry.RequestID())

	listed := node.listRunningRequests(reqregistry.Filter{DBName: "db"})
	require.Len(t, listed, 1)
	assert.Equal(t, int64(4242), listed[0].RequestID)
	assert.Equal(t, "10.0.0.2:1", listed[0].ClientAddr)
	assert.Equal(t, reqregistry.StateQueued, listed[0].State)
	assert.Empty(t, node.listRunningRequests(reqregistry.Filter{DBName: "other"}))

	cancelled, notFound := node.cancelRequests(context.Background(), []int64{4242, 1}, "root", "test")
	require.Len(t, cancelled, 1)
	assert.Equal(t, []int64{1}, notFound)
	assert.Equal(t, int64(4242), cancelled[0].RequestID)
	assert.ErrorIs(t, reqregistry.CancelCause(ctx), merr.ErrRequestCancelled)
	assert.True(t, cancelled[0].ElapsedMS >= 0)

	node.unregisterRequest(entry)
	assert.Empty(t, node.listRunningRequests(reqregistry.Filter{}))
	// unregister is nil-safe for the unregistered path
	node.unregisterRequest(nil)
}

func TestRegisterRequestIsBestEffort(t *testing.T) {
	t.Run("no registry", func(t *testing.T) {
		node := &Proxy{}
		ctx, entry := node.registerRequest(context.Background(), reqregistry.Info{})
		assert.Nil(t, entry)
		assert.Nil(t, reqregistry.FromContext(ctx))
		cancelled, notFound := node.cancelRequests(context.Background(), []int64{1}, "root", "")
		assert.Empty(t, cancelled)
		assert.Equal(t, []int64{1}, notFound)
		assert.Nil(t, node.listRunningRequests(reqregistry.Filter{}))
	})

	t.Run("id allocation fails", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.EXPECT().AllocID(mock.Anything).Return(int64(0), errors.New("rootcoord unreachable"))
		node := &Proxy{requests: reqregistry.New()}
		node.metaCache = cache
		ctx, entry := node.registerRequest(context.Background(), reqregistry.Info{})
		assert.Nil(t, entry, "the request still runs, only unregistered")
		assert.Nil(t, ctx.Err())
		assert.Equal(t, 0, node.requests.Len())
	})
}

func TestUnregisterReleasesContextWithoutOperatorCause(t *testing.T) {
	cache := NewMockCache(t)
	cache.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)
	node := &Proxy{requests: reqregistry.New()}
	node.metaCache = cache

	ctx, entry := node.registerRequest(context.Background(), reqregistry.Info{})
	node.unregisterRequest(entry)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("unregister must release the derived ctx")
	}
	assert.Nil(t, reqregistry.CancelCause(ctx))
}

// TestCancelCountsOnlyWhatItActuallyCancelled locks the counter to the number
// of requests that were really stopped, not to the number of ids asked for or
// to the number of API calls: an operator reading it must not be misled by a
// cancel that found nothing, or by one that named several ids at once.
func TestCancelCountsOnlyWhatItActuallyCancelled(t *testing.T) {
	paramtable.Init()
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	counter := metrics.ProxyRequestCancelledTotal.WithLabelValues(nodeID, reqregistry.TypeSearch)
	before := testutil.ToFloat64(counter)

	cache := NewMockCache(t)
	cache.EXPECT().AllocID(mock.Anything).Return(int64(11), nil).Once()
	cache.EXPECT().AllocID(mock.Anything).Return(int64(12), nil).Once()
	node := &Proxy{requests: reqregistry.New()}
	node.metaCache = cache

	_, e1 := node.registerRequest(context.Background(), searchRequestInfo(&milvuspb.SearchRequest{DbName: "db", CollectionName: "c"}))
	_, e2 := node.registerRequest(context.Background(), searchRequestInfo(&milvuspb.SearchRequest{DbName: "db", CollectionName: "c"}))
	defer node.unregisterRequest(e1)
	defer node.unregisterRequest(e2)

	// one call, two real cancellations and one id nobody holds
	cancelled, notFound := node.cancelRequests(context.Background(), []int64{11, 12, 99}, "root", "test")
	require.Len(t, cancelled, 2)
	assert.Equal(t, []int64{99}, notFound)
	assert.Equal(t, float64(2), testutil.ToFloat64(counter)-before)

	// cancelling the same requests again reports and counts nothing
	cancelled, _ = node.cancelRequests(context.Background(), []int64{11, 12}, "root", "test")
	assert.Empty(t, cancelled)
	assert.Equal(t, float64(2), testutil.ToFloat64(counter)-before)
}

func TestListLocalRunningRequests(t *testing.T) {
	cache := NewMockCache(t)
	cache.EXPECT().AllocID(mock.Anything).Return(int64(1), nil).Once()
	cache.EXPECT().AllocID(mock.Anything).Return(int64(2), nil).Once()
	node := &Proxy{requests: reqregistry.New()}
	node.metaCache = cache
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	_, e1 := node.registerRequest(context.Background(), searchRequestInfo(&milvuspb.SearchRequest{
		DbName: "db1", CollectionName: "c1", Nq: 3,
		SearchParams: []*commonpb.KeyValuePair{kv("topk", "10")},
	}))
	_, e2 := node.registerRequest(context.Background(), queryRequestInfo(&milvuspb.QueryRequest{
		DbName: "db2", CollectionName: "c2", Expr: "id > 0",
	}))
	defer node.unregisterRequest(e1)
	defer node.unregisterRequest(e2)

	resp, err := node.ListLocalRunningRequests(context.Background(), &milvuspb.ListRunningRequestsRequest{})
	require.NoError(t, err)
	require.True(t, merr.Ok(resp.GetStatus()))
	require.Len(t, resp.GetRequests(), 2)
	byID := lo.KeyBy(resp.GetRequests(), func(r *milvuspb.RunningRequestInfo) int64 { return r.GetRequestId() })
	assert.Equal(t, reqregistry.TypeSearch, byID[1].GetType())
	assert.Equal(t, "db1", byID[1].GetDbName())
	assert.Equal(t, int64(3), byID[1].GetNq())
	assert.Equal(t, int64(10), byID[1].GetTopk())
	assert.True(t, byID[1].GetCancellable())
	assert.NotZero(t, byID[1].GetStartTimeMs())
	assert.Equal(t, reqregistry.TypeQuery, byID[2].GetType())
	assert.Equal(t, "id > 0", byID[2].GetExpr())

	// the filters are applied on this proxy, not by the caller
	resp, err = node.ListLocalRunningRequests(context.Background(), &milvuspb.ListRunningRequestsRequest{DbName: "db2"})
	require.NoError(t, err)
	require.Len(t, resp.GetRequests(), 1)
	assert.Equal(t, int64(2), resp.GetRequests()[0].GetRequestId())

	resp, err = node.ListLocalRunningRequests(context.Background(), &milvuspb.ListRunningRequestsRequest{MinElapsedMs: 1 << 40})
	require.NoError(t, err)
	assert.Empty(t, resp.GetRequests())
}

func TestCancelLocalRequests(t *testing.T) {
	cache := NewMockCache(t)
	cache.EXPECT().AllocID(mock.Anything).Return(int64(7), nil)
	node := &Proxy{requests: reqregistry.New()}
	node.metaCache = cache
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	ctx, entry := node.registerRequest(context.Background(), searchRequestInfo(&milvuspb.SearchRequest{DbName: "db", CollectionName: "c"}))
	defer node.unregisterRequest(entry)

	resp, err := node.CancelLocalRequests(context.Background(), &milvuspb.CancelRequestsRequest{
		RequestIds: []int64{7, 8},
		Reason:     "too heavy",
	})
	require.NoError(t, err)
	require.True(t, merr.Ok(resp.GetStatus()))
	require.Len(t, resp.GetCancelled(), 1)
	assert.Equal(t, int64(7), resp.GetCancelled()[0].GetRequestId())
	// an id this proxy does not hold is reported back; only the coordinator,
	// which sees every proxy, can call it not found for the cluster
	assert.Equal(t, []int64{8}, resp.GetNotFound())

	cause := reqregistry.CancelCause(ctx)
	require.ErrorIs(t, cause, merr.ErrRequestCancelled)
	assert.Contains(t, cause.Error(), "too heavy")
	// this proxy did not authenticate the operator, so it names none
	assert.NotContains(t, cause.Error(), "operator")
}

func TestPublicRunningRequestHandlersDelegateToCoordinator(t *testing.T) {
	node := &Proxy{requests: reqregistry.New()}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	mixc := mocks.NewMockMixCoordClient(t)
	node.mixCoord = mixc

	mixc.EXPECT().ListRunningRequests(mock.Anything, mock.Anything).Return(&milvuspb.ListRunningRequestsResponse{
		Status:   merr.Success(),
		Requests: []*milvuspb.RunningRequestInfo{{RequestId: 5, ProxyId: 999}},
	}, nil)
	listResp, err := node.ListRunningRequests(context.Background(), &milvuspb.ListRunningRequestsRequest{})
	require.NoError(t, err)
	require.Len(t, listResp.GetRequests(), 1)
	// the answer is the cluster's, not this proxy's registry
	assert.Equal(t, int64(999), listResp.GetRequests()[0].GetProxyId())

	mixc.EXPECT().CancelRequests(mock.Anything, mock.Anything).Return(&milvuspb.CancelRequestsResponse{
		Status:    merr.Success(),
		Cancelled: []*milvuspb.RunningRequestInfo{{RequestId: 5, ProxyId: 999, ElapsedMs: 42}},
		NotFound:  []int64{6},
	}, nil)
	cancelResp, err := node.CancelRequests(context.Background(), &milvuspb.CancelRequestsRequest{RequestIds: []int64{5, 6}, Reason: "r"})
	require.NoError(t, err)
	require.Len(t, cancelResp.GetCancelled(), 1)
	assert.Equal(t, int64(42), cancelResp.GetCancelled()[0].GetElapsedMs())
	assert.Equal(t, []int64{6}, cancelResp.GetNotFound())
}

func TestCancelRequestsRejectsEmptyIDs(t *testing.T) {
	node := &Proxy{requests: reqregistry.New()}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	// no coordinator call is expected: the request never leaves this proxy
	resp, err := node.CancelRequests(context.Background(), &milvuspb.CancelRequestsRequest{})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrParameterMissing)
}

func TestRunningRequestHandlersRejectUnhealthyProxy(t *testing.T) {
	node := &Proxy{requests: reqregistry.New()}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	listResp, err := node.ListRunningRequests(context.Background(), &milvuspb.ListRunningRequestsRequest{})
	require.NoError(t, err)
	assert.False(t, merr.Ok(listResp.GetStatus()))

	cancelResp, err := node.CancelRequests(context.Background(), &milvuspb.CancelRequestsRequest{RequestIds: []int64{1}})
	require.NoError(t, err)
	assert.False(t, merr.Ok(cancelResp.GetStatus()))

	localListResp, err := node.ListLocalRunningRequests(context.Background(), &milvuspb.ListRunningRequestsRequest{})
	require.NoError(t, err)
	assert.False(t, merr.Ok(localListResp.GetStatus()))

	localCancelResp, err := node.CancelLocalRequests(context.Background(), &milvuspb.CancelRequestsRequest{RequestIds: []int64{1}})
	require.NoError(t, err)
	assert.False(t, merr.Ok(localCancelResp.GetStatus()))
}

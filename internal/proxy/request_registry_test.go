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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
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

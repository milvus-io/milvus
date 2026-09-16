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
	"strconv"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/peer"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/reqregistry"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// registerRequest records a DQL request at the top of its RPC method and
// returns the ctx the request must run under. The returned entry may be nil
// (registration is best effort: the request still executes, it is only not
// listable) and every helper here accepts a nil entry.
//
// This must be called from the public RPC method itself, not from the inner
// search/query helpers: those are re-entered by the retry wrapper, by the
// requery and by the search-by-primary-key vector fetch, and would register
// one request several times.
func (node *Proxy) registerRequest(ctx context.Context, info reqregistry.Info) (context.Context, *reqregistry.Entry) {
	if node.requests == nil {
		return ctx, nil
	}
	cache := node.GetMetaCache()
	if cache == nil {
		return ctx, nil
	}
	requestID, err := cache.AllocID(ctx)
	if err != nil {
		mlog.Warn(ctx, "failed to allocate a request id; the request runs unregistered", mlog.Err(err))
		return ctx, nil
	}
	info.RequestID = requestID
	info.ProxyID = paramtable.GetNodeID()
	info.User = GetCurUserFromContextOrDefault(ctx)
	info.ClientAddr = clientAddrFromContext(ctx)
	if info.TraceID == "" {
		if sc := trace.SpanFromContext(ctx).SpanContext(); sc.HasTraceID() {
			info.TraceID = sc.TraceID().String()
		}
	}
	return node.requests.Register(ctx, info)
}

// unregisterRequest removes the entry when the RPC method returns.
func (node *Proxy) unregisterRequest(entry *reqregistry.Entry) {
	if node.requests == nil || entry == nil {
		return
	}
	node.requests.Unregister(entry)
}

// statusIfCancelled replaces status with merr.ErrRequestCancelled when the
// request ctx was ended by an operator cancel. Inner layers only ever see the
// plain ctx error; this single conversion at the outermost RPC layer is what
// lets the client tell an operator cancel from its own timeout.
func statusIfCancelled(ctx context.Context, status *commonpb.Status) *commonpb.Status {
	if cause := reqregistry.CancelCause(ctx); cause != nil {
		return merr.Status(cause)
	}
	return status
}

// listRunningRequests returns the requests registered on this proxy.
func (node *Proxy) listRunningRequests(filter reqregistry.Filter) []reqregistry.Info {
	if node.requests == nil {
		return nil
	}
	return node.requests.List(filter, time.Now())
}

// cancelRequests cancels the given requests on this proxy on behalf of
// operator, writes one audit line per cancelled request and counts it.
func (node *Proxy) cancelRequests(ctx context.Context, requestIDs []int64, operator, reason string) (cancelled []reqregistry.Info, notFound []int64) {
	if node.requests == nil {
		return nil, requestIDs
	}
	cancelled, notFound = node.requests.Cancel(requestIDs, operator, reason, time.Now())
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	for _, info := range cancelled {
		mlog.Info(ctx, "request cancelled by operator",
			mlog.String("operator", operator),
			mlog.String("reason", reason),
			mlog.Int64("requestID", info.RequestID),
			mlog.String("type", info.Type),
			mlog.String("db", info.DBName),
			mlog.String("collection", info.CollectionName),
			mlog.String("user", info.User),
			mlog.String("clientAddr", info.ClientAddr),
			mlog.Int64("nq", info.NQ),
			mlog.Int64("topk", info.TopK),
			mlog.Int64("elapsedMs", info.ElapsedMS),
			mlog.Int64s("taskIDs", info.TaskIDs),
			mlog.String("traceID", info.TraceID))
		metrics.ProxyRequestCancelledTotal.WithLabelValues(nodeID, info.Type).Inc()
	}
	return cancelled, notFound
}

func clientAddrFromContext(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	return p.Addr.String()
}

// kvInt64 returns the first of keys present in kvs as an int64, or 0.
func kvInt64(kvs []*commonpb.KeyValuePair, keys ...string) int64 {
	for _, key := range keys {
		if raw, err := funcutil.GetAttrByKeyFromRepeatedKV(key, kvs); err == nil {
			if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
				return v
			}
		}
	}
	return 0
}

func searchRequestInfo(request *milvuspb.SearchRequest) reqregistry.Info {
	return reqregistry.Info{
		Type:           reqregistry.TypeSearch,
		DBName:         request.GetDbName(),
		CollectionName: request.GetCollectionName(),
		NQ:             request.GetNq(),
		TopK:           kvInt64(request.GetSearchParams(), common.TopKKey, LimitKey),
		Expr:           request.GetDsl(),
	}
}

func hybridSearchRequestInfo(request *milvuspb.HybridSearchRequest) reqregistry.Info {
	var nq, topk int64
	expr := ""
	for _, sub := range request.GetRequests() {
		nq += sub.GetNq()
		if k := kvInt64(sub.GetSearchParams(), common.TopKKey, LimitKey); k > topk {
			topk = k
		}
		if expr == "" {
			expr = sub.GetDsl()
		}
	}
	if k := kvInt64(request.GetRankParams(), LimitKey, common.TopKKey); k > 0 {
		topk = k
	}
	return reqregistry.Info{
		Type:           reqregistry.TypeHybridSearch,
		DBName:         request.GetDbName(),
		CollectionName: request.GetCollectionName(),
		NQ:             nq,
		TopK:           topk,
		Expr:           expr,
	}
}

func queryRequestInfo(request *milvuspb.QueryRequest) reqregistry.Info {
	return reqregistry.Info{
		Type:           reqregistry.TypeQuery,
		DBName:         request.GetDbName(),
		CollectionName: request.GetCollectionName(),
		TopK:           kvInt64(request.GetQueryParams(), LimitKey),
		Expr:           request.GetExpr(),
	}
}

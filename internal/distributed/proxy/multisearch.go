package grpcproxy

import (
	"context"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/multisearchpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Both network requests and MultiSearch items use this exact entry chain.
// Each item uses Search's method name and request type, so RBAC, quotas,
// hooks, access logs and standalone legacy forwarding retain Search semantics.
func proxyEntryInterceptor(limiter types.Limiter) grpc.UnaryServerInterceptor {
	searchEntry := grpc_middleware.ChainUnaryServer(
		streaming.ForwardLegacyProxyUnaryServerInterceptor(),
		proxy.DatabaseInterceptor(),
		UnaryRequestStatsInterceptor,
		accesslog.UnaryAccessLogInterceptor,
		proxy.GrpcAuthInterceptor(proxy.AuthenticationInterceptor),
		proxy.UnaryServerInterceptor(proxy.PrivilegeInterceptor),
		proxy.UnaryServerHookInterceptor(),
		mlog.UnaryServerInterceptor(typeutil.ProxyRole),
		proxy.RateLimitInterceptor(limiter),
		accesslog.UnaryUpdateAccessInfoInterceptor,
		proxy.TraceLogInterceptor,
		connection.KeepActiveInterceptor,
	)
	// The envelope has no single collection privilege or search quota. Do not
	// rely on the unknown-request fallbacks of those interceptors. Each item
	// explicitly passes through searchEntry, including collection RBAC and hooks.
	envelopeEntry := grpc_middleware.ChainUnaryServer(
		UnaryRequestStatsInterceptor,
		accesslog.UnaryAccessLogInterceptor,
		proxy.GrpcAuthInterceptor(proxy.AuthenticationInterceptor),
		mlog.UnaryServerInterceptor(typeutil.ProxyRole),
		accesslog.UnaryUpdateAccessInfoInterceptor,
		connection.KeepActiveInterceptor,
	)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == multisearchpb.MultiSearchService_MultiSearch_FullMethodName {
			return envelopeEntry(ctx, req, info, handler)
		}
		return searchEntry(ctx, req, info, handler)
	}
}

type multiSearchServer struct {
	executor *proxy.MultiSearchExecutor
}

func newMultiSearchServer(s *Server, limiter types.Limiter) *multiSearchServer {
	p := &paramtable.Get().ProxyCfg
	limits := proxy.MultiSearchLimits{
		MaxItems:          max(1, p.MultiSearchMaxItems.GetAsInt()),
		Parallelism:       max(1, p.MultiSearchParallelism.GetAsInt()),
		GlobalParallelism: max(1, p.MultiSearchGlobalParallelism.GetAsInt()),
		MaxBatches:        max(1, p.MultiSearchMaxBatches.GetAsInt()),
		MaxRequestBytes:   max(1024, p.MultiSearchMaxRequestBytes.GetAsInt()),
		MaxResponseBytes:  max(1024, p.MultiSearchMaxResponseBytes.GetAsInt()),
		Timeout:           time.Duration(max(1, p.MultiSearchTimeoutSeconds.GetAsInt())) * time.Second,
	}
	entry := proxyEntryInterceptor(limiter)
	search := func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		// Fork mutable stats; the access-log interceptor similarly allocates
		// an independent object. Identity, deadlines and tracing are inherited.
		ctx = metrics.WithIndependentRPCStats(ctx)
		resp, err := entry(ctx, req, &grpc.UnaryServerInfo{Server: s, FullMethod: milvuspb.MilvusService_Search_FullMethodName},
			func(ctx context.Context, req any) (any, error) {
				return s.Search(ctx, req.(*milvuspb.SearchRequest))
			})
		if err != nil {
			return nil, err
		}
		result, ok := resp.(*milvuspb.SearchResults)
		if !ok || result == nil {
			return &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrServiceInternalMsg("Search entry returned an invalid response"))}, nil
		}
		return result, nil
	}
	return &multiSearchServer{executor: proxy.NewMultiSearchExecutor(limits, search)}
}

func (s *multiSearchServer) MultiSearch(ctx context.Context, req *multisearchpb.MultiSearchRequest) (*multisearchpb.MultiSearchResponse, error) {
	return s.executor.Execute(ctx, req)
}

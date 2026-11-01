package grpcproxy

import (
	"context"

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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Both network requests and MultiSearch items use this exact entry chain.
// Each item uses Search's method name and request type, so RBAC, quotas,
// hooks, access logs and standalone legacy forwarding retain Search semantics.
func proxyEntryInterceptor(limiter types.Limiter, getMetaCache func() proxy.Cache) grpc.UnaryServerInterceptor {
	searchEntry := grpc_middleware.ChainUnaryServer(
		streaming.ForwardLegacyProxyUnaryServerInterceptor(),
		proxy.DatabaseInterceptor(),
		UnaryRequestStatsInterceptor,
		accesslog.UnaryAccessLogInterceptor,
		proxy.GrpcAuthInterceptor(proxy.AuthenticationInterceptorWithMetaCache(getMetaCache)),
		proxy.UnaryServerInterceptor(proxy.PrivilegeInterceptorWithMetaCache(getMetaCache)),
		proxy.UnaryServerHookInterceptor(),
		mlog.UnaryServerInterceptor(typeutil.ProxyRole),
		proxy.RateLimitInterceptorWithMetaCache(getMetaCache, limiter),
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
		proxy.GrpcAuthInterceptor(proxy.AuthenticationInterceptorWithMetaCache(getMetaCache)),
		mlog.UnaryServerInterceptor(typeutil.ProxyRole),
		accesslog.UnaryUpdateAccessInfoInterceptor,
		connection.KeepActiveInterceptor,
	)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == milvuspb.MilvusService_MultiSearch_FullMethodName {
			return envelopeEntry(ctx, req, info, handler)
		}
		return searchEntry(ctx, req, info, handler)
	}
}

// Configure the existing Proxy, without making the transport own batch state.
// Alternate ProxyComponent implementations may implement MultiSearch themselves.
func configureMultiSearch(component types.ProxyComponent, limiter types.Limiter, getMetaCache func() proxy.Cache) {
	if node, ok := component.(interface {
		SetMultiSearchEntry(func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error))
	}); ok {
		node.SetMultiSearchEntry(newMultiSearchEntry(component, limiter, getMetaCache))
	}
}

func newMultiSearchEntry(component types.ProxyComponent, limiter types.Limiter, getMetaCache func() proxy.Cache) func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	entry := proxyEntryInterceptor(limiter, getMetaCache)
	return func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		// Fork mutable stats; the access-log interceptor similarly allocates
		// an independent object. Identity, deadlines and tracing are inherited.
		ctx = metrics.WithIndependentRPCStats(ctx)
		resp, err := entry(ctx, req, &grpc.UnaryServerInfo{Server: component, FullMethod: milvuspb.MilvusService_Search_FullMethodName},
			func(ctx context.Context, req any) (any, error) {
				return component.Search(ctx, req.(*milvuspb.SearchRequest))
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
}

func (s *Server) MultiSearch(ctx context.Context, req *milvuspb.MultiSearchRequest) (*milvuspb.MultiSearchResponse, error) {
	return s.proxy.MultiSearch(ctx, req)
}

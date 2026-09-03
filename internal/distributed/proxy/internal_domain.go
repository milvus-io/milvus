package grpcproxy

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/tracer"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This file is the proxy's seam for the internal-surfaces capability: WHERE
// the extra listeners a form declares are opened, and with which chains. The
// declaration - and the isolation argument that makes an unauthenticated
// MilvusService acceptable - lives with the capability in pkg/extension.
//
// With no capability installed neither function opens anything, and the
// binary serves exactly the listeners it always did.

// internalDomainListeners resolves the listeners a form declared, or the zero
// value with no capability installed - which leaves both ports closed.
func internalDomainListeners() extension.InternalListeners {
	surfaces := extension.Caps().InternalSurfaces
	if surfaces == nil {
		return extension.InternalListeners{}
	}
	return surfaces.InternalDomainListeners()
}

// listenInternal opens one internal-domain listener on the declared bind
// address. An empty BindAddress binds every interface, which is what the fork
// this replaces did and what a deployment whose isolation is the pod network
// wants; a form that reaches its instance on one interface names it and the
// unauthenticated surface is then not reachable on the others.
func listenInternal(bindAddress string, port int) (net.Listener, error) {
	return net.Listen("tcp", fmt.Sprintf("%s:%d", bindAddress, port))
}

// startInternalDomainServers opens the declared internal-domain listeners.
// Called from start(), after the proxy component is serving; errors abort
// start-up, because a form that declared the surfaces cannot be operated
// without them.
func (s *Server) startInternalDomainServers() error {
	listeners := internalDomainListeners()
	if listeners.GRPCPort != 0 {
		if err := s.startInternalDomainGrpc(listeners.BindAddress, listeners.GRPCPort); err != nil {
			return merr.WrapErrServiceInternalErr(err, "start the internal-domain grpc listener")
		}
	}
	if listeners.RESTPort != 0 {
		if err := s.startInternalDomainRest(listeners.BindAddress, listeners.RESTPort); err != nil {
			return merr.WrapErrServiceInternalErr(err, "start the internal-domain rest listener")
		}
	}
	return nil
}

// startInternalDomainGrpc serves the full MilvusService with no
// authentication or privilege interceptors, for the control plane the
// capability documents. The chain keeps the request-shaping interceptors -
// tracing, cluster and server-id validation, access log, database context,
// the hook, connection keep-alive - and deliberately omits Auth, Privilege
// and the rate limiter: control-plane operations are neither end-user
// credentials nor end-user traffic.
func (s *Server) startInternalDomainGrpc(bindAddress string, port int) error {
	Params := &paramtable.Get().ProxyGrpcServerCfg
	// All interfaces, exactly like the external listeners: netutil's OptIP is
	// the ANNOUNCED address, not a bind address - every proxy listener binds
	// ":port" - so this unauthenticated surface is no wider than the
	// authenticated one, and both are confined by the same network boundary
	// (which in this deployment is the pod network policy, not the bind).
	// Narrowing the bind is a change to make for all of them together.
	listener, err := listenInternal(bindAddress, port)
	if err != nil {
		return err
	}

	limiterOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize.GetAsInt()),
		// The stream side needs its own chain: interceptors bind to one of
		// gRPC's two call kinds, and without this a stream method on this
		// listener (CreateReplicateStream) would arrive with neither the
		// provenance mark - so the admin seam would refuse the control
		// plane's own call - nor cluster validation.
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			internalDomainMarkStreamInterceptor,
			interceptor.ClusterValidationStreamServerInterceptor(),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			// First in the chain: every request this listener accepts is the
			// control plane's, and the mark is how handler-level seams -
			// shared with the external listener - tell the two apart.
			internalDomainMarkInterceptor,
			interceptor.ClusterValidationUnaryServerInterceptor(),
			interceptor.ServerIDValidationUnaryServerInterceptor(func() int64 {
				if s.serverID.Load() == 0 {
					s.serverID.Store(paramtable.GetNodeID())
				}
				return s.serverID.Load()
			}),
			accesslog.UnaryAccessLogInterceptor,
			proxy.DatabaseInterceptor(),
			proxy.UnaryServerHookInterceptor(),
			mlog.UnaryServerInterceptor(typeutil.ProxyRole),
			connection.KeepActiveInterceptor,
		)),
		grpc.StatsHandler(tracer.GetDynamicOtelGrpcServerStatsHandler()),
	}
	// TLS follows common.security.tlsMode as the gateway in front of this
	// listener expects, with a server certificate only: the gateway is not a
	// client-certificate holder, so mutual TLS never applied here - the fork
	// this replaces terminated mode 2 the same one-way way.
	if Params.TLSMode.GetAsInt() >= 1 {
		creds, err := credentials.NewServerTLSFromFile(Params.ServerPemPath.GetValue(), Params.ServerKeyPath.GetValue())
		if err != nil {
			return merr.WrapErrServiceInternalErr(err, "load the internal-domain server certificate")
		}
		limiterOpts = append(limiterOpts, grpc.Creds(creds))
	}
	s.internalDomainGrpcServer = grpc.NewServer(limiterOpts...)
	milvuspb.RegisterMilvusServiceServer(s.internalDomainGrpcServer, s.proxy)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.internalDomainGrpcServer.Serve(listener); err != nil && !isGracefulStopError(err) {
			mlog.Error(s.ctx, "internal-domain grpc server exited", mlog.Err(err))
		}
	}()
	mlog.Info(s.ctx, "internal-domain grpc listener serving", mlog.Int("port", port))
	return nil
}

func isGracefulStopError(err error) bool {
	return err == nil || err == grpc.ErrServerStopped
}

// internalDomainMarkInterceptor stamps every request accepted on the
// internal-domain gRPC listener with the internal-domain mark, so seams in
// the shared handlers can tell the control plane's calls from tenants'.
func internalDomainMarkInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(extension.WithInternalDomain(ctx), req)
}

// internalDomainMarkStreamInterceptor is the stream-side twin of the unary
// mark above.
func internalDomainMarkStreamInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, markedServerStream{ServerStream: ss, ctx: extension.WithInternalDomain(ss.Context())})
}

// markedServerStream carries the internal-domain mark on a stream's context.
type markedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s markedServerStream) Context() context.Context { return s.ctx }

// startInternalDomainRest serves /v2/vectordb with the handler-level
// authorization forced off, plus /metrics for the deployment's scraper. No
// authentication middleware: same posture, same isolation argument, as the
// gRPC listener above.
func (s *Server) startInternalDomainRest(bindAddress string, port int) error {
	// Same all-interfaces bind as the gRPC listener above; see there.
	listener, err := listenInternal(bindAddress, port)
	if err != nil {
		return err
	}

	ginHandler := gin.New()
	ginHandler.Use(httpserver.MetricsHandlerFunc)
	ginHandler.Use(httpserver.TraceIDHandlerFunc)
	ginHandler.Use(accesslog.AccessLogMiddleware)
	ginHandler.Use(httpserver.LoggerHandlerFunc(), gin.Recovery())
	ginHandler.Use(httpserver.RequestHandlerFunc)
	ginHandler.Use(func(c *gin.Context) {
		c.Set(httpserver.ContextUsername, "")
		// The same provenance mark the gRPC listener stamps. The v2 handlers
		// read it through c.Request.Context(); note gin's own Context.Value
		// does NOT fall through to the request context unless
		// ContextWithFallback is enabled (it is not here), so a handler that
		// passed the gin context itself as a context.Context would miss the
		// mark - the mark lives on the http.Request, deliberately.
		c.Request = c.Request.WithContext(extension.WithInternalDomain(c.Request.Context()))
	})
	ginHandler.GET("/metrics", gin.WrapH(promhttp.Handler()))
	appV2 := ginHandler.Group("/v2/vectordb")
	httpserver.NewHandlersV2WithCheckAuth(s.proxy, false).RegisterRoutesToV2(appV2)

	// The same timeout policy as the external REST listener in service.go:
	// this is the same class of server on the same handler set, and a
	// listener without ReadHeaderTimeout can be held open by a client that
	// dribbles headers. Reading them from the one HTTPCfg keeps the two
	// listeners from drifting apart as that policy is tuned.
	//
	// MaxHeaderBytes is deliberately NOT mirrored. Its 16MiB default exists
	// only because the external listener runs in shared-port mode and carries
	// external gRPC over HTTP/2, so it has to match grpc-go's max header list
	// size (see proxy.http.maxHeaderBytes' own doc). This listener is plain
	// HTTP/1 gin with no h2c, so copying it would raise the cap 16x over Go's
	// 1MiB default on an UNAUTHENTICATED socket and buy nothing.
	httpParams := &proxy.Params.HTTPCfg
	s.internalDomainHTTPServer = &http.Server{
		Handler:           ginHandler,
		ReadHeaderTimeout: httpParams.ReadHeaderTimeout.GetAsDurationByParse(),
		ReadTimeout:       httpParams.ReadTimeout.GetAsDurationByParse(),
		WriteTimeout:      httpParams.WriteTimeout.GetAsDurationByParse(),
		IdleTimeout:       httpParams.IdleTimeout.GetAsDurationByParse(),
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.internalDomainHTTPServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			mlog.Error(s.ctx, "internal-domain rest server exited", mlog.Err(err))
		}
	}()
	mlog.Info(s.ctx, "internal-domain rest listener serving", mlog.Int("port", port))
	return nil
}

// stopInternalDomainServers shuts the declared listeners down; safe when none
// were opened.
func (s *Server) stopInternalDomainServers() {
	if s.internalDomainGrpcServer != nil {
		utils.GracefulStopGRPCServer(s.internalDomainGrpcServer)
	}
	if s.internalDomainHTTPServer != nil {
		_ = s.internalDomainHTTPServer.Shutdown(context.Background())
	}
}

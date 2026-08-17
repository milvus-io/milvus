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
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"google.golang.org/grpc/credentials"
)

// This file is the proxy's seam for the internal-surfaces capability: WHERE
// the extra listeners a form declares are opened, and with which chains. The
// declaration - and the isolation argument that makes an unauthenticated
// MilvusService acceptable - lives with the capability in pkg/extension.
//
// With no capability installed neither function opens anything, and the
// binary serves exactly the listeners it always did.

// internalDomainPorts resolves the declared ports, or (0, 0) with no
// capability installed.
func internalDomainPorts() (int, int) {
	surfaces := extension.Caps().InternalSurfaces
	if surfaces == nil {
		return 0, 0
	}
	return surfaces.InternalDomainPorts()
}

// startInternalDomainServers opens the declared internal-domain listeners.
// Called from start(), after the proxy component is serving; errors abort
// start-up, because a form that declared the surfaces cannot be operated
// without them.
func (s *Server) startInternalDomainServers() error {
	grpcPort, restPort := internalDomainPorts()
	if grpcPort != 0 {
		if err := s.startInternalDomainGrpc(grpcPort); err != nil {
			return fmt.Errorf("start the internal-domain grpc listener: %w", err)
		}
	}
	if restPort != 0 {
		if err := s.startInternalDomainRest(restPort); err != nil {
			return fmt.Errorf("start the internal-domain rest listener: %w", err)
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
func (s *Server) startInternalDomainGrpc(port int) error {
	Params := &paramtable.Get().ProxyGrpcServerCfg
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	limiterOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
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
			return fmt.Errorf("load the internal-domain server certificate: %w", err)
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

// startInternalDomainRest serves /v2/vectordb with the handler-level
// authorization forced off, plus /metrics for the deployment's scraper. No
// authentication middleware: same posture, same isolation argument, as the
// gRPC listener above.
func (s *Server) startInternalDomainRest(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
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
	})
	ginHandler.GET("/metrics", gin.WrapH(promhttp.Handler()))
	appV2 := ginHandler.Group("/v2/vectordb")
	httpserver.NewHandlersV2WithCheckAuth(s.proxy, false).RegisterRoutesToV2(appV2)

	s.internalDomainHTTPServer = &http.Server{Handler: ginHandler}
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

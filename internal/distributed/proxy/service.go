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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"github.com/gin-gonic/gin"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/federpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	qcc "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
	errInvalidToken    = status.Errorf(codes.Unauthenticated, "invalid token")
	// registerHTTPHandlerOnce avoid register http handler multiple times
	registerHTTPHandlerOnce sync.Once
)

const apiPathPrefix = "/api/v1"

// Server is the Proxy Server
type Server struct {
	ctx                context.Context
	wg                 sync.WaitGroup
	proxy              types.ProxyComponent
	grpcInternalServer *grpc.Server
	grpcExternalServer *grpc.Server

	etcdCli          *clientv3.Client
	rootCoordClient  types.RootCoord
	dataCoordClient  types.DataCoord
	queryCoordClient types.QueryCoord
}

// NewServer create a Proxy server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {

	var err error
	server := &Server{
		ctx: ctx,
	}

	server.proxy, err = proxy.NewProxy(server.ctx, factory)
	if err != nil {
		return nil, err
	}
	return server, err
}

// registerHTTPServer register the http server, panic when failed
func (s *Server) registerHTTPServer() {
	// (Embedded Milvus Only) Discard gin logs if logging is disabled.
	// We might need to put these logs in some files in the further.
	// But we don't care about these logs now, at least not in embedded Milvus.
	if !proxy.Params.ProxyCfg.GinLogging.GetAsBool() {
		gin.DefaultWriter = io.Discard
		gin.DefaultErrorWriter = io.Discard
	}
	if !proxy.Params.HTTPCfg.DebugMode.GetAsBool() {
		gin.SetMode(gin.ReleaseMode)
	}
	ginHandler := gin.Default()
	apiv1 := ginHandler.Group(apiPathPrefix)
	httpserver.NewHandlers(s.proxy).RegisterRoutesTo(apiv1)
	http.Handle("/", ginHandler)
}

func (s *Server) startInternalRPCServer(grpcInternalPort int, errChan chan error) {
	s.wg.Add(1)
	go s.startInternalGrpc(grpcInternalPort, errChan)
}

func (s *Server) startExternalRPCServer(grpcExternalPort int, errChan chan error) {
	s.wg.Add(1)
	go s.startExternalGrpc(grpcExternalPort, errChan)
}

func (s *Server) startExternalGrpc(grpcPort int, errChan chan error) {
	defer s.wg.Done()
	Params := &paramtable.Get().ProxyGrpcServerCfg
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

	log.Debug("Proxy server listen on tcp", zap.Int("port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("Proxy server failed to listen on", zap.Error(err), zap.Int("port", grpcPort))
		errChan <- err
		return
	}
	log.Debug("Proxy server already listen on tcp", zap.Int("port", grpcPort))

	limiter, err := s.proxy.GetRateLimiter()
	if err != nil {
		log.Error("Get proxy rate limiter failed", zap.Int("port", grpcPort), zap.Error(err))
		errChan <- err
		return
	}
	log.Debug("Get proxy rate limiter done", zap.Int("port", grpcPort))

	opts := tracer.GetInterceptorOpts()
	grpcOpts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			otelgrpc.UnaryServerInterceptor(opts...),
			grpc_auth.UnaryServerInterceptor(proxy.AuthenticationInterceptor),
			proxy.DatabaseInterceptor(),
			proxy.UnaryServerHookInterceptor(),
			proxy.UnaryServerInterceptor(proxy.PrivilegeInterceptor),
			logutil.UnaryTraceLoggerInterceptor,
			proxy.RateLimitInterceptor(limiter),
			accesslog.UnaryAccessLoggerInterceptor,
			proxy.KeepActiveInterceptor,
		)),
	}

	if Params.TLSMode.GetAsInt() == 1 {
		creds, err := credentials.NewServerTLSFromFile(Params.ServerPemPath.GetValue(), Params.ServerKeyPath.GetValue())
		if err != nil {
			log.Warn("proxy can't create creds", zap.Error(err))
			errChan <- err
			return
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	} else if Params.TLSMode.GetAsInt() == 2 {
		cert, err := tls.LoadX509KeyPair(Params.ServerPemPath.GetValue(), Params.ServerKeyPath.GetValue())
		if err != nil {
			log.Warn("proxy cant load x509 key pair", zap.Error(err))
			errChan <- err
			return
		}

		certPool := x509.NewCertPool()
		rootBuf, err := ioutil.ReadFile(Params.CaPemPath.GetValue())
		if err != nil {
			log.Warn("failed read ca pem", zap.Error(err))
			errChan <- err
			return
		}
		if !certPool.AppendCertsFromPEM(rootBuf) {
			log.Warn("fail to append ca to cert")
			errChan <- fmt.Errorf("fail to append ca to cert")
			return
		}

		tlsConf := &tls.Config{
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{cert},
			ClientCAs:    certPool,
			MinVersion:   tls.VersionTLS13,
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConf)))
	}
	s.grpcExternalServer = grpc.NewServer(grpcOpts...)
	milvuspb.RegisterMilvusServiceServer(s.grpcExternalServer, s)
	grpc_health_v1.RegisterHealthServer(s.grpcExternalServer, s)
	errChan <- nil

	log.Debug("create Proxy grpc server",
		zap.Any("enforcement policy", kaep),
		zap.Any("server parameters", kasp))

	if err := s.grpcExternalServer.Serve(lis); err != nil {
		log.Error("failed to serve on Proxy's listener", zap.Error(err))
		errChan <- err
		return
	}
}

func (s *Server) startInternalGrpc(grpcPort int, errChan chan error) {
	defer s.wg.Done()
	Params := &paramtable.Get().ProxyGrpcServerCfg
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

	log.Debug("Proxy internal server listen on tcp", zap.Int("port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("Proxy internal server failed to listen on", zap.Error(err), zap.Int("port", grpcPort))
		errChan <- err
		return
	}
	log.Debug("Proxy internal server already listen on tcp", zap.Int("port", grpcPort))

	opts := tracer.GetInterceptorOpts()
	s.grpcInternalServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			otelgrpc.UnaryServerInterceptor(opts...),
			logutil.UnaryTraceLoggerInterceptor,
			interceptor.ClusterValidationUnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(interceptor.ClusterValidationStreamServerInterceptor()),
	)
	proxypb.RegisterProxyServer(s.grpcInternalServer, s)
	grpc_health_v1.RegisterHealthServer(s.grpcInternalServer, s)
	errChan <- nil

	log.Debug("create Proxy internal grpc server",
		zap.Any("enforcement policy", kaep),
		zap.Any("server parameters", kasp))

	if err := s.grpcInternalServer.Serve(lis); err != nil {
		log.Error("failed to internal serve on Proxy's listener", zap.Error(err))
		errChan <- err
		return
	}
}

// Start start the Proxy Server
func (s *Server) Run() error {
	log.Debug("init Proxy server")
	if err := s.init(); err != nil {
		log.Warn("init Proxy server failed", zap.Error(err))
		return err
	}
	log.Debug("init Proxy server done")

	log.Debug("start Proxy server")
	if err := s.start(); err != nil {
		log.Warn("start Proxy server failed", zap.Error(err))
		return err
	}
	log.Debug("start Proxy server done")

	return nil
}

func (s *Server) init() error {
	etcdConfig := &paramtable.Get().EtcdCfg
	Params := &paramtable.Get().ProxyGrpcServerCfg
	log.Debug("Proxy init service's parameter table done")
	HTTPParams := paramtable.Get().HTTPCfg
	log.Debug("Proxy init http server's parameter table done")

	if !funcutil.CheckPortAvailable(Params.Port.GetAsInt()) {
		paramtable.Get().Save(Params.Port.Key, fmt.Sprintf("%d", funcutil.GetAvailablePort()))
		log.Warn("Proxy get available port when init", zap.Int("Port", Params.Port.GetAsInt()))
	}

	log.Debug("init Proxy's parameter table done", zap.String("internal address", Params.GetInternalAddress()), zap.String("external address", Params.GetAddress()))

	serviceName := fmt.Sprintf("Proxy ip: %s, port: %d", Params.IP, Params.Port.GetAsInt())
	log.Debug("init Proxy's tracer done", zap.String("service name", serviceName))

	etcdCli, err := etcd.GetEtcdClient(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue())
	if err != nil {
		log.Debug("Proxy connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.proxy.SetEtcdClient(s.etcdCli)
	s.proxy.SetAddress(Params.GetInternalAddress())

	errChan := make(chan error, 1)
	{
		s.startInternalRPCServer(Params.InternalPort.GetAsInt(), errChan)
		if err := <-errChan; err != nil {
			log.Error("failed to create internal rpc server", zap.Error(err))
			return err
		}
	}
	{
		s.startExternalRPCServer(Params.Port.GetAsInt(), errChan)
		if err := <-errChan; err != nil {
			log.Error("failed to create external rpc server", zap.Error(err))
			return err
		}
	}

	if HTTPParams.Enabled.GetAsBool() {
		registerHTTPHandlerOnce.Do(func() {
			log.Info("register http server of proxy")
			s.registerHTTPServer()
		})
	}

	if s.rootCoordClient == nil {
		var err error
		log.Debug("create RootCoord client for Proxy")
		s.rootCoordClient, err = rcc.NewClient(s.ctx, proxy.Params.EtcdCfg.MetaRootPath.GetValue(), etcdCli)
		if err != nil {
			log.Warn("failed to create RootCoord client for Proxy", zap.Error(err))
			return err
		}
		log.Debug("create RootCoord client for Proxy done")
	}

	log.Debug("init RootCoord client for Proxy")
	if err := s.rootCoordClient.Init(); err != nil {
		log.Warn("failed to init RootCoord client for Proxy", zap.Error(err))
		return err
	}
	log.Debug("init RootCoord client for Proxy done")

	log.Debug("Proxy wait for RootCoord to be healthy")
	if err := componentutil.WaitForComponentHealthy(s.ctx, s.rootCoordClient, "RootCoord", 1000000, time.Millisecond*200); err != nil {
		log.Warn("Proxy failed to wait for RootCoord to be healthy", zap.Error(err))
		return err
	}
	log.Debug("Proxy wait for RootCoord to be healthy done")

	log.Debug("set RootCoord client for Proxy")
	s.proxy.SetRootCoordClient(s.rootCoordClient)
	log.Debug("set RootCoord client for Proxy done")

	if s.dataCoordClient == nil {
		var err error
		log.Debug("create DataCoord client for Proxy")
		s.dataCoordClient, err = dcc.NewClient(s.ctx, proxy.Params.EtcdCfg.MetaRootPath.GetValue(), etcdCli)
		if err != nil {
			log.Warn("failed to create DataCoord client for Proxy", zap.Error(err))
			return err
		}
		log.Debug("create DataCoord client for Proxy done")
	}

	log.Debug("init DataCoord client for Proxy")
	if err := s.dataCoordClient.Init(); err != nil {
		log.Warn("failed to init DataCoord client for Proxy", zap.Error(err))
		return err
	}
	log.Debug("init DataCoord client for Proxy done")

	log.Debug("Proxy wait for DataCoord to be healthy")
	if err := componentutil.WaitForComponentHealthy(s.ctx, s.dataCoordClient, "DataCoord", 1000000, time.Millisecond*200); err != nil {
		log.Warn("Proxy failed to wait for DataCoord to be healthy", zap.Error(err))
		return err
	}
	log.Debug("Proxy wait for DataCoord to be healthy done")

	log.Debug("set DataCoord client for Proxy")
	s.proxy.SetDataCoordClient(s.dataCoordClient)
	log.Debug("set DataCoord client for Proxy done")

	if s.queryCoordClient == nil {
		var err error
		log.Debug("create QueryCoord client for Proxy")
		s.queryCoordClient, err = qcc.NewClient(s.ctx, proxy.Params.EtcdCfg.MetaRootPath.GetValue(), etcdCli)
		if err != nil {
			log.Warn("failed to create QueryCoord client for Proxy", zap.Error(err))
			return err
		}
		log.Debug("create QueryCoord client for Proxy done")
	}

	log.Debug("init QueryCoord client for Proxy")
	if err := s.queryCoordClient.Init(); err != nil {
		log.Warn("failed to init QueryCoord client for Proxy", zap.Error(err))
		return err
	}
	log.Debug("init QueryCoord client for Proxy done")

	log.Debug("Proxy wait for QueryCoord to be healthy")
	if err := componentutil.WaitForComponentHealthy(s.ctx, s.queryCoordClient, "QueryCoord", 1000000, time.Millisecond*200); err != nil {
		log.Warn("Proxy failed to wait for QueryCoord to be healthy", zap.Error(err))
		return err
	}
	log.Debug("Proxy wait for QueryCoord to be healthy done")

	log.Debug("set QueryCoord client for Proxy")
	s.proxy.SetQueryCoordClient(s.queryCoordClient)
	log.Debug("set QueryCoord client for Proxy done")

	log.Debug(fmt.Sprintf("update Proxy's state to %s", commonpb.StateCode_Initializing.String()))
	s.proxy.UpdateStateCode(commonpb.StateCode_Initializing)

	log.Debug("init Proxy")
	if err := s.proxy.Init(); err != nil {
		log.Warn("failed to init Proxy", zap.Error(err))
		return err
	}
	log.Debug("init Proxy done")
	// nolint
	// Intentionally print to stdout, which is usually a sign that Milvus is ready to serve.
	fmt.Println("---Milvus Proxy successfully initialized and ready to serve!---")

	return nil
}

func (s *Server) start() error {
	if err := s.proxy.Start(); err != nil {
		log.Warn("failed to start Proxy server", zap.Error(err))
		return err
	}

	if err := s.proxy.Register(); err != nil {
		log.Warn("failed to register Proxy", zap.Error(err))
		return err
	}

	return nil
}

// Stop stop the Proxy Server
func (s *Server) Stop() error {
	Params := &paramtable.Get().ProxyGrpcServerCfg
	log.Debug("Proxy stop", zap.String("internal address", Params.GetInternalAddress()), zap.String("external address", Params.GetInternalAddress()))
	var err error

	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}

	gracefulWg := sync.WaitGroup{}

	gracefulWg.Add(1)
	go func() {
		defer gracefulWg.Done()
		if s.grpcInternalServer != nil {
			log.Debug("Graceful stop grpc internal server...")
			s.grpcInternalServer.GracefulStop()
		}
		if s.grpcExternalServer != nil {
			log.Debug("Graceful stop grpc external server...")
			s.grpcExternalServer.GracefulStop()
		}
	}()
	gracefulWg.Wait()

	s.wg.Wait()

	err = s.proxy.Stop()
	if err != nil {
		return err
	}

	return nil
}

// GetComponentStates get the component states
func (s *Server) GetComponentStates(ctx context.Context, request *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.proxy.GetComponentStates(ctx)
}

// GetStatisticsChannel get the statistics channel
func (s *Server) GetStatisticsChannel(ctx context.Context, request *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.proxy.GetStatisticsChannel(ctx)
}

// InvalidateCollectionMetaCache notifies Proxy to clear all the meta cache of specific collection.
func (s *Server) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return s.proxy.InvalidateCollectionMetaCache(ctx, request)
}

// CreateCollection notifies Proxy to create a collection
func (s *Server) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.CreateCollection(ctx, request)
}

// DropCollection notifies Proxy to drop a collection
func (s *Server) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.DropCollection(ctx, request)
}

// HasCollection notifies Proxy to check a collection's existence at specified timestamp
func (s *Server) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.proxy.HasCollection(ctx, request)
}

// LoadCollection notifies Proxy to load a collection's data
func (s *Server) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.LoadCollection(ctx, request)
}

// ReleaseCollection notifies Proxy to release a collection's data
func (s *Server) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.ReleaseCollection(ctx, request)
}

// DescribeCollection notifies Proxy to describe a collection
func (s *Server) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.proxy.DescribeCollection(ctx, request)
}

// GetCollectionStatistics notifies Proxy to get a collection's Statistics
func (s *Server) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	return s.proxy.GetCollectionStatistics(ctx, request)
}

func (s *Server) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return s.proxy.ShowCollections(ctx, request)
}

func (s *Server) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.AlterCollection(ctx, request)
}

// CreatePartition notifies Proxy to create a partition
func (s *Server) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.proxy.CreatePartition(ctx, request)
}

// DropPartition notifies Proxy to drop a partition
func (s *Server) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.proxy.DropPartition(ctx, request)
}

// HasPartition notifies Proxy to check a partition's existence
func (s *Server) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.proxy.HasPartition(ctx, request)
}

// LoadPartitions notifies Proxy to load the partitions data
func (s *Server) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return s.proxy.LoadPartitions(ctx, request)
}

// ReleasePartitions notifies Proxy to release the partitions data
func (s *Server) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return s.proxy.ReleasePartitions(ctx, request)
}

// GetPartitionStatistics notifies Proxy to get the partitions Statistics info.
func (s *Server) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	return s.proxy.GetPartitionStatistics(ctx, request)
}

// ShowPartitions notifies Proxy to show the partitions
func (s *Server) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.proxy.ShowPartitions(ctx, request)
}

func (s *Server) GetLoadingProgress(ctx context.Context, request *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
	return s.proxy.GetLoadingProgress(ctx, request)
}

func (s *Server) GetLoadState(ctx context.Context, request *milvuspb.GetLoadStateRequest) (*milvuspb.GetLoadStateResponse, error) {
	return s.proxy.GetLoadState(ctx, request)
}

// CreateIndex notifies Proxy to create index
func (s *Server) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.proxy.CreateIndex(ctx, request)
}

// DropIndex notifies Proxy to drop index
func (s *Server) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return s.proxy.DropIndex(ctx, request)
}

// DescribeIndex notifies Proxy to get index describe
func (s *Server) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.proxy.DescribeIndex(ctx, request)
}

// GetIndexStatistics get the information of index.
func (s *Server) GetIndexStatistics(ctx context.Context, request *milvuspb.GetIndexStatisticsRequest) (*milvuspb.GetIndexStatisticsResponse, error) {
	return s.proxy.GetIndexStatistics(ctx, request)
}

// GetIndexBuildProgress gets index build progress with filed_name and index_name.
// IndexRows is the num of indexed rows. And TotalRows is the total number of segment rows.
// Deprecated: use DescribeIndex instead
func (s *Server) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	return s.proxy.GetIndexBuildProgress(ctx, request)
}

// GetIndexState gets the index states from proxy.
// Deprecated: use DescribeIndex instead
func (s *Server) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	return s.proxy.GetIndexState(ctx, request)
}

func (s *Server) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	return s.proxy.Insert(ctx, request)
}

func (s *Server) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	return s.proxy.Delete(ctx, request)
}

func (s *Server) Upsert(ctx context.Context, request *milvuspb.UpsertRequest) (*milvuspb.MutationResult, error) {
	return s.proxy.Upsert(ctx, request)
}

func (s *Server) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return s.proxy.Search(ctx, request)
}

func (s *Server) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
	return s.proxy.Flush(ctx, request)
}

func (s *Server) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	return s.proxy.Query(ctx, request)
}

func (s *Server) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	return s.proxy.CalcDistance(ctx, request)
}

func (s *Server) FlushAll(ctx context.Context, request *milvuspb.FlushAllRequest) (*milvuspb.FlushAllResponse, error) {
	return s.proxy.FlushAll(ctx, request)
}

func (s *Server) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	return s.proxy.GetDdChannel(ctx, request)
}

// GetPersistentSegmentInfo notifies Proxy to get persistent segment info.
func (s *Server) GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	return s.proxy.GetPersistentSegmentInfo(ctx, request)
}

// GetQuerySegmentInfo notifies Proxy to get query segment info.
func (s *Server) GetQuerySegmentInfo(ctx context.Context, request *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	return s.proxy.GetQuerySegmentInfo(ctx, request)

}

func (s *Server) Dummy(ctx context.Context, request *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	return s.proxy.Dummy(ctx, request)
}

func (s *Server) RegisterLink(ctx context.Context, request *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	return s.proxy.RegisterLink(ctx, request)
}

// GetMetrics gets the metrics info of proxy.
func (s *Server) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.proxy.GetMetrics(ctx, request)
}

func (s *Server) LoadBalance(ctx context.Context, request *milvuspb.LoadBalanceRequest) (*commonpb.Status, error) {
	return s.proxy.LoadBalance(ctx, request)
}

// CreateAlias notifies Proxy to create alias
func (s *Server) CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return s.proxy.CreateAlias(ctx, request)
}

// DropAlias notifies Proxy to drop an alias
func (s *Server) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return s.proxy.DropAlias(ctx, request)
}

// AlterAlias alters the alias for the specified collection.
func (s *Server) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return s.proxy.AlterAlias(ctx, request)
}

func (s *Server) DescribeAlias(ctx context.Context, request *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	return &milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "TODO: implement me",
		},
	}, nil
}

func (s *Server) ListAliases(ctx context.Context, request *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	return &milvuspb.ListAliasesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "TODO: implement me",
		},
	}, nil
}

// GetCompactionState gets the state of a compaction
func (s *Server) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return s.proxy.GetCompactionState(ctx, req)
}

func (s *Server) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return s.proxy.ManualCompaction(ctx, req)
}

// GetCompactionStateWithPlans gets the state of a compaction by plan
func (s *Server) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return s.proxy.GetCompactionStateWithPlans(ctx, req)
}

// GetFlushState gets the flush state of multiple segments
func (s *Server) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return s.proxy.GetFlushState(ctx, req)
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (s *Server) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	return s.proxy.GetFlushAllState(ctx, req)
}

func (s *Server) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	return s.proxy.Import(ctx, req)
}

func (s *Server) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return s.proxy.GetImportState(ctx, req)
}

func (s *Server) ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	return s.proxy.ListImportTasks(ctx, req)
}

func (s *Server) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	return s.proxy.GetReplicas(ctx, req)
}

// Check is required by gRPC healthy checking
func (s *Server) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	ret := &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	}
	state, err := s.proxy.GetComponentStates(ctx)
	if err != nil {
		return ret, err
	}
	if state.Status.ErrorCode != commonpb.ErrorCode_Success {
		return ret, nil
	}
	if state.State.StateCode != commonpb.StateCode_Healthy {
		return ret, nil
	}
	ret.Status = grpc_health_v1.HealthCheckResponse_SERVING
	return ret, nil
}

// Watch is required by gRPC healthy checking
func (s *Server) Watch(req *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	ret := &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	}
	state, err := s.proxy.GetComponentStates(s.ctx)
	if err != nil {
		return server.Send(ret)
	}
	if state.Status.ErrorCode != commonpb.ErrorCode_Success {
		return server.Send(ret)
	}
	if state.State.StateCode != commonpb.StateCode_Healthy {
		return server.Send(ret)
	}
	ret.Status = grpc_health_v1.HealthCheckResponse_SERVING
	return server.Send(ret)
}

func (s *Server) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
	return s.proxy.InvalidateCredentialCache(ctx, request)
}

func (s *Server) UpdateCredentialCache(ctx context.Context, request *proxypb.UpdateCredCacheRequest) (*commonpb.Status, error) {
	return s.proxy.UpdateCredentialCache(ctx, request)
}

func (s *Server) CreateCredential(ctx context.Context, req *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	return s.proxy.CreateCredential(ctx, req)
}

func (s *Server) UpdateCredential(ctx context.Context, req *milvuspb.UpdateCredentialRequest) (*commonpb.Status, error) {
	return s.proxy.UpdateCredential(ctx, req)
}

func (s *Server) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return s.proxy.DeleteCredential(ctx, req)
}

func (s *Server) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return s.proxy.ListCredUsers(ctx, req)
}

func (s *Server) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	return s.proxy.CreateRole(ctx, req)
}

func (s *Server) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	return s.proxy.DropRole(ctx, req)
}

func (s *Server) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	return s.proxy.OperateUserRole(ctx, req)
}

func (s *Server) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	return s.proxy.SelectRole(ctx, req)
}

func (s *Server) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	return s.proxy.SelectUser(ctx, req)
}

func (s *Server) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	return s.proxy.OperatePrivilege(ctx, req)
}

func (s *Server) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	return s.proxy.SelectGrant(ctx, req)
}

func (s *Server) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
	return s.proxy.RefreshPolicyInfoCache(ctx, req)
}

// SetRates notifies Proxy to limit rates of requests.
func (s *Server) SetRates(ctx context.Context, request *proxypb.SetRatesRequest) (*commonpb.Status, error) {
	return s.proxy.SetRates(ctx, request)
}

// GetProxyMetrics gets the metrics of proxy.
func (s *Server) GetProxyMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.proxy.GetProxyMetrics(ctx, request)
}

func (s *Server) GetVersion(ctx context.Context, request *milvuspb.GetVersionRequest) (*milvuspb.GetVersionResponse, error) {
	buildTags := os.Getenv(metricsinfo.GitBuildTagsEnvKey)
	return &milvuspb.GetVersionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Version: buildTags,
	}, nil
}

func (s *Server) CheckHealth(ctx context.Context, request *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return s.proxy.CheckHealth(ctx, request)
}

func (s *Server) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.RenameCollection(ctx, req)
}

func (s *Server) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	return s.proxy.CreateResourceGroup(ctx, req)
}

func (s *Server) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	return s.proxy.DropResourceGroup(ctx, req)
}

func (s *Server) DescribeResourceGroup(ctx context.Context, req *milvuspb.DescribeResourceGroupRequest) (*milvuspb.DescribeResourceGroupResponse, error) {
	return s.proxy.DescribeResourceGroup(ctx, req)
}

func (s *Server) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	return s.proxy.TransferNode(ctx, req)
}

func (s *Server) TransferReplica(ctx context.Context, req *milvuspb.TransferReplicaRequest) (*commonpb.Status, error) {
	return s.proxy.TransferReplica(ctx, req)
}

func (s *Server) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	return s.proxy.ListResourceGroups(ctx, req)
}

func (s *Server) ListIndexedSegment(ctx context.Context, req *federpb.ListIndexedSegmentRequest) (*federpb.ListIndexedSegmentResponse, error) {
	panic("TODO: implement me")
}

func (s *Server) DescribeSegmentIndexData(ctx context.Context, req *federpb.DescribeSegmentIndexDataRequest) (*federpb.DescribeSegmentIndexDataResponse, error) {
	panic("TODO: implement me")
}

func (s *Server) Connect(ctx context.Context, req *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	return s.proxy.Connect(ctx, req)
}

func (s *Server) ListClientInfos(ctx context.Context, req *proxypb.ListClientInfosRequest) (*proxypb.ListClientInfosResponse, error) {
	return s.proxy.ListClientInfos(ctx, req)
}

func (s *Server) CreateDatabase(ctx context.Context, request *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return s.proxy.CreateDatabase(ctx, request)
}

func (s *Server) DropDatabase(ctx context.Context, request *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return s.proxy.DropDatabase(ctx, request)
}

func (s *Server) ListDatabases(ctx context.Context, request *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return s.proxy.ListDatabases(ctx, request)
}

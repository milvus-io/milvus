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
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gin-gonic/gin"
	ot "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/opentracing/opentracing-go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	icc "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	qcc "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var Params paramtable.GrpcServerConfig
var HTTPParams paramtable.HTTPConfig

var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
	errInvalidToken    = status.Errorf(codes.Unauthenticated, "invalid token")
)

// Server is the Proxy Server
type Server struct {
	ctx        context.Context
	wg         sync.WaitGroup
	proxy      types.ProxyComponent
	grpcServer *grpc.Server
	httpServer *http.Server
	// avoid race
	httpServerMtx sync.Mutex

	grpcErrChan chan error

	etcdCli          *clientv3.Client
	rootCoordClient  types.RootCoord
	dataCoordClient  types.DataCoord
	queryCoordClient types.QueryCoord
	indexCoordClient types.IndexCoord

	tracer opentracing.Tracer
	closer io.Closer
}

// NewServer create a Proxy server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {

	var err error
	server := &Server{
		ctx:         ctx,
		grpcErrChan: make(chan error),
	}

	server.proxy, err = proxy.NewProxy(server.ctx, factory)
	if err != nil {
		return nil, err
	}
	return server, err
}

// startHTTPServer starts the http server, panic when failed
func (s *Server) startHTTPServer(port int) {
	defer s.wg.Done()
	// (Embedded Milvus Only) Discard gin logs if logging is disabled.
	// We might need to put these logs in some files in the further.
	// But we don't care about these logs now, at least not in embedded Milvus.
	if !proxy.Params.ProxyCfg.GinLogging {
		gin.DefaultWriter = io.Discard
		gin.DefaultErrorWriter = io.Discard
	}
	ginHandler := gin.Default()
	apiv1 := ginHandler.Group("/api/v1")
	httpserver.NewHandlers(s.proxy).RegisterRoutesTo(apiv1)
	s.httpServerMtx.Lock()
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      ginHandler,
		ReadTimeout:  HTTPParams.ReadTimeout,
		WriteTimeout: HTTPParams.WriteTimeout,
	}
	s.httpServerMtx.Unlock()
	if err := s.httpServer.ListenAndServe(); err != nil {
		if err != http.ErrServerClosed {
			panic("failed to start http server: " + err.Error())
		}
	}
}

func (s *Server) startGrpcLoop(grpcPort int) {
	defer s.wg.Done()

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
		s.grpcErrChan <- err
		return
	}
	log.Debug("Proxy server already listen on tcp", zap.Int("port", grpcPort))

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			ot.UnaryServerInterceptor(opts...),
			grpc_auth.UnaryServerInterceptor(proxy.AuthenticationInterceptor),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			ot.StreamServerInterceptor(opts...),
			grpc_auth.StreamServerInterceptor(proxy.AuthenticationInterceptor),
		)),
	)
	proxypb.RegisterProxyServer(s.grpcServer, s)
	milvuspb.RegisterMilvusServiceServer(s.grpcServer, s)
	grpc_health_v1.RegisterHealthServer(s.grpcServer, s)
	log.Debug("create Proxy grpc server",
		zap.Any("enforcement policy", kaep),
		zap.Any("server parameters", kasp))

	log.Debug("waiting for Proxy grpc server to be ready")
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)

	log.Debug("Proxy grpc server has been ready, serve grpc requests on listen")
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Warn("failed to serve on Proxy's listener", zap.Error(err))
		s.grpcErrChan <- err
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
	Params.InitOnce(typeutil.ProxyRole)
	log.Debug("Proxy init service's parameter table done")
	HTTPParams.InitOnce()
	log.Debug("Proxy init http server's parameter table done")

	if !funcutil.CheckPortAvailable(Params.Port) {
		Params.Port = funcutil.GetAvailablePort()
		log.Warn("Proxy get available port when init", zap.Int("Port", Params.Port))
	}

	proxy.Params.InitOnce()
	proxy.Params.ProxyCfg.NetworkAddress = Params.GetAddress()
	log.Debug("init Proxy's parameter table done", zap.String("address", Params.GetAddress()))

	serviceName := fmt.Sprintf("Proxy ip: %s, port: %d", Params.IP, Params.Port)
	closer := trace.InitTracing(serviceName)
	s.closer = closer
	log.Debug("init Proxy's tracer done", zap.String("service name", serviceName))

	etcdCli, err := etcd.GetEtcdClient(&proxy.Params.EtcdCfg)
	if err != nil {
		log.Debug("Proxy connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.proxy.SetEtcdClient(s.etcdCli)
	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	log.Debug("waiting for grpc server of Proxy to be started")
	if err := <-s.grpcErrChan; err != nil {
		log.Warn("failed to start Proxy's grpc server", zap.Error(err))
		return err
	}
	log.Debug("grpc server of proxy has been started")
	if HTTPParams.Enabled {
		log.Info("start http server of proxy", zap.Int("port", HTTPParams.Port))
		s.wg.Add(1)
		go s.startHTTPServer(HTTPParams.Port)
	}

	if s.rootCoordClient == nil {
		var err error
		log.Debug("create RootCoord client for Proxy")
		s.rootCoordClient, err = rcc.NewClient(s.ctx, proxy.Params.EtcdCfg.MetaRootPath, etcdCli)
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
	if err := funcutil.WaitForComponentHealthy(s.ctx, s.rootCoordClient, "RootCoord", 1000000, time.Millisecond*200); err != nil {
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
		s.dataCoordClient, err = dcc.NewClient(s.ctx, proxy.Params.EtcdCfg.MetaRootPath, etcdCli)
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
	if err := funcutil.WaitForComponentHealthy(s.ctx, s.dataCoordClient, "DataCoord", 1000000, time.Millisecond*200); err != nil {
		log.Warn("Proxy failed to wait for DataCoord to be healthy", zap.Error(err))
		return err
	}
	log.Debug("Proxy wait for DataCoord to be healthy done")

	log.Debug("set DataCoord client for Proxy")
	s.proxy.SetDataCoordClient(s.dataCoordClient)
	log.Debug("set DataCoord client for Proxy done")

	if s.indexCoordClient == nil {
		var err error
		log.Debug("create IndexCoord client for Proxy")
		s.indexCoordClient, err = icc.NewClient(s.ctx, proxy.Params.EtcdCfg.MetaRootPath, etcdCli)
		if err != nil {
			log.Warn("failed to create IndexCoord client for Proxy", zap.Error(err))
			return err
		}
		log.Debug("create IndexCoord client for Proxy done")
	}

	log.Debug("init IndexCoord client for Proxy")
	if err := s.indexCoordClient.Init(); err != nil {
		log.Warn("failed to init IndexCoord client for Proxy", zap.Error(err))
		return err
	}
	log.Debug("init IndexCoord client for Proxy done")

	log.Debug("Proxy wait for IndexCoord to be healthy")
	if err := funcutil.WaitForComponentHealthy(s.ctx, s.indexCoordClient, "IndexCoord", 1000000, time.Millisecond*200); err != nil {
		log.Warn("Proxy failed to wait for IndexCoord to be healthy", zap.Error(err))
		return err
	}
	log.Debug("Proxy wait for IndexCoord to be healthy done")

	log.Debug("set IndexCoord client for Proxy")
	s.proxy.SetIndexCoordClient(s.indexCoordClient)
	log.Debug("set IndexCoord client for Proxy done")

	if s.queryCoordClient == nil {
		var err error
		log.Debug("create QueryCoord client for Proxy")
		s.queryCoordClient, err = qcc.NewClient(s.ctx, proxy.Params.EtcdCfg.MetaRootPath, etcdCli)
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
	if err := funcutil.WaitForComponentHealthy(s.ctx, s.queryCoordClient, "QueryCoord", 1000000, time.Millisecond*200); err != nil {
		log.Warn("Proxy failed to wait for QueryCoord to be healthy", zap.Error(err))
		return err
	}
	log.Debug("Proxy wait for QueryCoord to be healthy done")

	log.Debug("set QueryCoord client for Proxy")
	s.proxy.SetQueryCoordClient(s.queryCoordClient)
	log.Debug("set QueryCoord client for Proxy done")

	log.Debug(fmt.Sprintf("update Proxy's state to %s", internalpb.StateCode_Initializing.String()))
	s.proxy.UpdateStateCode(internalpb.StateCode_Initializing)

	log.Debug("init Proxy")
	if err := s.proxy.Init(); err != nil {
		log.Warn("failed to init Proxy", zap.Error(err))
		return err
	}
	log.Debug("init Proxy done")

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
	log.Debug("Proxy stop", zap.String("Address", Params.GetAddress()))
	var err error
	if s.closer != nil {
		if err = s.closer.Close(); err != nil {
			return err
		}
	}

	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}

	gracefulWg := sync.WaitGroup{}
	gracefulWg.Add(1)
	go func() {
		defer gracefulWg.Done()
		s.httpServerMtx.Lock()
		defer s.httpServerMtx.Unlock()
		if s.httpServer != nil {
			log.Debug("Graceful stop http server...")
			s.httpServer.Shutdown(context.TODO())
		}
	}()
	gracefulWg.Add(1)
	go func() {
		defer gracefulWg.Done()
		if s.grpcServer != nil {
			log.Debug("Graceful stop grpc server...")
			s.grpcServer.GracefulStop()
		}
	}()
	gracefulWg.Wait()

	err = s.proxy.Stop()
	if err != nil {
		return err
	}

	s.wg.Wait()

	return nil
}

// GetComponentStates get the component states
func (s *Server) GetComponentStates(ctx context.Context, request *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
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

// ReleaseDQLMessageStream notifies Proxy to release and close the search message stream of specific collection.
func (s *Server) ReleaseDQLMessageStream(ctx context.Context, request *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	return s.proxy.ReleaseDQLMessageStream(ctx, request)
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

// GetIndexBuildProgress gets index build progress with filed_name and index_name.
// IndexRows is the num of indexed rows. And TotalRows is the total number of segment rows.
func (s *Server) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	return s.proxy.GetIndexBuildProgress(ctx, request)
}

// GetIndexStates gets the index states from proxy.
func (s *Server) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	return s.proxy.GetIndexState(ctx, request)
}

func (s *Server) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	return s.proxy.Insert(ctx, request)
}

func (s *Server) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	return s.proxy.Delete(ctx, request)
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

func (s *Server) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	return s.proxy.GetDdChannel(ctx, request)
}

//GetPersistentSegmentInfo notifies Proxy to get persistent segment info.
func (s *Server) GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	return s.proxy.GetPersistentSegmentInfo(ctx, request)
}

//GetQuerySegmentInfo notifies Proxy to get query segment info.
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

func (s *Server) SendSearchResult(ctx context.Context, results *internalpb.SearchResults) (*commonpb.Status, error) {
	return s.proxy.SendSearchResult(ctx, results)
}

func (s *Server) SendRetrieveResult(ctx context.Context, results *internalpb.RetrieveResults) (*commonpb.Status, error) {
	return s.proxy.SendRetrieveResult(ctx, results)
}

func (s *Server) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	return s.proxy.Import(ctx, req)
}

func (s *Server) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return s.proxy.GetImportState(ctx, req)
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
	if state.State.StateCode != internalpb.StateCode_Healthy {
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
	if state.State.StateCode != internalpb.StateCode_Healthy {
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

func (s *Server) ClearCredUsersCache(ctx context.Context, request *internalpb.ClearCredUsersCacheRequest) (*commonpb.Status, error) {
	return s.proxy.ClearCredUsersCache(ctx, request)
}

func (s *Server) CreateCredential(ctx context.Context, req *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	return s.proxy.CreateCredential(ctx, req)
}

func (s *Server) UpdateCredential(ctx context.Context, req *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	return s.proxy.UpdateCredential(ctx, req)
}

func (s *Server) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return s.proxy.DeleteCredential(ctx, req)
}

func (s *Server) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return s.proxy.ListCredUsers(ctx, req)
}

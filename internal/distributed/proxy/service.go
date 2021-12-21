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
	"strconv"
	"sync"
	"time"

	ot "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	icc "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"
	qcc "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// GRPCMaxMagSize is the max size of grpc message.
	GRPCMaxMagSize = 2 << 30
)

var Params paramtable.GrpcServerConfig

// Server is the Proxy Server
type Server struct {
	ctx        context.Context
	wg         sync.WaitGroup
	proxy      types.ProxyComponent
	grpcServer *grpc.Server

	grpcErrChan chan error

	rootCoordClient  types.RootCoord
	dataCoordClient  types.DataCoord
	queryCooedClient types.QueryCoord
	indexCoordClient types.IndexCoord

	tracer opentracing.Tracer
	closer io.Closer
}

// NewServer create a Proxy server.
func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {

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

	log.Debug("proxy", zap.Int("network port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("proxy", zap.String("Server:failed to listen:", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.MaxRecvMsgSize(GRPCMaxMagSize),
		grpc.UnaryInterceptor(ot.UnaryServerInterceptor(opts...)),
		grpc.StreamInterceptor(ot.StreamServerInterceptor(opts...)))
	proxypb.RegisterProxyServer(s.grpcServer, s)
	milvuspb.RegisterMilvusServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

// Start start the Proxy Server
func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("Proxy node init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("Proxy node start done ...")
	return nil
}

func (s *Server) init() error {
	var err error
	Params.InitOnce(typeutil.ProxyRole)

	proxy.Params.InitOnce()
	log.Debug("init params done ...")

	// NetworkPort & IP don't matter here, NetworkAddress matters
	proxy.Params.NetworkPort = Params.Port
	proxy.Params.IP = Params.IP

	proxy.Params.NetworkAddress = Params.GetAddress()

	closer := trace.InitTracing(fmt.Sprintf("proxy ip: %s, port: %d", Params.IP, Params.Port))
	s.closer = closer

	log.Debug("proxy", zap.String("proxy host", Params.IP))
	log.Debug("proxy", zap.Int("proxy port", Params.Port))
	log.Debug("proxy", zap.String("proxy address", Params.GetAddress()))

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	log.Debug("create grpc server ...")
	if err != nil {
		return err
	}

	if s.rootCoordClient == nil {
		s.rootCoordClient, err = rcc.NewClient(s.ctx, proxy.Params.MetaRootPath, proxy.Params.EtcdEndpoints)
		if err != nil {
			log.Debug("Proxy new rootCoordClient failed ", zap.Error(err))
			return err
		}
	}
	err = s.rootCoordClient.Init()
	if err != nil {
		log.Debug("Proxy new rootCoordClient Init ", zap.Error(err))
		return err
	}
	err = funcutil.WaitForComponentHealthy(s.ctx, s.rootCoordClient, "RootCoord", 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("Proxy WaitForComponentHealthy RootCoord failed ", zap.Error(err))
		panic(err)
	}
	s.proxy.SetRootCoordClient(s.rootCoordClient)
	log.Debug("set rootcoord client ...")

	if s.dataCoordClient == nil {
		s.dataCoordClient, err = dcc.NewClient(s.ctx, proxy.Params.MetaRootPath, proxy.Params.EtcdEndpoints)
		if err != nil {
			log.Debug("Proxy new dataCoordClient failed ", zap.Error(err))
			return err
		}
	}
	err = s.dataCoordClient.Init()
	if err != nil {
		log.Debug("Proxy dataCoordClient init failed ", zap.Error(err))
		return err
	}

	s.proxy.SetDataCoordClient(s.dataCoordClient)
	log.Debug("set data coordinator address ...")

	if s.indexCoordClient == nil {
		s.indexCoordClient, err = icc.NewClient(s.ctx, proxy.Params.MetaRootPath, proxy.Params.EtcdEndpoints)
		if err != nil {
			log.Debug("Proxy new indexCoordClient failed ", zap.Error(err))
			return err
		}
	}
	err = s.indexCoordClient.Init()
	if err != nil {
		log.Debug("Proxy indexCoordClient init failed ", zap.Error(err))
		return err
	}

	s.proxy.SetIndexCoordClient(s.indexCoordClient)
	log.Debug("set index coordinator client ...")

	if s.queryCooedClient == nil {
		s.queryCooedClient, err = qcc.NewClient(s.ctx, proxy.Params.MetaRootPath, proxy.Params.EtcdEndpoints)
		if err != nil {
			return err
		}
	}
	err = s.queryCooedClient.Init()
	if err != nil {
		return err
	}
	s.proxy.SetQueryCoordClient(s.queryCooedClient)
	log.Debug("set query coordinator client ...")

	s.proxy.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("proxy", zap.Any("state of proxy", internalpb.StateCode_Initializing))

	if err := s.proxy.Init(); err != nil {
		log.Debug("proxy", zap.String("proxy init error", err.Error()))
		return err
	}

	return nil
}

func (s *Server) start() error {
	err := s.proxy.Start()
	if err != nil {
		log.Error("Proxy start failed", zap.Error(err))
	}
	err = s.proxy.Register()
	if err != nil {
		log.Error("Proxy register service failed ", zap.Error(err))
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

	if s.grpcServer != nil {
		log.Debug("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}

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

func (s *Server) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return s.proxy.AlterAlias(ctx, request)
}

func (s *Server) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return s.proxy.GetCompactionState(ctx, req)
}

func (s *Server) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return s.proxy.ManualCompaction(ctx, req)
}

func (s *Server) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return s.proxy.GetCompactionStateWithPlans(ctx, req)
}

func (s *Server) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return s.proxy.GetFlushState(ctx, req)
}

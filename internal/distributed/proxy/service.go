// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package grpcproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	grpcdatacoordclient "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	grpcindexcoordclient "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"
	grpcquerycoordclient "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
)

const (
	GRPCMaxMagSize = 2 << 30
)

type Server struct {
	ctx        context.Context
	wg         sync.WaitGroup
	proxy      *proxy.Proxy
	grpcServer *grpc.Server

	grpcErrChan chan error

	rootCoordClient  *rcc.GrpcClient
	dataCoordClient  *grpcdatacoordclient.Client
	queryCooedClient *grpcquerycoordclient.Client
	indexCoordClient *grpcindexcoordclient.Client

	tracer opentracing.Tracer
	closer io.Closer
}

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
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.MaxRecvMsgSize(GRPCMaxMagSize),
		grpc.UnaryInterceptor(
			grpc_opentracing.UnaryServerInterceptor(opts...)),
		grpc.StreamInterceptor(
			grpc_opentracing.StreamServerInterceptor(opts...)))
	proxypb.RegisterProxyServer(s.grpcServer, s)
	milvuspb.RegisterMilvusServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("proxy node init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("proxy node start done ...")
	return nil
}

func (s *Server) init() error {
	var err error
	Params.Init()
	if !funcutil.CheckPortAvailable(Params.Port) {
		Params.Port = funcutil.GetAvailablePort()
		log.Warn("Proxy init", zap.Any("Port", Params.Port))
	}
	Params.LoadFromEnv()
	Params.LoadFromArgs()
	Params.Address = Params.IP + ":" + strconv.FormatInt(int64(Params.Port), 10)

	proxy.Params.Init()
	log.Debug("init params done ...")

	// NetworkPort & IP don't matter here, NetworkAddress matters
	proxy.Params.NetworkPort = Params.Port
	proxy.Params.IP = Params.IP

	proxy.Params.NetworkAddress = Params.Address
	// for purpose of ID Allocator
	proxy.Params.RootCoordAddress = Params.RootCoordAddress

	closer := trace.InitTracing(fmt.Sprintf("proxy ip: %s, port: %d", Params.IP, Params.Port))
	s.closer = closer

	log.Debug("proxy", zap.String("proxy host", Params.IP))
	log.Debug("proxy", zap.Int("proxy port", Params.Port))
	log.Debug("proxy", zap.String("proxy address", Params.Address))

	err = s.proxy.Register()
	if err != nil {
		log.Debug("Proxy Register etcd failed ", zap.Error(err))
		return err
	}

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	log.Debug("create grpc server ...")
	if err != nil {
		return err
	}

	rootCoordAddr := Params.RootCoordAddress
	log.Debug("Proxy", zap.String("RootCoord address", rootCoordAddr))
	s.rootCoordClient, err = rcc.NewClient(s.ctx, proxy.Params.MetaRootPath, proxy.Params.EtcdEndpoints)
	if err != nil {
		log.Debug("Proxy new rootCoordClient failed ", zap.Error(err))
		return err
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

	dataCoordAddr := Params.DataCoordAddress
	log.Debug("Proxy", zap.String("data coordinator address", dataCoordAddr))
	s.dataCoordClient, err = grpcdatacoordclient.NewClient(s.ctx, proxy.Params.MetaRootPath, proxy.Params.EtcdEndpoints)
	if err != nil {
		log.Debug("Proxy new dataCoordClient failed ", zap.Error(err))
		return err
	}
	err = s.dataCoordClient.Init()
	if err != nil {
		log.Debug("Proxy dataCoordClient init failed ", zap.Error(err))
		return err
	}

	s.proxy.SetDataCoordClient(s.dataCoordClient)
	log.Debug("set data coordinator address ...")

	indexCoordAddr := Params.IndexCoordAddress
	log.Debug("Proxy", zap.String("index coordinator address", indexCoordAddr))
	s.indexCoordClient, err = grpcindexcoordclient.NewClient(s.ctx, proxy.Params.MetaRootPath, proxy.Params.EtcdEndpoints)
	if err != nil {
		log.Debug("Proxy new indexCoordClient failed ", zap.Error(err))
		return err
	}
	err = s.indexCoordClient.Init()
	if err != nil {
		log.Debug("Proxy indexCoordClient init failed ", zap.Error(err))
		return err
	}

	s.proxy.SetIndexCoordClient(s.indexCoordClient)
	log.Debug("set index coordinator client ...")

	queryCoordAddr := Params.QueryCoordAddress
	log.Debug("Proxy", zap.String("query coordinator address", queryCoordAddr))
	s.queryCooedClient, err = grpcquerycoordclient.NewClient(s.ctx, proxy.Params.MetaRootPath, proxy.Params.EtcdEndpoints)
	if err != nil {
		return err
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
	return s.proxy.Start()
}

func (s *Server) Stop() error {
	var err error
	if s.closer != nil {
		if err = s.closer.Close(); err != nil {
			return err
		}
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	err = s.proxy.Stop()
	if err != nil {
		return err
	}

	s.wg.Wait()

	return nil
}

func (s *Server) GetComponentStates(ctx context.Context, request *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.proxy.GetComponentStates(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, request *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.proxy.GetStatisticsChannel(ctx)
}

func (s *Server) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return s.proxy.InvalidateCollectionMetaCache(ctx, request)
}

func (s *Server) ReleaseDQLMessageStream(ctx context.Context, request *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	return s.proxy.ReleaseDQLMessageStream(ctx, request)
}

func (s *Server) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.CreateCollection(ctx, request)
}

func (s *Server) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.DropCollection(ctx, request)
}

func (s *Server) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.proxy.HasCollection(ctx, request)
}

func (s *Server) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.LoadCollection(ctx, request)
}

func (s *Server) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.proxy.ReleaseCollection(ctx, request)
}

func (s *Server) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.proxy.DescribeCollection(ctx, request)
}

func (s *Server) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	return s.proxy.GetCollectionStatistics(ctx, request)
}

func (s *Server) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return s.proxy.ShowCollections(ctx, request)
}

func (s *Server) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.proxy.CreatePartition(ctx, request)
}

func (s *Server) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.proxy.DropPartition(ctx, request)
}

func (s *Server) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.proxy.HasPartition(ctx, request)
}

func (s *Server) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return s.proxy.LoadPartitions(ctx, request)
}

func (s *Server) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return s.proxy.ReleasePartitions(ctx, request)
}

func (s *Server) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	return s.proxy.GetPartitionStatistics(ctx, request)
}

func (s *Server) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.proxy.ShowPartitions(ctx, request)
}

func (s *Server) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.proxy.CreateIndex(ctx, request)
}

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

func (s *Server) GetQuerySegmentInfo(ctx context.Context, request *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	return s.proxy.GetQuerySegmentInfo(ctx, request)

}

func (s *Server) Dummy(ctx context.Context, request *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	return s.proxy.Dummy(ctx, request)
}

func (s *Server) RegisterLink(ctx context.Context, request *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	return s.proxy.RegisterLink(ctx, request)
}

func (s *Server) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.proxy.GetMetrics(ctx, request)
}

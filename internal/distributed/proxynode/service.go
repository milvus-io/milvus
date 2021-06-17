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

package grpcproxynode

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	grpcdataserviceclient "github.com/milvus-io/milvus/internal/distributed/dataservice/client"
	grpcindexserviceclient "github.com/milvus-io/milvus/internal/distributed/indexservice/client"
	grpcmasterserviceclient "github.com/milvus-io/milvus/internal/distributed/masterservice/client"
	grpcqueryserviceclient "github.com/milvus-io/milvus/internal/distributed/queryservice/client"
	otgrpc "github.com/opentracing-contrib/go-grpc"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proxynode"
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
	proxynode  *proxynode.ProxyNode
	grpcServer *grpc.Server

	grpcErrChan chan error

	masterServiceClient *grpcmasterserviceclient.GrpcClient
	dataServiceClient   *grpcdataserviceclient.Client
	queryServiceClient  *grpcqueryserviceclient.Client
	indexServiceClient  *grpcindexserviceclient.Client

	tracer opentracing.Tracer
	closer io.Closer
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {

	var err error
	server := &Server{
		ctx:         ctx,
		grpcErrChan: make(chan error),
	}

	server.proxynode, err = proxynode.NewProxyNode(server.ctx, factory)
	if err != nil {
		return nil, err
	}
	return server, err
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()

	log.Debug("proxynode", zap.Int("network port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("proxynode", zap.String("Server:failed to listen:", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)),
		grpc.MaxRecvMsgSize(GRPCMaxMagSize))
	proxypb.RegisterProxyNodeServiceServer(s.grpcServer, s)
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
		log.Warn("ProxyNode init", zap.Any("Port", Params.Port))
	}
	Params.LoadFromEnv()
	Params.LoadFromArgs()
	Params.Address = Params.IP + ":" + strconv.FormatInt(int64(Params.Port), 10)

	proxynode.Params.Init()
	log.Debug("init params done ...")
	proxynode.Params.NetworkPort = Params.Port
	proxynode.Params.IP = Params.IP
	proxynode.Params.NetworkAddress = Params.Address
	// for purpose of ID Allocator
	proxynode.Params.MasterAddress = Params.MasterAddress

	closer := trace.InitTracing(fmt.Sprintf("proxy_node ip: %s, port: %d", Params.IP, Params.Port))
	s.closer = closer

	log.Debug("proxynode", zap.String("proxy host", Params.IP))
	log.Debug("proxynode", zap.Int("proxy port", Params.Port))
	log.Debug("proxynode", zap.String("proxy address", Params.Address))

	err = s.proxynode.Register()
	if err != nil {
		log.Debug("ProxyNode Register etcd failed ", zap.Error(err))
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

	masterServiceAddr := Params.MasterAddress
	log.Debug("ProxyNode", zap.String("master address", masterServiceAddr))
	timeout := 3 * time.Second
	s.masterServiceClient, err = grpcmasterserviceclient.NewClient(s.ctx, proxynode.Params.MetaRootPath, proxynode.Params.EtcdEndpoints, timeout)
	if err != nil {
		log.Debug("ProxyNode new masterServiceClient failed ", zap.Error(err))
		return err
	}
	err = s.masterServiceClient.Init()
	if err != nil {
		log.Debug("ProxyNode new masterServiceClient Init ", zap.Error(err))
		return err
	}
	err = funcutil.WaitForComponentHealthy(s.ctx, s.masterServiceClient, "MasterService", 1000000, time.Millisecond*200)

	if err != nil {
		log.Debug("ProxyNode WaitForComponentHealthy master service failed ", zap.Error(err))
		panic(err)
	}
	s.proxynode.SetMasterClient(s.masterServiceClient)
	log.Debug("set master client ...")

	dataServiceAddr := Params.DataServiceAddress
	log.Debug("ProxyNode", zap.String("data service address", dataServiceAddr))
	s.dataServiceClient = grpcdataserviceclient.NewClient(proxynode.Params.MetaRootPath, proxynode.Params.EtcdEndpoints, timeout)
	err = s.dataServiceClient.Init()
	if err != nil {
		log.Debug("ProxyNode dataServiceClient init failed ", zap.Error(err))
		return err
	}
	s.proxynode.SetDataServiceClient(s.dataServiceClient)
	log.Debug("set data service address ...")

	indexServiceAddr := Params.IndexServerAddress
	log.Debug("ProxyNode", zap.String("index server address", indexServiceAddr))
	s.indexServiceClient = grpcindexserviceclient.NewClient(proxynode.Params.MetaRootPath, proxynode.Params.EtcdEndpoints, timeout)
	err = s.indexServiceClient.Init()
	if err != nil {
		log.Debug("ProxyNode indexServiceClient init failed ", zap.Error(err))
		return err
	}
	s.proxynode.SetIndexServiceClient(s.indexServiceClient)
	log.Debug("set index service client ...")

	queryServiceAddr := Params.QueryServiceAddress
	log.Debug("ProxyNode", zap.String("query server address", queryServiceAddr))
	s.queryServiceClient, err = grpcqueryserviceclient.NewClient(proxynode.Params.MetaRootPath, proxynode.Params.EtcdEndpoints, timeout)
	if err != nil {
		return err
	}
	err = s.queryServiceClient.Init()
	if err != nil {
		return err
	}
	s.proxynode.SetQueryServiceClient(s.queryServiceClient)
	log.Debug("set query service client ...")

	s.proxynode.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("proxynode",
		zap.Any("state of proxynode", internalpb.StateCode_Initializing))

	if err := s.proxynode.Init(); err != nil {
		log.Debug("proxynode", zap.String("proxynode init error", err.Error()))
		return err
	}

	return nil
}

func (s *Server) start() error {
	return s.proxynode.Start()
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

	err = s.proxynode.Stop()
	if err != nil {
		return err
	}

	s.wg.Wait()

	return nil
}

func (s *Server) GetComponentStates(ctx context.Context, request *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.proxynode.GetComponentStates(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, request *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.proxynode.GetStatisticsChannel(ctx)
}

func (s *Server) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return s.proxynode.InvalidateCollectionMetaCache(ctx, request)
}

func (s *Server) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.proxynode.CreateCollection(ctx, request)
}

func (s *Server) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.proxynode.DropCollection(ctx, request)
}

func (s *Server) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.proxynode.HasCollection(ctx, request)
}

func (s *Server) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.proxynode.LoadCollection(ctx, request)
}

func (s *Server) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.proxynode.ReleaseCollection(ctx, request)
}

func (s *Server) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.proxynode.DescribeCollection(ctx, request)
}

func (s *Server) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	return s.proxynode.GetCollectionStatistics(ctx, request)
}

func (s *Server) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return s.proxynode.ShowCollections(ctx, request)
}

func (s *Server) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.proxynode.CreatePartition(ctx, request)
}

func (s *Server) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.proxynode.DropPartition(ctx, request)
}

func (s *Server) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.proxynode.HasPartition(ctx, request)
}

func (s *Server) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return s.proxynode.LoadPartitions(ctx, request)
}

func (s *Server) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return s.proxynode.ReleasePartitions(ctx, request)
}

func (s *Server) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	return s.proxynode.GetPartitionStatistics(ctx, request)
}

func (s *Server) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.proxynode.ShowPartitions(ctx, request)
}

func (s *Server) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.proxynode.CreateIndex(ctx, request)
}

func (s *Server) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return s.proxynode.DropIndex(ctx, request)
}

func (s *Server) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.proxynode.DescribeIndex(ctx, request)
}

// GetIndexBuildProgress gets index build progress with filed_name and index_name.
// IndexRows is the num of indexed rows. And TotalRows is the total number of segment rows.
func (s *Server) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	return s.proxynode.GetIndexBuildProgress(ctx, request)
}

func (s *Server) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	return s.proxynode.GetIndexState(ctx, request)
}

func (s *Server) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.InsertResponse, error) {
	return s.proxynode.Insert(ctx, request)
}

func (s *Server) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return s.proxynode.Search(ctx, request)
}

func (s *Server) Retrieve(ctx context.Context, request *milvuspb.RetrieveRequest) (*milvuspb.RetrieveResults, error) {
	return s.proxynode.Retrieve(ctx, request)
}

func (s *Server) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*commonpb.Status, error) {
	return s.proxynode.Flush(ctx, request)
}

func (s *Server) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	return s.proxynode.Query(ctx, request)
}

func (s *Server) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	return s.proxynode.GetDdChannel(ctx, request)
}

func (s *Server) GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	return s.proxynode.GetPersistentSegmentInfo(ctx, request)
}

func (s *Server) GetQuerySegmentInfo(ctx context.Context, request *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	return s.proxynode.GetQuerySegmentInfo(ctx, request)

}

func (s *Server) Dummy(ctx context.Context, request *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	return s.proxynode.Dummy(ctx, request)
}

func (s *Server) RegisterLink(ctx context.Context, request *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	return s.proxynode.RegisterLink(ctx, request)
}

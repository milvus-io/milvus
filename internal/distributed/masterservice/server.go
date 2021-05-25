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

package grpcmasterservice

import (
	"context"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	dsc "github.com/milvus-io/milvus/internal/distributed/dataservice/client"
	isc "github.com/milvus-io/milvus/internal/distributed/indexservice/client"
	psc "github.com/milvus-io/milvus/internal/distributed/proxyservice/client"
	qsc "github.com/milvus-io/milvus/internal/distributed/queryservice/client"
	"github.com/milvus-io/milvus/internal/log"
	cms "github.com/milvus-io/milvus/internal/masterservice"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/trace"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

// grpc wrapper
type Server struct {
	masterService types.MasterComponent
	grpcServer    *grpc.Server
	grpcErrChan   chan error

	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	proxyService types.ProxyService
	dataService  types.DataService
	indexService types.IndexService
	queryService types.QueryService

	newProxyServiceClient func(string) types.ProxyService
	newDataServiceClient  func(string) types.DataService
	newIndexServiceClient func(string) types.IndexService
	newQueryServiceClient func(string) (types.QueryService, error)

	closer io.Closer
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	s.setClient()
	var err error
	s.masterService, err = cms.NewCore(s.ctx, factory)
	if err != nil {
		return nil, err
	}
	return s, err
}

func (s *Server) setClient() {
	ctx := context.Background()

	s.newProxyServiceClient = func(s string) types.ProxyService {
		psClient := psc.NewClient(s)
		if err := psClient.Init(); err != nil {
			panic(err)
		}
		if err := psClient.Start(); err != nil {
			panic(err)
		}
		if err := funcutil.WaitForComponentInitOrHealthy(ctx, psClient, "ProxyService", 1000000, 200*time.Millisecond); err != nil {
			panic(err)
		}
		return psClient
	}
	s.newDataServiceClient = func(s string) types.DataService {
		dsClient := dsc.NewClient(s)
		if err := dsClient.Init(); err != nil {
			panic(err)
		}
		if err := dsClient.Start(); err != nil {
			panic(err)
		}
		if err := funcutil.WaitForComponentInitOrHealthy(ctx, dsClient, "DataService", 1000000, 200*time.Millisecond); err != nil {
			panic(err)
		}
		return dsClient
	}
	s.newIndexServiceClient = func(s string) types.IndexService {
		isClient := isc.NewClient(s)
		if err := isClient.Init(); err != nil {
			panic(err)
		}
		if err := isClient.Start(); err != nil {
			panic(err)
		}
		return isClient
	}
	s.newQueryServiceClient = func(s string) (types.QueryService, error) {
		qsClient, err := qsc.NewClient(s, 5*time.Second)
		if err != nil {
			panic(err)
		}
		if err := qsClient.Init(); err != nil {
			panic(err)
		}
		if err := qsClient.Start(); err != nil {
			panic(err)
		}
		return qsClient, nil
	}
}

func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	Params.Init()

	cms.Params.Init()
	cms.Params.Address = Params.Address
	cms.Params.Port = Params.Port
	log.Debug("grpc init done ...")

	ctx := context.Background()

	closer := trace.InitTracing("master_service")
	s.closer = closer

	log.Debug("init params done")

	err := s.masterService.Register()
	if err != nil {
		return err
	}

	err = s.startGrpc()
	if err != nil {
		return err
	}

	s.masterService.UpdateStateCode(internalpb.StateCode_Initializing)

	if s.newProxyServiceClient != nil {
		log.Debug("proxy service", zap.String("address", Params.ProxyServiceAddress))
		proxyService := s.newProxyServiceClient(Params.ProxyServiceAddress)
		if err := s.masterService.SetProxyService(ctx, proxyService); err != nil {
			panic(err)
		}
		s.proxyService = proxyService
	}
	if s.newDataServiceClient != nil {
		log.Debug("data service", zap.String("address", Params.DataServiceAddress))
		dataService := s.newDataServiceClient(Params.DataServiceAddress)
		if err := s.masterService.SetDataService(ctx, dataService); err != nil {
			panic(err)
		}
		s.dataService = dataService
	}
	if s.newIndexServiceClient != nil {
		log.Debug("index service", zap.String("address", Params.IndexServiceAddress))
		indexService := s.newIndexServiceClient(Params.IndexServiceAddress)
		if err := s.masterService.SetIndexService(indexService); err != nil {
			panic(err)
		}
		s.indexService = indexService
	}
	if s.newQueryServiceClient != nil {
		log.Debug("query service", zap.String("address", Params.QueryServiceAddress))
		queryService, _ := s.newQueryServiceClient(Params.QueryServiceAddress)
		if err := s.masterService.SetQueryService(queryService); err != nil {
			panic(err)
		}
		s.queryService = queryService
	}

	return s.masterService.Init()
}

func (s *Server) startGrpc() error {
	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()

	log.Debug("start grpc ", zap.Int("port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Error("GrpcServer:failed to listen", zap.String("error", err.Error()))
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
			otgrpc.OpenTracingStreamServerInterceptor(tracer)))
	masterpb.RegisterMasterServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) start() error {
	log.Debug("Master Core start ...")
	if err := s.masterService.Start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() error {
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			log.Error("close opentracing", zap.Error(err))
		}
	}
	if s.proxyService != nil {
		if err := s.proxyService.Stop(); err != nil {
			log.Debug("close proxyService client", zap.Error(err))
		}
	}
	if s.indexService != nil {
		if err := s.indexService.Stop(); err != nil {
			log.Debug("close indexService client", zap.Error(err))
		}
	}
	if s.dataService != nil {
		if err := s.dataService.Stop(); err != nil {
			log.Debug("close dataService client", zap.Error(err))
		}
	}
	if s.queryService != nil {
		if err := s.queryService.Stop(); err != nil {
			log.Debug("close queryService client", zap.Error(err))
		}
	}
	if s.masterService != nil {
		if err := s.masterService.Stop(); err != nil {
			log.Debug("close masterService", zap.Error(err))
		}
	}
	s.cancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.masterService.GetComponentStates(ctx)
}

//receiver time tick from proxy service, and put it into this channel
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.masterService.GetTimeTickChannel(ctx)
}

//just define a channel, not used currently
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.masterService.GetStatisticsChannel(ctx)
}

//receive ddl from rpc and time tick from proxy service, and put them into this channel
func (s *Server) GetDdChannel(ctx context.Context, req *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	return s.masterService.GetDdChannel(ctx)
}

//DDL request
func (s *Server) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.masterService.CreateCollection(ctx, in)
}

func (s *Server) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.masterService.DropCollection(ctx, in)
}

func (s *Server) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.masterService.HasCollection(ctx, in)
}

func (s *Server) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.masterService.DescribeCollection(ctx, in)
}

func (s *Server) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return s.masterService.ShowCollections(ctx, in)
}

func (s *Server) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.masterService.CreatePartition(ctx, in)
}

func (s *Server) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.masterService.DropPartition(ctx, in)
}

func (s *Server) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.masterService.HasPartition(ctx, in)
}

func (s *Server) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.masterService.ShowPartitions(ctx, in)
}

//index builder service
func (s *Server) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.masterService.CreateIndex(ctx, in)
}

func (s *Server) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return s.masterService.DropIndex(ctx, in)
}

func (s *Server) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.masterService.DescribeIndex(ctx, in)
}

//global timestamp allocator
func (s *Server) AllocTimestamp(ctx context.Context, in *masterpb.AllocTimestampRequest) (*masterpb.AllocTimestampResponse, error) {
	return s.masterService.AllocTimestamp(ctx, in)
}

func (s *Server) AllocID(ctx context.Context, in *masterpb.AllocIDRequest) (*masterpb.AllocIDResponse, error) {
	return s.masterService.AllocID(ctx, in)
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (s *Server) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	return s.masterService.UpdateChannelTimeTick(ctx, in)
}

func (s *Server) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return s.masterService.DescribeSegment(ctx, in)
}

func (s *Server) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	return s.masterService.ShowSegments(ctx, in)
}

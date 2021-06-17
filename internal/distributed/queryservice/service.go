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

package grpcqueryservice

import (
	"context"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	dsc "github.com/milvus-io/milvus/internal/distributed/dataservice/client"
	msc "github.com/milvus-io/milvus/internal/distributed/masterservice/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	qs "github.com/milvus-io/milvus/internal/queryservice"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type Server struct {
	wg         sync.WaitGroup
	loopCtx    context.Context
	loopCancel context.CancelFunc
	grpcServer *grpc.Server

	grpcErrChan chan error

	queryservice *qs.QueryService

	msFactory msgstream.Factory

	dataService   *dsc.Client
	masterService *msc.GrpcClient

	closer io.Closer
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	svr, err := qs.NewQueryService(ctx1, factory)
	if err != nil {
		cancel()
		return nil, err
	}

	return &Server{
		queryservice: svr,
		loopCtx:      ctx1,
		loopCancel:   cancel,
		msFactory:    factory,
		grpcErrChan:  make(chan error),
	}, nil
}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("QueryService init done ...")

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	Params.Init()
	qs.Params.Init()
	qs.Params.Port = Params.Port

	closer := trace.InitTracing("query_service")
	s.closer = closer

	if err := s.queryservice.Register(); err != nil {
		return err
	}

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	if err := <-s.grpcErrChan; err != nil {
		return err
	}

	// --- Master Server Client ---
	log.Debug("QueryService try to new MasterService client", zap.Any("MasterServiceAddress", Params.MasterAddress))
	masterService, err := msc.NewClient(s.loopCtx, qs.Params.MetaRootPath, qs.Params.EtcdEndpoints, 3*time.Second)
	if err != nil {
		log.Debug("QueryService try to new MasterService client failed", zap.Error(err))
		panic(err)
	}

	if err = masterService.Init(); err != nil {
		log.Debug("QueryService MasterServiceClient Init failed", zap.Error(err))
		panic(err)
	}

	if err = masterService.Start(); err != nil {
		log.Debug("QueryService MasterServiceClient Start failed", zap.Error(err))
		panic(err)
	}
	// wait for master init or healthy
	log.Debug("QueryService try to wait for MasterService ready")
	err = funcutil.WaitForComponentInitOrHealthy(s.loopCtx, masterService, "MasterService", 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("QueryService wait for MasterService ready failed", zap.Error(err))
		panic(err)
	}

	if err := s.SetMasterService(masterService); err != nil {
		panic(err)
	}
	log.Debug("QueryService report MasterService ready")

	// --- Data service client ---
	log.Debug("QueryService try to new DataService client", zap.Any("DataServiceAddress", Params.DataServiceAddress))

	dataService := dsc.NewClient(qs.Params.MetaRootPath, qs.Params.EtcdEndpoints, 3*time.Second)
	if err = dataService.Init(); err != nil {
		log.Debug("QueryService DataServiceClient Init failed", zap.Error(err))
		panic(err)
	}
	if err = dataService.Start(); err != nil {
		log.Debug("QueryService DataServiceClient Start failed", zap.Error(err))
		panic(err)
	}
	log.Debug("QueryService try to wait for DataService ready")
	err = funcutil.WaitForComponentInitOrHealthy(s.loopCtx, dataService, "DataService", 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("QueryService wait for DataService ready failed", zap.Error(err))
		panic(err)
	}
	if err := s.SetDataService(dataService); err != nil {
		panic(err)
	}
	log.Debug("QueryService report DataService ready")

	s.queryservice.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("QueryService", zap.Any("State", internalpb.StateCode_Initializing))
	if err := s.queryservice.Init(); err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()

	log.Debug("network", zap.String("port", strconv.Itoa(grpcPort)))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Debug("GrpcServer:failed to listen:", zap.String("error", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)))
	querypb.RegisterQueryServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	return s.queryservice.Start()
}

func (s *Server) Stop() error {
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}
	err := s.queryservice.Stop()
	s.loopCancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	return err
}

func (s *Server) SetMasterService(m types.MasterService) error {
	s.queryservice.SetMasterService(m)
	return nil
}

func (s *Server) SetDataService(d types.DataService) error {
	s.queryservice.SetDataService(d)
	return nil
}

func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.queryservice.GetComponentStates(ctx)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.queryservice.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.queryservice.GetStatisticsChannel(ctx)
}

func (s *Server) RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	return s.queryservice.RegisterNode(ctx, req)
}

func (s *Server) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return s.queryservice.ShowCollections(ctx, req)
}

func (s *Server) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.queryservice.LoadCollection(ctx, req)
}

func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.queryservice.ReleaseCollection(ctx, req)
}

func (s *Server) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	return s.queryservice.ShowPartitions(ctx, req)
}

func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	return s.queryservice.GetPartitionStates(ctx, req)
}

func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return s.queryservice.LoadPartitions(ctx, req)
}

func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return s.queryservice.ReleasePartitions(ctx, req)
}

func (s *Server) CreateQueryChannel(ctx context.Context, req *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error) {
	return s.queryservice.CreateQueryChannel(ctx, req)
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return s.queryservice.GetSegmentInfo(ctx, req)
}

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

package grpcdataserviceclient

import (
	"context"
	"io"
	"math"
	"net"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/logutil"

	"go.uber.org/zap"

	"google.golang.org/grpc"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/milvus-io/milvus/internal/dataservice"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

type Server struct {
	dataService *dataservice.Server
	ctx         context.Context
	cancel      context.CancelFunc

	grpcErrChan chan error
	wg          sync.WaitGroup

	grpcServer    *grpc.Server
	masterService types.MasterService

	closer io.Closer
}

// NewServer new data service grpc server
func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	var err error
	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	s.dataService, err = dataservice.CreateServer(s.ctx, factory)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) init() error {
	Params.Init()
	Params.LoadFromEnv()

	closer := trace.InitTracing("data_service")
	s.closer = closer

	dataservice.Params.Init()
	dataservice.Params.IP = Params.IP
	dataservice.Params.Port = Params.Port

	err := s.dataService.Register()
	if err != nil {
		log.Debug("DataService Register etcd failed", zap.Error(err))
		return err
	}
	log.Debug("DataService Register etcd success")

	err = s.startGrpc()
	if err != nil {
		log.Debug("DataService startGrpc failed", zap.Error(err))
		return err
	}

	if err := s.dataService.Init(); err != nil {
		log.Error("dataService init error", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) startGrpc() error {
	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop(grpcPort int) {
	defer logutil.LogPanic()
	defer s.wg.Done()

	log.Debug("network port", zap.Int("port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Error("grpc server failed to listen error", zap.Error(err))
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
	//grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor))
	datapb.RegisterDataServiceServer(s.grpcServer, s)
	grpc_prometheus.Register(s.grpcServer)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	return s.dataService.Start()
}

func (s *Server) Stop() error {
	var err error
	if s.closer != nil {
		if err = s.closer.Close(); err != nil {
			return err
		}
	}
	s.cancel()

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	err = s.dataService.Stop()
	if err != nil {
		return err
	}

	s.wg.Wait()

	return nil
}

func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Debug("DataService init done ...")

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.dataService.GetComponentStates(ctx)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataService.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataService.GetStatisticsChannel(ctx)
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return s.dataService.GetSegmentInfo(ctx, req)
}

func (s *Server) RegisterNode(ctx context.Context, req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return s.dataService.RegisterNode(ctx, req)
}

func (s *Server) Flush(ctx context.Context, req *datapb.FlushRequest) (*commonpb.Status, error) {
	return s.dataService.Flush(ctx, req)
}

func (s *Server) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	return s.dataService.AssignSegmentID(ctx, req)
}

func (s *Server) ShowSegments(ctx context.Context, req *datapb.ShowSegmentsRequest) (*datapb.ShowSegmentsResponse, error) {
	return s.dataService.ShowSegments(ctx, req)
}

func (s *Server) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	return s.dataService.GetSegmentStates(ctx, req)
}

func (s *Server) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	return s.dataService.GetInsertBinlogPaths(ctx, req)
}

func (s *Server) GetInsertChannels(ctx context.Context, req *datapb.GetInsertChannelsRequest) (*internalpb.StringList, error) {
	return s.dataService.GetInsertChannels(ctx, req)
}

func (s *Server) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	return s.dataService.GetCollectionStatistics(ctx, req)
}

func (s *Server) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	return s.dataService.GetPartitionStatistics(ctx, req)
}

func (s *Server) GetSegmentInfoChannel(ctx context.Context, req *datapb.GetSegmentInfoChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataService.GetSegmentInfoChannel(ctx)
}

//SaveBinlogPaths implement DataServiceServer, saves segment, collection binlog according to datanode request
func (s *Server) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	return s.dataService.SaveBinlogPaths(ctx, req)
}

func (s *Server) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	return s.dataService.GetRecoveryInfo(ctx, req)
}

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

package grpcdatacoordclient

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/logutil"

	"go.uber.org/zap"

	"google.golang.org/grpc"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

type Server struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg        sync.WaitGroup
	dataCoord *datacoord.Server

	grpcErrChan chan error
	grpcServer  *grpc.Server
	closer      io.Closer
}

// NewServer new data service grpc server
func NewServer(ctx context.Context, factory msgstream.Factory, opts ...datacoord.Option) (*Server, error) {
	var err error
	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	s.dataCoord, err = datacoord.CreateServer(s.ctx, factory, opts...)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) init() error {
	Params.Init()
	Params.LoadFromEnv()

	closer := trace.InitTracing("datacoord")
	s.closer = closer

	datacoord.Params.Init()
	datacoord.Params.IP = Params.IP
	datacoord.Params.Port = Params.Port

	err := s.dataCoord.Register()
	if err != nil {
		log.Debug("DataCoord Register etcd failed", zap.Error(err))
		return err
	}
	log.Debug("DataCoord Register etcd success")

	err = s.startGrpc()
	if err != nil {
		log.Debug("DataCoord startGrpc failed", zap.Error(err))
		return err
	}

	if err := s.dataCoord.Init(); err != nil {
		log.Error("dataCoord init error", zap.Error(err))
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

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(
			grpc_opentracing.UnaryServerInterceptor(opts...)),
		grpc.StreamInterceptor(
			grpc_opentracing.StreamServerInterceptor(opts...)))
	//grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor))
	datapb.RegisterDataCoordServer(s.grpcServer, s)
	grpc_prometheus.Register(s.grpcServer)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	return s.dataCoord.Start()
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

	err = s.dataCoord.Stop()
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
	log.Debug("DataCoord init done ...")

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.dataCoord.GetComponentStates(ctx)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetStatisticsChannel(ctx)
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return s.dataCoord.GetSegmentInfo(ctx, req)
}

func (s *Server) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	return s.dataCoord.Flush(ctx, req)
}

func (s *Server) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	return s.dataCoord.AssignSegmentID(ctx, req)
}

func (s *Server) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	return s.dataCoord.GetSegmentStates(ctx, req)
}

func (s *Server) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	return s.dataCoord.GetInsertBinlogPaths(ctx, req)
}

func (s *Server) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	return s.dataCoord.GetCollectionStatistics(ctx, req)
}

func (s *Server) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	return s.dataCoord.GetPartitionStatistics(ctx, req)
}

func (s *Server) GetSegmentInfoChannel(ctx context.Context, req *datapb.GetSegmentInfoChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetSegmentInfoChannel(ctx)
}

//SaveBinlogPaths implement DataCoordServer, saves segment, collection binlog according to datanode request
func (s *Server) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	return s.dataCoord.SaveBinlogPaths(ctx, req)
}

func (s *Server) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	return s.dataCoord.GetRecoveryInfo(ctx, req)
}

func (s *Server) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	return s.dataCoord.GetFlushedSegments(ctx, req)
}

func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.dataCoord.GetMetrics(ctx, req)
}

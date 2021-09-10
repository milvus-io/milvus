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

package grpcindexcoord

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/types"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	ot "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/indexcoord"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type Server struct {
	indexcoord types.IndexCoord

	grpcServer  *grpc.Server
	grpcErrChan chan error

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	closer io.Closer
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
	indexcoord.Params.Init()
	indexcoord.Params.Address = Params.ServiceAddress
	indexcoord.Params.Port = Params.ServicePort

	closer := trace.InitTracing("index_coord")
	s.closer = closer

	if err := s.indexcoord.Register(); err != nil {
		log.Error("IndexCoord", zap.Any("register session error", err))
		return err
	}

	s.loopWg.Add(1)
	go s.startGrpcLoop(Params.ServicePort)
	// wait for grpc IndexCoord loop start
	if err := <-s.grpcErrChan; err != nil {
		log.Error("IndexCoord", zap.Any("init error", err))
		return err
	}
	if err := s.indexcoord.Init(); err != nil {
		log.Error("IndexCoord", zap.Any("init error", err))
		return err
	}
	return nil
}

func (s *Server) start() error {
	if err := s.indexcoord.Start(); err != nil {
		return err
	}
	log.Debug("indexCoord started")
	return nil
}

func (s *Server) Stop() error {
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}
	if s.indexcoord != nil {
		s.indexcoord.Stop()
	}

	s.loopCancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	s.loopWg.Wait()
	return nil
}

func (s *Server) SetClient(indexCoordClient types.IndexCoord) error {
	s.indexcoord = indexCoordClient
	return nil
}

func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.indexcoord.GetComponentStates(ctx)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexcoord.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexcoord.GetStatisticsChannel(ctx)
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	return s.indexcoord.BuildIndex(ctx, req)
}

func (s *Server) GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
	return s.indexcoord.GetIndexStates(ctx, req)
}

func (s *Server) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return s.indexcoord.DropIndex(ctx, request)
}

func (s *Server) GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	return s.indexcoord.GetIndexFilePaths(ctx, req)
}

func (s *Server) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.indexcoord.GetMetrics(ctx, request)
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.loopWg.Done()

	log.Debug("IndexCoord", zap.String("network address", Params.ServiceAddress), zap.Int("network port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("IndexCoord", zap.String("GrpcServer:failed to listen", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(ot.UnaryServerInterceptor(opts...)),
		grpc.StreamInterceptor(ot.StreamServerInterceptor(opts...)))
	indexpb.RegisterIndexCoordServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
	log.Debug("IndexCoord grpcServer loop exit")
}

func NewServer(ctx context.Context) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	serverImp, err := indexcoord.NewIndexCoord(ctx)
	if err != nil {
		defer cancel()
		return nil, err
	}
	s := &Server{
		loopCtx:     ctx1,
		loopCancel:  cancel,
		indexcoord:  serverImp,
		grpcErrChan: make(chan error),
	}

	return s, nil
}

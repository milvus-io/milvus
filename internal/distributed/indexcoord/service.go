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

// UniqueID is an alias of int64, is used as a unique identifier for the request.
type UniqueID = typeutil.UniqueID

// Server is the grpc wrapper of IndexCoord.
type Server struct {
	indexcoord types.IndexCoord

	grpcServer  *grpc.Server
	grpcErrChan chan error

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	closer io.Closer
}

// Server.Run initializes and starts IndexCoord's grpc service.
func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

//Server.init initializes IndexCoord's grpc service.
func (s *Server) init() error {
	Params.Init()

	indexcoord.Params.InitOnce()
	indexcoord.Params.Address = Params.ServiceAddress
	indexcoord.Params.Port = Params.ServicePort

	closer := trace.InitTracing("IndexCoord")
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

//Server.start starts IndexCoord's grpc service.
func (s *Server) start() error {
	if err := s.indexcoord.Start(); err != nil {
		return err
	}
	log.Debug("indexCoord started")
	return nil
}

//Server.Stop stops IndexCoord's grpc service.
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

//Server.SetClient set the IndexCoord's instance.
func (s *Server) SetClient(indexCoordClient types.IndexCoord) error {
	s.indexcoord = indexCoordClient
	return nil
}

//Server.GetComponentStates gets the component states of IndexCoord.
func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.indexcoord.GetComponentStates(ctx)
}

//Server.GetTimeTickChannel gets the time tick channel of IndexCoord.
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexcoord.GetTimeTickChannel(ctx)
}

//Server.GetStatisticsChannel gets the statistics channel of IndexCoord.
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexcoord.GetStatisticsChannel(ctx)
}

//Server.BuildIndex sends the build index request to IndexCoord.
func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	return s.indexcoord.BuildIndex(ctx, req)
}

//Server.GetIndexStates gets the index states from IndexCoord.
func (s *Server) GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
	return s.indexcoord.GetIndexStates(ctx, req)
}

//Server.DropIndex sends the drop index request to IndexCoord.
func (s *Server) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return s.indexcoord.DropIndex(ctx, request)
}

//Server.GetIndexFilePaths gets the index file paths from IndexCoord.
func (s *Server) GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	return s.indexcoord.GetIndexFilePaths(ctx, req)
}

//Server.GetMetrics gets the metrics info of IndexCoord.
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

// NewServer create a new IndexCoord grpc server.
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

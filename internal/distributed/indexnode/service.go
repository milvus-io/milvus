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

package grpcindexnode

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/types"

	"go.uber.org/zap"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"google.golang.org/grpc"
)

// Server is the grpc wrapper of IndexNode.
type Server struct {
	indexnode types.IndexNode

	grpcServer  *grpc.Server
	grpcErrChan chan error

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	closer io.Closer
}

// Run initializes and starts IndexNode's grpc service.
func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.loopWg.Done()

	log.Debug("IndexNode", zap.String("network address", Params.Address), zap.Int("network port: ", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("IndexNode", zap.String("GrpcServer:failed to listen", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(grpc_opentracing.UnaryServerInterceptor(opts...)),
		grpc.StreamInterceptor(grpc_opentracing.StreamServerInterceptor(opts...)))
	indexpb.RegisterIndexNodeServer(s.grpcServer, s)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

// init initializes IndexNode's grpc service.
func (s *Server) init() error {
	var err error
	Params.Init()

	indexnode.Params.InitOnce()
	indexnode.Params.Port = Params.Port
	indexnode.Params.IP = Params.IP
	indexnode.Params.Address = Params.Address

	closer := trace.InitTracing(fmt.Sprintf("IndexNode-%d", indexnode.Params.NodeID))
	s.closer = closer

	Params.Address = Params.IP + ":" + strconv.FormatInt(int64(Params.Port), 10)

	defer func() {
		if err != nil {
			err = s.Stop()
			if err != nil {
				log.Error("IndexNode Init failed, and Stop failed")
			}
		}
	}()

	err = s.indexnode.Register()
	if err != nil {
		log.Error("IndexNode Register etcd failed", zap.Error(err))
		return err
	}
	log.Debug("IndexNode Register etcd success")

	s.loopWg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		log.Error("IndexNode", zap.Any("grpc error", err))
		return err
	}

	err = s.indexnode.Init()
	if err != nil {
		log.Error("IndexNode Init failed", zap.Error(err))
		return err
	}
	return nil
}

// start starts IndexNode's grpc service.
func (s *Server) start() error {
	err := s.indexnode.Start()
	if err != nil {
		return err
	}
	return nil
}

// Stop stops IndexNode's grpc service.
func (s *Server) Stop() error {
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}
	s.loopCancel()
	if s.indexnode != nil {
		s.indexnode.Stop()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.loopWg.Wait()

	return nil
}

// SetClient sets the IndexNode's instance.
func (s *Server) SetClient(indexNodeClient types.IndexNode) error {
	s.indexnode = indexNodeClient
	return nil
}

// GetComponentStates gets the component states of IndexNode.
func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.indexnode.GetComponentStates(ctx)
}

// GetTimeTickChannel gets the time tick channel of IndexNode.
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexnode.GetTimeTickChannel(ctx)
}

// GetStatisticsChannel gets the statistics channel of IndexNode.
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexnode.GetStatisticsChannel(ctx)
}

// CreateIndex sends the create index request to IndexNode.
func (s *Server) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.indexnode.CreateIndex(ctx, req)
}

// GetMetrics gets the metrics info of IndexNode.
func (s *Server) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.indexnode.GetMetrics(ctx, request)
}

// NewServer create a new IndexNode grpc server.
func NewServer(ctx context.Context) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	node, err := indexnode.NewIndexNode(ctx1)
	if err != nil {
		defer cancel()
		return nil, err
	}

	return &Server{
		loopCtx:     ctx1,
		loopCancel:  cancel,
		indexnode:   node,
		grpcErrChan: make(chan error),
	}, nil
}

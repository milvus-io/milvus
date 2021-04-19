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

package grpcproxyservice

import (
	"context"
	"io"
	"math"
	"net"
	"strconv"
	"sync"

	"go.uber.org/zap"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/proxyservice"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
	"google.golang.org/grpc"
)

type Server struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	grpcServer  *grpc.Server
	grpcErrChan chan error

	proxyservice *proxyservice.ProxyService

	tracer opentracing.Tracer
	closer io.Closer
}

func NewServer(ctx1 context.Context, factory msgstream.Factory) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx1)
	var err error

	server := &Server{
		ctx:         ctx,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}

	server.proxyservice, err = proxyservice.NewProxyService(server.ctx, factory)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("proxy service init done ...")

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	Params.Init()
	proxyservice.Params.Init()
	log.Debug("init params done")

	closer := trace.InitTracing("proxy_service")
	s.closer = closer

	s.wg.Add(1)
	go s.startGrpcLoop(Params.ServicePort)
	// wait for grpc server loop start
	if err := <-s.grpcErrChan; err != nil {
		return err
	}
	s.proxyservice.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("grpc init done ...")

	if err := s.proxyservice.Init(); err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()

	log.Debug("proxyservice", zap.Int("network port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("proxyservice", zap.String("GrpcServer:failed to listen", err.Error()))
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
	proxypb.RegisterProxyServiceServer(s.grpcServer, s)
	milvuspb.RegisterProxyServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) start() error {
	log.Debug("proxy ProxyService start ...")
	if err := s.proxyservice.Start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() error {
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}
	s.cancel()
	err := s.proxyservice.Stop()
	if err != nil {
		return err
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.proxyservice.GetComponentStates(ctx)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.proxyservice.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.proxyservice.GetStatisticsChannel(ctx)
}

func (s *Server) RegisterLink(ctx context.Context, req *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	return s.proxyservice.RegisterLink(ctx)
}

func (s *Server) RegisterNode(ctx context.Context, request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error) {
	return s.proxyservice.RegisterNode(ctx, request)
}

func (s *Server) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return s.proxyservice.InvalidateCollectionMetaCache(ctx, request)
}

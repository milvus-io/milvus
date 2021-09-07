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

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type Server struct {
	*datacoord.Server
	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

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
	s.Server, err = datacoord.CreateServer(s.ctx, factory, opts...)
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

	err := s.Register()
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

	if err := s.Init(); err != nil {
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
	return s.Start()
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

	err = s.Stop()
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

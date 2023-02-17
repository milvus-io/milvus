// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package grpcdatacoord implements grpc server for datacoord
package grpcdatacoord

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	ot "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord"
	icc "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Params is the parameters for DataCoord grpc server
var Params paramtable.GrpcServerConfig

// Server is the grpc server of datacoord
type Server struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg        sync.WaitGroup
	dataCoord types.DataCoordComponent

	etcdCli    *clientv3.Client
	indexCoord types.IndexCoord

	grpcErrChan chan error
	grpcServer  *grpc.Server
	closer      io.Closer
}

// NewServer new data service grpc server
func NewServer(ctx context.Context, factory dependency.Factory, opts ...datacoord.Option) *Server {
	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	s.dataCoord = datacoord.CreateServer(s.ctx, factory, opts...)
	return s
}

func (s *Server) init() error {
	Params.InitOnce(typeutil.DataCoordRole)

	closer := trace.InitTracing("datacoord")
	s.closer = closer

	datacoord.Params.InitOnce()
	datacoord.Params.DataCoordCfg.IP = Params.IP
	datacoord.Params.DataCoordCfg.Port = Params.Port
	datacoord.Params.DataCoordCfg.Address = Params.GetAddress()

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd,
		Params.EtcdCfg.EtcdUseSSL,
		Params.EtcdCfg.Endpoints,
		Params.EtcdCfg.EtcdTLSCert,
		Params.EtcdCfg.EtcdTLSKey,
		Params.EtcdCfg.EtcdTLSCACert,
		Params.EtcdCfg.EtcdTLSMinVersion)
	if err != nil {
		log.Warn("DataCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.dataCoord.SetEtcdClient(etcdCli)

	if s.indexCoord == nil {
		var err error
		log.Info("create IndexCoord client for DataCoord")
		s.indexCoord, err = icc.NewClient(s.ctx, Params.EtcdCfg.MetaRootPath, etcdCli)
		if err != nil {
			log.Warn("failed to create IndexCoord client for DataCoord", zap.Error(err))
			return err
		}
		log.Info("create IndexCoord client for DataCoord done")
	}

	log.Info("init IndexCoord client for DataCoord")
	if err := s.indexCoord.Init(); err != nil {
		log.Warn("failed to init IndexCoord client for DataCoord", zap.Error(err))
		return err
	}
	log.Info("init IndexCoord client for DataCoord done")
	s.dataCoord.SetIndexCoord(s.indexCoord)

	err = s.startGrpc()
	if err != nil {
		log.Warn("DataCoord startGrpc failed", zap.Error(err))
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

	log.Info("network port", zap.Int("port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Error("grpc server failed to listen error", zap.Error(err))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			ot.UnaryServerInterceptor(opts...),
			logutil.UnaryTraceLoggerInterceptor)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			ot.StreamServerInterceptor(opts...),
			logutil.StreamTraceLoggerInterceptor)))
	datapb.RegisterDataCoordServer(s.grpcServer, s)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	err := s.dataCoord.Register()
	if err != nil {
		log.Warn("DataCoord register service failed", zap.Error(err))
		return err
	}

	err = s.dataCoord.Start()
	if err != nil {
		log.Error("DataCoord start failed", zap.Error(err))
		return err
	}
	return nil
}

// Stop stops the DataCoord server gracefully.
// Need to call the GracefulStop interface of grpc server and call the stop method of the inner DataCoord object.
func (s *Server) Stop() error {
	log.Info("Datacoord stop", zap.String("Address", Params.GetAddress()))
	var err error
	if s.closer != nil {
		if err = s.closer.Close(); err != nil {
			return err
		}
	}
	s.cancel()

	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	if s.grpcServer != nil {
		log.Info("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}

	err = s.dataCoord.Stop()
	if err != nil {
		return err
	}

	s.wg.Wait()

	return nil
}

// Run starts the Server. Need to call inner init and start method.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Info("DataCoord init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Info("DataCoord start done ...")
	return nil
}

// GetComponentStates gets states of datacoord and datanodes
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.dataCoord.GetComponentStates(ctx)
}

// GetTimeTickChannel gets timetick channel
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetTimeTickChannel(ctx)
}

// GetStatisticsChannel gets statistics channel
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetStatisticsChannel(ctx)
}

func (s *Server) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return s.dataCoord.CheckHealth(ctx, req)
}

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

package grpcrootcoord

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
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	icc "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"
	qcc "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
)

var Params paramtable.GrpcServerConfig

// Server grpc wrapper
type Server struct {
	rootCoord   types.RootCoordComponent
	grpcServer  *grpc.Server
	grpcErrChan chan error

	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	etcdCli    *clientv3.Client
	dataCoord  types.DataCoord
	indexCoord types.IndexCoord
	queryCoord types.QueryCoord

	newIndexCoordClient func(string, *clientv3.Client) types.IndexCoord
	newDataCoordClient  func(string, *clientv3.Client) types.DataCoord
	newQueryCoordClient func(string, *clientv3.Client) types.QueryCoord

	closer io.Closer
}

// NewServer create a new RootCoord grpc server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	s.setClient()
	var err error
	s.rootCoord, err = rootcoord.NewCore(s.ctx, factory)
	if err != nil {
		return nil, err
	}
	return s, err
}

func (s *Server) setClient() {
	s.newDataCoordClient = func(etcdMetaRoot string, etcdCli *clientv3.Client) types.DataCoord {
		dsClient, err := dcc.NewClient(s.ctx, etcdMetaRoot, etcdCli)
		if err != nil {
			panic(err)
		}
		return dsClient
	}
	s.newIndexCoordClient = func(metaRootPath string, etcdCli *clientv3.Client) types.IndexCoord {
		isClient, err := icc.NewClient(s.ctx, metaRootPath, etcdCli)
		if err != nil {
			panic(err)
		}
		return isClient
	}
	s.newQueryCoordClient = func(metaRootPath string, etcdCli *clientv3.Client) types.QueryCoord {
		qsClient, err := qcc.NewClient(s.ctx, metaRootPath, etcdCli)
		if err != nil {
			panic(err)
		}
		return qsClient
	}
}

// Run initializes and starts RootCoord's grpc service.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Info("RootCoord init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Info("RootCoord start done ...")
	return nil
}

func (s *Server) init() error {
	Params.InitOnce(typeutil.RootCoordRole)

	rootcoord.Params.InitOnce()
	rootcoord.Params.RootCoordCfg.Address = Params.GetAddress()
	rootcoord.Params.RootCoordCfg.Port = Params.Port
	log.Info("init params done..")

	closer := trace.InitTracing("root_coord")
	s.closer = closer

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd,
		Params.EtcdCfg.EtcdUseSSL,
		Params.EtcdCfg.Endpoints,
		Params.EtcdCfg.EtcdTLSCert,
		Params.EtcdCfg.EtcdTLSKey,
		Params.EtcdCfg.EtcdTLSCACert,
		Params.EtcdCfg.EtcdTLSMinVersion)
	if err != nil {
		log.Warn("RootCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.rootCoord.SetEtcdClient(s.etcdCli)
	log.Info("etcd connect done ...")

	err = s.startGrpc(Params.Port)
	if err != nil {
		return err
	}
	log.Info("grpc init done ...")

	if s.newDataCoordClient != nil {
		log.Info("RootCoord start to create DataCoord client")
		dataCoord := s.newDataCoordClient(rootcoord.Params.EtcdCfg.MetaRootPath, s.etcdCli)
		if err := s.rootCoord.SetDataCoord(s.ctx, dataCoord); err != nil {
			panic(err)
		}
		s.dataCoord = dataCoord
	}
	if s.newIndexCoordClient != nil {
		log.Info("RootCoord start to create IndexCoord client")
		indexCoord := s.newIndexCoordClient(rootcoord.Params.EtcdCfg.MetaRootPath, s.etcdCli)
		if err := s.rootCoord.SetIndexCoord(indexCoord); err != nil {
			panic(err)
		}
		s.indexCoord = indexCoord
	}
	if s.newQueryCoordClient != nil {
		log.Info("RootCoord start to create QueryCoord client")
		queryCoord := s.newQueryCoordClient(rootcoord.Params.EtcdCfg.MetaRootPath, s.etcdCli)
		if err := s.rootCoord.SetQueryCoord(queryCoord); err != nil {
			panic(err)
		}
		s.queryCoord = queryCoord
	}

	return s.rootCoord.Init()
}

func (s *Server) startGrpc(port int) error {
	s.wg.Add(1)
	go s.startGrpcLoop(port)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop(port int) {
	defer s.wg.Done()
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}
	log.Info("start grpc ", zap.Int("port", port))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Error("GrpcServer:failed to listen", zap.String("error", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

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
	rootcoordpb.RegisterRootCoordServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	log.Info("RootCoord Core start ...")
	if err := s.rootCoord.Register(); err != nil {
		log.Error("RootCoord registers service failed", zap.Error(err))
		return err
	}

	if err := s.rootCoord.Start(); err != nil {
		log.Error("RootCoord start service failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *Server) Stop() error {
	log.Info("Rootcoord stop", zap.String("Address", Params.GetAddress()))
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			log.Error("Failed to close opentracing", zap.Error(err))
		}
	}
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	if s.indexCoord != nil {
		if err := s.indexCoord.Stop(); err != nil {
			log.Error("Failed to close indexCoord client", zap.Error(err))
		}
	}
	if s.dataCoord != nil {
		if err := s.dataCoord.Stop(); err != nil {
			log.Error("Failed to close dataCoord client", zap.Error(err))
		}
	}
	if s.queryCoord != nil {
		if err := s.queryCoord.Stop(); err != nil {
			log.Error("Failed to close queryCoord client", zap.Error(err))
		}
	}
	if s.rootCoord != nil {
		if err := s.rootCoord.Stop(); err != nil {
			log.Error("Failed to close close rootCoord", zap.Error(err))
		}
	}
	log.Info("Rootcoord begin to stop grpc server")
	s.cancel()
	if s.grpcServer != nil {
		log.Info("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) CheckHealth(ctx context.Context, request *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return s.rootCoord.CheckHealth(ctx, request)
}

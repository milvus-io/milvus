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

package grpcindexnode

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	ot "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"

	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var Params paramtable.GrpcServerConfig

// Server is the grpc wrapper of IndexNode.
type Server struct {
	indexnode types.IndexNodeComponent

	grpcServer  *grpc.Server
	grpcErrChan chan error

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	etcdCli *clientv3.Client
	closer  io.Closer
}

// Run initializes and starts IndexNode's grpc service.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Debug("IndexNode init done ...")
	if err := s.start(); err != nil {
		return err
	}
	log.Debug("IndexNode start done ...")
	return nil
}

// startGrpcLoop starts the grep loop of IndexNode component.
func (s *Server) startGrpcLoop(grpcPort int) {
	defer s.loopWg.Done()

	log.Debug("IndexNode", zap.String("network address", Params.GetAddress()), zap.Int("network port: ", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("IndexNode", zap.String("GrpcServer:failed to listen", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
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
	indexpb.RegisterIndexNodeServer(s.grpcServer, s)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

// init initializes IndexNode's grpc service.
func (s *Server) init() error {
	var err error
	Params.InitOnce(typeutil.IndexNodeRole)
	if !funcutil.CheckPortAvailable(Params.Port) {
		Params.Port = funcutil.GetAvailablePort()
		log.Warn("IndexNode get available port when init", zap.Int("Port", Params.Port))
	}
	indexnode.Params.InitOnce()
	indexnode.Params.IndexNodeCfg.Port = Params.Port
	indexnode.Params.IndexNodeCfg.IP = Params.IP
	indexnode.Params.IndexNodeCfg.Address = Params.GetAddress()

	closer := trace.InitTracing(fmt.Sprintf("IndexNode-%d", indexnode.Params.IndexNodeCfg.GetNodeID()))
	s.closer = closer

	defer func() {
		if err != nil {
			err = s.Stop()
			if err != nil {
				log.Error("IndexNode Init failed, and Stop failed")
			}
		}
	}()

	s.loopWg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		log.Error("IndexNode", zap.Any("grpc error", err))
		return err
	}

	etcdCli, err := etcd.GetEtcdClient(&indexnode.Params.EtcdCfg)
	if err != nil {
		log.Debug("IndexNode connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.indexnode.SetEtcdClient(etcdCli)
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
	err = s.indexnode.Register()
	if err != nil {
		log.Error("IndexNode Register etcd failed", zap.Error(err))
		return err
	}
	log.Debug("IndexNode Register etcd success")
	return nil
}

// Stop stops IndexNode's grpc service.
func (s *Server) Stop() error {
	log.Debug("IndexNode stop", zap.String("Address", Params.GetAddress()))
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}
	s.loopCancel()
	if s.indexnode != nil {
		s.indexnode.Stop()
	}
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	if s.grpcServer != nil {
		log.Debug("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}
	s.loopWg.Wait()

	return nil
}

// SetClient sets the IndexNode's instance.
func (s *Server) SetClient(indexNodeClient types.IndexNodeComponent) error {
	s.indexnode = indexNodeClient
	return nil
}

// SetEtcdClient sets the etcd client for QueryNode component.
func (s *Server) SetEtcdClient(etcdCli *clientv3.Client) {
	s.indexnode.SetEtcdClient(etcdCli)
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

func (s *Server) GetTaskSlots(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
	return s.indexnode.GetTaskSlots(ctx, req)
}

// GetMetrics gets the metrics info of IndexNode.
func (s *Server) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.indexnode.GetMetrics(ctx, request)
}

// NewServer create a new IndexNode grpc server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	node, err := indexnode.NewIndexNode(ctx1, factory)
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

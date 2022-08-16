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

package grpcindexcoord

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
	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/indexcoord"
	ic "github.com/milvus-io/milvus/internal/indexcoord"
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

// Params contains parameters for indexcoord grpc server.
var Params paramtable.GrpcServerConfig

// UniqueID is an alias of int64, is used as a unique identifier for the request.
type UniqueID = typeutil.UniqueID

// Server is the grpc wrapper of IndexCoord.
type Server struct {
	indexcoord types.IndexCoordComponent

	grpcServer  *grpc.Server
	grpcErrChan chan error

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	etcdCli *clientv3.Client

	dataCoord types.DataCoord
	rootCoord types.RootCoord

	closer io.Closer
}

// Run initializes and starts IndexCoord's grpc service.
func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("IndexCoord init done ...")
	if err := s.start(); err != nil {
		return err
	}
	log.Debug("IndexCoord start done ...")
	return nil
}

// init initializes IndexCoord's grpc service.
func (s *Server) init() error {
	Params.InitOnce(typeutil.IndexCoordRole)

	indexcoord.Params.InitOnce()
	indexcoord.Params.IndexCoordCfg.Address = Params.GetAddress()
	indexcoord.Params.IndexCoordCfg.Port = Params.Port

	closer := trace.InitTracing("IndexCoord", &Params.BaseTable, fmt.Sprintf("%s:%d", Params.IP, Params.Port))
	s.closer = closer

	etcdCli, err := etcd.GetEtcdClient(&indexcoord.Params.EtcdCfg)
	if err != nil {
		log.Debug("IndexCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.indexcoord.SetEtcdClient(s.etcdCli)

	s.loopWg.Add(1)
	go s.startGrpcLoop(indexcoord.Params.IndexCoordCfg.Port)
	// wait for grpc IndexCoord loop start
	if err := <-s.grpcErrChan; err != nil {
		log.Error("IndexCoord", zap.Any("init error", err))
		return err
	}
	if err := s.indexcoord.Init(); err != nil {
		log.Error("IndexCoord", zap.Any("init error", err))
		return err
	}

	// --- RootCoord ---
	if s.rootCoord == nil {
		s.rootCoord, err = rcc.NewClient(s.loopCtx, ic.Params.EtcdCfg.MetaRootPath, s.etcdCli)
		if err != nil {
			log.Debug("IndexCoord try to new RootCoord client failed", zap.Error(err))
			panic(err)
		}
	}

	if err = s.rootCoord.Init(); err != nil {
		log.Debug("IndexCoord RootCoord client init failed", zap.Error(err))
		panic(err)
	}
	if err = s.rootCoord.Start(); err != nil {
		log.Debug("IndexCoord RootCoord client start failed", zap.Error(err))
		panic(err)
	}
	log.Debug("IndexCoord try to wait for RootCoord ready")
	err = funcutil.WaitForComponentHealthy(s.loopCtx, s.rootCoord, typeutil.RootCoordRole, 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("IndexCoord wait for RootCoord ready failed", zap.Error(err))
		panic(err)
	}

	if err := s.SetRootCoord(s.rootCoord); err != nil {
		panic(err)
	}

	// --- DataCoord ---
	if s.dataCoord == nil {
		s.dataCoord, err = dcc.NewClient(s.loopCtx, ic.Params.EtcdCfg.MetaRootPath, s.etcdCli)
		if err != nil {
			log.Debug("IndexCoord try to new DataCoord client failed", zap.Error(err))
			panic(err)
		}
	}

	if err = s.dataCoord.Init(); err != nil {
		log.Debug("IndexCoord DataCoordClient Init failed", zap.Error(err))
		panic(err)
	}
	if err = s.dataCoord.Start(); err != nil {
		log.Debug("IndexCoord DataCoordClient Start failed", zap.Error(err))
		panic(err)
	}
	log.Debug("IndexCoord try to wait for DataCoord ready")
	err = funcutil.WaitForComponentHealthy(s.loopCtx, s.dataCoord, typeutil.DataCoordRole, 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("IndexCoord wait for DataCoord ready failed", zap.Error(err))
		panic(err)
	}

	if err := s.SetDataCoord(s.dataCoord); err != nil {
		panic(err)
	}

	return nil
}

// start starts IndexCoord's grpc service.
func (s *Server) start() error {
	if err := s.indexcoord.Start(); err != nil {
		return err
	}
	log.Debug("indexCoord started")
	if err := s.indexcoord.Register(); err != nil {
		log.Error("IndexCoord", zap.Any("register session error", err))
		return err
	}
	log.Debug("IndexCoord registers service successfully")
	return nil
}

// Stop stops IndexCoord's grpc service.
func (s *Server) Stop() error {
	log.Debug("Indexcoord stop", zap.String("Address", Params.GetAddress()))
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}
	if s.indexcoord != nil {
		s.indexcoord.Stop()
	}
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	s.loopCancel()
	if s.grpcServer != nil {
		log.Debug("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}

	s.loopWg.Wait()
	return nil
}

// SetClient sets the IndexCoord's instance.
func (s *Server) SetClient(indexCoordClient types.IndexCoordComponent) error {
	s.indexcoord = indexCoordClient
	return nil
}

// SetDataCoord sets the DataCoord's client for IndexCoord component.
func (s *Server) SetDataCoord(d types.DataCoord) error {
	s.dataCoord = d
	return s.indexcoord.SetDataCoord(d)
}

// SetRootCoord sets the RootCoord's client for IndexCoord component.
func (s *Server) SetRootCoord(d types.RootCoord) error {
	s.rootCoord = d
	return s.indexcoord.SetRootCoord(d)
}

// GetComponentStates gets the component states of IndexCoord.
func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.indexcoord.GetComponentStates(ctx)
}

// GetStatisticsChannel gets the statistics channel of IndexCoord.
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexcoord.GetStatisticsChannel(ctx)
}

// CreateIndex sends the build index request to IndexCoord.
func (s *Server) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.indexcoord.CreateIndex(ctx, req)
}

// GetIndexState gets the index states from IndexCoord.
func (s *Server) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	return s.indexcoord.GetIndexState(ctx, req)
}

func (s *Server) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return s.indexcoord.GetSegmentIndexState(ctx, req)
}

// GetIndexInfos gets the index file paths from IndexCoord.
func (s *Server) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	return s.indexcoord.GetIndexInfos(ctx, req)
}

// DescribeIndex gets all indexes of the collection.
func (s *Server) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return s.indexcoord.DescribeIndex(ctx, req)
}

// DropIndex sends the drop index request to IndexCoord.
func (s *Server) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return s.indexcoord.DropIndex(ctx, request)
}

func (s *Server) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	return s.indexcoord.GetIndexBuildProgress(ctx, req)
}

// ShowConfigurations gets specified configurations para of IndexCoord
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return s.indexcoord.ShowConfigurations(ctx, req)
}

// GetMetrics gets the metrics info of IndexCoord.
func (s *Server) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.indexcoord.GetMetrics(ctx, request)
}

// startGrpcLoop starts the grep loop of IndexCoord component.
func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.loopWg.Done()
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

	log.Debug("IndexCoord", zap.String("network address", Params.IP), zap.Int("network port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("IndexCoord", zap.String("GrpcServer:failed to listen", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			trace.UnaryServerInterceptor(),
			logutil.UnaryTraceLoggerInterceptor)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			trace.StreamServerInterceptor(),
			logutil.StreamTraceLoggerInterceptor)))
	indexpb.RegisterIndexCoordServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
	log.Debug("IndexCoord grpcServer loop exit")
}

// NewServer create a new IndexCoord grpc server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	serverImp, err := indexcoord.NewIndexCoord(ctx, factory)
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

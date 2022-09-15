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

package grpcquerycoord

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

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	icc "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	qc "github.com/milvus-io/milvus/internal/querycoord"
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

// Server is the grpc server of QueryCoord.
type Server struct {
	wg         sync.WaitGroup
	loopCtx    context.Context
	loopCancel context.CancelFunc
	grpcServer *grpc.Server

	grpcErrChan chan error

	queryCoord types.QueryCoordComponent

	factory dependency.Factory

	etcdCli *clientv3.Client

	dataCoord  types.DataCoord
	rootCoord  types.RootCoord
	indexCoord types.IndexCoord

	closer io.Closer
}

// NewServer create a new QueryCoord grpc server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	svr, err := qc.NewQueryCoord(ctx1, factory)
	if err != nil {
		cancel()
		return nil, err
	}

	return &Server{
		queryCoord:  svr,
		loopCtx:     ctx1,
		loopCancel:  cancel,
		factory:     factory,
		grpcErrChan: make(chan error),
	}, nil
}

// Run initializes and starts QueryCoord's grpc service.
func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("QueryCoord init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("QueryCoord start done ...")
	return nil
}

// init initializes QueryCoord's grpc service.
func (s *Server) init() error {
	Params.InitOnce(typeutil.QueryCoordRole)

	qc.Params.InitOnce()
	qc.Params.QueryCoordCfg.Address = Params.GetAddress()
	qc.Params.QueryCoordCfg.Port = Params.Port

	closer := trace.InitTracing("querycoord")
	s.closer = closer

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		log.Debug("QueryCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.SetEtcdClient(etcdCli)

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		return err
	}

	// --- Master Server Client ---
	if s.rootCoord == nil {
		s.rootCoord, err = rcc.NewClient(s.loopCtx, qc.Params.EtcdCfg.MetaRootPath, s.etcdCli)
		if err != nil {
			log.Debug("QueryCoord try to new RootCoord client failed", zap.Error(err))
			panic(err)
		}
	}

	if err = s.rootCoord.Init(); err != nil {
		log.Debug("QueryCoord RootCoordClient Init failed", zap.Error(err))
		panic(err)
	}

	if err = s.rootCoord.Start(); err != nil {
		log.Debug("QueryCoord RootCoordClient Start failed", zap.Error(err))
		panic(err)
	}
	// wait for master init or healthy
	log.Debug("QueryCoord try to wait for RootCoord ready")
	err = funcutil.WaitForComponentHealthy(s.loopCtx, s.rootCoord, "RootCoord", 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("QueryCoord wait for RootCoord ready failed", zap.Error(err))
		panic(err)
	}

	if err := s.SetRootCoord(s.rootCoord); err != nil {
		panic(err)
	}
	log.Debug("QueryCoord report RootCoord ready")

	// --- Data service client ---
	if s.dataCoord == nil {
		s.dataCoord, err = dcc.NewClient(s.loopCtx, qc.Params.EtcdCfg.MetaRootPath, s.etcdCli)
		if err != nil {
			log.Debug("QueryCoord try to new DataCoord client failed", zap.Error(err))
			panic(err)
		}
	}

	if err = s.dataCoord.Init(); err != nil {
		log.Debug("QueryCoord DataCoordClient Init failed", zap.Error(err))
		panic(err)
	}
	if err = s.dataCoord.Start(); err != nil {
		log.Debug("QueryCoord DataCoordClient Start failed", zap.Error(err))
		panic(err)
	}
	log.Debug("QueryCoord try to wait for DataCoord ready")
	err = funcutil.WaitForComponentHealthy(s.loopCtx, s.dataCoord, "DataCoord", 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("QueryCoord wait for DataCoord ready failed", zap.Error(err))
		panic(err)
	}
	if err := s.SetDataCoord(s.dataCoord); err != nil {
		panic(err)
	}
	log.Debug("QueryCoord report DataCoord ready")

	// --- IndexCoord ---
	if s.indexCoord == nil {
		s.indexCoord, err = icc.NewClient(s.loopCtx, qc.Params.EtcdCfg.MetaRootPath, s.etcdCli)
		if err != nil {
			log.Debug("QueryCoord try to new IndexCoord client failed", zap.Error(err))
			panic(err)
		}
	}

	if err := s.indexCoord.Init(); err != nil {
		log.Debug("QueryCoord IndexCoordClient Init failed", zap.Error(err))
		panic(err)
	}

	if err := s.indexCoord.Start(); err != nil {
		log.Debug("QueryCoord IndexCoordClient Start failed", zap.Error(err))
		panic(err)
	}
	// wait IndexCoord healthy
	log.Debug("QueryCoord try to wait for IndexCoord ready")
	err = funcutil.WaitForComponentHealthy(s.loopCtx, s.indexCoord, "IndexCoord", 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("QueryCoord wait for IndexCoord ready failed", zap.Error(err))
		panic(err)
	}
	log.Debug("QueryCoord report IndexCoord is ready")

	if err := s.SetIndexCoord(s.indexCoord); err != nil {
		panic(err)
	}

	s.queryCoord.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("QueryCoord", zap.Any("State", internalpb.StateCode_Initializing))
	if err := s.queryCoord.Init(); err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}
	log.Debug("network", zap.String("port", strconv.Itoa(grpcPort)))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Debug("GrpcServer:failed to listen:", zap.String("error", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
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
	querypb.RegisterQueryCoordServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

// start starts QueryCoord's grpc service.
func (s *Server) start() error {
	err := s.queryCoord.Start()
	if err != nil {
		return err
	}
	return s.queryCoord.Register()
}

// Stop stops QueryCoord's grpc service.
func (s *Server) Stop() error {
	log.Debug("QueryCoord stop", zap.String("Address", Params.GetAddress()))
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	err := s.queryCoord.Stop()
	s.loopCancel()
	if s.grpcServer != nil {
		log.Debug("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}
	return err
}

// SetRootCoord sets root coordinator's client
func (s *Server) SetEtcdClient(etcdClient *clientv3.Client) {
	s.queryCoord.SetEtcdClient(etcdClient)
}

// SetRootCoord sets the RootCoord's client for QueryCoord component.
func (s *Server) SetRootCoord(m types.RootCoord) error {
	s.queryCoord.SetRootCoord(m)
	return nil
}

// SetDataCoord sets the DataCoord's client for QueryCoord component.
func (s *Server) SetDataCoord(d types.DataCoord) error {
	s.queryCoord.SetDataCoord(d)
	return nil
}

// SetIndexCoord sets the IndexCoord's client for QueryCoord component.
func (s *Server) SetIndexCoord(d types.IndexCoord) error {
	s.queryCoord.SetIndexCoord(d)
	return nil
}

// GetComponentStates gets the component states of QueryCoord.
func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.queryCoord.GetComponentStates(ctx)
}

// GetTimeTickChannel gets the time tick channel of QueryCoord.
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.queryCoord.GetTimeTickChannel(ctx)
}

// GetStatisticsChannel gets the statistics channel of QueryCoord.
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.queryCoord.GetStatisticsChannel(ctx)
}

// ShowCollections shows the collections in the QueryCoord.
func (s *Server) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return s.queryCoord.ShowCollections(ctx, req)
}

// LoadCollection loads the data of the specified collection in QueryCoord.
func (s *Server) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.queryCoord.LoadCollection(ctx, req)
}

// ReleaseCollection releases the data of the specified collection in QueryCoord.
func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.queryCoord.ReleaseCollection(ctx, req)
}

// ShowPartitions shows the partitions in the QueryCoord.
func (s *Server) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	return s.queryCoord.ShowPartitions(ctx, req)
}

// GetPartitionStates gets the states of the specified partition.
func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	return s.queryCoord.GetPartitionStates(ctx, req)
}

// LoadPartitions loads the data of the specified partition in QueryCoord.
func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return s.queryCoord.LoadPartitions(ctx, req)
}

// ReleasePartitions releases the data of the specified partition in QueryCoord.
func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return s.queryCoord.ReleasePartitions(ctx, req)
}

// GetSegmentInfo gets the information of the specified segment from QueryCoord.
func (s *Server) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return s.queryCoord.GetSegmentInfo(ctx, req)
}

// LoadBalance migrate the sealed segments on the source node to the dst nodes
func (s *Server) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	return s.queryCoord.LoadBalance(ctx, req)
}

// ShowConfigurations gets specified configurations para of QueryCoord
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return s.queryCoord.ShowConfigurations(ctx, req)
}

// GetMetrics gets the metrics information of QueryCoord.
func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.queryCoord.GetMetrics(ctx, req)
}

// GetReplicas returns the shard leaders of a certain collection.
func (s *Server) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	return s.queryCoord.GetReplicas(ctx, req)
}

// GetShardLeaders returns the shard leaders of a certain collection.
func (s *Server) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	return s.queryCoord.GetShardLeaders(ctx, req)
}

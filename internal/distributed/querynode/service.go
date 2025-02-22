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

package grpcquerynode

import (
	"context"
	"strconv"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	qn "github.com/milvus-io/milvus/internal/querynodev2"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	_ "github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/tracer"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/util/logutil"
	"github.com/milvus-io/milvus/pkg/v2/util/netutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// UniqueID is an alias for type typeutil.UniqueID, used as a unique identifier for the request.
type UniqueID = typeutil.UniqueID

// Server is the grpc server of QueryNode.
type Server struct {
	querynode   types.QueryNodeComponent
	grpcWG      sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	grpcErrChan chan error

	serverID atomic.Int64

	grpcServer *grpc.Server
	listener   *netutil.NetListener

	etcdCli *clientv3.Client
}

func (s *Server) GetStatistics(ctx context.Context, request *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	return s.querynode.GetStatistics(ctx, request)
}

func (s *Server) GetQueryNode() types.QueryNodeComponent {
	return s.querynode
}

// NewServer create a new QueryNode grpc server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		querynode:   qn.NewQueryNode(ctx, factory),
		grpcErrChan: make(chan error),
	}
	return s, nil
}

func (s *Server) Prepare() error {
	listener, err := netutil.NewListener(
		netutil.OptIP(paramtable.Get().QueryNodeGrpcServerCfg.IP),
		netutil.OptHighPriorityToUsePort(paramtable.Get().QueryNodeGrpcServerCfg.Port.GetAsInt()),
	)
	if err != nil {
		log.Ctx(s.ctx).Warn("QueryNode fail to create net listener", zap.Error(err))
		return err
	}
	s.listener = listener
	log.Ctx(s.ctx).Info("QueryNode listen on", zap.String("address", listener.Addr().String()), zap.Int("port", listener.Port()))
	paramtable.Get().Save(
		paramtable.Get().QueryNodeGrpcServerCfg.Port.Key,
		strconv.FormatInt(int64(listener.Port()), 10))
	return nil
}

// init initializes QueryNode's grpc service.
func (s *Server) init() error {
	etcdConfig := &paramtable.Get().EtcdCfg
	log := log.Ctx(s.ctx)
	log.Debug("QueryNode", zap.Int("port", s.listener.Port()))

	etcdCli, err := etcd.CreateEtcdClient(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdEnableAuth.GetAsBool(),
		etcdConfig.EtcdAuthUserName.GetValue(),
		etcdConfig.EtcdAuthPassword.GetValue(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue())
	if err != nil {
		log.Debug("QueryNode connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.SetEtcdClient(etcdCli)
	s.querynode.SetAddress(s.listener.Address())
	log.Debug("QueryNode connect to etcd successfully")
	s.grpcWG.Add(1)
	go s.startGrpcLoop()
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		return err
	}

	s.querynode.UpdateStateCode(commonpb.StateCode_Initializing)
	log.Debug("QueryNode", zap.Any("State", commonpb.StateCode_Initializing))
	if err := s.querynode.Init(); err != nil {
		log.Error("QueryNode init error: ", zap.Error(err))
		return err
	}
	s.serverID.Store(s.querynode.GetNodeID())

	return nil
}

// start starts QueryNode's grpc service.
func (s *Server) start() error {
	log := log.Ctx(s.ctx)
	if err := s.querynode.Start(); err != nil {
		log.Error("QueryNode start failed", zap.Error(err))
		return err
	}
	if err := s.querynode.Register(); err != nil {
		log.Error("QueryNode register service failed", zap.Error(err))
		return err
	}
	return nil
}

// startGrpcLoop starts the grpc loop of QueryNode component.
func (s *Server) startGrpcLoop() {
	defer s.grpcWG.Done()
	Params := &paramtable.Get().QueryNodeGrpcServerCfg
	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	kasp := keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

	grpcOpts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			// otelgrpc.UnaryServerInterceptor(opts...),
			logutil.UnaryTraceLoggerInterceptor,
			interceptor.ClusterValidationUnaryServerInterceptor(),
			interceptor.ServerIDValidationUnaryServerInterceptor(func() int64 {
				if s.serverID.Load() == 0 {
					s.serverID.Store(paramtable.GetNodeID())
				}
				return s.serverID.Load()
			}),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			// otelgrpc.StreamServerInterceptor(opts...),
			logutil.StreamTraceLoggerInterceptor,
			interceptor.ClusterValidationStreamServerInterceptor(),
			interceptor.ServerIDValidationStreamServerInterceptor(func() int64 {
				if s.serverID.Load() == 0 {
					s.serverID.Store(paramtable.GetNodeID())
				}
				return s.serverID.Load()
			}),
		)),
		grpc.StatsHandler(tracer.GetDynamicOtelGrpcServerStatsHandler()),
	}

	grpcOpts = append(grpcOpts, utils.EnableInternalTLS("QueryNode"))
	s.grpcServer = grpc.NewServer(grpcOpts...)
	querypb.RegisterQueryNodeServer(s.grpcServer, s)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.Ctx(s.ctx).Debug("QueryNode Start Grpc Failed!!!!")
		s.grpcErrChan <- err
	}
}

// Run initializes and starts QueryNode's grpc service.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Debug("QueryNode init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Debug("QueryNode start done ...")
	return nil
}

// Stop stops QueryNode's grpc service.
func (s *Server) Stop() (err error) {
	logger := log.Ctx(s.ctx)
	if s.listener != nil {
		logger = logger.With(zap.String("address", s.listener.Address()))
	}
	logger.Info("QueryNode stopping")
	defer func() {
		logger.Info("QueryNode stopped", zap.Error(err))
	}()

	logger.Info("internal server[querynode] start to stop")
	err = s.querynode.Stop()
	if err != nil {
		logger.Error("failed to close querynode", zap.Error(err))
		return err
	}
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}

	if s.grpcServer != nil {
		utils.GracefulStopGRPCServer(s.grpcServer)
	}
	s.grpcWG.Wait()

	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	return nil
}

// SetEtcdClient sets the etcd client for QueryNode component.
func (s *Server) SetEtcdClient(etcdCli *clientv3.Client) {
	s.querynode.SetEtcdClient(etcdCli)
}

// GetTimeTickChannel gets the time tick channel of QueryNode.
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.querynode.GetTimeTickChannel(ctx, req)
}

// GetStatisticsChannel gets the statistics channel of QueryNode.
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.querynode.GetStatisticsChannel(ctx, req)
}

// GetComponentStates gets the component states of QueryNode.
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	// ignore ctx and in
	return s.querynode.GetComponentStates(ctx, req)
}

// WatchDmChannels watches the channels about data manipulation.
func (s *Server) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.WatchDmChannels(ctx, req)
}

func (s *Server) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	return s.querynode.UnsubDmChannel(ctx, req)
}

// LoadSegments loads the segments to search.
func (s *Server) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.LoadSegments(ctx, req)
}

// ReleaseCollection releases the data of the specified collection in QueryNode.
func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.ReleaseCollection(ctx, req)
}

// LoadPartitions updates partitions meta info in QueryNode.
func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return s.querynode.LoadPartitions(ctx, req)
}

// ReleasePartitions releases the data of the specified partitions in QueryNode.
func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.ReleasePartitions(ctx, req)
}

// ReleaseSegments releases the data of the specified segments in QueryNode.
func (s *Server) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.ReleaseSegments(ctx, req)
}

// GetSegmentInfo gets the information of the specified segments in QueryNode.
func (s *Server) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return s.querynode.GetSegmentInfo(ctx, req)
}

// Search performs search of streaming/historical replica on QueryNode.
func (s *Server) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	return s.querynode.Search(ctx, req)
}

func (s *Server) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	return s.querynode.SearchSegments(ctx, req)
}

// Query performs query of streaming/historical replica on QueryNode.
func (s *Server) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return s.querynode.Query(ctx, req)
}

func (s *Server) QueryStream(req *querypb.QueryRequest, srv querypb.QueryNode_QueryStreamServer) error {
	return s.querynode.QueryStream(req, srv)
}

func (s *Server) QueryStreamSegments(req *querypb.QueryRequest, srv querypb.QueryNode_QueryStreamSegmentsServer) error {
	return s.querynode.QueryStreamSegments(req, srv)
}

func (s *Server) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return s.querynode.QuerySegments(ctx, req)
}

// SyncReplicaSegments syncs replica segment information to shard leader
func (s *Server) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	return s.querynode.SyncReplicaSegments(ctx, req)
}

// ShowConfigurations gets specified configurations para of QueryNode
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return s.querynode.ShowConfigurations(ctx, req)
}

// GetMetrics gets the metrics information of QueryNode.
func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.querynode.GetMetrics(ctx, req)
}

// GetDataDistribution gets the distribution information of QueryNode.
func (s *Server) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	return s.querynode.GetDataDistribution(ctx, req)
}

func (s *Server) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	return s.querynode.SyncDistribution(ctx, req)
}

// Delete is used to forward delete message between delegator and workers.
func (s *Server) Delete(ctx context.Context, req *querypb.DeleteRequest) (*commonpb.Status, error) {
	return s.querynode.Delete(ctx, req)
}

// DeleteBatch is the API to apply same delete data into multiple segments.
// it's basically same as `Delete` but cost less memory pressure.
func (s *Server) DeleteBatch(ctx context.Context, req *querypb.DeleteBatchRequest) (*querypb.DeleteBatchResponse, error) {
	return s.querynode.DeleteBatch(ctx, req)
}

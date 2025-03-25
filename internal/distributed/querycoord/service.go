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
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/coordinator/coordclient"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	qc "github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	_ "github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/tracer"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/util/logutil"
	"github.com/milvus-io/milvus/pkg/v2/util/netutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tikv"
)

// Server is the grpc server of QueryCoord.
type Server struct {
	grpcWG     sync.WaitGroup
	loopCtx    context.Context
	loopCancel context.CancelFunc
	grpcServer *grpc.Server
	listener   *netutil.NetListener

	serverID atomic.Int64

	grpcErrChan chan error

	queryCoord types.QueryCoordComponent

	factory dependency.Factory

	etcdCli *clientv3.Client
	tikvCli *txnkv.Client

	dataCoord types.DataCoordClient
	rootCoord types.RootCoordClient
}

// NewServer create a new QueryCoord grpc server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	svr, err := qc.NewQueryCoord(ctx1)
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

func (s *Server) Prepare() error {
	listener, err := netutil.NewListener(
		netutil.OptIP(paramtable.Get().QueryCoordGrpcServerCfg.IP),
		netutil.OptPort(paramtable.Get().QueryCoordGrpcServerCfg.Port.GetAsInt()),
	)
	if err != nil {
		log.Ctx(s.loopCtx).Warn("QueryCoord fail to create net listener", zap.Error(err))
		return err
	}
	log.Ctx(s.loopCtx).Info("QueryCoord listen on", zap.String("address", listener.Addr().String()), zap.Int("port", listener.Port()))
	s.listener = listener
	return nil
}

// Run initializes and starts QueryCoord's grpc service.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Ctx(s.loopCtx).Info("QueryCoord init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Ctx(s.loopCtx).Info("QueryCoord start done ...")
	return nil
}

var getTiKVClient = tikv.GetTiKVClient

// init initializes QueryCoord's grpc service.
func (s *Server) init() error {
	log := log.Ctx(s.loopCtx)
	params := paramtable.Get()
	etcdConfig := &params.EtcdCfg

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
		log.Warn("QueryCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.SetEtcdClient(etcdCli)
	s.queryCoord.SetAddress(s.listener.Address())

	if params.MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
		log.Info("Connecting to tikv metadata storage.")
		s.tikvCli, err = getTiKVClient(&paramtable.Get().TiKVCfg)
		if err != nil {
			log.Warn("QueryCoord failed to connect to tikv", zap.Error(err))
			return err
		}
		s.SetTiKVClient(s.tikvCli)
		log.Info("Connected to tikv. Using tikv as metadata storage.")
	}

	s.grpcWG.Add(1)
	go s.startGrpcLoop()
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		return err
	}

	// --- Master Server Client ---
	if s.rootCoord == nil {
		s.rootCoord = coordclient.GetRootCoordClient(s.loopCtx)
	}

	// wait for master init or healthy
	if err := s.SetRootCoord(s.rootCoord); err != nil {
		panic(err)
	}

	// --- Data service client ---
	if s.dataCoord == nil {
		s.dataCoord = coordclient.GetDataCoordClient(s.loopCtx)
	}

	if err := s.SetDataCoord(s.dataCoord); err != nil {
		panic(err)
	}

	if err := s.queryCoord.Init(); err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpcLoop() {
	defer s.grpcWG.Done()
	Params := &paramtable.Get().QueryCoordGrpcServerCfg
	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	kasp := keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}
	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	grpcOpts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
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

	grpcOpts = append(grpcOpts, utils.EnableInternalTLS("QueryCoord"))
	s.grpcServer = grpc.NewServer(grpcOpts...)
	querypb.RegisterQueryCoordServer(s.grpcServer, s)
	coordclient.RegisterQueryCoordServer(s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(s.listener); err != nil {
		s.grpcErrChan <- err
	}
}

// start starts QueryCoord's grpc service.
func (s *Server) start() error {
	err := s.queryCoord.Register()
	if err != nil {
		return err
	}
	return s.queryCoord.Start()
}

func (s *Server) GetQueryCoord() types.QueryCoordComponent {
	return s.queryCoord
}

// Stop stops QueryCoord's grpc service.
func (s *Server) Stop() (err error) {
	logger := log.Ctx(s.loopCtx)
	if s.listener != nil {
		logger = log.With(zap.String("address", s.listener.Address()))
	}
	logger.Info("QueryCoord stopping")
	defer func() {
		logger.Info("QueryCoord stopped", zap.Error(err))
	}()

	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}

	if s.grpcServer != nil {
		utils.GracefulStopGRPCServer(s.grpcServer)
	}
	s.grpcWG.Wait()

	logger.Info("internal server[queryCoord] start to stop")
	if err := s.queryCoord.Stop(); err != nil {
		log.Error("failed to close queryCoord", zap.Error(err))
	}
	s.loopCancel()

	// release port resource
	if s.listener != nil {
		s.listener.Close()
	}
	return nil
}

// SetRootCoord sets root coordinator's client
func (s *Server) SetEtcdClient(etcdClient *clientv3.Client) {
	s.queryCoord.SetEtcdClient(etcdClient)
}

func (s *Server) SetTiKVClient(client *txnkv.Client) {
	s.queryCoord.SetTiKVClient(client)
}

// SetRootCoord sets the RootCoord's client for QueryCoord component.
func (s *Server) SetRootCoord(m types.RootCoordClient) error {
	s.queryCoord.SetRootCoordClient(m)
	return nil
}

// SetDataCoord sets the DataCoord's client for QueryCoord component.
func (s *Server) SetDataCoord(d types.DataCoordClient) error {
	s.queryCoord.SetDataCoordClient(d)
	return nil
}

// GetComponentStates gets the component states of QueryCoord.
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.queryCoord.GetComponentStates(ctx, req)
}

// GetTimeTickChannel gets the time tick channel of QueryCoord.
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.queryCoord.GetTimeTickChannel(ctx, req)
}

// GetStatisticsChannel gets the statistics channel of QueryCoord.
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.queryCoord.GetStatisticsChannel(ctx, req)
}

// ShowCollections shows the collections in the QueryCoord.
func (s *Server) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return s.queryCoord.ShowLoadCollections(ctx, req)
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
func (s *Server) ShowLoadPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	return s.queryCoord.ShowLoadPartitions(ctx, req)
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

// SyncNewCreatedPartition notifies QueryCoord to sync new created partition if collection is loaded.
func (s *Server) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest) (*commonpb.Status, error) {
	return s.queryCoord.SyncNewCreatedPartition(ctx, req)
}

// GetSegmentInfo gets the information of the specified segment from QueryCoord.
func (s *Server) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return s.queryCoord.GetLoadSegmentInfo(ctx, req)
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

func (s *Server) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return s.queryCoord.CheckHealth(ctx, req)
}

func (s *Server) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	return s.queryCoord.CreateResourceGroup(ctx, req)
}

func (s *Server) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
	return s.queryCoord.UpdateResourceGroups(ctx, req)
}

func (s *Server) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	return s.queryCoord.DropResourceGroup(ctx, req)
}

func (s *Server) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	return s.queryCoord.TransferNode(ctx, req)
}

func (s *Server) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) (*commonpb.Status, error) {
	return s.queryCoord.TransferReplica(ctx, req)
}

func (s *Server) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	return s.queryCoord.ListResourceGroups(ctx, req)
}

func (s *Server) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	return s.queryCoord.DescribeResourceGroup(ctx, req)
}

func (s *Server) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest) (*commonpb.Status, error) {
	return s.queryCoord.ActivateChecker(ctx, req)
}

func (s *Server) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest) (*commonpb.Status, error) {
	return s.queryCoord.DeactivateChecker(ctx, req)
}

func (s *Server) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error) {
	return s.queryCoord.ListCheckers(ctx, req)
}

func (s *Server) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest) (*querypb.ListQueryNodeResponse, error) {
	return s.queryCoord.ListQueryNode(ctx, req)
}

func (s *Server) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest) (*querypb.GetQueryNodeDistributionResponse, error) {
	return s.queryCoord.GetQueryNodeDistribution(ctx, req)
}

func (s *Server) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest) (*commonpb.Status, error) {
	return s.queryCoord.SuspendBalance(ctx, req)
}

func (s *Server) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest) (*commonpb.Status, error) {
	return s.queryCoord.ResumeBalance(ctx, req)
}

func (s *Server) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest) (*querypb.CheckBalanceStatusResponse, error) {
	return s.queryCoord.CheckBalanceStatus(ctx, req)
}

func (s *Server) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest) (*commonpb.Status, error) {
	return s.queryCoord.SuspendNode(ctx, req)
}

func (s *Server) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest) (*commonpb.Status, error) {
	return s.queryCoord.ResumeNode(ctx, req)
}

func (s *Server) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest) (*commonpb.Status, error) {
	return s.queryCoord.TransferSegment(ctx, req)
}

func (s *Server) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
	return s.queryCoord.TransferChannel(ctx, req)
}

func (s *Server) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest) (*commonpb.Status, error) {
	return s.queryCoord.CheckQueryNodeDistribution(ctx, req)
}

func (s *Server) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest) (*commonpb.Status, error) {
	return s.queryCoord.UpdateLoadConfig(ctx, req)
}

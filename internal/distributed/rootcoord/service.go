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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	_ "github.com/milvus-io/milvus/internal/util/grpcclient"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/netutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tikv"
)

// Server grpc wrapper
type Server struct {
	rootCoord   types.RootCoordComponent
	grpcServer  *grpc.Server
	listener    *netutil.NetListener
	grpcErrChan chan error

	grpcWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	serverID atomic.Int64

	etcdCli    *clientv3.Client
	tikvCli    *txnkv.Client
	dataCoord  types.DataCoordClient
	queryCoord types.QueryCoordClient

	newDataCoordClient  func(ctx context.Context) types.DataCoordClient
	newQueryCoordClient func(ctx context.Context) types.QueryCoordClient
}

func (s *Server) DescribeDatabase(ctx context.Context, request *rootcoordpb.DescribeDatabaseRequest) (*rootcoordpb.DescribeDatabaseResponse, error) {
	return s.rootCoord.DescribeDatabase(ctx, request)
}

func (s *Server) CreateDatabase(ctx context.Context, request *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreateDatabase(ctx, request)
}

func (s *Server) DropDatabase(ctx context.Context, request *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropDatabase(ctx, request)
}

func (s *Server) ListDatabases(ctx context.Context, request *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return s.rootCoord.ListDatabases(ctx, request)
}

func (s *Server) AlterDatabase(ctx context.Context, request *rootcoordpb.AlterDatabaseRequest) (*commonpb.Status, error) {
	return s.rootCoord.AlterDatabase(ctx, request)
}

func (s *Server) CheckHealth(ctx context.Context, request *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return s.rootCoord.CheckHealth(ctx, request)
}

// CreateAlias creates an alias for specified collection.
func (s *Server) CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreateAlias(ctx, request)
}

// DropAlias drops the specified alias.
func (s *Server) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropAlias(ctx, request)
}

// AlterAlias alters the alias for the specified collection.
func (s *Server) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return s.rootCoord.AlterAlias(ctx, request)
}

// DescribeAlias show the alias-collection relation for the specified alias.
func (s *Server) DescribeAlias(ctx context.Context, request *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	return s.rootCoord.DescribeAlias(ctx, request)
}

// ListAliases show all alias in db.
func (s *Server) ListAliases(ctx context.Context, request *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	return s.rootCoord.ListAliases(ctx, request)
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

func (s *Server) Prepare() error {
	log := log.Ctx(s.ctx)
	listener, err := netutil.NewListener(
		netutil.OptIP(paramtable.Get().RootCoordGrpcServerCfg.IP),
		netutil.OptPort(paramtable.Get().RootCoordGrpcServerCfg.Port.GetAsInt()),
	)
	if err != nil {
		log.Warn("RootCoord fail to create net listener", zap.Error(err))
		return err
	}
	log.Info("RootCoord listen on", zap.String("address", listener.Addr().String()), zap.Int("port", listener.Port()))
	s.listener = listener
	return nil
}

func (s *Server) setClient() {
	s.newDataCoordClient = coordclient.GetDataCoordClient
	s.newQueryCoordClient = coordclient.GetQueryCoordClient
}

// Run initializes and starts RootCoord's grpc service.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Info("RootCoord init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Info("RootCoord start done ...")
	return nil
}

var getTiKVClient = tikv.GetTiKVClient

func (s *Server) init() error {
	params := paramtable.Get()
	etcdConfig := &params.EtcdCfg
	log := log.Ctx(s.ctx)
	log.Info("init params done..")

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
		log.Warn("RootCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.rootCoord.SetEtcdClient(s.etcdCli)
	s.rootCoord.SetAddress(s.listener.Address())
	log.Info("etcd connect done ...")

	if params.MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
		log.Info("Connecting to tikv metadata storage.")
		s.tikvCli, err = getTiKVClient(&paramtable.Get().TiKVCfg)
		if err != nil {
			log.Warn("RootCoord failed to connect to tikv", zap.Error(err))
			return err
		}
		s.rootCoord.SetTiKVClient(s.tikvCli)
		log.Info("Connected to tikv. Using tikv as metadata storage.")
	}

	if s.newDataCoordClient != nil {
		log.Info("RootCoord start to create DataCoord client")
		dataCoord := s.newDataCoordClient(s.ctx)
		s.dataCoord = dataCoord
		if err := s.rootCoord.SetDataCoordClient(dataCoord); err != nil {
			panic(err)
		}
	}

	if s.newQueryCoordClient != nil {
		log.Info("RootCoord start to create QueryCoord client")
		queryCoord := s.newQueryCoordClient(s.ctx)
		s.queryCoord = queryCoord
		if err := s.rootCoord.SetQueryCoordClient(queryCoord); err != nil {
			panic(err)
		}
	}

	if err := s.rootCoord.Init(); err != nil {
		return err
	}
	log.Info("RootCoord init done ...")

	err = s.startGrpc()
	if err != nil {
		return err
	}
	log.Info("grpc init done ...")
	return nil
}

func (s *Server) startGrpc() error {
	s.grpcWG.Add(1)
	go s.startGrpcLoop()
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop() {
	defer s.grpcWG.Done()
	Params := &paramtable.Get().RootCoordGrpcServerCfg
	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	kasp := keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}
	log := log.Ctx(s.ctx)
	log.Info("start grpc ", zap.Int("port", s.listener.Port()))

	ctx, cancel := context.WithCancel(s.ctx)
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
			streamingserviceinterceptor.NewStreamingServiceUnaryServerInterceptor(),
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
			streamingserviceinterceptor.NewStreamingServiceStreamServerInterceptor(),
		)),
		grpc.StatsHandler(tracer.GetDynamicOtelGrpcServerStatsHandler()),
	}

	grpcOpts = append(grpcOpts, utils.EnableInternalTLS("RootCoord"))
	s.grpcServer = grpc.NewServer(grpcOpts...)
	rootcoordpb.RegisterRootCoordServer(s.grpcServer, s)
	s.rootCoord.RegisterStreamingCoordGRPCService(s.grpcServer)
	coordclient.RegisterRootCoordServer(s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(s.listener); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	log := log.Ctx(s.ctx)
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

func (s *Server) Stop() (err error) {
	logger := log.Ctx(s.ctx)
	if s.listener != nil {
		logger = logger.With(zap.String("address", s.listener.Address()))
	}
	logger.Info("Rootcoord stopping")
	defer func() {
		logger.Info("Rootcoord stopped", zap.Error(err))
	}()

	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	if s.tikvCli != nil {
		defer s.tikvCli.Close()
	}

	if s.grpcServer != nil {
		utils.GracefulStopGRPCServer(s.grpcServer)
	}
	s.grpcWG.Wait()

	if s.dataCoord != nil {
		if err := s.dataCoord.Close(); err != nil {
			log.Error("Failed to close dataCoord client", zap.Error(err))
		}
	}
	if s.queryCoord != nil {
		if err := s.queryCoord.Close(); err != nil {
			log.Error("Failed to close queryCoord client", zap.Error(err))
		}
	}
	if s.rootCoord != nil {
		logger.Info("internal server[rootCoord] start to stop")
		if err := s.rootCoord.Stop(); err != nil {
			log.Error("Failed to close rootCoord", zap.Error(err))
		}
	}

	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	return nil
}

// GetComponentStates gets the component states of RootCoord.
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.rootCoord.GetComponentStates(ctx, req)
}

// GetTimeTickChannel receiver time tick from proxy service, and put it into this channel
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.rootCoord.GetTimeTickChannel(ctx, req)
}

// GetStatisticsChannel just define a channel, not used currently
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.rootCoord.GetStatisticsChannel(ctx, req)
}

// CreateCollection creates a collection
func (s *Server) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreateCollection(ctx, in)
}

// DropCollection drops a collection
func (s *Server) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropCollection(ctx, in)
}

// HasCollection checks whether a collection is created
func (s *Server) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.rootCoord.HasCollection(ctx, in)
}

// DescribeCollection gets meta info of a collection
func (s *Server) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.rootCoord.DescribeCollection(ctx, in)
}

// DescribeCollectionInternal gets meta info of a collection
func (s *Server) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.rootCoord.DescribeCollectionInternal(ctx, in)
}

// ShowCollections gets all collections
func (s *Server) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return s.rootCoord.ShowCollections(ctx, in)
}

// CreatePartition creates a partition in a collection
func (s *Server) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreatePartition(ctx, in)
}

// DropPartition drops the specified partition.
func (s *Server) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropPartition(ctx, in)
}

// HasPartition checks whether a partition is created.
func (s *Server) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.rootCoord.HasPartition(ctx, in)
}

// ShowPartitions gets all partitions for the specified collection.
func (s *Server) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.rootCoord.ShowPartitions(ctx, in)
}

// ShowPartitionsInternal gets all partitions for the specified collection.
func (s *Server) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.rootCoord.ShowPartitionsInternal(ctx, in)
}

// AllocTimestamp global timestamp allocator
func (s *Server) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	return s.rootCoord.AllocTimestamp(ctx, in)
}

// AllocID allocates an ID
func (s *Server) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	return s.rootCoord.AllocID(ctx, in)
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (s *Server) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	return s.rootCoord.UpdateChannelTimeTick(ctx, in)
}

// ShowSegments gets all segments
func (s *Server) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	return s.rootCoord.ShowSegments(ctx, in)
}

// GetPChannelInfo gets the physical channel information
func (s *Server) GetPChannelInfo(ctx context.Context, in *rootcoordpb.GetPChannelInfoRequest) (*rootcoordpb.GetPChannelInfoResponse, error) {
	return s.rootCoord.GetPChannelInfo(ctx, in)
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (s *Server) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return s.rootCoord.InvalidateCollectionMetaCache(ctx, in)
}

// ShowConfigurations gets specified configurations para of RootCoord
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return s.rootCoord.ShowConfigurations(ctx, req)
}

// GetMetrics gets the metrics of RootCoord.
func (s *Server) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.rootCoord.GetMetrics(ctx, in)
}

func (s *Server) CreateCredential(ctx context.Context, request *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return s.rootCoord.CreateCredential(ctx, request)
}

func (s *Server) GetCredential(ctx context.Context, request *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	return s.rootCoord.GetCredential(ctx, request)
}

func (s *Server) UpdateCredential(ctx context.Context, request *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return s.rootCoord.UpdateCredential(ctx, request)
}

func (s *Server) DeleteCredential(ctx context.Context, request *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return s.rootCoord.DeleteCredential(ctx, request)
}

func (s *Server) ListCredUsers(ctx context.Context, request *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return s.rootCoord.ListCredUsers(ctx, request)
}

func (s *Server) CreateRole(ctx context.Context, request *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreateRole(ctx, request)
}

func (s *Server) DropRole(ctx context.Context, request *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropRole(ctx, request)
}

func (s *Server) OperateUserRole(ctx context.Context, request *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	return s.rootCoord.OperateUserRole(ctx, request)
}

func (s *Server) SelectRole(ctx context.Context, request *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	return s.rootCoord.SelectRole(ctx, request)
}

func (s *Server) SelectUser(ctx context.Context, request *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	return s.rootCoord.SelectUser(ctx, request)
}

func (s *Server) OperatePrivilege(ctx context.Context, request *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	return s.rootCoord.OperatePrivilege(ctx, request)
}

func (s *Server) SelectGrant(ctx context.Context, request *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	return s.rootCoord.SelectGrant(ctx, request)
}

func (s *Server) ListPolicy(ctx context.Context, request *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	return s.rootCoord.ListPolicy(ctx, request)
}

func (s *Server) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	return s.rootCoord.AlterCollection(ctx, request)
}

func (s *Server) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest) (*commonpb.Status, error) {
	return s.rootCoord.AlterCollectionField(ctx, request)
}

func (s *Server) RenameCollection(ctx context.Context, request *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return s.rootCoord.RenameCollection(ctx, request)
}

func (s *Server) BackupRBAC(ctx context.Context, request *milvuspb.BackupRBACMetaRequest) (*milvuspb.BackupRBACMetaResponse, error) {
	return s.rootCoord.BackupRBAC(ctx, request)
}

func (s *Server) RestoreRBAC(ctx context.Context, request *milvuspb.RestoreRBACMetaRequest) (*commonpb.Status, error) {
	return s.rootCoord.RestoreRBAC(ctx, request)
}

func (s *Server) CreatePrivilegeGroup(ctx context.Context, request *milvuspb.CreatePrivilegeGroupRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreatePrivilegeGroup(ctx, request)
}

func (s *Server) DropPrivilegeGroup(ctx context.Context, request *milvuspb.DropPrivilegeGroupRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropPrivilegeGroup(ctx, request)
}

func (s *Server) ListPrivilegeGroups(ctx context.Context, request *milvuspb.ListPrivilegeGroupsRequest) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	return s.rootCoord.ListPrivilegeGroups(ctx, request)
}

func (s *Server) OperatePrivilegeGroup(ctx context.Context, request *milvuspb.OperatePrivilegeGroupRequest) (*commonpb.Status, error) {
	return s.rootCoord.OperatePrivilegeGroup(ctx, request)
}

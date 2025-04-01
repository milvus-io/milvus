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

package grpcmixcoord

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
	mixcoord "github.com/milvus-io/milvus/internal/coordinator"
	mix "github.com/milvus-io/milvus/internal/distributed/mixcoord/client"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	_ "github.com/milvus-io/milvus/internal/util/grpcclient"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
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

// Server grpc wrapper
type Server struct {
	mixCoord    types.MixCoordComponent
	grpcServer  *grpc.Server
	listener    *netutil.NetListener
	grpcErrChan chan error

	grpcWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	serverID atomic.Int64

	etcdCli *clientv3.Client
	tikvCli *txnkv.Client

	mixCoordClient types.MixCoordClient
}

func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}

	var err error
	s.mixCoord, err = mixcoord.NewMixCoordServer(ctx, factory)
	mixCoordClient, _ := mix.NewClient(ctx1)
	s.mixCoordClient = mixCoordClient
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
		log.Warn("MixCoord fail to create net listener", zap.Error(err))
		return err
	}
	log.Info("MixCoord listen on", zap.String("address", listener.Addr().String()), zap.Int("port", listener.Port()))
	s.listener = listener
	return nil
}

// Run initializes and starts MixCoord's grpc service.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Info("MixCoord init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Info("MixCoord start done ...")
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
		log.Warn("MixCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.mixCoord.SetEtcdClient(s.etcdCli)
	s.mixCoord.SetAddress(s.listener.Address())
	s.mixCoord.SetMixCoordClient(s.mixCoordClient)
	log.Info("etcd connect done ...")

	if params.MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
		log.Info("Connecting to tikv metadata storage.")
		s.tikvCli, err = getTiKVClient(&paramtable.Get().TiKVCfg)
		if err != nil {
			log.Warn("MixCoord failed to connect to tikv", zap.Error(err))
			return err
		}
		s.mixCoord.SetTiKVClient(s.tikvCli)
		log.Info("Connected to tikv. Using tikv as metadata storage.")
	}

	if err := s.mixCoord.Init(); err != nil {
		return err
	}
	log.Info("MixCoord init done ...")

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

	grpcOpts = append(grpcOpts, utils.EnableInternalTLS("MixCoord"))
	s.grpcServer = grpc.NewServer(grpcOpts...)
	rootcoordpb.RegisterRootCoordServer(s.grpcServer, s)
	querypb.RegisterQueryCoordServer(s.grpcServer, s)
	datapb.RegisterDataCoordServer(s.grpcServer, s)
	s.mixCoord.RegisterStreamingCoordGRPCService(s.grpcServer)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(s.listener); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	log := log.Ctx(s.ctx)
	log.Info("MixCoord Core start ...")
	if err := s.mixCoord.Register(); err != nil {
		log.Error("MixCoord registers service failed", zap.Error(err))
		return err
	}

	if err := s.mixCoord.Start(); err != nil {
		log.Error("MixCoord start service failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *Server) Stop() (err error) {
	logger := log.Ctx(s.ctx)
	if s.listener != nil {
		logger = logger.With(zap.String("address", s.listener.Address()))
	}
	logger.Info("MixCoord stopping")
	defer func() {
		logger.Info("MixCoord stopped", zap.Error(err))
	}()

	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	if s.tikvCli != nil {
		defer s.tikvCli.Close()
	}

	if s.mixCoord != nil {
		log.Info("graceful stop rootCoord")
		s.mixCoord.GracefulStop()
		log.Info("graceful stop rootCoord done")
	}

	if s.grpcServer != nil {
		utils.GracefulStopGRPCServer(s.grpcServer)
	}
	s.grpcWG.Wait()

	if s.mixCoord != nil {
		logger.Info("internal server[rootCoord] start to stop")
		if err := s.mixCoord.Stop(); err != nil {
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
	return s.mixCoord.GetComponentStates(ctx, req)
}

// GetTimeTickChannel receiver time tick from proxy service, and put it into this channel
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.mixCoord.GetTimeTickChannel(ctx, req)
}

// GetStatisticsChannel just define a channel, not used currently
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.mixCoord.GetStatisticsChannel(ctx, req)
}

func (s *Server) DescribeDatabase(ctx context.Context, request *rootcoordpb.DescribeDatabaseRequest) (*rootcoordpb.DescribeDatabaseResponse, error) {
	return s.mixCoord.DescribeDatabase(ctx, request)
}

func (s *Server) CreateDatabase(ctx context.Context, request *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return s.mixCoord.CreateDatabase(ctx, request)
}

func (s *Server) DropDatabase(ctx context.Context, request *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return s.mixCoord.DropDatabase(ctx, request)
}

func (s *Server) ListDatabases(ctx context.Context, request *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return s.mixCoord.ListDatabases(ctx, request)
}

func (s *Server) AlterDatabase(ctx context.Context, request *rootcoordpb.AlterDatabaseRequest) (*commonpb.Status, error) {
	return s.mixCoord.AlterDatabase(ctx, request)
}

func (s *Server) CheckHealth(ctx context.Context, request *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return s.mixCoord.CheckHealth(ctx, request)
}

// CreateAlias creates an alias for specified collection.
func (s *Server) CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return s.mixCoord.CreateAlias(ctx, request)
}

// DropAlias drops the specified alias.
func (s *Server) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return s.mixCoord.DropAlias(ctx, request)
}

// AlterAlias alters the alias for the specified collection.
func (s *Server) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return s.mixCoord.AlterAlias(ctx, request)
}

// DescribeAlias show the alias-collection relation for the specified alias.
func (s *Server) DescribeAlias(ctx context.Context, request *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	return s.mixCoord.DescribeAlias(ctx, request)
}

// ListAliases show all alias in db.
func (s *Server) ListAliases(ctx context.Context, request *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	return s.mixCoord.ListAliases(ctx, request)
}

// CreateCollection creates a collection
func (s *Server) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.mixCoord.CreateCollection(ctx, in)
}

// DropCollection drops a collection
func (s *Server) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.mixCoord.DropCollection(ctx, in)
}

// HasCollection checks whether a collection is created
func (s *Server) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.mixCoord.HasCollection(ctx, in)
}

// DescribeCollection gets meta info of a collection
func (s *Server) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.mixCoord.DescribeCollection(ctx, in)
}

// DescribeCollectionInternal gets meta info of a collection
func (s *Server) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.mixCoord.DescribeCollectionInternal(ctx, in)
}

// ShowCollections gets all collections
func (s *Server) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return s.mixCoord.ShowCollections(ctx, in)
}

// ShowCollectionIDs returns all collection IDs.
func (s *Server) ShowCollectionIDs(ctx context.Context, in *rootcoordpb.ShowCollectionIDsRequest) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	return s.mixCoord.ShowCollectionIDs(ctx, in)
}

func (s *Server) AddCollectionField(ctx context.Context, in *milvuspb.AddCollectionFieldRequest) (*commonpb.Status, error) {
	return s.mixCoord.AddCollectionField(ctx, in)
}

// CreatePartition creates a partition in a collection
func (s *Server) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.mixCoord.CreatePartition(ctx, in)
}

// DropPartition drops the specified partition.
func (s *Server) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.mixCoord.DropPartition(ctx, in)
}

// HasPartition checks whether a partition is created.
func (s *Server) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.mixCoord.HasPartition(ctx, in)
}

// ShowPartitions gets all partitions for the specified collection.
func (s *Server) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.mixCoord.ShowPartitions(ctx, in)
}

// ShowPartitionsInternal gets all partitions for the specified collection.
func (s *Server) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.mixCoord.ShowPartitionsInternal(ctx, in)
}

// AllocTimestamp global timestamp allocator
func (s *Server) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	return s.mixCoord.AllocTimestamp(ctx, in)
}

// AllocID allocates an ID
func (s *Server) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	return s.mixCoord.AllocID(ctx, in)
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (s *Server) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	return s.mixCoord.UpdateChannelTimeTick(ctx, in)
}

// ShowSegments gets all segments
func (s *Server) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	return s.mixCoord.ShowSegments(ctx, in)
}

// GetPChannelInfo gets the physical channel information
func (s *Server) GetPChannelInfo(ctx context.Context, in *rootcoordpb.GetPChannelInfoRequest) (*rootcoordpb.GetPChannelInfoResponse, error) {
	return s.mixCoord.GetPChannelInfo(ctx, in)
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (s *Server) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return s.mixCoord.InvalidateCollectionMetaCache(ctx, in)
}

// ShowConfigurations gets specified configurations para of RootCoord
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return s.mixCoord.ShowConfigurations(ctx, req)
}

// GetMetrics gets the metrics of RootCoord.
func (s *Server) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.mixCoord.GetMetrics(ctx, in)
}

func (s *Server) CreateCredential(ctx context.Context, request *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return s.mixCoord.CreateCredential(ctx, request)
}

func (s *Server) GetCredential(ctx context.Context, request *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	return s.mixCoord.GetCredential(ctx, request)
}

func (s *Server) UpdateCredential(ctx context.Context, request *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return s.mixCoord.UpdateCredential(ctx, request)
}

func (s *Server) DeleteCredential(ctx context.Context, request *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return s.mixCoord.DeleteCredential(ctx, request)
}

func (s *Server) ListCredUsers(ctx context.Context, request *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return s.mixCoord.ListCredUsers(ctx, request)
}

func (s *Server) CreateRole(ctx context.Context, request *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	return s.mixCoord.CreateRole(ctx, request)
}

func (s *Server) DropRole(ctx context.Context, request *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	return s.mixCoord.DropRole(ctx, request)
}

func (s *Server) OperateUserRole(ctx context.Context, request *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	return s.mixCoord.OperateUserRole(ctx, request)
}

func (s *Server) SelectRole(ctx context.Context, request *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	return s.mixCoord.SelectRole(ctx, request)
}

func (s *Server) SelectUser(ctx context.Context, request *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	return s.mixCoord.SelectUser(ctx, request)
}

func (s *Server) OperatePrivilege(ctx context.Context, request *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	return s.mixCoord.OperatePrivilege(ctx, request)
}

func (s *Server) SelectGrant(ctx context.Context, request *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	return s.mixCoord.SelectGrant(ctx, request)
}

func (s *Server) ListPolicy(ctx context.Context, request *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	return s.mixCoord.ListPolicy(ctx, request)
}

func (s *Server) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	return s.mixCoord.AlterCollection(ctx, request)
}

func (s *Server) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest) (*commonpb.Status, error) {
	return s.mixCoord.AlterCollectionField(ctx, request)
}

func (s *Server) RenameCollection(ctx context.Context, request *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return s.mixCoord.RenameCollection(ctx, request)
}

func (s *Server) BackupRBAC(ctx context.Context, request *milvuspb.BackupRBACMetaRequest) (*milvuspb.BackupRBACMetaResponse, error) {
	return s.mixCoord.BackupRBAC(ctx, request)
}

func (s *Server) RestoreRBAC(ctx context.Context, request *milvuspb.RestoreRBACMetaRequest) (*commonpb.Status, error) {
	return s.mixCoord.RestoreRBAC(ctx, request)
}

func (s *Server) CreatePrivilegeGroup(ctx context.Context, request *milvuspb.CreatePrivilegeGroupRequest) (*commonpb.Status, error) {
	return s.mixCoord.CreatePrivilegeGroup(ctx, request)
}

func (s *Server) DropPrivilegeGroup(ctx context.Context, request *milvuspb.DropPrivilegeGroupRequest) (*commonpb.Status, error) {
	return s.mixCoord.DropPrivilegeGroup(ctx, request)
}

func (s *Server) ListPrivilegeGroups(ctx context.Context, request *milvuspb.ListPrivilegeGroupsRequest) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	return s.mixCoord.ListPrivilegeGroups(ctx, request)
}

func (s *Server) OperatePrivilegeGroup(ctx context.Context, request *milvuspb.OperatePrivilegeGroupRequest) (*commonpb.Status, error) {
	return s.mixCoord.OperatePrivilegeGroup(ctx, request)
}

// ShowCollections shows the collections in the QueryCoord.
func (s *Server) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return s.mixCoord.ShowLoadCollections(ctx, req)
}

// LoadCollection loads the data of the specified collection in QueryCoord.
func (s *Server) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.mixCoord.LoadCollection(ctx, req)
}

// ReleaseCollection releases the data of the specified collection in QueryCoord.
func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.mixCoord.ReleaseCollection(ctx, req)
}

// ShowPartitions shows the partitions in the QueryCoord.
func (s *Server) ShowLoadPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	return s.mixCoord.ShowLoadPartitions(ctx, req)
}

// GetPartitionStates gets the states of the specified partition.
func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	return s.mixCoord.GetPartitionStates(ctx, req)
}

// LoadPartitions loads the data of the specified partition in QueryCoord.
func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return s.mixCoord.LoadPartitions(ctx, req)
}

// ReleasePartitions releases the data of the specified partition in QueryCoord.
func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return s.mixCoord.ReleasePartitions(ctx, req)
}

// SyncNewCreatedPartition notifies QueryCoord to sync new created partition if collection is loaded.
func (s *Server) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest) (*commonpb.Status, error) {
	return s.mixCoord.SyncNewCreatedPartition(ctx, req)
}

// GetSegmentInfo gets the information of the specified segment from QueryCoord.
func (s *Server) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return s.mixCoord.GetLoadSegmentInfo(ctx, req)
}

// LoadBalance migrate the sealed segments on the source node to the dst nodes
func (s *Server) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	return s.mixCoord.LoadBalance(ctx, req)
}

// GetReplicas returns the shard leaders of a certain collection.
func (s *Server) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	return s.mixCoord.GetReplicas(ctx, req)
}

// GetShardLeaders returns the shard leaders of a certain collection.
func (s *Server) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	return s.mixCoord.GetShardLeaders(ctx, req)
}

func (s *Server) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	return s.mixCoord.CreateResourceGroup(ctx, req)
}

func (s *Server) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
	return s.mixCoord.UpdateResourceGroups(ctx, req)
}

func (s *Server) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	return s.mixCoord.DropResourceGroup(ctx, req)
}

func (s *Server) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	return s.mixCoord.TransferNode(ctx, req)
}

func (s *Server) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) (*commonpb.Status, error) {
	return s.mixCoord.TransferReplica(ctx, req)
}

func (s *Server) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	return s.mixCoord.ListResourceGroups(ctx, req)
}

func (s *Server) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	return s.mixCoord.DescribeResourceGroup(ctx, req)
}

func (s *Server) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest) (*commonpb.Status, error) {
	return s.mixCoord.ActivateChecker(ctx, req)
}

func (s *Server) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest) (*commonpb.Status, error) {
	return s.mixCoord.DeactivateChecker(ctx, req)
}

func (s *Server) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error) {
	return s.mixCoord.ListCheckers(ctx, req)
}

func (s *Server) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest) (*querypb.ListQueryNodeResponse, error) {
	return s.mixCoord.ListQueryNode(ctx, req)
}

func (s *Server) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest) (*querypb.GetQueryNodeDistributionResponse, error) {
	return s.mixCoord.GetQueryNodeDistribution(ctx, req)
}

func (s *Server) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest) (*commonpb.Status, error) {
	return s.mixCoord.SuspendBalance(ctx, req)
}

func (s *Server) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest) (*commonpb.Status, error) {
	return s.mixCoord.ResumeBalance(ctx, req)
}

func (s *Server) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest) (*querypb.CheckBalanceStatusResponse, error) {
	return s.mixCoord.CheckBalanceStatus(ctx, req)
}

func (s *Server) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest) (*commonpb.Status, error) {
	return s.mixCoord.SuspendNode(ctx, req)
}

func (s *Server) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest) (*commonpb.Status, error) {
	return s.mixCoord.ResumeNode(ctx, req)
}

func (s *Server) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest) (*commonpb.Status, error) {
	return s.mixCoord.TransferSegment(ctx, req)
}

func (s *Server) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
	return s.mixCoord.TransferChannel(ctx, req)
}

func (s *Server) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest) (*commonpb.Status, error) {
	return s.mixCoord.CheckQueryNodeDistribution(ctx, req)
}

func (s *Server) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest) (*commonpb.Status, error) {
	return s.mixCoord.UpdateLoadConfig(ctx, req)
}

// GetSegmentInfo gets segment information according to segment id
func (s *Server) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return s.mixCoord.GetSegmentInfo(ctx, req)
}

// Flush flushes a collection's data
func (s *Server) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	return s.mixCoord.Flush(ctx, req)
}

// AssignSegmentID requests to allocate segment space for insert
func (s *Server) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	return s.mixCoord.AssignSegmentID(ctx, req)
}

// AllocSegment alloc a new growing segment, add it into segment meta.
func (s *Server) AllocSegment(ctx context.Context, req *datapb.AllocSegmentRequest) (*datapb.AllocSegmentResponse, error) {
	return s.mixCoord.AllocSegment(ctx, req)
}

// GetSegmentStates gets states of segments
func (s *Server) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	return s.mixCoord.GetSegmentStates(ctx, req)
}

// GetInsertBinlogPaths gets insert binlog paths of a segment
func (s *Server) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	return s.mixCoord.GetInsertBinlogPaths(ctx, req)
}

// GetCollectionStatistics gets statistics of a collection
func (s *Server) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	return s.mixCoord.GetCollectionStatistics(ctx, req)
}

// GetPartitionStatistics gets statistics of a partition
func (s *Server) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	return s.mixCoord.GetPartitionStatistics(ctx, req)
}

// GetSegmentInfoChannel gets channel to which datacoord sends segment information
func (s *Server) GetSegmentInfoChannel(ctx context.Context, req *datapb.GetSegmentInfoChannelRequest) (*milvuspb.StringResponse, error) {
	return s.mixCoord.GetSegmentInfoChannel(ctx, req)
}

// SaveBinlogPaths implement DataCoordServer, saves segment, collection binlog according to datanode request
func (s *Server) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	return s.mixCoord.SaveBinlogPaths(ctx, req)
}

// GetRecoveryInfo gets information for recovering channels
func (s *Server) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	return s.mixCoord.GetRecoveryInfo(ctx, req)
}

// GetRecoveryInfoV2 gets information for recovering channels
func (s *Server) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2) (*datapb.GetRecoveryInfoResponseV2, error) {
	return s.mixCoord.GetRecoveryInfoV2(ctx, req)
}

// GetChannelRecoveryInfo gets the corresponding vchannel info.
func (s *Server) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest) (*datapb.GetChannelRecoveryInfoResponse, error) {
	return s.mixCoord.GetChannelRecoveryInfo(ctx, req)
}

// GetFlushedSegments get all flushed segments of a partition
func (s *Server) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	return s.mixCoord.GetFlushedSegments(ctx, req)
}

// GetSegmentsByStates get all segments of a partition by given states
func (s *Server) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error) {
	return s.mixCoord.GetSegmentsByStates(ctx, req)
}

// ManualCompaction triggers a compaction for a collection
func (s *Server) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return s.mixCoord.ManualCompaction(ctx, req)
}

// GetCompactionState gets the state of a compaction
func (s *Server) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return s.mixCoord.GetCompactionState(ctx, req)
}

// GetCompactionStateWithPlans gets the state of a compaction by plan
func (s *Server) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return s.mixCoord.GetCompactionStateWithPlans(ctx, req)
}

// WatchChannels starts watch channels by give request
func (s *Server) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	return s.mixCoord.WatchChannels(ctx, req)
}

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (s *Server) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return s.mixCoord.GetFlushState(ctx, req)
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (s *Server) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	return s.mixCoord.GetFlushAllState(ctx, req)
}

// DropVirtualChannel drop virtual channel in datacoord
func (s *Server) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	return s.mixCoord.DropVirtualChannel(ctx, req)
}

// SetSegmentState sets the state of a segment.
func (s *Server) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	return s.mixCoord.SetSegmentState(ctx, req)
}

// UpdateSegmentStatistics is the dataCoord service caller of UpdateSegmentStatistics.
func (s *Server) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	return s.mixCoord.UpdateSegmentStatistics(ctx, req)
}

// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
func (s *Server) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	return s.mixCoord.UpdateChannelCheckpoint(ctx, req)
}

// MarkSegmentsDropped is the distributed caller of MarkSegmentsDropped.
func (s *Server) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	return s.mixCoord.MarkSegmentsDropped(ctx, req)
}

func (s *Server) BroadcastAlteredCollection(ctx context.Context, request *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	return s.mixCoord.BroadcastAlteredCollection(ctx, request)
}

func (s *Server) GcConfirm(ctx context.Context, request *datapb.GcConfirmRequest) (*datapb.GcConfirmResponse, error) {
	return s.mixCoord.GcConfirm(ctx, request)
}

// CreateIndex sends the build index request to DataCoord.
func (s *Server) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.mixCoord.CreateIndex(ctx, req)
}

func (s *Server) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest) (*commonpb.Status, error) {
	return s.mixCoord.AlterIndex(ctx, req)
}

// GetIndexState gets the index states from DataCoord.
// Deprecated: use DescribeIndex instead
func (s *Server) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	return s.mixCoord.GetIndexState(ctx, req)
}

func (s *Server) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return s.mixCoord.GetSegmentIndexState(ctx, req)
}

// GetIndexInfos gets the index file paths from DataCoord.
func (s *Server) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	return s.mixCoord.GetIndexInfos(ctx, req)
}

// DescribeIndex gets all indexes of the collection.
func (s *Server) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return s.mixCoord.DescribeIndex(ctx, req)
}

// GetIndexStatistics get the information of index..
func (s *Server) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error) {
	return s.mixCoord.GetIndexStatistics(ctx, req)
}

// DropIndex sends the drop index request to DataCoord.
func (s *Server) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return s.mixCoord.DropIndex(ctx, request)
}

// Deprecated: use DescribeIndex instead
func (s *Server) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	return s.mixCoord.GetIndexBuildProgress(ctx, req)
}

func (s *Server) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	return s.mixCoord.ReportDataNodeTtMsgs(ctx, req)
}

func (s *Server) GcControl(ctx context.Context, req *datapb.GcControlRequest) (*commonpb.Status, error) {
	return s.mixCoord.GcControl(ctx, req)
}

func (s *Server) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal) (*internalpb.ImportResponse, error) {
	return s.mixCoord.ImportV2(ctx, in)
}

func (s *Server) GetImportProgress(ctx context.Context, in *internalpb.GetImportProgressRequest) (*internalpb.GetImportProgressResponse, error) {
	return s.mixCoord.GetImportProgress(ctx, in)
}

func (s *Server) ListImports(ctx context.Context, in *internalpb.ListImportsRequestInternal) (*internalpb.ListImportsResponse, error) {
	return s.mixCoord.ListImports(ctx, in)
}

func (s *Server) ListIndexes(ctx context.Context, in *indexpb.ListIndexesRequest) (*indexpb.ListIndexesResponse, error) {
	return s.mixCoord.ListIndexes(ctx, in)
}

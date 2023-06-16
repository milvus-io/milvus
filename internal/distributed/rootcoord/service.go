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
	"net"
	"strconv"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"

	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	qcc "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
)

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
	queryCoord types.QueryCoord

	newDataCoordClient  func(string, *clientv3.Client) types.DataCoord
	newQueryCoordClient func(string, *clientv3.Client) types.QueryCoord
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
	log.Debug("RootCoord init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("RootCoord start done ...")
	return nil
}

func (s *Server) init() error {
	etcdConfig := &paramtable.Get().EtcdCfg
	Params := &paramtable.Get().RootCoordGrpcServerCfg
	log.Debug("init params done..")

	etcdCli, err := etcd.GetEtcdClient(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue())
	if err != nil {
		log.Debug("RootCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.rootCoord.SetEtcdClient(s.etcdCli)
	s.rootCoord.SetAddress(Params.GetAddress())
	log.Debug("etcd connect done ...")

	err = s.startGrpc(Params.Port.GetAsInt())
	if err != nil {
		return err
	}
	log.Debug("grpc init done ...")

	if s.newDataCoordClient != nil {
		log.Debug("RootCoord start to create DataCoord client")
		dataCoord := s.newDataCoordClient(rootcoord.Params.EtcdCfg.MetaRootPath.GetValue(), s.etcdCli)
		s.dataCoord = dataCoord
		if err = s.dataCoord.Init(); err != nil {
			log.Error("RootCoord DataCoordClient Init failed", zap.Error(err))
			panic(err)
		}
		if err = s.dataCoord.Start(); err != nil {
			log.Error("RootCoord DataCoordClient Start failed", zap.Error(err))
			panic(err)
		}
		if err := s.rootCoord.SetDataCoord(dataCoord); err != nil {
			panic(err)
		}
	}

	if s.newQueryCoordClient != nil {
		log.Debug("RootCoord start to create QueryCoord client")
		queryCoord := s.newQueryCoordClient(rootcoord.Params.EtcdCfg.MetaRootPath.GetValue(), s.etcdCli)
		s.queryCoord = queryCoord
		if err := s.queryCoord.Init(); err != nil {
			log.Error("RootCoord QueryCoordClient Init failed", zap.Error(err))
			panic(err)
		}
		if err := s.queryCoord.Start(); err != nil {
			log.Error("RootCoord QueryCoordClient Start failed", zap.Error(err))
			panic(err)
		}
		if err := s.rootCoord.SetQueryCoord(queryCoord); err != nil {
			panic(err)
		}
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
	Params := &paramtable.Get().RootCoordGrpcServerCfg
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}
	log.Debug("start grpc ", zap.Int("port", port))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Error("GrpcServer:failed to listen", zap.String("error", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	opts := tracer.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			otelgrpc.UnaryServerInterceptor(opts...),
			logutil.UnaryTraceLoggerInterceptor,
			interceptor.ClusterValidationUnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			otelgrpc.StreamServerInterceptor(opts...),
			logutil.StreamTraceLoggerInterceptor,
			interceptor.ClusterValidationStreamServerInterceptor(),
		)))
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
	Params := &paramtable.Get().RootCoordGrpcServerCfg
	log.Debug("Rootcoord stop", zap.String("Address", Params.GetAddress()))
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
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
	log.Debug("Rootcoord begin to stop grpc server")
	s.cancel()
	if s.grpcServer != nil {
		log.Debug("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}
	s.wg.Wait()
	return nil
}

// GetComponentStates gets the component states of RootCoord.
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.rootCoord.GetComponentStates(ctx)
}

// GetTimeTickChannel receiver time tick from proxy service, and put it into this channel
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.rootCoord.GetTimeTickChannel(ctx)
}

// GetStatisticsChannel just define a channel, not used currently
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.rootCoord.GetStatisticsChannel(ctx)
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

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (s *Server) Import(ctx context.Context, in *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	return s.rootCoord.Import(ctx, in)
}

// Check import task state from datanode
func (s *Server) GetImportState(ctx context.Context, in *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return s.rootCoord.GetImportState(ctx, in)
}

// Returns id array of all import tasks
func (s *Server) ListImportTasks(ctx context.Context, in *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	return s.rootCoord.ListImportTasks(ctx, in)
}

// Report impot task state to datacoord
func (s *Server) ReportImport(ctx context.Context, in *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	return s.rootCoord.ReportImport(ctx, in)
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

func (s *Server) RenameCollection(ctx context.Context, request *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return s.rootCoord.RenameCollection(ctx, request)
}

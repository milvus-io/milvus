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

package grpcdatanode

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
	dn "github.com/milvus-io/milvus/internal/datanode"
	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	_ "github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/netutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type Server struct {
	datanode    types.DataNodeComponent
	grpcWG      sync.WaitGroup
	grpcErrChan chan error
	grpcServer  *grpc.Server
	listener    *netutil.NetListener
	ctx         context.Context
	cancel      context.CancelFunc
	etcdCli     *clientv3.Client
	factory     dependency.Factory

	serverID atomic.Int64

	newRootCoordClient func() (types.RootCoordClient, error)
	newDataCoordClient func() (types.DataCoordClient, error)
}

// NewServer new DataNode grpc server
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		factory:     factory,
		grpcErrChan: make(chan error),
		newRootCoordClient: func() (types.RootCoordClient, error) {
			return rcc.NewClient(ctx1)
		},
		newDataCoordClient: func() (types.DataCoordClient, error) {
			return dcc.NewClient(ctx1)
		},
	}

	s.serverID.Store(paramtable.GetNodeID())
	s.datanode = dn.NewDataNode(s.ctx, s.factory)
	return s, nil
}

func (s *Server) Prepare() error {
	listener, err := netutil.NewListener(
		netutil.OptIP(paramtable.Get().DataNodeGrpcServerCfg.IP),
		netutil.OptHighPriorityToUsePort(paramtable.Get().DataNodeGrpcServerCfg.Port.GetAsInt()),
	)
	if err != nil {
		log.Ctx(s.ctx).Warn("DataNode fail to create net listener", zap.Error(err))
		return err
	}
	log.Ctx(s.ctx).Info("DataNode listen on", zap.String("address", listener.Addr().String()), zap.Int("port", listener.Port()))
	s.listener = listener
	paramtable.Get().Save(
		paramtable.Get().DataNodeGrpcServerCfg.Port.Key,
		strconv.FormatInt(int64(listener.Port()), 10))
	return nil
}

func (s *Server) startGrpc() error {
	s.grpcWG.Add(1)
	go s.startGrpcLoop()
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

// startGrpcLoop starts the grep loop of datanode component.
func (s *Server) startGrpcLoop() {
	defer s.grpcWG.Done()
	Params := &paramtable.Get().DataNodeGrpcServerCfg
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

	grpcOpts = append(grpcOpts, utils.EnableInternalTLS("DataNode"))
	s.grpcServer = grpc.NewServer(grpcOpts...)
	datapb.RegisterDataNodeServer(s.grpcServer, s)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.Ctx(s.ctx).Warn("DataNode failed to start gRPC")
		s.grpcErrChan <- err
	}
}

func (s *Server) SetEtcdClient(client *clientv3.Client) {
	s.datanode.SetEtcdClient(client)
}

func (s *Server) SetRootCoordInterface(ms types.RootCoordClient) error {
	return s.datanode.SetRootCoordClient(ms)
}

func (s *Server) SetDataCoordInterface(ds types.DataCoordClient) error {
	return s.datanode.SetDataCoordClient(ds)
}

// Run initializes and starts Datanode's grpc service.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		// errors are propagated upstream as panic.
		return err
	}
	log.Ctx(s.ctx).Info("DataNode gRPC services successfully initialized")
	if err := s.start(); err != nil {
		// errors are propagated upstream as panic.
		return err
	}
	log.Ctx(s.ctx).Info("DataNode gRPC services successfully started")
	return nil
}

// Stop stops Datanode's grpc service.
func (s *Server) Stop() (err error) {
	logger := log.Ctx(s.ctx)
	if s.listener != nil {
		logger = logger.With(zap.String("address", s.listener.Address()))
	}
	logger.Info("datanode stopping")
	defer func() {
		logger.Info("datanode stopped", zap.Error(err))
	}()

	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	if s.grpcServer != nil {
		utils.GracefulStopGRPCServer(s.grpcServer)
	}
	s.grpcWG.Wait()

	logger.Info("internal server[datanode] start to stop")
	err = s.datanode.Stop()
	if err != nil {
		logger.Error("failed to close datanode", zap.Error(err))
		return err
	}
	s.cancel()

	if s.listener != nil {
		s.listener.Close()
	}
	return nil
}

// init initializes Datanode's grpc service.
func (s *Server) init() error {
	etcdConfig := &paramtable.Get().EtcdCfg
	log := log.Ctx(s.ctx)

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
		log.Error("failed to connect to etcd", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.SetEtcdClient(s.etcdCli)
	s.datanode.SetAddress(s.listener.Address())
	log.Info("DataNode address", zap.String("address", s.listener.Address()))

	err = s.startGrpc()
	if err != nil {
		return err
	}

	// --- RootCoord Client ---
	if s.newRootCoordClient != nil {
		log.Info("initializing RootCoord client for DataNode")
		rootCoordClient, err := s.newRootCoordClient()
		if err != nil {
			log.Error("failed to create new RootCoord client", zap.Error(err))
			panic(err)
		}

		if err = componentutil.WaitForComponentHealthy(s.ctx, rootCoordClient, "RootCoord", 1000000, time.Millisecond*200); err != nil {
			log.Error("failed to wait for RootCoord client to be ready", zap.Error(err))
			panic(err)
		}
		log.Info("RootCoord client is ready for DataNode")
		if err = s.SetRootCoordInterface(rootCoordClient); err != nil {
			panic(err)
		}
	}

	// --- DataCoord Client ---
	if s.newDataCoordClient != nil {
		log.Debug("starting DataCoord client for DataNode")
		dataCoordClient, err := s.newDataCoordClient()
		if err != nil {
			log.Error("failed to create new DataCoord client", zap.Error(err))
			panic(err)
		}

		if err = componentutil.WaitForComponentInitOrHealthy(s.ctx, dataCoordClient, "DataCoord", 1000000, time.Millisecond*200); err != nil {
			log.Error("failed to wait for DataCoord client to be ready", zap.Error(err))
			panic(err)
		}
		log.Info("DataCoord client is ready for DataNode")
		if err = s.SetDataCoordInterface(dataCoordClient); err != nil {
			panic(err)
		}
	}

	s.datanode.UpdateStateCode(commonpb.StateCode_Initializing)

	if err := s.datanode.Init(); err != nil {
		log.Error("failed to init DataNode server", zap.Error(err))
		return err
	}
	log.Info("current DataNode state", zap.Any("state", s.datanode.GetStateCode()))
	return nil
}

// start starts datanode's grpc service.
func (s *Server) start() error {
	if err := s.datanode.Start(); err != nil {
		return err
	}
	err := s.datanode.Register()
	if err != nil {
		log.Ctx(s.ctx).Debug("failed to register to Etcd", zap.Error(err))
		return err
	}
	return nil
}

// GetComponentStates gets the component states of Datanode
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.datanode.GetComponentStates(ctx, req)
}

// GetStatisticsChannel gets the statistics channel of Datanode.
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.datanode.GetStatisticsChannel(ctx, req)
}

// Deprecated
func (s *Server) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return s.datanode.WatchDmChannels(ctx, req)
}

func (s *Server) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.datanode.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	return s.datanode.FlushSegments(ctx, req)
}

// ShowConfigurations gets specified configurations para of DataNode
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return s.datanode.ShowConfigurations(ctx, req)
}

// GetMetrics gets the metrics info of Datanode.
func (s *Server) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.datanode.GetMetrics(ctx, request)
}

func (s *Server) CompactionV2(ctx context.Context, request *datapb.CompactionPlan) (*commonpb.Status, error) {
	return s.datanode.CompactionV2(ctx, request)
}

// GetCompactionState gets the Compaction tasks state of DataNode
func (s *Server) GetCompactionState(ctx context.Context, request *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	return s.datanode.GetCompactionState(ctx, request)
}

func (s *Server) ResendSegmentStats(ctx context.Context, request *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error) {
	return s.datanode.ResendSegmentStats(ctx, request)
}

func (s *Server) SyncSegments(ctx context.Context, request *datapb.SyncSegmentsRequest) (*commonpb.Status, error) {
	return s.datanode.SyncSegments(ctx, request)
}

func (s *Server) FlushChannels(ctx context.Context, req *datapb.FlushChannelsRequest) (*commonpb.Status, error) {
	return s.datanode.FlushChannels(ctx, req)
}

func (s *Server) NotifyChannelOperation(ctx context.Context, req *datapb.ChannelOperationsRequest) (*commonpb.Status, error) {
	return s.datanode.NotifyChannelOperation(ctx, req)
}

func (s *Server) CheckChannelOperationProgress(ctx context.Context, req *datapb.ChannelWatchInfo) (*datapb.ChannelOperationProgressResponse, error) {
	return s.datanode.CheckChannelOperationProgress(ctx, req)
}

func (s *Server) PreImport(ctx context.Context, req *datapb.PreImportRequest) (*commonpb.Status, error) {
	return s.datanode.PreImport(ctx, req)
}

func (s *Server) ImportV2(ctx context.Context, req *datapb.ImportRequest) (*commonpb.Status, error) {
	return s.datanode.ImportV2(ctx, req)
}

func (s *Server) QueryPreImport(ctx context.Context, req *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error) {
	return s.datanode.QueryPreImport(ctx, req)
}

func (s *Server) QueryImport(ctx context.Context, req *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
	return s.datanode.QueryImport(ctx, req)
}

func (s *Server) DropImport(ctx context.Context, req *datapb.DropImportRequest) (*commonpb.Status, error) {
	return s.datanode.DropImport(ctx, req)
}

func (s *Server) QuerySlot(ctx context.Context, req *datapb.QuerySlotRequest) (*datapb.QuerySlotResponse, error) {
	return s.datanode.QuerySlot(ctx, req)
}

func (s *Server) DropCompactionPlan(ctx context.Context, req *datapb.DropCompactionPlanRequest) (*commonpb.Status, error) {
	return s.datanode.DropCompactionPlan(ctx, req)
}

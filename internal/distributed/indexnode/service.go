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
	"net"
	"strconv"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	_ "github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// Server is the grpc wrapper of IndexNode.
type Server struct {
	indexnode types.IndexNodeComponent

	grpcServer  *grpc.Server
	grpcErrChan chan error

	serverID atomic.Int64

	loopCtx    context.Context
	loopCancel func()
	grpcWG     sync.WaitGroup

	etcdCli *clientv3.Client
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
	defer s.grpcWG.Done()

	Params := &paramtable.Get().IndexNodeGrpcServerCfg
	log.Debug("IndexNode", zap.String("network address", Params.GetAddress()), zap.Int("network port: ", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("IndexNode", zap.Error(err))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	kasp := keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

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
			interceptor.ServerIDValidationUnaryServerInterceptor(func() int64 {
				if s.serverID.Load() == 0 {
					s.serverID.Store(paramtable.GetNodeID())
				}
				return s.serverID.Load()
			}),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			otelgrpc.StreamServerInterceptor(opts...),
			logutil.StreamTraceLoggerInterceptor,
			interceptor.ClusterValidationStreamServerInterceptor(),
			interceptor.ServerIDValidationStreamServerInterceptor(func() int64 {
				if s.serverID.Load() == 0 {
					s.serverID.Store(paramtable.GetNodeID())
				}
				return s.serverID.Load()
			}),
		)))
	workerpb.RegisterIndexNodeServer(s.grpcServer, s)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

// init initializes IndexNode's grpc service.
func (s *Server) init() error {
	etcdConfig := &paramtable.Get().EtcdCfg
	Params := &paramtable.Get().IndexNodeGrpcServerCfg
	var err error
	if !funcutil.CheckPortAvailable(Params.Port.GetAsInt()) {
		paramtable.Get().Save(Params.Port.Key, fmt.Sprintf("%d", funcutil.GetAvailablePort()))
		log.Warn("IndexNode get available port when init", zap.Int("Port", Params.Port.GetAsInt()))
	}

	defer func() {
		if err != nil {
			err = s.Stop()
			if err != nil {
				log.Error("IndexNode Init failed, and Stop failed")
			}
		}
	}()

	s.grpcWG.Add(1)
	go s.startGrpcLoop(Params.Port.GetAsInt())
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		log.Error("IndexNode", zap.Error(err))
		return err
	}

	var etcdCli *clientv3.Client
	etcdCli, err = etcd.CreateEtcdClient(
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
		log.Debug("IndexNode connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.indexnode.SetEtcdClient(etcdCli)
	s.indexnode.SetAddress(Params.GetAddress())
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
func (s *Server) Stop() (err error) {
	Params := &paramtable.Get().IndexNodeGrpcServerCfg
	logger := log.With(zap.String("address", Params.GetAddress()))
	logger.Info("IndexNode stopping")
	defer func() {
		logger.Info("IndexNode stopped", zap.Error(err))
	}()

	if s.indexnode != nil {
		err := s.indexnode.Stop()
		if err != nil {
			log.Error("failed to close indexnode", zap.Error(err))
			return err
		}
	}
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	if s.grpcServer != nil {
		utils.GracefulStopGRPCServer(s.grpcServer)
	}
	s.grpcWG.Wait()

	s.loopCancel()
	return nil
}

// setServer sets the IndexNode's instance.
func (s *Server) setServer(indexNode types.IndexNodeComponent) error {
	s.indexnode = indexNode
	return nil
}

// SetEtcdClient sets the etcd client for QueryNode component.
func (s *Server) SetEtcdClient(etcdCli *clientv3.Client) {
	s.indexnode.SetEtcdClient(etcdCli)
}

// GetComponentStates gets the component states of IndexNode.
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.indexnode.GetComponentStates(ctx, req)
}

// GetStatisticsChannel gets the statistics channel of IndexNode.
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexnode.GetStatisticsChannel(ctx, req)
}

// CreateJob sends the create index request to IndexNode.
func (s *Server) CreateJob(ctx context.Context, req *workerpb.CreateJobRequest) (*commonpb.Status, error) {
	return s.indexnode.CreateJob(ctx, req)
}

// QueryJobs querys index jobs statues
func (s *Server) QueryJobs(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsResponse, error) {
	return s.indexnode.QueryJobs(ctx, req)
}

// DropJobs drops index build jobs
func (s *Server) DropJobs(ctx context.Context, req *workerpb.DropJobsRequest) (*commonpb.Status, error) {
	return s.indexnode.DropJobs(ctx, req)
}

// GetJobNum gets indexnode's job statisctics
func (s *Server) GetJobStats(ctx context.Context, req *workerpb.GetJobStatsRequest) (*workerpb.GetJobStatsResponse, error) {
	return s.indexnode.GetJobStats(ctx, req)
}

// ShowConfigurations gets specified configurations para of IndexNode
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return s.indexnode.ShowConfigurations(ctx, req)
}

// GetMetrics gets the metrics info of IndexNode.
func (s *Server) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.indexnode.GetMetrics(ctx, request)
}

func (s *Server) CreateJobV2(ctx context.Context, request *workerpb.CreateJobV2Request) (*commonpb.Status, error) {
	return s.indexnode.CreateJobV2(ctx, request)
}

func (s *Server) QueryJobsV2(ctx context.Context, request *workerpb.QueryJobsV2Request) (*workerpb.QueryJobsV2Response, error) {
	return s.indexnode.QueryJobsV2(ctx, request)
}

func (s *Server) DropJobsV2(ctx context.Context, request *workerpb.DropJobsV2Request) (*commonpb.Status, error) {
	return s.indexnode.DropJobsV2(ctx, request)
}

// NewServer create a new IndexNode grpc server.
func NewServer(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	node := indexnode.NewIndexNode(ctx1, factory)

	return &Server{
		loopCtx:     ctx1,
		loopCancel:  cancel,
		indexnode:   node,
		grpcErrChan: make(chan error),
	}, nil
}

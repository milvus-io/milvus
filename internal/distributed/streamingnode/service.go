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

package streamingnode

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	mix "github.com/milvus-io/milvus/internal/distributed/mixcoord/client"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/storage"
	streamingnodeserver "github.com/milvus-io/milvus/internal/streamingnode/server"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/tracer"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/util/logutil"
	"github.com/milvus-io/milvus/pkg/v2/util/netutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/tikv"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Server is the grpc server of streamingnode.
type Server struct {
	stopOnce       sync.Once
	grpcServerChan chan struct{}

	// session of current server.
	session *sessionutil.Session
	metaKV  kv.MetaKv

	ctx    context.Context
	cancel context.CancelFunc

	// server
	streamingnode *streamingnodeserver.Server

	// rpc
	grpcServer *grpc.Server
	listener   *netutil.NetListener

	factory dependency.Factory

	// component client
	etcdCli        *clientv3.Client
	tikvCli        *txnkv.Client
	mixCoord       *syncutil.Future[types.MixCoordClient]
	chunkManager   storage.ChunkManager
	componentState *componentutil.ComponentStateService
}

// NewServer create a new StreamingNode server.
func NewServer(ctx context.Context, f dependency.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	return &Server{
		stopOnce:       sync.Once{},
		factory:        f,
		mixCoord:       syncutil.NewFuture[types.MixCoordClient](),
		grpcServerChan: make(chan struct{}),
		componentState: componentutil.NewComponentStateService(typeutil.StreamingNodeRole),
		ctx:            ctx1,
		cancel:         cancel,
	}, nil
}

func (s *Server) Prepare() error {
	listener, err := netutil.NewListener(
		netutil.OptIP(paramtable.Get().StreamingNodeGrpcServerCfg.IP),
		netutil.OptHighPriorityToUsePort(paramtable.Get().StreamingNodeGrpcServerCfg.Port.GetAsInt()),
	)
	if err != nil {
		log.Ctx(s.ctx).Warn("StreamingNode fail to create net listener", zap.Error(err))
		return err
	}
	s.listener = listener
	log.Ctx(s.ctx).Info("StreamingNode listen on", zap.String("address", listener.Addr().String()), zap.Int("port", listener.Port()))
	paramtable.Get().Save(
		paramtable.Get().StreamingNodeGrpcServerCfg.Port.Key,
		strconv.FormatInt(int64(listener.Port()), 10))
	return nil
}

// Run runs the server.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Info("streamingnode init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Ctx(s.ctx).Info("streamingnode start done ...")
	return nil
}

// Stop stops the server, should be call after Run returned.
func (s *Server) Stop() (err error) {
	s.stopOnce.Do(s.stop)
	return nil
}

// stop stops the server.
func (s *Server) stop() {
	s.componentState.OnStopping()
	log := log.Ctx(s.ctx)

	log.Info("streamingnode stop", zap.String("Address", s.listener.Address()))

	// Unregister current server from etcd.
	log.Info("streamingnode unregister session from etcd...")
	if err := s.session.GoingStop(); err != nil {
		log.Warn("streamingnode unregister session failed", zap.Error(err))
	}

	// Stop StreamingNode service.
	log.Info("streamingnode stop service...")
	s.streamingnode.Stop()

	// Stop grpc server.
	log.Info("streamingnode stop grpc server...")
	s.grpcServer.GracefulStop()

	// Stop all session
	log.Info("streamingnode stop session...")
	s.session.Stop()

	// Stop rootCoord client.
	log.Info("streamingnode stop mixCoord client...")

	if s.mixCoord.Ready() {
		if err := s.mixCoord.Get().Close(); err != nil {
			log.Warn("streamingnode stop mixCoord client failed", zap.Error(err))
		}
	}

	// Stop tikv
	if s.tikvCli != nil {
		if err := s.tikvCli.Close(); err != nil {
			log.Warn("streamingnode stop tikv client failed", zap.Error(err))
		}
	}

	// Wait for grpc server to stop.
	log.Info("wait for grpc server stop...")
	<-s.grpcServerChan
	log.Info("streamingnode stop done")

	s.cancel()
	if err := s.listener.Close(); err != nil {
		log.Warn("streamingnode stop listener failed", zap.Error(err))
	}
}

// Health check the health status of streamingnode.
func (s *Server) Health(ctx context.Context) commonpb.StateCode {
	resp, _ := s.componentState.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	return resp.GetState().StateCode
}

func (s *Server) init() (err error) {
	log := log.Ctx(s.ctx)
	defer func() {
		if err != nil {
			log.Error("StreamingNode init failed", zap.Error(err))
			return
		}
		log.Info("init StreamingNode server finished")
	}()

	// Create etcd client.
	s.etcdCli, _ = kvfactory.GetEtcdAndPath()

	if err := s.initMeta(); err != nil {
		return err
	}
	if err := s.initChunkManager(); err != nil {
		return err
	}
	if err := s.initSession(); err != nil {
		return err
	}

	s.initMixCoord()
	s.initGRPCServer()
	// Create StreamingNode service.
	s.streamingnode = streamingnodeserver.NewServerBuilder().
		WithETCD(s.etcdCli).
		WithChunkManager(s.chunkManager).
		WithGRPCServer(s.grpcServer).
		WithMixCoordClient(s.mixCoord).
		WithSession(s.session).
		WithMetaKV(s.metaKV).
		Build()
	return nil
}

func (s *Server) start() (err error) {
	log := log.Ctx(s.ctx)
	defer func() {
		if err != nil {
			log.Error("StreamingNode start failed", zap.Error(err))
			return
		}
		log.Info("start StreamingNode server finished")
	}()

	// Start grpc server.
	if err := s.startGPRCServer(s.ctx); err != nil {
		return errors.Wrap(err, "StreamingNode start gRPC server fail")
	}
	// Register current server to etcd.
	s.registerSessionToETCD()

	s.componentState.OnInitialized(s.session.ServerID)
	return nil
}

func (s *Server) initSession() error {
	s.session = sessionutil.NewSession(s.ctx)
	if s.session == nil {
		return errors.New("session is nil, the etcd client connection may have failed")
	}
	s.session.Init(typeutil.StreamingNodeRole, s.listener.Address(), false, true)
	paramtable.SetNodeID(s.session.ServerID)
	log.Ctx(s.ctx).Info("StreamingNode init session", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("node address", s.listener.Address()))
	return nil
}

func (s *Server) initMeta() error {
	params := paramtable.Get()
	metaType := params.MetaStoreCfg.MetaStoreType.GetValue()
	log := log.Ctx(s.ctx)
	log.Info("data coordinator connecting to metadata store", zap.String("metaType", metaType))
	metaRootPath := ""
	if metaType == util.MetaStoreTypeTiKV {
		var err error
		s.tikvCli, err = tikv.GetTiKVClient(&paramtable.Get().TiKVCfg)
		if err != nil {
			log.Warn("Streamingnode init tikv client failed", zap.Error(err))
			return err
		}
		metaRootPath = params.TiKVCfg.MetaRootPath.GetValue()
		s.metaKV = tikvkv.NewTiKV(s.tikvCli, metaRootPath,
			tikvkv.WithRequestTimeout(paramtable.Get().ServiceParam.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	} else if metaType == util.MetaStoreTypeEtcd {
		metaRootPath = params.EtcdCfg.MetaRootPath.GetValue()
		s.metaKV = etcdkv.NewEtcdKV(s.etcdCli, metaRootPath,
			etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	}
	return nil
}

func (s *Server) initMixCoord() {
	log := log.Ctx(s.ctx)
	go func() {
		retry.Do(s.ctx, func() error {
			log.Info("StreamingNode connect to mixCoord...")
			mixCoord, err := mix.NewClient(s.ctx)
			if err != nil {
				return errors.Wrap(err, "StreamingNode try to new mixCoord client failed")
			}

			log.Info("StreamingNode try to wait for mixCoord ready")
			err = componentutil.WaitForComponentHealthy(s.ctx, mixCoord, "mixCoord", 1000000, time.Millisecond*200)
			if err != nil {
				return errors.Wrap(err, "StreamingNode wait for mixCoord ready failed")
			}
			log.Info("StreamingNode wait for mixCoord ready")
			s.mixCoord.Set(mixCoord)
			return nil
		}, retry.AttemptAlways())
	}()
}

func (s *Server) initChunkManager() (err error) {
	log.Ctx(s.ctx).Info("StreamingNode init chunk manager...")
	s.factory.Init(paramtable.Get())
	manager, err := s.factory.NewPersistentStorageChunkManager(s.ctx)
	if err != nil {
		return errors.Wrap(err, "StreamingNode try to new chunk manager failed")
	}
	s.chunkManager = manager
	return nil
}

func (s *Server) initGRPCServer() {
	log.Ctx(s.ctx).Info("create StreamingNode server...")
	cfg := &paramtable.Get().StreamingNodeGrpcServerCfg
	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}
	kasp := keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

	serverIDGetter := func() int64 {
		return s.session.ServerID
	}
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(cfg.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(cfg.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			logutil.UnaryTraceLoggerInterceptor,
			interceptor.ClusterValidationUnaryServerInterceptor(),
			interceptor.ServerIDValidationUnaryServerInterceptor(serverIDGetter),
			streamingserviceinterceptor.NewStreamingServiceUnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			logutil.StreamTraceLoggerInterceptor,
			interceptor.ClusterValidationStreamServerInterceptor(),
			interceptor.ServerIDValidationStreamServerInterceptor(serverIDGetter),
			streamingserviceinterceptor.NewStreamingServiceStreamServerInterceptor(),
		)),
		grpc.StatsHandler(tracer.GetDynamicOtelGrpcServerStatsHandler()),
	)
	streamingpb.RegisterStreamingNodeStateServiceServer(s.grpcServer, s.componentState)
}

// startGRPCServer starts the grpc server.
func (s *Server) startGPRCServer(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		defer close(s.grpcServerChan)

		if err := s.grpcServer.Serve(s.listener); err != nil {
			select {
			case errCh <- err:
				// failure at initial startup.
			default:
				// failure at runtime.
				panic(errors.Wrapf(err, "grpc server stop with unexpected error"))
			}
		}
	}()
	funcutil.CheckGrpcReady(ctx, errCh)
	return <-errCh
}

// registerSessionToETCD registers current server to etcd.
func (s *Server) registerSessionToETCD() {
	s.session.Register()
	// start liveness check
	s.session.LivenessCheck(context.Background(), func() {
		log.Ctx(s.ctx).Error("StreamingNode disconnected from etcd, process will exit", zap.Int64("Server Id", paramtable.GetNodeID()))
		os.Exit(1)
	})
}

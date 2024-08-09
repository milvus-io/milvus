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
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
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
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tikv"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Server is the grpc server of streamingnode.
type Server struct {
	stopOnce       sync.Once
	grpcServerChan chan struct{}

	// session of current server.
	session *sessionutil.Session
	metaKV  kv.MetaKv

	// server
	streamingnode *streamingnodeserver.Server

	// rpc
	grpcServer *grpc.Server
	lis        net.Listener

	factory dependency.Factory

	// component client
	etcdCli      *clientv3.Client
	tikvCli      *txnkv.Client
	rootCoord    types.RootCoordClient
	dataCoord    types.DataCoordClient
	chunkManager storage.ChunkManager
}

// NewServer create a new StreamingNode server.
func NewServer(f dependency.Factory) (*Server, error) {
	return &Server{
		stopOnce:       sync.Once{},
		factory:        f,
		grpcServerChan: make(chan struct{}),
	}, nil
}

// Run runs the server.
func (s *Server) Run() error {
	// TODO: We should set a timeout for the process startup.
	// But currently, we don't implement.
	ctx := context.Background()

	if err := s.init(ctx); err != nil {
		return err
	}
	log.Info("streamingnode init done ...")

	if err := s.start(ctx); err != nil {
		return err
	}
	log.Info("streamingnode start done ...")
	return nil
}

// Stop stops the server, should be call after Run returned.
func (s *Server) Stop() (err error) {
	s.stopOnce.Do(s.stop)
	return nil
}

// stop stops the server.
func (s *Server) stop() {
	addr, _ := s.getAddress()
	log.Info("streamingnode stop", zap.String("Address", addr))

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
	log.Info("streamingnode stop rootCoord client...")
	if err := s.rootCoord.Close(); err != nil {
		log.Warn("streamingnode stop rootCoord client failed", zap.Error(err))
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
}

// Health check the health status of streamingnode.
func (s *Server) Health(ctx context.Context) commonpb.StateCode {
	return s.streamingnode.Health(ctx)
}

func (s *Server) init(ctx context.Context) (err error) {
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
	if err := s.initChunkManager(ctx); err != nil {
		return err
	}
	if err := s.allocateAddress(); err != nil {
		return err
	}
	if err := s.initSession(ctx); err != nil {
		return err
	}
	if err := s.initRootCoord(ctx); err != nil {
		return err
	}
	if err := s.initDataCoord(ctx); err != nil {
		return err
	}
	s.initGRPCServer()

	// Create StreamingNode service.
	s.streamingnode = streamingnodeserver.NewServerBuilder().
		WithETCD(s.etcdCli).
		WithChunkManager(s.chunkManager).
		WithGRPCServer(s.grpcServer).
		WithRootCoordClient(s.rootCoord).
		WithDataCoordClient(s.dataCoord).
		WithSession(s.session).
		WithMetaKV(s.metaKV).
		WithChunkManager(s.chunkManager).
		Build()
	if err := s.streamingnode.Init(ctx); err != nil {
		return errors.Wrap(err, "StreamingNode service init failed")
	}
	return nil
}

func (s *Server) start(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			log.Error("StreamingNode start failed", zap.Error(err))
			return
		}
		log.Info("start StreamingNode server finished")
	}()

	// Start StreamingNode service.
	s.streamingnode.Start()

	// Start grpc server.
	if err := s.startGPRCServer(ctx); err != nil {
		return errors.Wrap(err, "StreamingNode start gRPC server fail")
	}

	// Register current server to etcd.
	s.registerSessionToETCD()
	return nil
}

func (s *Server) initSession(ctx context.Context) error {
	s.session = sessionutil.NewSession(ctx)
	if s.session == nil {
		return errors.New("session is nil, the etcd client connection may have failed")
	}
	addr, err := s.getAddress()
	if err != nil {
		return err
	}
	s.session.Init(typeutil.StreamingNodeRole, addr, false, true)
	paramtable.SetNodeID(s.session.ServerID)
	log.Info("StreamingNode init session", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("node address", addr))
	return nil
}

func (s *Server) initMeta() error {
	params := paramtable.Get()
	metaType := params.MetaStoreCfg.MetaStoreType.GetValue()
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

func (s *Server) initRootCoord(ctx context.Context) (err error) {
	log.Info("StreamingNode connect to rootCoord...")
	s.rootCoord, err = rcc.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, "StreamingNode try to new RootCoord client failed")
	}

	log.Info("StreamingNode try to wait for RootCoord ready")
	err = componentutil.WaitForComponentHealthy(ctx, s.rootCoord, "RootCoord", 1000000, time.Millisecond*200)
	if err != nil {
		return errors.Wrap(err, "StreamingNode wait for RootCoord ready failed")
	}
	return nil
}

func (s *Server) initDataCoord(ctx context.Context) (err error) {
	log.Info("StreamingNode connect to dataCoord...")
	s.dataCoord, err = dcc.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, "StreamingNode try to new DataCoord client failed")
	}

	log.Info("StreamingNode try to wait for DataCoord ready")
	err = componentutil.WaitForComponentHealthy(ctx, s.dataCoord, "DataCoord", 1000000, time.Millisecond*200)
	if err != nil {
		return errors.Wrap(err, "StreamingNode wait for DataCoord ready failed")
	}
	return nil
}

func (s *Server) initChunkManager(ctx context.Context) (err error) {
	log.Info("StreamingNode init chunk manager...")
	s.factory.Init(paramtable.Get())
	manager, err := s.factory.NewPersistentStorageChunkManager(ctx)
	if err != nil {
		return errors.Wrap(err, "StreamingNode try to new chunk manager failed")
	}
	s.chunkManager = manager
	return nil
}

func (s *Server) initGRPCServer() {
	log.Info("create StreamingNode server...")
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
	opts := tracer.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(cfg.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(cfg.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			otelgrpc.UnaryServerInterceptor(opts...),
			logutil.UnaryTraceLoggerInterceptor,
			interceptor.ClusterValidationUnaryServerInterceptor(),
			interceptor.ServerIDValidationUnaryServerInterceptor(serverIDGetter),
			streamingserviceinterceptor.NewStreamingServiceUnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			otelgrpc.StreamServerInterceptor(opts...),
			logutil.StreamTraceLoggerInterceptor,
			interceptor.ClusterValidationStreamServerInterceptor(),
			interceptor.ServerIDValidationStreamServerInterceptor(serverIDGetter),
			streamingserviceinterceptor.NewStreamingServiceStreamServerInterceptor(),
		)))
}

// allocateAddress allocates a available address for streamingnode grpc server.
func (s *Server) allocateAddress() (err error) {
	port := paramtable.Get().StreamingNodeGrpcServerCfg.Port.GetAsInt()

	retry.Do(context.Background(), func() error {
		addr := ":" + strconv.Itoa(port)
		s.lis, err = net.Listen("tcp", addr)
		if err != nil {
			if port != 0 {
				// set port=0 to get next available port by os
				log.Warn("StreamingNode suggested port is in used, try to get by os", zap.Error(err))
				port = 0
			}
		}
		return err
	}, retry.Attempts(10))
	return err
}

// getAddress returns the address of streamingnode grpc server.
// must be called after allocateAddress.
func (s *Server) getAddress() (string, error) {
	if s.lis == nil {
		return "", errors.New("StreamingNode grpc server is not initialized")
	}
	ip := paramtable.Get().StreamingNodeGrpcServerCfg.IP
	return fmt.Sprintf("%s:%d", ip, s.lis.Addr().(*net.TCPAddr).Port), nil
}

// startGRPCServer starts the grpc server.
func (s *Server) startGPRCServer(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		defer close(s.grpcServerChan)

		if err := s.grpcServer.Serve(s.lis); err != nil {
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
		log.Error("StreamingNode disconnected from etcd, process will exit", zap.Int64("Server Id", paramtable.GetNodeID()))
		os.Exit(1)
	})
}

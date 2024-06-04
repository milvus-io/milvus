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

package grpclognode

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/lognode/server"
	lognodeserver "github.com/milvus-io/milvus/internal/lognode/server"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	logserviceinterceptor "github.com/milvus-io/milvus/internal/util/logserviceutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Server is the grpc server of lognode.
type Server struct {
	stopOnce       sync.Once
	grpcServerChan chan struct{}

	// session of current server.
	session *sessionutil.Session

	// server
	lognode *lognodeserver.LogNode

	// rpc
	grpcServer *grpc.Server
	lis        net.Listener

	// component client
	etcdCli   *clientv3.Client
	rootCoord types.RootCoordClient

	factory dependency.Factory
}

// NewServer create a new QueryCoord grpc server.
func NewServer(factory dependency.Factory) (*Server, error) {
	// Ctx is redundant control block, all resources is managed by close/stop function.
	return &Server{
		stopOnce:       sync.Once{},
		grpcServerChan: make(chan struct{}),
		etcdCli:        kvfactory.GetEtcd(),
		factory:        factory,
	}, nil
}

// Run runs the server.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Info("lognode init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Info("lognode start done ...")
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
	log.Info("lognode stop", zap.String("Address", addr))

	// Unregister current server from etcd.
	log.Info("lognode unregister session from etcd...")
	if err := s.session.GoingStop(); err != nil {
		log.Warn("lognode unregister session failed", zap.Error(err))
	}

	// Stop grpc server.
	log.Info("lognode stop grpc server...")
	s.grpcServer.GracefulStop()

	// Stop log node service.
	log.Info("lognode stop service...")
	s.lognode.Stop()

	// Stop rootCoord client.
	log.Info("lognode stop rootCoord client...")
	if err := s.rootCoord.Close(); err != nil {
		log.Warn("lognode stop rootCoord client failed", zap.Error(err))
	}

	// Stop etcd.
	log.Info("lognode stop etcd client...")
	if err := s.etcdCli.Close(); err != nil {
		log.Warn("lognode stop etcd client failed", zap.Error(err))
	}

	// Wait for grpc server to stop.
	log.Info("wait for grpc server stop...")
	<-s.grpcServerChan
	log.Info("lognode stop done")
}

// Health check the health status of lognode.
func (s *Server) Health(ctx context.Context) commonpb.StateCode {
	return s.lognode.Health(ctx)
}

func (s *Server) init() (err error) {
	defer func() {
		if err != nil {
			log.Error("Log Node init failed", zap.Error(err))
			return
		}
		log.Info("init log node server finished")
	}()

	if err := s.allocateAddress(); err != nil {
		return err
	}
	if err := s.initSession(); err != nil {
		return err
	}
	if err := s.initRootCoord(); err != nil {
		return err
	}
	s.initGRPCServer()

	// Create log node service.
	s.lognode = server.NewServerBuilder().
		WithETCD(s.etcdCli).
		WithGRPCServer(s.grpcServer).
		WithRootCoordClient(s.rootCoord).
		WithSession(s.session).
		Build()
	if err := s.lognode.Init(context.Background()); err != nil {
		return errors.Wrap(err, "log node service init failed")
	}
	return nil
}

func (s *Server) start() (err error) {
	defer func() {
		if err != nil {
			log.Error("Log Node start failed", zap.Error(err))
			return
		}
		log.Info("start log node server finished")
	}()

	// Start log node service.
	s.lognode.Start()

	// Start grpc server.
	if err := s.startGPRCServer(); err != nil {
		return errors.Wrap(err, "log node start gRPC server fail")
	}

	// Register current server to etcd.
	s.registerSessionToETCD()
	return nil
}

func (s *Server) initSession() error {
	s.session = sessionutil.NewSession(context.Background())
	if s.session == nil {
		return errors.New("session is nil, the etcd client connection may have failed")
	}
	addr, err := s.getAddress()
	if err != nil {
		return err
	}
	s.session.Init(typeutil.LogNodeRole, addr, false, true)
	paramtable.SetNodeID(s.session.ServerID)
	sessionutil.SaveServerInfo(typeutil.DataNodeRole, s.session.ServerID)
	log.Info("LogNode init session", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("node address", addr))
	return nil
}

func (s *Server) initRootCoord() (err error) {
	log.Info("log node connect to rootCoord...")
	s.rootCoord, err = rcc.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "log node try to new RootCoord client failed")
	}

	log.Info("Log Node try to wait for RootCoord ready")
	err = componentutil.WaitForComponentHealthy(context.Background(), s.rootCoord, "RootCoord", 1000000, time.Millisecond*200)
	if err != nil {
		return errors.Wrap(err, "log node wait for RootCoord ready failed")
	}
	return nil
}

func (s *Server) initGRPCServer() {
	log.Info("create log node server...")
	cfg := &paramtable.Get().LogNodeGrpcServerCfg
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
			logserviceinterceptor.NewLogServiceUnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			otelgrpc.StreamServerInterceptor(opts...),
			logutil.StreamTraceLoggerInterceptor,
			interceptor.ClusterValidationStreamServerInterceptor(),
			interceptor.ServerIDValidationStreamServerInterceptor(serverIDGetter),
			logserviceinterceptor.NewLogServiceStreamServerInterceptor(),
		)))
}

// allocateAddress allocates a available address for lognode grpc server.
func (s *Server) allocateAddress() (err error) {
	port := paramtable.Get().LogNodeGrpcServerCfg.Port.GetAsInt()

	retry.Do(context.Background(), func() error {
		addr := ":" + strconv.Itoa(port)
		s.lis, err = net.Listen("tcp", addr)
		if err != nil {
			if port != 0 {
				// set port=0 to get next available port by os
				log.Warn("log node suggested port is in used, try to get by os", zap.Error(err))
				port = 0
			}
		}
		return err
	}, retry.Attempts(10))
	return err
}

// getAddress returns the address of lognode grpc server.
// must be called after allocateAddress.
func (s *Server) getAddress() (string, error) {
	if s.lis == nil {
		return "", errors.New("log node grpc server is not initialized")
	}
	ip := paramtable.Get().LogNodeGrpcServerCfg.IP
	return fmt.Sprintf("%s:%d", ip, s.lis.Addr().(*net.TCPAddr).Port), nil
}

// startGRPCServer starts the grpc server.
func (s *Server) startGPRCServer() error {
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
	funcutil.CheckGrpcReady(context.Background(), errCh)
	return <-errCh
}

// registerSessionToETCD registers current server to etcd.
func (s *Server) registerSessionToETCD() {
	s.session.Register()
	// start liveness check
	s.session.LivenessCheck(context.Background(), func() {
		log.Error("LogNode disconnected from etcd, process will exit", zap.Int64("LogNode Id", paramtable.GetNodeID()))
		if err := s.Stop(); err != nil {
			log.Warn("failed to stop server", zap.Error(err))
		}

		// manually send signal to starter goroutine
		if s.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})
}

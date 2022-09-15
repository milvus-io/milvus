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
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	ot "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	qn "github.com/milvus-io/milvus/internal/querynode"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var Params paramtable.GrpcServerConfig

// UniqueID is an alias for type typeutil.UniqueID, used as a unique identifier for the request.
type UniqueID = typeutil.UniqueID

// Server is the grpc server of QueryNode.
type Server struct {
	querynode   types.QueryNodeComponent
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	grpcErrChan chan error

	grpcServer *grpc.Server

	etcdCli *clientv3.Client

	closer io.Closer
}

func (s *Server) GetStatistics(ctx context.Context, request *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	return s.querynode.GetStatistics(ctx, request)
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

// init initializes QueryNode's grpc service.
func (s *Server) init() error {
	Params.InitOnce(typeutil.QueryNodeRole)

	if !funcutil.CheckPortAvailable(Params.Port) {
		Params.Port = funcutil.GetAvailablePort()
		log.Warn("QueryNode get available port when init", zap.Int("Port", Params.Port))
	}

	qn.Params.InitOnce()
	qn.Params.QueryNodeCfg.QueryNodeIP = Params.IP
	qn.Params.QueryNodeCfg.QueryNodePort = int64(Params.Port)
	//qn.Params.QueryNodeID = Params.QueryNodeID

	closer := trace.InitTracing(fmt.Sprintf("query_node ip: %s, port: %d", Params.IP, Params.Port))
	s.closer = closer

	log.Debug("QueryNode", zap.Int("port", Params.Port))

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		log.Debug("QueryNode connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.SetEtcdClient(etcdCli)
	log.Debug("QueryNode connect to etcd successfully")
	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		return err
	}

	s.querynode.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("QueryNode", zap.Any("State", internalpb.StateCode_Initializing))
	if err := s.querynode.Init(); err != nil {
		log.Error("QueryNode init error: ", zap.Error(err))
		return err
	}

	return nil
}

// start starts QueryNode's grpc service.
func (s *Server) start() error {
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
func (s *Server) startGrpcLoop(grpcPort int) {
	defer s.wg.Done()
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}
	var lis net.Listener
	var err error
	err = retry.Do(s.ctx, func() error {
		addr := ":" + strconv.Itoa(grpcPort)
		lis, err = net.Listen("tcp", addr)
		if err == nil {
			qn.Params.QueryNodeCfg.QueryNodePort = int64(lis.Addr().(*net.TCPAddr).Port)
		} else {
			// set port=0 to get next available port
			grpcPort = 0
		}
		return err
	}, retry.Attempts(10))
	if err != nil {
		log.Error("QueryNode GrpcServer:failed to listen", zap.Error(err))
		s.grpcErrChan <- err
		return
	}

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			ot.UnaryServerInterceptor(opts...),
			logutil.UnaryTraceLoggerInterceptor)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			ot.StreamServerInterceptor(opts...),
			logutil.StreamTraceLoggerInterceptor)))
	querypb.RegisterQueryNodeServer(s.grpcServer, s)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Debug("QueryNode Start Grpc Failed!!!!")
		s.grpcErrChan <- err
	}

}

// Run initializes and starts QueryNode's grpc service.
func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("QueryNode init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("QueryNode start done ...")
	return nil
}

// Stop stops QueryNode's grpc service.
func (s *Server) Stop() error {
	log.Debug("QueryNode stop", zap.String("Address", Params.GetAddress()))
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}

	s.cancel()
	if s.grpcServer != nil {
		log.Debug("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}

	err := s.querynode.Stop()
	if err != nil {
		return err
	}
	s.wg.Wait()
	return nil
}

// SetEtcdClient sets the etcd client for QueryNode component.
func (s *Server) SetEtcdClient(etcdCli *clientv3.Client) {
	s.querynode.SetEtcdClient(etcdCli)
}

// GetTimeTickChannel gets the time tick channel of QueryNode.
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.querynode.GetTimeTickChannel(ctx)
}

// GetStatisticsChannel gets the statistics channel of QueryNode.
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.querynode.GetStatisticsChannel(ctx)
}

// GetComponentStates gets the component states of QueryNode.
func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	// ignore ctx and in
	return s.querynode.GetComponentStates(ctx)
}

// WatchDmChannels watches the channels about data manipulation.
func (s *Server) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.WatchDmChannels(ctx, req)
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

// Query performs query of streaming/historical replica on QueryNode.
func (s *Server) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return s.querynode.Query(ctx, req)
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

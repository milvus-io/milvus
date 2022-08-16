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

// Package grpcdatacoord implements grpc server for datacoord
package grpcdatacoord

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Params is the parameters for DataCoord grpc server
var Params paramtable.GrpcServerConfig

// Server is the grpc server of datacoord
type Server struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg        sync.WaitGroup
	dataCoord types.DataCoordComponent

	etcdCli *clientv3.Client

	grpcErrChan chan error
	grpcServer  *grpc.Server
	closer      io.Closer
}

// NewServer new data service grpc server
func NewServer(ctx context.Context, factory dependency.Factory, opts ...datacoord.Option) *Server {
	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	s.dataCoord = datacoord.CreateServer(s.ctx, factory, opts...)
	return s
}

func (s *Server) init() error {
	Params.InitOnce(typeutil.DataCoordRole)

	closer := trace.InitTracing("DataCoord", &Params.BaseTable, fmt.Sprintf("%s:%d", Params.IP, Params.Port))
	s.closer = closer

	datacoord.Params.InitOnce()
	datacoord.Params.DataCoordCfg.IP = Params.IP
	datacoord.Params.DataCoordCfg.Port = Params.Port
	datacoord.Params.DataCoordCfg.Address = Params.GetAddress()

	etcdCli, err := etcd.GetEtcdClient(&datacoord.Params.EtcdCfg)
	if err != nil {
		log.Debug("DataCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.dataCoord.SetEtcdClient(etcdCli)

	err = s.startGrpc()
	if err != nil {
		log.Debug("DataCoord startGrpc failed", zap.Error(err))
		return err
	}

	if err := s.dataCoord.Init(); err != nil {
		log.Error("dataCoord init error", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) startGrpc() error {
	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop(grpcPort int) {
	defer logutil.LogPanic()
	defer s.wg.Done()

	log.Debug("network port", zap.Int("port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Error("grpc server failed to listen error", zap.Error(err))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			trace.UnaryServerInterceptor(),
			logutil.UnaryTraceLoggerInterceptor)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			trace.StreamServerInterceptor(),
			logutil.StreamTraceLoggerInterceptor)))
	datapb.RegisterDataCoordServer(s.grpcServer, s)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	err := s.dataCoord.Start()
	if err != nil {
		log.Error("DataCoord start failed", zap.Error(err))
		return err
	}
	err = s.dataCoord.Register()
	if err != nil {
		log.Debug("DataCoord register service failed", zap.Error(err))
		return err
	}
	return nil
}

// Stop stops the DataCoord server gracefully.
// Need to call the GracefulStop interface of grpc server and call the stop method of the inner DataCoord object.
func (s *Server) Stop() error {
	log.Debug("Datacoord stop", zap.String("Address", Params.GetAddress()))
	var err error
	if s.closer != nil {
		if err = s.closer.Close(); err != nil {
			return err
		}
	}
	s.cancel()

	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	if s.grpcServer != nil {
		log.Debug("Graceful stop grpc server...")
		s.grpcServer.GracefulStop()
	}

	err = s.dataCoord.Stop()
	if err != nil {
		return err
	}

	s.wg.Wait()

	return nil
}

// Run starts the Server. Need to call inner init and start method.
func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	log.Debug("DataCoord init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("DataCoord start done ...")
	return nil
}

// GetComponentStates gets states of datacoord and datanodes
func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.dataCoord.GetComponentStates(ctx)
}

// GetTimeTickChannel gets timetick channel
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetTimeTickChannel(ctx)
}

// GetStatisticsChannel gets statistics channel
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetStatisticsChannel(ctx)
}

// GetSegmentInfo gets segment information according to segment id
func (s *Server) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return s.dataCoord.GetSegmentInfo(ctx, req)
}

// Flush flushes a collection's data
func (s *Server) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	return s.dataCoord.Flush(ctx, req)
}

// AssignSegmentID requests to allocate segment space for insert
func (s *Server) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	return s.dataCoord.AssignSegmentID(ctx, req)
}

// GetSegmentStates gets states of segments
func (s *Server) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	return s.dataCoord.GetSegmentStates(ctx, req)
}

// GetInsertBinlogPaths gets insert binlog paths of a segment
func (s *Server) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	return s.dataCoord.GetInsertBinlogPaths(ctx, req)
}

// GetCollectionStatistics gets statistics of a collection
func (s *Server) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	return s.dataCoord.GetCollectionStatistics(ctx, req)
}

// GetPartitionStatistics gets statistics of a partition
func (s *Server) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	return s.dataCoord.GetPartitionStatistics(ctx, req)
}

// GetSegmentInfoChannel gets channel to which datacoord sends segment information
func (s *Server) GetSegmentInfoChannel(ctx context.Context, req *datapb.GetSegmentInfoChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetSegmentInfoChannel(ctx)
}

// SaveBinlogPaths implement DataCoordServer, saves segment, collection binlog according to datanode request
func (s *Server) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	return s.dataCoord.SaveBinlogPaths(ctx, req)
}

// GetRecoveryInfo gets information for recovering channels
func (s *Server) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	return s.dataCoord.GetRecoveryInfo(ctx, req)
}

// GetFlushedSegments get all flushed segments of a partition
func (s *Server) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	return s.dataCoord.GetFlushedSegments(ctx, req)
}

// ShowConfigurations gets specified configurations para of DataCoord
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return s.dataCoord.ShowConfigurations(ctx, req)
}

// GetMetrics gets metrics of data coordinator and datanodes
func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.dataCoord.GetMetrics(ctx, req)
}

// ManualCompaction triggers a compaction for a collection
func (s *Server) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return s.dataCoord.ManualCompaction(ctx, req)
}

// GetCompactionState gets the state of a compaction
func (s *Server) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return s.dataCoord.GetCompactionState(ctx, req)
}

// GetCompactionStateWithPlans gets the state of a compaction by plan
func (s *Server) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return s.dataCoord.GetCompactionStateWithPlans(ctx, req)
}

// WatchChannels starts watch channels by give request
func (s *Server) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	return s.dataCoord.WatchChannels(ctx, req)
}

// GetFlushState gets the flush state of multiple segments
func (s *Server) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return s.dataCoord.GetFlushState(ctx, req)
}

// DropVirtualChannel drop virtual channel in datacoord
func (s *Server) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	return s.dataCoord.DropVirtualChannel(ctx, req)
}

// SetSegmentState sets the state of a segment.
func (s *Server) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	return s.dataCoord.SetSegmentState(ctx, req)
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (s *Server) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	return s.dataCoord.Import(ctx, req)
}

// UpdateSegmentStatistics is the dataCoord service caller of UpdateSegmentStatistics.
func (s *Server) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	return s.dataCoord.UpdateSegmentStatistics(ctx, req)
}

// AcquireSegmentLock acquire the reference lock of the segments.
func (s *Server) AcquireSegmentLock(ctx context.Context, req *datapb.AcquireSegmentLockRequest) (*commonpb.Status, error) {
	return s.dataCoord.AcquireSegmentLock(ctx, req)
}

// ReleaseSegmentLock release the reference lock of the segments.
func (s *Server) ReleaseSegmentLock(ctx context.Context, req *datapb.ReleaseSegmentLockRequest) (*commonpb.Status, error) {
	return s.dataCoord.ReleaseSegmentLock(ctx, req)
}

func (s *Server) AddSegment(ctx context.Context, request *datapb.AddSegmentRequest) (*commonpb.Status, error) {
	return s.dataCoord.AddSegment(ctx, request)
}

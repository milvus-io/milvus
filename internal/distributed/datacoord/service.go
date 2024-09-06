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
	"net"
	"strconv"
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
	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	_ "github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tikv"
)

// Server is the grpc server of datacoord
type Server struct {
	ctx    context.Context
	cancel context.CancelFunc

	serverID atomic.Int64

	grpcWG    sync.WaitGroup
	dataCoord types.DataCoordComponent

	etcdCli *clientv3.Client
	tikvCli *txnkv.Client

	grpcErrChan chan error
	grpcServer  *grpc.Server
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

var getTiKVClient = tikv.GetTiKVClient

func (s *Server) init() error {
	params := paramtable.Get()
	etcdConfig := &params.EtcdCfg

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
		log.Debug("DataCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.dataCoord.SetEtcdClient(etcdCli)
	s.dataCoord.SetAddress(params.DataCoordGrpcServerCfg.GetAddress())

	if params.MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
		log.Info("Connecting to tikv metadata storage.")
		tikvCli, err := getTiKVClient(&paramtable.Get().TiKVCfg)
		if err != nil {
			log.Warn("DataCoord failed to connect to tikv", zap.Error(err))
			return err
		}
		s.dataCoord.SetTiKVClient(tikvCli)
		log.Info("Connected to tikv. Using tikv as metadata storage.")
	}

	if err := s.dataCoord.Init(); err != nil {
		log.Error("dataCoord init error", zap.Error(err))
		return err
	}

	err = s.startGrpc()
	if err != nil {
		log.Debug("DataCoord startGrpc failed", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) startGrpc() error {
	Params := &paramtable.Get().DataCoordGrpcServerCfg
	s.grpcWG.Add(1)
	go s.startGrpcLoop(Params.Port.GetAsInt())
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop(grpcPort int) {
	defer logutil.LogPanic()
	defer s.grpcWG.Done()

	Params := &paramtable.Get().DataCoordGrpcServerCfg
	log.Debug("network port", zap.Int("port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Error("grpc server failed to listen error", zap.Error(err))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	kasp := keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			logutil.UnaryTraceLoggerInterceptor,
			interceptor.ClusterValidationUnaryServerInterceptor(),
			interceptor.ServerIDValidationUnaryServerInterceptor(func() int64 {
				if s.serverID.Load() == 0 {
					s.serverID.Store(s.dataCoord.(*datacoord.Server).GetServerID())
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
					s.serverID.Store(s.dataCoord.(*datacoord.Server).GetServerID())
				}
				return s.serverID.Load()
			}),
			streamingserviceinterceptor.NewStreamingServiceStreamServerInterceptor(),
		)),
		grpc.StatsHandler(tracer.GetDynamicOtelGrpcServerStatsHandler()))
	indexpb.RegisterIndexCoordServer(s.grpcServer, s)
	datapb.RegisterDataCoordServer(s.grpcServer, s)
	// register the streaming coord grpc service.
	if streamingutil.IsStreamingServiceEnabled() {
		s.dataCoord.RegisterStreamingCoordGRPCService(s.grpcServer)
	}
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	err := s.dataCoord.Register()
	if err != nil {
		log.Debug("DataCoord register service failed", zap.Error(err))
		return err
	}

	err = s.dataCoord.Start()
	if err != nil {
		log.Error("DataCoord start failed", zap.Error(err))
		return err
	}
	return nil
}

// Stop stops the DataCoord server gracefully.
// Need to call the GracefulStop interface of grpc server and call the stop method of the inner DataCoord object.
func (s *Server) Stop() (err error) {
	Params := &paramtable.Get().DataCoordGrpcServerCfg
	logger := log.With(zap.String("address", Params.GetAddress()))
	logger.Info("Datacoord stopping")
	defer func() {
		logger.Info("Datacoord stopped", zap.Error(err))
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

	logger.Info("internal server[dataCoord] start to stop")
	err = s.dataCoord.Stop()
	if err != nil {
		log.Error("failed to close dataCoord", zap.Error(err))
		return err
	}

	s.cancel()
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
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.dataCoord.GetComponentStates(ctx, req)
}

// GetTimeTickChannel gets timetick channel
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetTimeTickChannel(ctx, req)
}

// GetStatisticsChannel gets statistics channel
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.dataCoord.GetStatisticsChannel(ctx, req)
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

// AllocSegment alloc a new growing segment, add it into segment meta.
func (s *Server) AllocSegment(ctx context.Context, req *datapb.AllocSegmentRequest) (*datapb.AllocSegmentResponse, error) {
	return s.dataCoord.AllocSegment(ctx, req)
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
	return s.dataCoord.GetSegmentInfoChannel(ctx, req)
}

// SaveBinlogPaths implement DataCoordServer, saves segment, collection binlog according to datanode request
func (s *Server) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	return s.dataCoord.SaveBinlogPaths(ctx, req)
}

// GetRecoveryInfo gets information for recovering channels
func (s *Server) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	return s.dataCoord.GetRecoveryInfo(ctx, req)
}

// GetRecoveryInfoV2 gets information for recovering channels
func (s *Server) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2) (*datapb.GetRecoveryInfoResponseV2, error) {
	return s.dataCoord.GetRecoveryInfoV2(ctx, req)
}

// GetChannelRecoveryInfo gets the corresponding vchannel info.
func (s *Server) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest) (*datapb.GetChannelRecoveryInfoResponse, error) {
	return s.dataCoord.GetChannelRecoveryInfo(ctx, req)
}

// GetFlushedSegments get all flushed segments of a partition
func (s *Server) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	return s.dataCoord.GetFlushedSegments(ctx, req)
}

// GetSegmentsByStates get all segments of a partition by given states
func (s *Server) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error) {
	return s.dataCoord.GetSegmentsByStates(ctx, req)
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

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (s *Server) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return s.dataCoord.GetFlushState(ctx, req)
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (s *Server) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	return s.dataCoord.GetFlushAllState(ctx, req)
}

// DropVirtualChannel drop virtual channel in datacoord
func (s *Server) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	return s.dataCoord.DropVirtualChannel(ctx, req)
}

// SetSegmentState sets the state of a segment.
func (s *Server) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	return s.dataCoord.SetSegmentState(ctx, req)
}

// UpdateSegmentStatistics is the dataCoord service caller of UpdateSegmentStatistics.
func (s *Server) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	return s.dataCoord.UpdateSegmentStatistics(ctx, req)
}

// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
func (s *Server) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	return s.dataCoord.UpdateChannelCheckpoint(ctx, req)
}

// MarkSegmentsDropped is the distributed caller of MarkSegmentsDropped.
func (s *Server) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	return s.dataCoord.MarkSegmentsDropped(ctx, req)
}

func (s *Server) BroadcastAlteredCollection(ctx context.Context, request *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	return s.dataCoord.BroadcastAlteredCollection(ctx, request)
}

func (s *Server) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return s.dataCoord.CheckHealth(ctx, req)
}

func (s *Server) GcConfirm(ctx context.Context, request *datapb.GcConfirmRequest) (*datapb.GcConfirmResponse, error) {
	return s.dataCoord.GcConfirm(ctx, request)
}

// CreateIndex sends the build index request to DataCoord.
func (s *Server) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.dataCoord.CreateIndex(ctx, req)
}

func (s *Server) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest) (*commonpb.Status, error) {
	return s.dataCoord.AlterIndex(ctx, req)
}

// GetIndexState gets the index states from DataCoord.
// Deprecated: use DescribeIndex instead
func (s *Server) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	return s.dataCoord.GetIndexState(ctx, req)
}

func (s *Server) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return s.dataCoord.GetSegmentIndexState(ctx, req)
}

// GetIndexInfos gets the index file paths from DataCoord.
func (s *Server) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	return s.dataCoord.GetIndexInfos(ctx, req)
}

// DescribeIndex gets all indexes of the collection.
func (s *Server) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return s.dataCoord.DescribeIndex(ctx, req)
}

// GetIndexStatistics get the information of index..
func (s *Server) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error) {
	return s.dataCoord.GetIndexStatistics(ctx, req)
}

// DropIndex sends the drop index request to DataCoord.
func (s *Server) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return s.dataCoord.DropIndex(ctx, request)
}

// Deprecated: use DescribeIndex instead
func (s *Server) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	return s.dataCoord.GetIndexBuildProgress(ctx, req)
}

func (s *Server) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	return s.dataCoord.ReportDataNodeTtMsgs(ctx, req)
}

func (s *Server) GcControl(ctx context.Context, req *datapb.GcControlRequest) (*commonpb.Status, error) {
	return s.dataCoord.GcControl(ctx, req)
}

func (s *Server) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal) (*internalpb.ImportResponse, error) {
	return s.dataCoord.ImportV2(ctx, in)
}

func (s *Server) GetImportProgress(ctx context.Context, in *internalpb.GetImportProgressRequest) (*internalpb.GetImportProgressResponse, error) {
	return s.dataCoord.GetImportProgress(ctx, in)
}

func (s *Server) ListImports(ctx context.Context, in *internalpb.ListImportsRequestInternal) (*internalpb.ListImportsResponse, error) {
	return s.dataCoord.ListImports(ctx, in)
}

func (s *Server) ListIndexes(ctx context.Context, in *indexpb.ListIndexesRequest) (*indexpb.ListIndexesResponse, error) {
	return s.dataCoord.ListIndexes(ctx, in)
}

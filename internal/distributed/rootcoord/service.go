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
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	ot "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	pnc "github.com/milvus-io/milvus/internal/distributed/proxy/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	icc "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"
	qcc "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
)

var Params paramtable.GrpcServerConfig

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
	indexCoord types.IndexCoord
	queryCoord types.QueryCoord

	newIndexCoordClient func(string, *clientv3.Client) types.IndexCoord
	newDataCoordClient  func(string, *clientv3.Client) types.DataCoord
	newQueryCoordClient func(string, *clientv3.Client) types.QueryCoord

	closer io.Closer
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
func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
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
	s.newIndexCoordClient = func(metaRootPath string, etcdCli *clientv3.Client) types.IndexCoord {
		isClient, err := icc.NewClient(s.ctx, metaRootPath, etcdCli)
		if err != nil {
			panic(err)
		}
		return isClient
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
	Params.InitOnce(typeutil.RootCoordRole)

	rootcoord.Params.InitOnce()
	rootcoord.Params.RootCoordCfg.Address = Params.GetAddress()
	rootcoord.Params.RootCoordCfg.Port = Params.Port
	log.Debug("init params done..")

	closer := trace.InitTracing("root_coord")
	s.closer = closer

	etcdCli, err := etcd.GetEtcdClient(&Params.BaseParamTable)
	if err != nil {
		log.Debug("RootCoord connect to etcd failed", zap.Error(err))
		return err
	}
	s.etcdCli = etcdCli
	s.rootCoord.SetEtcdClient(s.etcdCli)
	log.Debug("etcd connect done ...")

	err = s.startGrpc(Params.Port)
	if err != nil {
		return err
	}
	log.Debug("grpc init done ...")

	s.rootCoord.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("RootCoord", zap.Any("State", internalpb.StateCode_Initializing))
	s.rootCoord.SetNewProxyClient(
		func(se *sessionutil.Session) (types.Proxy, error) {
			cli, err := pnc.NewClient(s.ctx, se.Address)
			if err != nil {
				return nil, err
			}
			if err := cli.Init(); err != nil {
				return nil, err
			}
			if err := cli.Start(); err != nil {
				return nil, err
			}
			return cli, nil
		},
	)

	if s.newDataCoordClient != nil {
		log.Debug("RootCoord start to create DataCoord client")
		dataCoord := s.newDataCoordClient(rootcoord.Params.BaseParams.MetaRootPath, s.etcdCli)
		if err := s.rootCoord.SetDataCoord(s.ctx, dataCoord); err != nil {
			panic(err)
		}
		s.dataCoord = dataCoord
	}
	if s.newIndexCoordClient != nil {
		log.Debug("RootCoord start to create IndexCoord client")
		indexCoord := s.newIndexCoordClient(rootcoord.Params.BaseParams.MetaRootPath, s.etcdCli)
		if err := s.rootCoord.SetIndexCoord(indexCoord); err != nil {
			panic(err)
		}
		s.indexCoord = indexCoord
	}
	if s.newQueryCoordClient != nil {
		log.Debug("RootCoord start to create QueryCoord client")
		queryCoord := s.newQueryCoordClient(rootcoord.Params.BaseParams.MetaRootPath, s.etcdCli)
		if err := s.rootCoord.SetQueryCoord(queryCoord); err != nil {
			panic(err)
		}
		s.queryCoord = queryCoord
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

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(ot.UnaryServerInterceptor(opts...)),
		grpc.StreamInterceptor(ot.StreamServerInterceptor(opts...)))
	rootcoordpb.RegisterRootCoordServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	log.Debug("RootCoord Core start ...")
	if err := s.rootCoord.Start(); err != nil {
		log.Error(err.Error())
		return err
	}
	if err := s.rootCoord.Register(); err != nil {
		log.Error("RootCoord registers service failed", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) Stop() error {
	log.Debug("Rootcoord stop", zap.String("Address", Params.GetAddress()))
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			log.Error("Failed to close opentracing", zap.Error(err))
		}
	}
	if s.etcdCli != nil {
		defer s.etcdCli.Close()
	}
	if s.indexCoord != nil {
		if err := s.indexCoord.Stop(); err != nil {
			log.Error("Failed to close indexCoord client", zap.Error(err))
		}
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
func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
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

// CreateIndex index builder service
func (s *Server) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreateIndex(ctx, in)
}

// DropIndex drops the index.
func (s *Server) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropIndex(ctx, in)
}

// DescribeIndex get the index information for the specified index name.
func (s *Server) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.rootCoord.DescribeIndex(ctx, in)
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

// DescribeSegment gets meta info of the segment
func (s *Server) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return s.rootCoord.DescribeSegment(ctx, in)
}

// ShowSegments gets all segments
func (s *Server) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	return s.rootCoord.ShowSegments(ctx, in)
}

// ReleaseDQLMessageStream notifies RootCoord to release and close the search message stream of specific collection.
func (s *Server) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	return s.rootCoord.ReleaseDQLMessageStream(ctx, in)
}

// SegmentFlushCompleted notifies RootCoord that specified segment has been flushed.
func (s *Server) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	return s.rootCoord.SegmentFlushCompleted(ctx, in)
}

// GetMetrics gets the metrics of RootCoord.
func (s *Server) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.rootCoord.GetMetrics(ctx, in)
}

// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package grpcmasterservice

import (
	"context"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	dsc "github.com/milvus-io/milvus/internal/distributed/dataservice/client"
	isc "github.com/milvus-io/milvus/internal/distributed/indexservice/client"
	pnc "github.com/milvus-io/milvus/internal/distributed/proxynode/client"
	qsc "github.com/milvus-io/milvus/internal/distributed/queryservice/client"
	"github.com/milvus-io/milvus/internal/log"
	cms "github.com/milvus-io/milvus/internal/masterservice"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/trace"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

// Server grpc wrapper
type Server struct {
	rootCoord   types.MasterComponent
	grpcServer  *grpc.Server
	grpcErrChan chan error

	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	dataService  types.DataService
	indexService types.IndexService
	queryService types.QueryService

	newIndexCoordClient func(string, []string, time.Duration) types.IndexService
	newDataCoordClient  func(string, []string, time.Duration) types.DataService
	newQueryCoordClient func(string, []string, time.Duration) types.QueryService

	closer io.Closer
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	s.setClient()
	var err error
	s.rootCoord, err = cms.NewCore(s.ctx, factory)
	if err != nil {
		return nil, err
	}
	return s, err
}

func (s *Server) setClient() {
	ctx := context.Background()

	s.newDataCoordClient = func(etcdMetaRoot string, etcdEndpoints []string, timeout time.Duration) types.DataService {
		dsClient := dsc.NewClient(etcdMetaRoot, etcdEndpoints, timeout)
		if err := dsClient.Init(); err != nil {
			panic(err)
		}
		if err := dsClient.Start(); err != nil {
			panic(err)
		}
		if err := funcutil.WaitForComponentInitOrHealthy(ctx, dsClient, "DataService", 1000000, 200*time.Millisecond); err != nil {
			panic(err)
		}
		return dsClient
	}
	s.newIndexCoordClient = func(metaRootPath string, etcdEndpoints []string, timeout time.Duration) types.IndexService {
		isClient := isc.NewClient(metaRootPath, etcdEndpoints, timeout)
		if err := isClient.Init(); err != nil {
			panic(err)
		}
		if err := isClient.Start(); err != nil {
			panic(err)
		}
		return isClient
	}
	s.newQueryCoordClient = func(metaRootPath string, etcdEndpoints []string, timeout time.Duration) types.QueryService {
		qsClient, err := qsc.NewClient(metaRootPath, etcdEndpoints, timeout)
		if err != nil {
			panic(err)
		}
		if err := qsClient.Init(); err != nil {
			panic(err)
		}
		if err := qsClient.Start(); err != nil {
			panic(err)
		}
		return qsClient
	}
}

func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	Params.Init()

	cms.Params.Init()
	cms.Params.Address = Params.Address
	cms.Params.Port = Params.Port
	log.Debug("grpc init done ...")

	ctx := context.Background()

	closer := trace.InitTracing("root_coord")
	s.closer = closer

	log.Debug("init params done")

	err := s.rootCoord.Register()
	if err != nil {
		return err
	}

	err = s.startGrpc()
	if err != nil {
		return err
	}

	s.rootCoord.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("MasterService", zap.Any("State", internalpb.StateCode_Initializing))
	s.rootCoord.SetNewProxyClient(
		func(s *sessionutil.Session) (types.ProxyNode, error) {
			cli := pnc.NewClient(s.Address, 3*time.Second)
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
		log.Debug("MasterService start to create DataService client")
		dataService := s.newDataCoordClient(cms.Params.MetaRootPath, cms.Params.EtcdEndpoints, 3*time.Second)
		if err := s.rootCoord.SetDataCoord(ctx, dataService); err != nil {
			panic(err)
		}
		s.dataService = dataService
	}
	if s.newIndexCoordClient != nil {
		log.Debug("MasterService start to create IndexService client")
		indexService := s.newIndexCoordClient(cms.Params.MetaRootPath, cms.Params.EtcdEndpoints, 3*time.Second)
		if err := s.rootCoord.SetIndexCoord(indexService); err != nil {
			panic(err)
		}
		s.indexService = indexService
	}
	if s.newQueryCoordClient != nil {
		log.Debug("MasterService start to create QueryService client")
		queryService := s.newQueryCoordClient(cms.Params.MetaRootPath, cms.Params.EtcdEndpoints, 3*time.Second)
		if err := s.rootCoord.SetQueryCoord(queryService); err != nil {
			panic(err)
		}
		s.queryService = queryService
	}

	return s.rootCoord.Init()
}

func (s *Server) startGrpc() error {
	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()

	log.Debug("start grpc ", zap.Int("port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Error("GrpcServer:failed to listen", zap.String("error", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(
			grpc_opentracing.UnaryServerInterceptor(opts...)),
		grpc.StreamInterceptor(
			grpc_opentracing.StreamServerInterceptor(opts...)))
	masterpb.RegisterMasterServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) start() error {
	log.Debug("Master Core start ...")
	if err := s.rootCoord.Start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() error {
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			log.Error("close opentracing", zap.Error(err))
		}
	}
	if s.indexService != nil {
		if err := s.indexService.Stop(); err != nil {
			log.Debug("close indexService client", zap.Error(err))
		}
	}
	if s.dataService != nil {
		if err := s.dataService.Stop(); err != nil {
			log.Debug("close dataService client", zap.Error(err))
		}
	}
	if s.queryService != nil {
		if err := s.queryService.Stop(); err != nil {
			log.Debug("close queryService client", zap.Error(err))
		}
	}
	if s.rootCoord != nil {
		if err := s.rootCoord.Stop(); err != nil {
			log.Debug("close rootCoord", zap.Error(err))
		}
	}
	s.cancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.wg.Wait()
	return nil
}

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

//DDL request
func (s *Server) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreateCollection(ctx, in)
}

func (s *Server) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropCollection(ctx, in)
}

func (s *Server) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.rootCoord.HasCollection(ctx, in)
}

func (s *Server) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.rootCoord.DescribeCollection(ctx, in)
}

func (s *Server) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return s.rootCoord.ShowCollections(ctx, in)
}

func (s *Server) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreatePartition(ctx, in)
}

func (s *Server) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropPartition(ctx, in)
}

func (s *Server) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.rootCoord.HasPartition(ctx, in)
}

func (s *Server) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.rootCoord.ShowPartitions(ctx, in)
}

// CreateIndex index builder service
func (s *Server) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.rootCoord.CreateIndex(ctx, in)
}

func (s *Server) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return s.rootCoord.DropIndex(ctx, in)
}

func (s *Server) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.rootCoord.DescribeIndex(ctx, in)
}

// AllocTimestamp global timestamp allocator
func (s *Server) AllocTimestamp(ctx context.Context, in *masterpb.AllocTimestampRequest) (*masterpb.AllocTimestampResponse, error) {
	return s.rootCoord.AllocTimestamp(ctx, in)
}

func (s *Server) AllocID(ctx context.Context, in *masterpb.AllocIDRequest) (*masterpb.AllocIDResponse, error) {
	return s.rootCoord.AllocID(ctx, in)
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (s *Server) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	return s.rootCoord.UpdateChannelTimeTick(ctx, in)
}

func (s *Server) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return s.rootCoord.DescribeSegment(ctx, in)
}

func (s *Server) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	return s.rootCoord.ShowSegments(ctx, in)
}
func (s *Server) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	return s.rootCoord.ReleaseDQLMessageStream(ctx, in)
}

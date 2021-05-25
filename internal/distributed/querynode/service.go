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

package grpcquerynode

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/retry"

	"github.com/milvus-io/milvus/internal/types"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	dsc "github.com/milvus-io/milvus/internal/distributed/dataservice/client"
	isc "github.com/milvus-io/milvus/internal/distributed/indexservice/client"
	msc "github.com/milvus-io/milvus/internal/distributed/masterservice/client"
	qsc "github.com/milvus-io/milvus/internal/distributed/queryservice/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	qn "github.com/milvus-io/milvus/internal/querynode"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type Server struct {
	querynode   *qn.QueryNode
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	grpcErrChan chan error

	grpcServer *grpc.Server

	dataService   *dsc.Client
	masterService *msc.GrpcClient
	indexService  *isc.Client
	queryService  *qsc.Client

	closer io.Closer
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		querynode:   qn.NewQueryNodeWithoutID(ctx, factory),
		grpcErrChan: make(chan error),
	}
	return s, nil
}

func (s *Server) init() error {
	ctx := context.Background()
	Params.Init()
	Params.LoadFromEnv()
	Params.LoadFromArgs()

	qn.Params.Init()
	qn.Params.QueryNodeIP = Params.QueryNodeIP
	qn.Params.QueryNodePort = int64(Params.QueryNodePort)
	qn.Params.QueryNodeID = Params.QueryNodeID

	closer := trace.InitTracing(fmt.Sprintf("query_node ip: %s, port: %d", Params.QueryNodeIP, Params.QueryNodePort))
	s.closer = closer

	if err := s.querynode.Register(); err != nil {
		return err
	}

	log.Debug("QueryNode", zap.Int("port", Params.QueryNodePort))
	s.wg.Add(1)
	go s.startGrpcLoop(Params.QueryNodePort)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	if err != nil {
		return err
	}
	// --- QueryService ---
	log.Debug("QueryService", zap.String("address", Params.QueryServiceAddress))
	log.Debug("Init Query service client ...")
	queryService, err := qsc.NewClient(Params.QueryServiceAddress, 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = queryService.Init(); err != nil {
		panic(err)
	}

	if err = queryService.Start(); err != nil {
		panic(err)
	}

	err = funcutil.WaitForComponentInitOrHealthy(ctx, queryService, "QueryService", 1000000, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetQueryService(queryService); err != nil {
		panic(err)
	}

	// --- Master Server Client ---
	//ms.Params.Init()
	addr := Params.MasterAddress
	log.Debug("Master service", zap.String("address", addr))
	log.Debug("Init master service client ...")

	masterService, err := msc.NewClient(addr, []string{qn.Params.EtcdAddress}, 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = masterService.Init(); err != nil {
		panic(err)
	}

	if err = masterService.Start(); err != nil {
		panic(err)
	}

	err = funcutil.WaitForComponentHealthy(ctx, masterService, "MasterService", 1000000, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetMasterService(masterService); err != nil {
		panic(err)
	}

	// --- IndexService ---
	log.Debug("Index service", zap.String("address", Params.IndexServiceAddress))
	indexService := isc.NewClient(Params.IndexServiceAddress)

	if err := indexService.Init(); err != nil {
		panic(err)
	}

	if err := indexService.Start(); err != nil {
		panic(err)
	}
	// wait indexservice healthy
	err = funcutil.WaitForComponentHealthy(ctx, indexService, "IndexService", 1000000, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetIndexService(indexService); err != nil {
		panic(err)
	}

	// --- DataService ---
	log.Debug("Data service", zap.String("address", Params.DataServiceAddress))
	log.Debug("QueryNode Init data service client ...")

	dataService := dsc.NewClient(Params.DataServiceAddress)
	if err = dataService.Init(); err != nil {
		panic(err)
	}
	if err = dataService.Start(); err != nil {
		panic(err)
	}
	err = funcutil.WaitForComponentInitOrHealthy(ctx, dataService, "DataService", 1000000, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetDataService(dataService); err != nil {
		panic(err)
	}

	s.querynode.UpdateStateCode(internalpb.StateCode_Initializing)

	if err := s.querynode.Init(); err != nil {
		log.Error("querynode init error: ", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) start() error {
	return s.querynode.Start()
}

func (s *Server) startGrpcLoop(grpcPort int) {
	defer s.wg.Done()

	var lis net.Listener
	var err error
	err = retry.Retry(10, 0, func() error {
		addr := ":" + strconv.Itoa(grpcPort)
		lis, err = net.Listen("tcp", addr)
		if err == nil {
			qn.Params.QueryNodePort = int64(lis.Addr().(*net.TCPAddr).Port)
		} else {
			// set port=0 to get next available port
			grpcPort = 0
		}
		return err
	})
	if err != nil {
		log.Error("QueryNode GrpcServer:failed to listen", zap.Error(err))
		s.grpcErrChan <- err
		return
	}

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)))
	querypb.RegisterQueryNodeServer(s.grpcServer, s)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Debug("QueryNode Start Grpc Failed!!!!")
		s.grpcErrChan <- err
	}

}

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

func (s *Server) Stop() error {
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}

	s.cancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	err := s.querynode.Stop()
	if err != nil {
		return err
	}
	s.wg.Wait()
	return nil
}

func (s *Server) SetMasterService(masterService types.MasterService) error {
	return s.querynode.SetMasterService(masterService)
}

func (s *Server) SetQueryService(queryService types.QueryService) error {
	return s.querynode.SetQueryService(queryService)
}

func (s *Server) SetIndexService(indexService types.IndexService) error {
	return s.querynode.SetIndexService(indexService)
}

func (s *Server) SetDataService(dataService types.DataService) error {
	return s.querynode.SetDataService(dataService)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.querynode.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.querynode.GetStatisticsChannel(ctx)
}

func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	// ignore ctx and in
	return s.querynode.GetComponentStates(ctx)
}

func (s *Server) AddQueryChannel(ctx context.Context, req *querypb.AddQueryChannelRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.AddQueryChannel(ctx, req)
}

func (s *Server) RemoveQueryChannel(ctx context.Context, req *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.RemoveQueryChannel(ctx, req)
}

func (s *Server) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.WatchDmChannels(ctx, req)
}

func (s *Server) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.LoadSegments(ctx, req)
}

func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.ReleaseCollection(ctx, req)
}

func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.ReleasePartitions(ctx, req)
}

func (s *Server) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.querynode.ReleaseSegments(ctx, req)
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return s.querynode.GetSegmentInfo(ctx, req)
}

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
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/retry"

	"github.com/milvus-io/milvus/internal/types"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	dsc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	isc "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
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

	dataCoord  *dsc.Client
	rootCoord  *rcc.GrpcClient
	indexCoord *isc.Client

	closer io.Closer
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		querynode:   qn.NewQueryNode(ctx, factory),
		grpcErrChan: make(chan error),
	}
	return s, nil
}

func (s *Server) init() error {
	Params.Init()

	qn.Params.InitOnce()
	qn.Params.QueryNodeIP = Params.QueryNodeIP
	qn.Params.QueryNodePort = int64(Params.QueryNodePort)
	qn.Params.QueryNodeID = Params.QueryNodeID

	closer := trace.InitTracing(fmt.Sprintf("query_node ip: %s, port: %d", Params.QueryNodeIP, Params.QueryNodePort))
	s.closer = closer

	log.Debug("QueryNode", zap.Int("port", Params.QueryNodePort))
	s.wg.Add(1)
	go s.startGrpcLoop(Params.QueryNodePort)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	if err != nil {
		return err
	}

	// --- RootCoord Client ---
	//ms.Params.Init()
	addr := Params.RootCoordAddress

	log.Debug("QueryNode start to new RootCoordClient", zap.Any("QueryCoordAddress", addr))
	rootCoord, err := rcc.NewClient(s.ctx, qn.Params.MetaRootPath, qn.Params.EtcdEndpoints)
	if err != nil {
		log.Debug("QueryNode new RootCoordClient failed", zap.Error(err))
		panic(err)
	}

	if err = rootCoord.Init(); err != nil {
		log.Debug("QueryNode RootCoordClient Init failed", zap.Error(err))
		panic(err)
	}

	if err = rootCoord.Start(); err != nil {
		log.Debug("QueryNode RootCoordClient Start failed", zap.Error(err))
		panic(err)
	}
	log.Debug("QueryNode start to wait for RootCoord ready")
	err = funcutil.WaitForComponentHealthy(s.ctx, rootCoord, "RootCoord", 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("QueryNode wait for RootCoord ready failed", zap.Error(err))
		panic(err)
	}
	log.Debug("QueryNode report RootCoord is ready")

	if err := s.SetRootCoord(rootCoord); err != nil {
		panic(err)
	}

	// --- IndexCoord ---
	log.Debug("Index coord", zap.String("address", Params.IndexCoordAddress))
	indexCoord, err := isc.NewClient(s.ctx, qn.Params.MetaRootPath, qn.Params.EtcdEndpoints)

	if err != nil {
		log.Debug("QueryNode new IndexCoordClient failed", zap.Error(err))
		panic(err)
	}

	if err := indexCoord.Init(); err != nil {
		log.Debug("QueryNode IndexCoordClient Init failed", zap.Error(err))
		panic(err)
	}

	if err := indexCoord.Start(); err != nil {
		log.Debug("QueryNode IndexCoordClient Start failed", zap.Error(err))
		panic(err)
	}
	// wait IndexCoord healthy
	log.Debug("QueryNode start to wait for IndexCoord ready")
	err = funcutil.WaitForComponentHealthy(s.ctx, indexCoord, "IndexCoord", 1000000, time.Millisecond*200)
	if err != nil {
		log.Debug("QueryNode wait for IndexCoord ready failed", zap.Error(err))
		panic(err)
	}
	log.Debug("QueryNode report IndexCoord is ready")

	if err := s.SetIndexCoord(indexCoord); err != nil {
		panic(err)
	}

	s.querynode.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("QueryNode", zap.Any("State", internalpb.StateCode_Initializing))
	if err := s.querynode.Init(); err != nil {
		log.Error("QueryNode init error: ", zap.Error(err))
		return err
	}

	if err := s.querynode.Register(); err != nil {
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
	err = retry.Do(s.ctx, func() error {
		addr := ":" + strconv.Itoa(grpcPort)
		lis, err = net.Listen("tcp", addr)
		if err == nil {
			qn.Params.QueryNodePort = int64(lis.Addr().(*net.TCPAddr).Port)
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
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(
			grpc_opentracing.UnaryServerInterceptor(opts...)),
		grpc.StreamInterceptor(
			grpc_opentracing.StreamServerInterceptor(opts...)))
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

func (s *Server) SetRootCoord(rootCoord types.RootCoord) error {
	return s.querynode.SetRootCoord(rootCoord)
}

func (s *Server) SetIndexCoord(indexCoord types.IndexCoord) error {
	return s.querynode.SetIndexCoord(indexCoord)
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

func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.querynode.GetMetrics(ctx, req)
}

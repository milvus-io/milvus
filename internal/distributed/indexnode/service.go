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

package grpcindexnode

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	grpcindexserviceclient "github.com/milvus-io/milvus/internal/distributed/indexservice/client"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

type Server struct {
	indexnode *indexnode.IndexNode

	grpcServer  *grpc.Server
	grpcErrChan chan error

	indexServiceClient types.IndexService
	loopCtx            context.Context
	loopCancel         func()
	loopWg             sync.WaitGroup

	closer io.Closer
}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return nil
	}

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.loopWg.Done()

	log.Debug("IndexNode", zap.Int("network port: ", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("IndexNode", zap.String("GrpcServer:failed to listen", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)))
	indexpb.RegisterIndexNodeServer(s.grpcServer, s)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) init() error {
	var err error
	Params.Init()
	if !funcutil.CheckPortAvailable(Params.Port) {
		Params.Port = funcutil.GetAvailablePort()
		log.Warn("IndexNode init", zap.Any("Port", Params.Port))
	}
	Params.LoadFromEnv()
	Params.LoadFromArgs()

	indexnode.Params.Init()
	indexnode.Params.Port = Params.Port
	indexnode.Params.IP = Params.IP
	indexnode.Params.Address = Params.Address

	closer := trace.InitTracing(fmt.Sprintf("index_node_%d", indexnode.Params.NodeID))
	s.closer = closer

	Params.Address = Params.IP + ":" + strconv.FormatInt(int64(Params.Port), 10)

	defer func() {
		if err != nil {
			err = s.Stop()
			if err != nil {
				log.Debug("IndexNode Init failed, and Stop failed")
			}
		}
	}()

	err = s.indexnode.Register()
	if err != nil {
		log.Debug("IndexNode Register etcd failed", zap.Error(err))
		return err
	}
	log.Debug("IndexNode Register etcd success")

	s.loopWg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		return err
	}

	s.indexServiceClient = grpcindexserviceclient.NewClient(indexnode.Params.MetaRootPath, indexnode.Params.EtcdEndpoints, 3*time.Second)
	err = s.indexServiceClient.Init()
	if err != nil {
		log.Debug("IndexNode indexSerticeClient init failed", zap.Error(err))
		return err
	}
	s.indexnode.SetIndexServiceClient(s.indexServiceClient)

	s.indexnode.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("IndexNode", zap.Any("State", internalpb.StateCode_Initializing))
	err = s.indexnode.Init()
	if err != nil {
		log.Debug("IndexNode Init failed", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) start() error {
	err := s.indexnode.Start()
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() error {
	if s.closer != nil {
		if err := s.closer.Close(); err != nil {
			return err
		}
	}
	s.loopCancel()
	if s.indexnode != nil {
		s.indexnode.Stop()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.loopWg.Wait()

	return nil
}

func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.indexnode.GetComponentStates(ctx)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexnode.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.indexnode.GetStatisticsChannel(ctx)
}

func (s *Server) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.indexnode.CreateIndex(ctx, req)
}

func NewServer(ctx context.Context) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	node, err := indexnode.NewIndexNode(ctx1)
	if err != nil {
		defer cancel()
		return nil, err
	}

	return &Server{
		loopCtx:     ctx1,
		loopCancel:  cancel,
		indexnode:   node,
		grpcErrChan: make(chan error),
	}, nil
}

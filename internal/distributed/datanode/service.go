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

package grpcdatanode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	dn "github.com/zilliztech/milvus-distributed/internal/datanode"
	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice/client"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice/client"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/types"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
)

type Server struct {
	datanode    *dn.DataNode
	wg          sync.WaitGroup
	grpcErrChan chan error
	grpcServer  *grpc.Server
	ctx         context.Context
	cancel      context.CancelFunc

	msFactory msgstream.Factory

	masterService types.MasterService
	dataService   types.DataService

	closer io.Closer
}

func New(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	var s = &Server{
		ctx:         ctx1,
		cancel:      cancel,
		msFactory:   factory,
		grpcErrChan: make(chan error),
	}

	s.datanode = dn.NewDataNode(s.ctx, s.msFactory)

	return s, nil
}

func (s *Server) startGrpcLoop(grpcPort int) {
	defer s.wg.Done()

	addr := ":" + strconv.Itoa(grpcPort)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Warn("GrpcServer failed to listen", zap.Error(err))
		s.grpcErrChan <- err
		return
	}
	log.Debug("DataNode address", zap.String("address", addr))

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)))
	datapb.RegisterDataNodeServer(s.grpcServer, s)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Warn("DataNode Start Grpc Failed!")
		s.grpcErrChan <- err
	}

}

func (s *Server) SetMasterServiceInterface(ms types.MasterService) error {
	return s.datanode.SetMasterServiceInterface(ms)
}

func (s *Server) SetDataServiceInterface(ds types.DataService) error {
	return s.datanode.SetDataServiceInterface(ds)
}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("data node init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("data node start done ...")
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

	err := s.datanode.Stop()
	if err != nil {
		return err
	}
	s.wg.Wait()
	return nil
}

func (s *Server) init() error {
	ctx := context.Background()
	Params.Init()
	if !funcutil.CheckPortAvailable(Params.Port) {
		Params.Port = funcutil.GetAvailablePort()
		log.Warn("DataNode init", zap.Any("Port", Params.Port))
	}
	Params.LoadFromEnv()
	Params.LoadFromArgs()

	dn.Params.Init()
	dn.Params.Port = Params.Port
	dn.Params.IP = Params.IP

	log.Debug("DataNode port", zap.Int("port", Params.Port))

	closer := trace.InitTracing(fmt.Sprintf("data_node ip: %s, port: %d", Params.IP, Params.Port))
	s.closer = closer

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	if err != nil {
		return err
	}

	// --- Master Server Client ---
	log.Debug("Master service address", zap.String("address", Params.MasterAddress))
	log.Debug("Init master service client ...")
	masterClient, err := msc.NewClient(Params.MasterAddress, 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = masterClient.Init(); err != nil {
		panic(err)
	}

	if err = masterClient.Start(); err != nil {
		panic(err)
	}
	err = funcutil.WaitForComponentHealthy(ctx, masterClient, "MasterService", 1000000, time.Millisecond*200)

	if err != nil {
		panic(err)
	}

	if err := s.SetMasterServiceInterface(masterClient); err != nil {
		panic(err)
	}

	// --- Data Server Client ---
	log.Debug("Data service address", zap.String("address", Params.DataServiceAddress))
	log.Debug("DataNode Init data service client ...")
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
	if err := s.SetDataServiceInterface(dataService); err != nil {
		panic(err)
	}

	s.datanode.NodeID = dn.Params.NodeID
	s.datanode.UpdateStateCode(internalpb.StateCode_Initializing)

	if err := s.datanode.Init(); err != nil {
		log.Warn("datanode init error: ", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) start() error {
	return s.datanode.Start()
}

func (s *Server) GetComponentStates(ctx context.Context, req *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.datanode.GetComponentStates(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.datanode.GetStatisticsChannel(ctx)
}

func (s *Server) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return s.datanode.WatchDmChannels(ctx, req)
}

func (s *Server) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	if s.datanode.State.Load().(internalpb.StateCode) != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "DataNode isn't healthy.",
		}, errors.New("DataNode is not ready yet")
	}
	return s.datanode.FlushSegments(ctx, req)
}

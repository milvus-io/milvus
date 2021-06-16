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

	dn "github.com/milvus-io/milvus/internal/datanode"
	dsc "github.com/milvus-io/milvus/internal/distributed/dataservice/client"
	msc "github.com/milvus-io/milvus/internal/distributed/masterservice/client"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
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

	newMasterServiceClient func() (types.MasterService, error)
	newDataServiceClient   func(string, []string, time.Duration) types.DataService

	closer io.Closer
}

// NewServer new data node grpc server
func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	var s = &Server{
		ctx:         ctx1,
		cancel:      cancel,
		msFactory:   factory,
		grpcErrChan: make(chan error),
		newMasterServiceClient: func() (types.MasterService, error) {
			return msc.NewClient(ctx1, dn.Params.MetaRootPath, dn.Params.EtcdEndpoints, 3*time.Second)
		},
		newDataServiceClient: func(etcdMetaRoot string, etcdEndpoints []string, timeout time.Duration) types.DataService {
			return dsc.NewClient(etcdMetaRoot, etcdEndpoints, timeout)
		},
	}

	s.datanode = dn.NewDataNode(s.ctx, s.msFactory)

	return s, nil
}

func (s *Server) startGrpc() error {
	s.wg.Add(1)
	go s.startGrpcLoop(Params.listener)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop(listener net.Listener) {
	defer s.wg.Done()

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
	if err := s.grpcServer.Serve(listener); err != nil {
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
	Params.LoadFromEnv()
	Params.LoadFromArgs()

	dn.Params.Init()
	dn.Params.Port = Params.Port
	dn.Params.IP = Params.IP

	closer := trace.InitTracing(fmt.Sprintf("data_node ip: %s, port: %d", Params.IP, Params.Port))
	s.closer = closer
	addr := Params.IP + ":" + strconv.Itoa(Params.Port)
	log.Debug("DataNode address", zap.String("address", addr))

	err := s.startGrpc()
	if err != nil {
		return err
	}

	// --- Master Server Client ---
	if s.newMasterServiceClient != nil {
		log.Debug("Master service address", zap.String("address", Params.MasterAddress))
		log.Debug("Init master service client ...")
		masterServiceClient, err := s.newMasterServiceClient()
		if err != nil {
			log.Debug("DataNode newMasterServiceClient failed", zap.Error(err))
			panic(err)
		}
		if err = masterServiceClient.Init(); err != nil {
			log.Debug("DataNode masterServiceClient Init failed", zap.Error(err))
			panic(err)
		}
		if err = masterServiceClient.Start(); err != nil {
			log.Debug("DataNode masterServiceClient Start failed", zap.Error(err))
			panic(err)
		}
		err = funcutil.WaitForComponentHealthy(ctx, masterServiceClient, "MasterService", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("DataNode wait masterService ready failed", zap.Error(err))
			panic(err)
		}
		log.Debug("DataNode masterService is ready")
		if err = s.SetMasterServiceInterface(masterServiceClient); err != nil {
			panic(err)
		}
	}

	// --- Data Server Client ---
	if s.newDataServiceClient != nil {
		log.Debug("Data service address", zap.String("address", Params.DataServiceAddress))
		log.Debug("DataNode Init data service client ...")
		dataServiceClient := s.newDataServiceClient(dn.Params.MetaRootPath, dn.Params.EtcdEndpoints, 10*time.Second)
		if err = dataServiceClient.Init(); err != nil {
			log.Debug("DataNode newDataServiceClient failed", zap.Error(err))
			panic(err)
		}
		if err = dataServiceClient.Start(); err != nil {
			log.Debug("DataNode dataServiceClient Start failed", zap.Error(err))
			panic(err)
		}
		err = funcutil.WaitForComponentInitOrHealthy(ctx, dataServiceClient, "DataService", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("DataNode wait dataServiceClient ready failed", zap.Error(err))
			panic(err)
		}
		log.Debug("DataNode dataService is ready")
		if err = s.SetDataServiceInterface(dataServiceClient); err != nil {
			panic(err)
		}
	}

	s.datanode.NodeID = dn.Params.NodeID
	s.datanode.UpdateStateCode(internalpb.StateCode_Initializing)

	if err := s.datanode.Init(); err != nil {
		log.Warn("datanode init error: ", zap.Error(err))
		return err
	}
	log.Debug("DataNode", zap.Any("State", internalpb.StateCode_Initializing))
	return nil
}

func (s *Server) start() error {
	if err := s.datanode.Start(); err != nil {
		return err
	}
	err := s.datanode.Register()
	if err != nil {
		log.Debug("DataNode Register etcd failed", zap.Error(err))
		return err
	}
	return nil
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

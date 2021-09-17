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
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	dn "github.com/milvus-io/milvus/internal/datanode"
	dsc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
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

	rootCoord types.RootCoord
	dataCoord types.DataCoord

	newRootCoordClient func(string, []string) (types.RootCoord, error)
	newDataCoordClient func(string, []string) (types.DataCoord, error)

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
		newRootCoordClient: func(etcdMetaRoot string, etcdEndpoints []string) (types.RootCoord, error) {
			return rcc.NewClient(ctx1, etcdMetaRoot, etcdEndpoints)
		},
		newDataCoordClient: func(etcdMetaRoot string, etcdEndpoints []string) (types.DataCoord, error) {
			return dsc.NewClient(ctx1, etcdMetaRoot, etcdEndpoints)
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

	opts := trace.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(Params.ServerMaxRecvSize),
		grpc.MaxSendMsgSize(Params.ServerMaxSendSize),
		grpc.UnaryInterceptor(
			grpc_opentracing.UnaryServerInterceptor(opts...)),
		grpc.StreamInterceptor(
			grpc_opentracing.StreamServerInterceptor(opts...)))
	datapb.RegisterDataNodeServer(s.grpcServer, s)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(listener); err != nil {
		log.Warn("DataNode Start Grpc Failed!")
		s.grpcErrChan <- err
	}

}

func (s *Server) SetRootCoordInterface(ms types.RootCoord) error {
	return s.datanode.SetRootCoordInterface(ms)
}

func (s *Server) SetDataCoordInterface(ds types.DataCoord) error {
	return s.datanode.SetDataCoordInterface(ds)
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
		// make graceful stop has a timeout
		stopped := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(stopped)
		}()

		t := time.NewTimer(10 * time.Second)
		select {
		case <-t.C:
			// hard stop since grace timeout
			s.grpcServer.Stop()
		case <-stopped:
			t.Stop()
		}
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

	// --- RootCoord Client ---
	if s.newRootCoordClient != nil {
		log.Debug("RootCoord address", zap.String("address", Params.RootCoordAddress))
		log.Debug("Init root coord client ...")
		rootCoordClient, err := s.newRootCoordClient(dn.Params.MetaRootPath, dn.Params.EtcdEndpoints)
		if err != nil {
			log.Debug("DataNode newRootCoordClient failed", zap.Error(err))
			panic(err)
		}
		if err = rootCoordClient.Init(); err != nil {
			log.Debug("DataNode rootCoordClient Init failed", zap.Error(err))
			panic(err)
		}
		if err = rootCoordClient.Start(); err != nil {
			log.Debug("DataNode rootCoordClient Start failed", zap.Error(err))
			panic(err)
		}
		err = funcutil.WaitForComponentHealthy(ctx, rootCoordClient, "RootCoord", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("DataNode wait rootCoord ready failed", zap.Error(err))
			panic(err)
		}
		log.Debug("DataNode rootCoord is ready")
		if err = s.SetRootCoordInterface(rootCoordClient); err != nil {
			panic(err)
		}
	}

	// --- Data Server Client ---
	if s.newDataCoordClient != nil {
		log.Debug("Data service address", zap.String("address", Params.DataCoordAddress))
		log.Debug("DataNode Init data service client ...")
		dataCoordClient, err := s.newDataCoordClient(dn.Params.MetaRootPath, dn.Params.EtcdEndpoints)
		if err != nil {
			log.Debug("DataNode newDataCoordClient failed", zap.Error(err))
			panic(err)
		}
		if err = dataCoordClient.Init(); err != nil {
			log.Debug("DataNode newDataCoord failed", zap.Error(err))
			panic(err)
		}
		if err = dataCoordClient.Start(); err != nil {
			log.Debug("DataNode dataCoordClient Start failed", zap.Error(err))
			panic(err)
		}
		err = funcutil.WaitForComponentInitOrHealthy(ctx, dataCoordClient, "DataCoord", 1000000, time.Millisecond*200)
		if err != nil {
			log.Debug("DataNode wait dataCoordClient ready failed", zap.Error(err))
			panic(err)
		}
		log.Debug("DataNode dataCoord is ready")
		if err = s.SetDataCoordInterface(dataCoordClient); err != nil {
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

func (s *Server) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.datanode.GetMetrics(ctx, request)
}

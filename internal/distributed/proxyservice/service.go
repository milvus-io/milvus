package grpcproxyservice

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"

	"github.com/zilliztech/milvus-distributed/internal/proxyservice"
	"google.golang.org/grpc"
)

type Server struct {
	ctx        context.Context
	wg         sync.WaitGroup
	impl       proxyservice.ProxyService
	grpcServer *grpc.Server
}

func (s *Server) GetTimeTickChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	channel, err := s.impl.GetTimeTickChannel()
	if err != nil {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Value: "",
		}, nil
	}
	return &milvuspb.StringResponse{
		Value: channel,
	}, nil
}

func (s *Server) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.impl.GetComponentStates()
}

func CreateProxyServiceServer() (*Server, error) {
	return &Server{}, nil
}

func (s *Server) Init() error {
	s.ctx = context.Background()
	Params.Init()
	proxyservice.Params.Init()
	s.impl, _ = proxyservice.CreateProxyService(s.ctx)
	s.impl.Init()
	return nil
}

func (s *Server) Start() error {
	fmt.Println("proxy service start ...")
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// TODO: use config
		fmt.Println("network port: ", Params.NetworkPort())
		lis, err := net.Listen("tcp", ":"+strconv.Itoa(Params.NetworkPort()))
		if err != nil {
			panic(err)
		}

		s.grpcServer = grpc.NewServer()
		proxypb.RegisterProxyServiceServer(s.grpcServer, s)
		milvuspb.RegisterProxyServiceServer(s.grpcServer, s)
		if err = s.grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()

	s.impl.Start()

	return nil
}

func (s *Server) Stop() error {
	s.impl.Stop()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) RegisterLink(ctx context.Context, empty *commonpb.Empty) (*milvuspb.RegisterLinkResponse, error) {
	return s.impl.RegisterLink()
}

func (s *Server) RegisterNode(ctx context.Context, request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error) {
	return s.impl.RegisterNode(request)
}

func (s *Server) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, s.impl.InvalidateCollectionMetaCache(request)
}

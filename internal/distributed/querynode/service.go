package grpcquerynode

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/dataservice"
	grpcdataserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
	grpcindexserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	grpcmasterserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	grpcqueryserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice/client"
	"github.com/zilliztech/milvus-distributed/internal/indexservice"
	"github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/querynode"
	"github.com/zilliztech/milvus-distributed/internal/queryservice"
)

type Server struct {
	grpcServer *grpc.Server
	node       *querynode.QueryNode
}

func NewServer(ctx context.Context) *Server {
	server := &Server{
		node: querynode.NewQueryNodeWithoutID(ctx),
	}

	queryservice.Params.Init()
	queryClient := grpcqueryserviceclient.NewClient(queryservice.Params.Address)
	if err := server.node.SetQueryService(queryClient); err != nil {
		panic(err)
	}

	masterservice.Params.Init()
	masterConnectTimeout := 10 * time.Second
	masterClient, err := grpcmasterserviceclient.NewGrpcClient(masterservice.Params.Address, masterConnectTimeout)
	if err != nil {
		panic(err)
	}
	if err = server.node.SetMasterService(masterClient); err != nil {
		panic(err)
	}

	indexservice.Params.Init()
	indexClient := grpcindexserviceclient.NewClient(indexservice.Params.Address)
	if err := server.node.SetIndexService(indexClient); err != nil {
		panic(err)
	}

	dataservice.Params.Init()
	log.Println("connect to data service, address =", fmt.Sprint(dataservice.Params.Address, ":", dataservice.Params.Port))
	dataClient := grpcdataserviceclient.NewClient(fmt.Sprint(dataservice.Params.Address, ":", dataservice.Params.Port))
	if err := server.node.SetDataService(dataClient); err != nil {
		panic(err)
	}

	return server
}

func (s *Server) StartGrpcServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", querynode.Params.QueryNodePort))
	if err != nil {
		panic(err)
	}

	s.grpcServer = grpc.NewServer()
	querypb.RegisterQueryNodeServer(s.grpcServer, s)
	fmt.Println("start query node grpc server...")
	if err = s.grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}

func (s *Server) Init() error {
	return s.node.Init()
}

func (s *Server) Start() error {
	go s.StartGrpcServer()
	return s.node.Start()
}

func (s *Server) Stop() error {
	return s.Stop()
}

func (s *Server) GetTimeTickChannel(ctx context.Context, in *commonpb.Empty) (*milvuspb.StringResponse, error) {
	// ignore ctx and in
	channel, err := s.node.GetTimeTickChannel()
	if err != nil {
		return nil, err
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: channel,
	}, nil
}

func (s *Server) GetStatsChannel(ctx context.Context, in *commonpb.Empty) (*milvuspb.StringResponse, error) {
	// ignore ctx and in
	channel, err := s.node.GetStatisticsChannel()
	if err != nil {
		return nil, err
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: channel,
	}, nil
}

func (s *Server) GetComponentStates(ctx context.Context, in *commonpb.Empty) (*querypb.ComponentStatesResponse, error) {
	// ignore ctx and in
	componentStates, err := s.node.GetComponentStates()
	if err != nil {
		return nil, err
	}
	return &querypb.ComponentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		States: componentStates,
	}, nil
}

func (s *Server) AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.node.AddQueryChannel(in)
}

func (s *Server) RemoveQueryChannel(ctx context.Context, in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.node.RemoveQueryChannel(in)
}

func (s *Server) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.node.WatchDmChannels(in)
}

func (s *Server) LoadSegments(ctx context.Context, in *querypb.LoadSegmentRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.node.LoadSegments(in)
}

func (s *Server) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.node.ReleaseSegments(in)
}

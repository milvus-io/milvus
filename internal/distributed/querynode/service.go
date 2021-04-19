package grpcquerynode

import (
	"context"
	"net"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/querynode"
)

type Server struct {
	grpcServer *grpc.Server
	node       *querynode.QueryNode
}

func NewServer(ctx context.Context, queryNodeID uint64) *Server {
	return &Server{
		node: querynode.NewQueryNode(ctx, queryNodeID),
	}
}

func (s *Server) StartGrpcServer() {
	// TODO: add address
	lis, err := net.Listen("tcp", "")
	if err != nil {
		panic(err)
	}

	s.grpcServer = grpc.NewServer()
	querypb.RegisterQueryNodeServer(s.grpcServer, s)
	if err = s.grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}

func (s *Server) Init() error {
	return s.Init()
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

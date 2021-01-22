package querynode

import (
	"context"
	"net"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
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

func (s *Server) GetTimeTickChannel(ctx context.Context, in *commonpb.Empty) (*querypb.GetTimeTickChannelResponse, error) {
	// ignore ctx and in
	channel, err := s.node.GetTimeTickChannel()
	if err != nil {
		return nil, err
	}
	return &querypb.GetTimeTickChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		TimeTickChannelID: channel,
	}, nil
}

func (s *Server) GetStatsChannel(ctx context.Context, in *commonpb.Empty) (*querypb.GetStatsChannelResponse, error) {
	// ignore ctx and in
	channel, err := s.node.GetStatisticsChannel()
	if err != nil {
		return nil, err
	}
	return &querypb.GetStatsChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		StatsChannelID: channel,
	}, nil
}

func (s *Server) GetComponentStates(ctx context.Context, in *commonpb.Empty) (*querypb.ServiceStatesResponse, error) {
	// ignore ctx and in
	componentStates, err := s.node.GetComponentStates()
	if err != nil {
		return nil, err
	}
	return &querypb.ServiceStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		ServerStates: componentStates,
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

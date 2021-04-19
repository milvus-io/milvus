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
	node       querynode.Node
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

func (s *Server) Start() {
	go s.StartGrpcServer()
	if err := s.node.Start(); err != nil {
		panic(err)
	}
}

func (s *Server) AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error) {
	return s.node.AddQueryChannel(ctx, in)
}

func (s *Server) RemoveQueryChannel(ctx context.Context, in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error) {
	return s.node.RemoveQueryChannel(ctx, in)
}

func (s *Server) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return s.node.WatchDmChannels(ctx, in)
}

func (s *Server) LoadSegments(ctx context.Context, in *querypb.LoadSegmentRequest) (*commonpb.Status, error) {
	return s.node.LoadSegments(ctx, in)
}

func (s *Server) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	return s.node.ReleaseSegments(ctx, in)
}

func (s *Server) GetPartitionState(ctx context.Context, in *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
	return s.node.GetPartitionState(ctx, in)
}

package datanode

import (
	"context"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/datanode"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"google.golang.org/grpc"
)

type Server struct {
	node datanode.Interface
	core datanode.DataNode

	grpcServer *grpc.Server
	grpcError  error
	grpcErrMux sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

func NewGrpcServer() (*Server, error) {
	panic("implement me")
}

func (s *Server) Init() error {
	return s.core.Init()
}

func (s *Server) Start() error {
	return s.core.Start()
}

func (s *Server) Stop() error {
	return s.core.Stop()
}

func (s *Server) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return nil, nil
}

func (s *Server) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelRequest) error {
	return s.core.WatchDmChannels(in)
}

func (s *Server) FlushSegments(ctx context.Context, in *datapb.FlushSegRequest) error {
	return s.core.FlushSegments(in)
}

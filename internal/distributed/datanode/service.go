package datanode

import (
	"context"
	"net"
	"strconv"
	"sync"

	dn "github.com/zilliztech/milvus-distributed/internal/datanode"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"google.golang.org/grpc"
)

type Server struct {
	core *dn.DataNode

	grpcServer *grpc.Server
	grpcError  error
	grpcErrMux sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

func New(ctx context.Context) (*Server, error) {
	var s = &Server{
		ctx: ctx,
	}

	s.core = dn.NewDataNode(s.ctx)
	s.grpcServer = grpc.NewServer()
	datapb.RegisterDataNodeServer(s.grpcServer, s)
	addr := dn.Params.IP + ":" + strconv.FormatInt(dn.Params.Port, 10)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	go func() {
		if err = s.grpcServer.Serve(lis); err != nil {
			s.grpcErrMux.Lock()
			defer s.grpcErrMux.Unlock()
			s.grpcError = err
		}
	}()

	s.grpcErrMux.Lock()
	err = s.grpcError
	s.grpcErrMux.Unlock()

	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) SetMasterServiceInterface(ms dn.MasterServiceInterface) error {
	return s.core.SetMasterServiceInterface(ms)
}

func (s *Server) SetDataServiceInterface(ds dn.DataServiceInterface) error {
	return s.core.SetDataServiceInterface(ds)
}

func (s *Server) Init() error {
	err := s.core.Init()
	if err != nil {
		return errors.Errorf("Init failed: %v", err)
	}

	return s.core.Init()
}

func (s *Server) Start() error {
	return s.core.Start()
}

func (s *Server) Stop() error {
	return s.core.Stop()
}

func (s *Server) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.core.GetComponentStates()
}

func (s *Server) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelRequest) (*commonpb.Status, error) {
	return s.core.WatchDmChannels(in)
}

func (s *Server) FlushSegments(ctx context.Context, in *datapb.FlushSegRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, s.core.FlushSegments(in)
}

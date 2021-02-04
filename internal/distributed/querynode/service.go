package grpcquerynode

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	qn "github.com/zilliztech/milvus-distributed/internal/querynode"
)

type Server struct {
	node *qn.QueryNode

	grpcServer *grpc.Server
	grpcError  error
	grpcErrMux sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

func NewServer(ctx context.Context) (*Server, error) {
	s := &Server{
		ctx:  ctx,
		node: qn.NewQueryNodeWithoutID(ctx),
	}

	qn.Params.Init()
	s.grpcServer = grpc.NewServer()
	querypb.RegisterQueryNodeServer(s.grpcServer, s)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", qn.Params.QueryNodePort))
	if err != nil {
		return nil, err
	}

	go func() {
		log.Println("start query node grpc server...")
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

func (s *Server) Init() error {
	return s.node.Init()
}

func (s *Server) Start() error {
	return s.node.Start()
}

func (s *Server) Stop() error {
	err := s.node.Stop()
	s.cancel()
	s.grpcServer.GracefulStop()
	return err
}

func (s *Server) SetMasterService(master qn.MasterServiceInterface) error {
	return s.node.SetMasterService(master)
}

func (s *Server) SetQueryService(query qn.QueryServiceInterface) error {
	return s.node.SetQueryService(query)
}

func (s *Server) SetIndexService(index qn.IndexServiceInterface) error {
	return s.node.SetIndexService(index)
}

func (s *Server) SetDataService(data qn.DataServiceInterface) error {
	return s.node.SetDataService(data)
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

func (s *Server) GetSegmentInfo(ctx context.Context, in *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return s.node.GetSegmentInfo(in)
}

package grpcindexservice

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/indexservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"google.golang.org/grpc"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type Server struct {
	server typeutil.IndexServiceInterface

	grpcServer *grpc.Server

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup
}

func (s *Server) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.server.GetComponentStates()
}

func (s *Server) GetTimeTickChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	resp := &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	channel, err := s.server.GetTimeTickChannel()
	if err != nil {
		resp.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	resp.Value = channel
	return resp, nil
}

func (s *Server) GetStatisticsChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	resp := &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	channel, err := s.server.GetStatisticsChannel()
	if err != nil {
		resp.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	resp.Value = channel
	return resp, nil
}

func Init() error {
	indexservice.Params.Init()
	return nil
}

func (s *Server) Start() error {
	return s.startIndexServer()
}

func (s *Server) Stop() error {
	s.server.Stop()
	s.loopCancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	s.loopWg.Wait()
	return nil
}

func (s *Server) RegisterNode(ctx context.Context, req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {

	return s.server.RegisterNode(req)
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {

	return s.server.BuildIndex(req)
}

func (s *Server) GetIndexStates(ctx context.Context, req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {

	return s.server.GetIndexStates(req)
}

func (s *Server) GetIndexFilePaths(ctx context.Context, req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error) {

	return s.server.GetIndexFilePaths(req)
}

func (s *Server) NotifyBuildIndex(ctx context.Context, nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {

	return s.server.NotifyBuildIndex(nty)
}

func (s *Server) grpcLoop() {
	defer s.loopWg.Done()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(indexservice.Params.Port))
	if err != nil {
		log.Fatalf("IndexServer grpc server fatal error=%v", err)
	}

	s.grpcServer = grpc.NewServer()
	indexpb.RegisterIndexServiceServer(s.grpcServer, s)

	if err = s.grpcServer.Serve(lis); err != nil {
		log.Fatalf("IndexServer grpc server fatal error=%v", err)
	}
}

func (s *Server) startIndexServer() error {
	s.loopWg.Add(1)
	go s.grpcLoop()
	log.Println("IndexServer grpc server start successfully")

	return s.server.Start()
}

func NewServer(ctx context.Context) (*Server, error) {

	ctx1, cancel := context.WithCancel(ctx)
	serverImp, err := indexservice.CreateIndexService(ctx)
	if err != nil {
		defer cancel()
		return nil, err
	}
	s := &Server{
		loopCtx:    ctx1,
		loopCancel: cancel,

		server: serverImp,
	}

	return s, nil
}

package grpcindexservice

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	"github.com/zilliztech/milvus-distributed/internal/indexservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"google.golang.org/grpc"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type Server struct {
	impl *indexservice.ServiceImpl

	grpcServer  *grpc.Server
	grpcErrChan chan error

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup
}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	Params.Init()
	indexservice.Params.Init()

	s.loopWg.Add(1)
	go s.startGrpcLoop(Params.ServicePort)
	// wait for grpc impl loop start
	if err := <-s.grpcErrChan; err != nil {
		return err
	}
	s.impl.UpdateStateCode(internalpb2.StateCode_INITIALIZING)

	if err := s.impl.Init(); err != nil {
		return err
	}
	return nil
}

func (s *Server) start() error {
	if err := s.impl.Start(); err != nil {
		return err
	}
	log.Println("indexService started")
	return nil
}

func (s *Server) Stop() error {
	if s.impl != nil {
		s.impl.Stop()
	}

	s.loopCancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	s.loopWg.Wait()
	return nil
}

func (s *Server) RegisterNode(ctx context.Context, req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {

	return s.impl.RegisterNode(req)
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {

	return s.impl.BuildIndex(req)
}

func (s *Server) GetIndexStates(ctx context.Context, req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {

	return s.impl.GetIndexStates(req)
}

func (s *Server) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {

	return s.impl.DropIndex(request)
}

func (s *Server) GetIndexFilePaths(ctx context.Context, req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error) {

	return s.impl.GetIndexFilePaths(req)
}

func (s *Server) NotifyBuildIndex(ctx context.Context, nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {

	return s.impl.NotifyBuildIndex(nty)
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.loopWg.Done()

	log.Println("network port: ", grpcPort)
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Printf("GrpcServer:failed to listen: %v", err)
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	s.grpcServer = grpc.NewServer()
	indexpb.RegisterIndexServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.impl.GetComponentStates()
}

func (s *Server) GetTimeTickChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	resp := &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	channel, err := s.impl.GetTimeTickChannel()
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
	channel, err := s.impl.GetStatisticsChannel()
	if err != nil {
		resp.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	resp.Value = channel
	return resp, nil
}

func NewServer(ctx context.Context) (*Server, error) {

	ctx1, cancel := context.WithCancel(ctx)
	serverImp, err := indexservice.NewServiceImpl(ctx)
	if err != nil {
		defer cancel()
		return nil, err
	}
	s := &Server{
		loopCtx:     ctx1,
		loopCancel:  cancel,
		impl:        serverImp,
		grpcErrChan: make(chan error),
	}

	return s, nil
}

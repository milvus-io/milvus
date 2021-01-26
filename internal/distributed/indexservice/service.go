package grpcindexservice

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/indexservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
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

func (s *Server) GetComponentStates() (*internalpb2.ComponentStates, error) {
	panic("implement me")
}

func (s *Server) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (s *Server) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (s *Server) RegisterNode(ctx context.Context, req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {

	log.Println("Register IndexNode starting..., node address = ", req.Address)
	return s.server.RegisterNode(req)
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {

	log.Println("Build Index ...")
	return s.server.BuildIndex(req)
}

func (s *Server) GetIndexStates(ctx context.Context, req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {

	return s.server.GetIndexStates(req)
}

func (s *Server) GetIndexFilePaths(ctx context.Context, req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error) {

	return s.server.GetIndexFilePaths(req)
}

func (s *Server) NotifyBuildIndex(ctx context.Context, nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {

	log.Println("build index finished.")
	return s.server.NotifyBuildIndex(nty)
}

func NewServer() *Server {

	return &Server{
		//server: &indexservice.IndexService{},
		//grpcServer: indexservice.IndexService{},
	}
}

func (s *Server) grpcLoop() {
	defer s.loopWg.Done()

	log.Println("Starting start IndexServer")
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(indexservice.Params.Port))
	if err != nil {
		log.Fatalf("IndexServer grpc server fatal error=%v", err)
	}

	s.grpcServer = grpc.NewServer()
	indexpb.RegisterIndexServiceServer(s.grpcServer, s)

	log.Println("IndexServer register finished")
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

func CreateIndexServer(ctx context.Context) (*Server, error) {

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

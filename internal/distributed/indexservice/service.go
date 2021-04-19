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
	server indexservice.Interface

	grpcServer *grpc.Server

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup
}

func (s *Server) Init() {
	log.Println("initing params ...")
	Params.Init()
}

func (s *Server) Start() error {
	s.Init()
	log.Println("stringing indexserver ...")
	return s.startIndexServer()
}

func (s *Server) Stop() {
	s.loopWg.Wait()
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

	log.Println("Register IndexNode starting...")
	return s.server.RegisterNode(req)
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {

	return s.server.BuildIndex(req)
	//indexID := int64(0)
	//request := &indexpb.BuildIndexCmd{
	//	IndexID: indexID,
	//	Req:     req,
	//}
	//
	//indexNodeClient := grpcindexnode.NewClient()
	//
	//status, err := indexNodeClient.BuildIndex(request)
	//response := &indexpb.BuildIndexResponse{
	//	Status:  status,
	//	IndexID: indexID,
	//}
	//return response, err
}

func (s *Server) GetIndexStates(ctx context.Context, req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {

	return s.server.GetIndexStates(req)
}

func (s *Server) GetIndexFilePaths(ctx context.Context, req *indexpb.IndexFilePathRequest) (*indexpb.IndexFilePathsResponse, error) {

	return s.server.GetIndexFilePaths(req)
}

func (s *Server) NotifyBuildIndex(ctx context.Context, nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {

	return s.server.NotifyBuildIndex(nty)
}

//varindex

func NewServer() *Server {

	return &Server{
		//server: &indexservice.IndexService{},
		//grpcServer: indexservice.IndexService{},
	}
}

func (s *Server) grpcLoop() {
	defer s.loopWg.Done()

	log.Println("Starting start IndexServer")
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(Params.Port))
	if err != nil {
		log.Fatalf("IndexServer grpc server fatal error=%v", err)
	}

	s.grpcServer = grpc.NewServer()
	indexpb.RegisterIndexServiceServer(s.grpcServer, s)

	log.Println("IndexServer register finished")
	if err = s.grpcServer.Serve(lis); err != nil {
		log.Fatalf("IndexServer grpc server fatal error=%v", err)
	}
	log.Println("IndexServer grpc server starting...")
}

func (s *Server) startIndexServer() error {
	s.loopWg.Add(1)
	go s.grpcLoop()

	return nil
}

func Init() {
	Params.Init()
}

func CreateIndexServer(ctx context.Context) (*Server, error) {

	ctx1, cancel := context.WithCancel(ctx)
	s := &Server{
		loopCtx:    ctx1,
		loopCancel: cancel,

		server: indexservice.NewIndexServiceImpl(ctx),
	}

	return s, nil
}

func (s *Server) Close() {

	s.Stop()
}

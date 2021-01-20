package grpcindexnode

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	grpcindexservice "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice"
	"github.com/zilliztech/milvus-distributed/internal/indexnode"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"google.golang.org/grpc"
)

type Server struct {
	node indexnode.Interface

	grpcServer *grpc.Server

	indexNodeLoopCtx    context.Context
	indexNodeLoopCancel func()
	indexNodeLoopWg     sync.WaitGroup
}

func NewGrpcServer(ctx context.Context, indexID int64) *Server {

	return &Server{
		node: indexnode.NewIndexNode(ctx, indexID),
	}
}

func registerNode() error {

	indexServiceClient := grpcindexservice.NewClient(Params.ServiceAddress)

	request := &indexpb.RegisterNodeRequest{
		Base: nil,
		Address: &commonpb.Address{
			Ip:   Params.Address,
			Port: int64(Params.Port),
		},
	}
	resp, err := indexServiceClient.RegisterNode(request)
	if err != nil {
		log.Printf("IndexNode connect to IndexService failed, error= %v", err)
		return err
	}

	Params.NodeID = resp.InitParams.NodeID
	log.Println("Register indexNode successful with nodeID=", Params.NodeID)

	err = Params.LoadFromKVPair(resp.InitParams.StartParams)
	return err
}

func (s *Server) grpcLoop() {
	defer s.indexNodeLoopWg.Done()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(Params.Port))
	if err != nil {
		log.Fatalf("IndexNode grpc server fatal error=%v", err)
	}

	s.grpcServer = grpc.NewServer()
	indexpb.RegisterIndexNodeServer(s.grpcServer, s)
	if err = s.grpcServer.Serve(lis); err != nil {
		log.Fatalf("IndexNode grpc server fatal error=%v", err)
	}
	log.Println("IndexNode grpc server starting...")
}

func (s *Server) startIndexNode() error {
	s.indexNodeLoopWg.Add(1)
	//TODO: How to make sure that grpc server has started successfully
	go s.grpcLoop()

	err := registerNode()
	if err != nil {
		return err
	}

	Params.Init()
	return nil
}

func Init() {
	Params.Init()
}

func CreateIndexNode(ctx context.Context) (*Server, error) {

	ctx1, cancel := context.WithCancel(ctx)
	s := &Server{
		indexNodeLoopCtx:    ctx1,
		indexNodeLoopCancel: cancel,
	}

	return s, nil
}

func (s *Server) Start() error {

	return s.startIndexNode()
}

func (s *Server) Stop() {
	s.indexNodeLoopWg.Wait()
}

func (s *Server) Close() {

	s.Stop()
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexCmd) (*commonpb.Status, error) {
	return s.node.BuildIndex(req)
}

package grpcindexnode

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	serviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	"github.com/zilliztech/milvus-distributed/internal/indexnode"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"google.golang.org/grpc"
)

type Server struct {
	node indexnode.Interface

	grpcServer *grpc.Server

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup
}

func NewGrpcServer(ctx context.Context, nodeID int64) *Server {
	ctx1, cancel := context.WithCancel(ctx)
	return &Server{
		loopCtx:    ctx1,
		loopCancel: cancel,
		node:       indexnode.NewIndexNode(ctx, nodeID),
	}
}

func registerNode() error {

	indexServiceClient := serviceclient.NewClient(indexnode.Params.ServiceAddress)

	request := &indexpb.RegisterNodeRequest{
		Base: nil,
		Address: &commonpb.Address{
			Ip:   indexnode.Params.NodeIP,
			Port: int64(indexnode.Params.NodePort),
		},
	}
	resp, err := indexServiceClient.RegisterNode(request)
	if err != nil {
		log.Printf("IndexNode connect to IndexService failed, error= %v", err)
		return err
	}

	indexnode.Params.NodeID = resp.InitParams.NodeID
	log.Println("Register indexNode successful with nodeID=", indexnode.Params.NodeID)

	err = indexnode.Params.LoadFromKVPair(resp.InitParams.StartParams)
	return err
}

func (s *Server) grpcLoop() {
	defer s.loopWg.Done()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(indexnode.Params.NodePort))
	if err != nil {
		log.Fatalf("IndexNode grpc server fatal error=%v", err)
	}

	s.grpcServer = grpc.NewServer()
	indexpb.RegisterIndexNodeServer(s.grpcServer, s)
	if err = s.grpcServer.Serve(lis); err != nil {
		log.Fatalf("IndexNode grpc server fatal error=%v", err)
	}
}

func (s *Server) startIndexNode() error {
	s.loopWg.Add(1)
	//TODO: How to make sure that grpc server has started successfully
	go s.grpcLoop()

	log.Println("IndexNode grpc server start successfully")

	err := registerNode()
	if err != nil {
		return err
	}

	indexnode.Params.Init()
	return nil
}

func (s *Server) Init() {
	indexnode.Params.Init()
	log.Println("IndexNode init successfully, nodeAddress=", indexnode.Params.NodeAddress)
}

func CreateIndexNode(ctx context.Context) (*Server, error) {

	return NewGrpcServer(ctx, indexnode.Params.NodeID), nil
}

func (s *Server) Start() error {
	s.Init()
	return s.startIndexNode()
}

func (s *Server) Stop() {
	s.loopWg.Wait()
}

func (s *Server) Close() {

	s.Stop()
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexCmd) (*commonpb.Status, error) {
	return s.node.BuildIndex(req)
}

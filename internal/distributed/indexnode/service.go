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
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"google.golang.org/grpc"
)

type Server struct {
	node typeutil.IndexNodeInterface

	grpcServer   *grpc.Server
	serverClient typeutil.IndexServiceInterface
	loopCtx      context.Context
	loopCancel   func()
	loopWg       sync.WaitGroup
}

func (s *Server) registerNode() error {

	log.Printf("Registering node. IP = %s, Port = %d", indexnode.Params.NodeIP, indexnode.Params.NodePort)

	request := &indexpb.RegisterNodeRequest{
		Base: nil,
		Address: &commonpb.Address{
			Ip:   indexnode.Params.NodeIP,
			Port: int64(indexnode.Params.NodePort),
		},
	}
	resp, err := s.serverClient.RegisterNode(request)
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

	err := s.registerNode()
	if err != nil {
		return err
	}

	indexnode.Params.Init()
	return s.node.Start()
}

func Init() error {
	indexnode.Params.Init()

	//Get native ip
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}

	for _, value := range addresses {
		if ipnet, ok := value.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				indexnode.Params.NodeIP = ipnet.IP.String()
				break
			}
		}
	}

	//Generate random and available port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return err
	}

	indexnode.Params.NodePort = listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	indexnode.Params.NodeAddress = indexnode.Params.NodeIP + ":" + strconv.FormatInt(int64(indexnode.Params.NodePort), 10)
	log.Println("IndexNode init successfully, nodeAddress=", indexnode.Params.NodeAddress)

	return nil
}

func (s *Server) Start() error {
	return s.startIndexNode()
}

func (s *Server) Stop() error {
	s.node.Stop()
	s.loopCancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.loopWg.Wait()

	return nil
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexCmd) (*commonpb.Status, error) {
	return s.node.BuildIndex(req)
}

func NewGrpcServer(ctx context.Context) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	indexServiceClient := serviceclient.NewClient(indexnode.Params.ServiceAddress)

	node, err := indexnode.CreateIndexNode(ctx1)
	if err != nil {
		defer cancel()
		return nil, err
	}

	node.SetServiceClient(indexServiceClient)

	return &Server{
		loopCtx:      ctx1,
		loopCancel:   cancel,
		node:         node,
		serverClient: indexServiceClient,
	}, nil
}

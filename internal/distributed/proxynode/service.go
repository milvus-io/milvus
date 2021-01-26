package grpcproxynode

import (
	"context"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/go-basic/ipv4"

	grpcproxyservice "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/proxynode"

	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"

	"google.golang.org/grpc"
)

type Server struct {
	ctx                 context.Context
	wg                  sync.WaitGroup
	impl                proxynode.ProxyNode
	grpcServer          *grpc.Server
	ip                  string
	port                int
	proxyServiceAddress string
	proxyServiceClient  *grpcproxyservice.Client
}

func CreateProxyNodeServer() (*Server, error) {
	return &Server{}, nil
}

func (s *Server) connectProxyService() error {
	proxynode.Params.Init()

	s.proxyServiceAddress = proxynode.Params.ProxyServiceAddress()
	s.proxyServiceClient = grpcproxyservice.NewClient(s.proxyServiceAddress)

	getAvailablePort := func() int {
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			panic(err)
		}
		defer listener.Close()

		return listener.Addr().(*net.TCPAddr).Port
	}
	getLocalIP := func() string {
		localIP := ipv4.LocalIP()
		host := os.Getenv("PROXY_NODE_HOST")
		// TODO: shall we write this to ParamTable?
		if len(host) <= 0 {
			return localIP
		}
		return host
	}
	s.ip = getLocalIP()
	s.port = getAvailablePort()

	request := &proxypb.RegisterNodeRequest{
		Address: &commonpb.Address{
			Ip:   s.ip,
			Port: int64(s.port),
		},
	}
	response, err := s.proxyServiceClient.RegisterNode(request)
	if err != nil {
		panic(err)
	}

	proxynode.Params.Save("_proxyID", strconv.Itoa(int(response.InitParams.NodeID)))

	for _, params := range response.InitParams.StartParams {
		proxynode.Params.Save(params.Key, params.Value)
	}

	return err
}

func (s *Server) Init() error {
	s.ctx = context.Background()
	var err error
	s.impl, err = proxynode.CreateProxyNodeImpl(s.ctx)
	if err != nil {
		return err
	}
	err = s.connectProxyService()
	if err != nil {
		return err
	}
	return s.impl.Init()
}

func (s *Server) Start() error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// TODO: use config
		lis, err := net.Listen("tcp", ":"+strconv.Itoa(s.port))
		if err != nil {
			panic(err)
		}

		s.grpcServer = grpc.NewServer()
		proxypb.RegisterProxyNodeServiceServer(s.grpcServer, s)
		milvuspb.RegisterMilvusServiceServer(s.grpcServer, s)
		if err = s.grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()

	return s.impl.Start()
}

func (s *Server) Stop() error {
	var err error

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	err = s.impl.Stop()
	if err != nil {
		return err
	}

	s.wg.Wait()

	return nil
}

func (s *Server) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return s.impl.InvalidateCollectionMetaCache(ctx, request)
}

func (s *Server) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.impl.CreateCollection(ctx, request)
}

func (s *Server) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.impl.DropCollection(ctx, request)
}

func (s *Server) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.impl.HasCollection(ctx, request)
}

func (s *Server) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.impl.LoadCollection(ctx, request)
}

func (s *Server) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.impl.ReleaseCollection(ctx, request)
}

func (s *Server) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.impl.DescribeCollection(ctx, request)
}

func (s *Server) GetCollectionStatistics(ctx context.Context, request *milvuspb.CollectionStatsRequest) (*milvuspb.CollectionStatsResponse, error) {
	return s.impl.GetCollectionStatistics(ctx, request)
}

func (s *Server) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	return s.impl.ShowCollections(ctx, request)
}

func (s *Server) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.impl.CreatePartition(ctx, request)
}

func (s *Server) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.impl.DropPartition(ctx, request)
}

func (s *Server) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.impl.HasPartition(ctx, request)
}

func (s *Server) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitonRequest) (*commonpb.Status, error) {
	return s.impl.LoadPartitions(ctx, request)
}

func (s *Server) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return s.impl.ReleasePartitions(ctx, request)
}

func (s *Server) GetPartitionStatistics(ctx context.Context, request *milvuspb.PartitionStatsRequest) (*milvuspb.PartitionStatsResponse, error) {
	return s.impl.GetPartitionStatistics(ctx, request)
}

func (s *Server) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	return s.impl.ShowPartitions(ctx, request)
}

func (s *Server) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.impl.CreateIndex(ctx, request)
}

func (s *Server) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.impl.DescribeIndex(ctx, request)
}

func (s *Server) GetIndexState(ctx context.Context, request *milvuspb.IndexStateRequest) (*milvuspb.IndexStateResponse, error) {
	return s.impl.GetIndexState(ctx, request)
}

func (s *Server) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.InsertResponse, error) {
	return s.impl.Insert(ctx, request)
}

func (s *Server) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return s.impl.Search(ctx, request)
}

func (s *Server) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*commonpb.Status, error) {
	return s.impl.Flush(ctx, request)
}

func (s *Server) GetDdChannel(ctx context.Context, request *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.impl.GetDdChannel(ctx, request)
}

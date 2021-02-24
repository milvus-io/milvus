package grpcproxynode

import (
	"context"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	grpcdataserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice/client"
	grpcindexserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	grpcmasterserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice/client"
	grpcproxyserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice/client"
	grpcqueryserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice/client"

	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/proxynode"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
)

type Server struct {
	ctx        context.Context
	wg         sync.WaitGroup
	impl       *proxynode.NodeImpl
	grpcServer *grpc.Server

	grpcErrChan chan error

	proxyServiceClient  *grpcproxyserviceclient.Client
	masterServiceClient *grpcmasterserviceclient.GrpcClient
	dataServiceClient   *grpcdataserviceclient.Client
	queryServiceClient  *grpcqueryserviceclient.Client
	indexServiceClient  *grpcindexserviceclient.Client

	tracer opentracing.Tracer
	closer io.Closer
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {

	var err error
	server := &Server{
		ctx:         ctx,
		grpcErrChan: make(chan error),
	}

	server.impl, err = proxynode.NewProxyNodeImpl(server.ctx, factory)
	if err != nil {
		return nil, err
	}
	return server, err
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()

	log.Println("network port: ", grpcPort)
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Printf("Server:failed to listen: %v", err)
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	s.grpcServer = grpc.NewServer()
	proxypb.RegisterProxyNodeServiceServer(s.grpcServer, s)
	milvuspb.RegisterMilvusServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Println("proxy node init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Println("proxy node start done ...")
	return nil
}

func (s *Server) init() error {
	var err error
	Params.Init()
	if !funcutil.CheckPortAvailable(Params.Port) {
		Params.Port = funcutil.GetAvailablePort()
	}
	Params.LoadFromEnv()
	Params.LoadFromArgs()

	Params.Address = Params.IP + ":" + strconv.FormatInt(int64(Params.Port), 10)

	log.Println("proxy host: ", Params.IP)
	log.Println("proxy port: ", Params.Port)
	log.Println("proxy address: ", Params.Address)

	defer func() {
		if err != nil {
			err2 := s.Stop()
			if err2 != nil {
				log.Println("Init failed, and Stop failed")
			}
		}
	}()

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	log.Println("create grpc server ...")
	if err != nil {
		return err
	}

	s.proxyServiceClient = grpcproxyserviceclient.NewClient(Params.ProxyServiceAddress)
	err = s.proxyServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetProxyServiceClient(s.proxyServiceClient)
	log.Println("set proxy service client ...")

	masterServiceAddr := Params.MasterAddress
	log.Println("master address: ", masterServiceAddr)
	timeout := 3 * time.Second
	s.masterServiceClient, err = grpcmasterserviceclient.NewClient(masterServiceAddr, timeout)
	if err != nil {
		return err
	}
	err = s.masterServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetMasterClient(s.masterServiceClient)
	log.Println("set master client ...")

	dataServiceAddr := Params.DataServiceAddress
	log.Println("data service address ...", dataServiceAddr)
	s.dataServiceClient = grpcdataserviceclient.NewClient(dataServiceAddr)
	err = s.dataServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetDataServiceClient(s.dataServiceClient)
	log.Println("set data service address ...")

	indexServiceAddr := Params.IndexServerAddress
	log.Println("index server address: ", indexServiceAddr)
	s.indexServiceClient = grpcindexserviceclient.NewClient(indexServiceAddr)
	err = s.indexServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetIndexServiceClient(s.indexServiceClient)
	log.Println("set index service client ...")

	queryServiceAddr := Params.QueryServiceAddress
	log.Println("query server address: ", queryServiceAddr)
	s.queryServiceClient, err = grpcqueryserviceclient.NewClient(queryServiceAddr, timeout)
	if err != nil {
		return err
	}
	err = s.queryServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetQueryServiceClient(s.queryServiceClient)
	log.Println("set query service client ...")

	proxynode.Params.Init()
	log.Println("init params done ...")
	proxynode.Params.NetworkPort = Params.Port
	proxynode.Params.IP = Params.IP
	proxynode.Params.NetworkAddress = Params.Address
	// for purpose of ID Allocator
	proxynode.Params.MasterAddress = Params.MasterAddress

	s.impl.UpdateStateCode(internalpb2.StateCode_INITIALIZING)

	if err := s.impl.Init(); err != nil {
		log.Println("impl init error: ", err)
		return err
	}

	return nil
}

func (s *Server) start() error {
	return s.impl.Start()
}

func (s *Server) Stop() error {
	var err error
	s.closer.Close()

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
	return s.impl.CreateCollection(request)
}

func (s *Server) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.impl.DropCollection(request)
}

func (s *Server) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.impl.HasCollection(request)
}

func (s *Server) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.impl.LoadCollection(request)
}

func (s *Server) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.impl.ReleaseCollection(request)
}

func (s *Server) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.impl.DescribeCollection(request)
}

func (s *Server) GetCollectionStatistics(ctx context.Context, request *milvuspb.CollectionStatsRequest) (*milvuspb.CollectionStatsResponse, error) {
	return s.impl.GetCollectionStatistics(request)
}

func (s *Server) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	return s.impl.ShowCollections(request)
}

func (s *Server) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.impl.CreatePartition(request)
}

func (s *Server) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.impl.DropPartition(request)
}

func (s *Server) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.impl.HasPartition(request)
}

func (s *Server) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitonRequest) (*commonpb.Status, error) {
	return s.impl.LoadPartitions(request)
}

func (s *Server) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return s.impl.ReleasePartitions(request)
}

func (s *Server) GetPartitionStatistics(ctx context.Context, request *milvuspb.PartitionStatsRequest) (*milvuspb.PartitionStatsResponse, error) {
	return s.impl.GetPartitionStatistics(request)
}

func (s *Server) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	return s.impl.ShowPartitions(request)
}

func (s *Server) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.impl.CreateIndex(request)
}

func (s *Server) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return s.impl.DropIndex(request)
}

func (s *Server) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.impl.DescribeIndex(request)
}

func (s *Server) GetIndexState(ctx context.Context, request *milvuspb.IndexStateRequest) (*milvuspb.IndexStateResponse, error) {
	return s.impl.GetIndexState(request)
}

func (s *Server) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.InsertResponse, error) {
	return s.impl.Insert(request)
}

func (s *Server) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return s.impl.Search(request)
}

func (s *Server) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*commonpb.Status, error) {
	return s.impl.Flush(request)
}

func (s *Server) GetDdChannel(ctx context.Context, request *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.impl.GetDdChannel(request)
}

func (s *Server) GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.PersistentSegmentInfoRequest) (*milvuspb.PersistentSegmentInfoResponse, error) {
	return s.impl.GetPersistentSegmentInfo(request)
}

func (s *Server) GetQuerySegmentInfo(ctx context.Context, request *milvuspb.QuerySegmentInfoRequest) (*milvuspb.QuerySegmentInfoResponse, error) {
	return s.impl.GetQuerySegmentInfo(request)

}

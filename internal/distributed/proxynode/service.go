package grpcproxynode

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"google.golang.org/grpc"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	grpcdataserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice/client"
	grpcindexserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	grpcmasterserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice/client"
	grpcproxyserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice/client"
	grpcqueryserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice/client"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/proxynode"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
)

const (
	GRPCMaxMagSize = 2 << 30
)

type Server struct {
	ctx        context.Context
	wg         sync.WaitGroup
	impl       *proxynode.ProxyNode
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

	server.impl, err = proxynode.NewProxyNode(server.ctx, factory)
	if err != nil {
		return nil, err
	}
	return server, err
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()

	log.Debug("proxynode", zap.Int("network port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Warn("proxynode", zap.String("Server:failed to listen:", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)),
		grpc.MaxRecvMsgSize(GRPCMaxMagSize))
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
	log.Debug("proxy node init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("proxy node start done ...")
	return nil
}

func (s *Server) init() error {
	ctx := context.Background()
	var err error
	Params.Init()
	if !funcutil.CheckPortAvailable(Params.Port) {
		Params.Port = funcutil.GetAvailablePort()
	}
	Params.LoadFromEnv()
	Params.LoadFromArgs()

	Params.Address = Params.IP + ":" + strconv.FormatInt(int64(Params.Port), 10)

	log.Debug("proxynode", zap.String("proxy host", Params.IP))
	log.Debug("proxynode", zap.Int("proxy port", Params.Port))
	log.Debug("proxynode", zap.String("proxy address", Params.Address))

	// TODO
	cfg := &config.Configuration{
		ServiceName: fmt.Sprintf("proxy_node ip: %s, port: %d", Params.IP, Params.Port),
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(tracer)
	s.closer = closer

	defer func() {
		if err != nil {
			err2 := s.Stop()
			if err2 != nil {
				log.Debug("Init failed, and Stop failed")
			}
		}
	}()

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	log.Debug("create grpc server ...")
	if err != nil {
		return err
	}

	s.proxyServiceClient = grpcproxyserviceclient.NewClient(Params.ProxyServiceAddress)
	err = s.proxyServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetProxyServiceClient(s.proxyServiceClient)
	log.Debug("set proxy service client ...")

	masterServiceAddr := Params.MasterAddress
	log.Debug("proxynode", zap.String("master address", masterServiceAddr))
	timeout := 3 * time.Second
	s.masterServiceClient, err = grpcmasterserviceclient.NewClient(masterServiceAddr, timeout)
	if err != nil {
		return err
	}
	err = s.masterServiceClient.Init()
	if err != nil {
		return err
	}
	err = funcutil.WaitForComponentHealthy(ctx, s.masterServiceClient, "MasterService", 100, time.Millisecond*200)

	if err != nil {
		panic(err)
	}
	s.impl.SetMasterClient(s.masterServiceClient)
	log.Debug("set master client ...")

	dataServiceAddr := Params.DataServiceAddress
	log.Debug("proxynode", zap.String("data service address", dataServiceAddr))
	s.dataServiceClient = grpcdataserviceclient.NewClient(dataServiceAddr)
	err = s.dataServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetDataServiceClient(s.dataServiceClient)
	log.Debug("set data service address ...")

	indexServiceAddr := Params.IndexServerAddress
	log.Debug("proxynode", zap.String("index server address", indexServiceAddr))
	s.indexServiceClient = grpcindexserviceclient.NewClient(indexServiceAddr)
	err = s.indexServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetIndexServiceClient(s.indexServiceClient)
	log.Debug("set index service client ...")

	queryServiceAddr := Params.QueryServiceAddress
	log.Debug("proxynode", zap.String("query server address", queryServiceAddr))
	s.queryServiceClient, err = grpcqueryserviceclient.NewClient(queryServiceAddr, timeout)
	if err != nil {
		return err
	}
	err = s.queryServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetQueryServiceClient(s.queryServiceClient)
	log.Debug("set query service client ...")

	proxynode.Params.Init()
	log.Debug("init params done ...")
	proxynode.Params.NetworkPort = Params.Port
	proxynode.Params.IP = Params.IP
	proxynode.Params.NetworkAddress = Params.Address
	// for purpose of ID Allocator
	proxynode.Params.MasterAddress = Params.MasterAddress

	s.impl.UpdateStateCode(internalpb2.StateCode_INITIALIZING)

	if err := s.impl.Init(); err != nil {
		log.Debug("proxynode", zap.String("impl init error", err.Error()))
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

func (s *Server) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return s.impl.DropIndex(ctx, request)
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

func (s *Server) GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.PersistentSegmentInfoRequest) (*milvuspb.PersistentSegmentInfoResponse, error) {
	return s.impl.GetPersistentSegmentInfo(ctx, request)
}

func (s *Server) GetQuerySegmentInfo(ctx context.Context, request *milvuspb.QuerySegmentInfoRequest) (*milvuspb.QuerySegmentInfoResponse, error) {
	return s.impl.GetQuerySegmentInfo(ctx, request)

}

func (s *Server) RegisterLink(ctx context.Context, empty *commonpb.Empty) (*milvuspb.RegisterLinkResponse, error) {
	return s.impl.RegisterLink(empty)
}

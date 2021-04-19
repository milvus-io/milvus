package grpcmasterservice

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice/client"
	isc "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	psc "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice/client"
	qsc "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice/client"
	"github.com/zilliztech/milvus-distributed/internal/log"
	cms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
)

// grpc wrapper
type Server struct {
	core        *cms.Core
	grpcServer  *grpc.Server
	grpcErrChan chan error

	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	proxyService *psc.Client
	dataService  *dsc.Client
	indexService *isc.Client
	queryService *qsc.Client

	connectProxyService bool
	connectDataService  bool
	connectIndexService bool
	connectQueryService bool

	closer io.Closer
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {

	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:                 ctx1,
		cancel:              cancel,
		grpcErrChan:         make(chan error),
		connectDataService:  true,
		connectProxyService: true,
		connectIndexService: true,
		connectQueryService: true,
	}

	//TODO
	cfg := &config.Configuration{
		ServiceName: "master_service",
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

	s.core, err = cms.NewCore(s.ctx, factory)
	if err != nil {
		return nil, err
	}
	return s, err
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
	ctx := context.Background()

	log.Debug("init params done")

	err := s.startGrpc()
	if err != nil {
		return err
	}

	s.core.UpdateStateCode(internalpb2.StateCode_INITIALIZING)

	if s.connectProxyService {
		log.Debug("proxy service", zap.String("address", Params.ProxyServiceAddress))
		proxyService := psc.NewClient(Params.ProxyServiceAddress)
		if err := proxyService.Init(); err != nil {
			panic(err)
		}

		err := funcutil.WaitForComponentInitOrHealthy(ctx, proxyService, "ProxyService", 100, 200*time.Millisecond)
		if err != nil {
			panic(err)
		}

		if err = s.core.SetProxyService(ctx, proxyService); err != nil {
			panic(err)
		}
	}
	if s.connectDataService {
		log.Debug("data service", zap.String("address", Params.DataServiceAddress))
		dataService := dsc.NewClient(Params.DataServiceAddress)
		if err := dataService.Init(); err != nil {
			panic(err)
		}
		if err := dataService.Start(); err != nil {
			panic(err)
		}
		err := funcutil.WaitForComponentInitOrHealthy(ctx, dataService, "DataService", 100, 200*time.Millisecond)
		if err != nil {
			panic(err)
		}

		if err = s.core.SetDataService(ctx, dataService); err != nil {
			panic(err)
		}
	}
	if s.connectIndexService {
		log.Debug("index service", zap.String("address", Params.IndexServiceAddress))
		indexService := isc.NewClient(Params.IndexServiceAddress)
		if err := indexService.Init(); err != nil {
			panic(err)
		}

		if err := s.core.SetIndexService(ctx, indexService); err != nil {
			panic(err)

		}
	}
	if s.connectQueryService {
		queryService, err := qsc.NewClient(Params.QueryServiceAddress, 5*time.Second)
		if err != nil {
			panic(err)
		}
		if err = queryService.Init(); err != nil {
			panic(err)
		}
		if err = queryService.Start(); err != nil {
			panic(err)
		}
		if err = s.core.SetQueryService(ctx, queryService); err != nil {
			panic(err)
		}
	}
	cms.Params.Init()
	log.Debug("grpc init done ...")

	if err := s.core.Init(); err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpc() error {
	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()

	log.Debug("start grpc ", zap.Int("port", grpcPort))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Error("GrpcServer:failed to listen", zap.String("error", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(grpc.UnaryInterceptor(
		otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)))
	masterpb.RegisterMasterServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) start() error {
	log.Debug("Master Core start ...")
	if err := s.core.Start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() error {
	if err := s.closer.Close(); err != nil {
		return err
	}
	if s.proxyService != nil {
		_ = s.proxyService.Stop()
	}
	if s.indexService != nil {
		_ = s.indexService.Stop()
	}
	if s.dataService != nil {
		_ = s.dataService.Stop()
	}
	if s.queryService != nil {
		_ = s.queryService.Stop()
	}
	if s.core != nil {
		return s.core.Stop()
	}
	s.cancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) GetComponentStatesRPC(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.core.GetComponentStates(ctx)
}

//DDL request
func (s *Server) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.core.CreateCollection(ctx, in)
}

func (s *Server) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.core.DropCollection(ctx, in)
}

func (s *Server) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.core.HasCollection(ctx, in)
}

func (s *Server) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.core.DescribeCollection(ctx, in)
}

func (s *Server) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	return s.core.ShowCollections(ctx, in)
}

func (s *Server) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.core.CreatePartition(ctx, in)
}

func (s *Server) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.core.DropPartition(ctx, in)
}

func (s *Server) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.core.HasPartition(ctx, in)
}

func (s *Server) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	return s.core.ShowPartitions(ctx, in)
}

//index builder service
func (s *Server) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.core.CreateIndex(ctx, in)
}

func (s *Server) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return s.core.DropIndex(ctx, in)
}

func (s *Server) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.core.DescribeIndex(ctx, in)
}

//global timestamp allocator
func (s *Server) AllocTimestamp(ctx context.Context, in *masterpb.TsoRequest) (*masterpb.TsoResponse, error) {
	return s.core.AllocTimestamp(ctx, in)
}

func (s *Server) AllocID(ctx context.Context, in *masterpb.IDRequest) (*masterpb.IDResponse, error) {
	return s.core.AllocID(ctx, in)
}

//receiver time tick from proxy service, and put it into this channel
func (s *Server) GetTimeTickChannelRPC(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.core.GetTimeTickChannel(ctx)
}

//receive ddl from rpc and time tick from proxy service, and put them into this channel
func (s *Server) GetDdChannelRPC(ctx context.Context, in *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.core.GetDdChannel(ctx)
}

//just define a channel, not used currently
func (s *Server) GetStatisticsChannelRPC(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.core.GetStatisticsChannel(ctx)
}

func (s *Server) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return s.core.DescribeSegment(ctx, in)
}

func (s *Server) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	return s.core.ShowSegments(ctx, in)
}

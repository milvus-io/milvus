package grpcqueryservice

import (
	"context"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice/client"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice/client"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	qs "github.com/zilliztech/milvus-distributed/internal/queryservice"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
)

type Server struct {
	wg         sync.WaitGroup
	loopCtx    context.Context
	loopCancel context.CancelFunc
	grpcServer *grpc.Server

	grpcErrChan chan error

	impl *qs.QueryService

	msFactory msgstream.Factory

	dataService   *dsc.Client
	masterService *msc.GrpcClient
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	svr, err := qs.NewQueryService(ctx1, factory)
	if err != nil {
		cancel()
		return nil, err
	}

	return &Server{
		impl:        svr,
		loopCtx:     ctx1,
		loopCancel:  cancel,
		msFactory:   factory,
		grpcErrChan: make(chan error),
	}, nil
}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("queryservice init done ...")

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	ctx := context.Background()
	Params.Init()

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	if err := <-s.grpcErrChan; err != nil {
		return err
	}

	// --- Master Server Client ---
	log.Debug("Master service", zap.String("address", Params.MasterAddress))
	log.Debug("Init master service client ...")

	masterService, err := msc.NewClient(Params.MasterAddress, 20*time.Second)

	if err != nil {
		panic(err)
	}

	if err = masterService.Init(); err != nil {
		panic(err)
	}

	if err = masterService.Start(); err != nil {
		panic(err)
	}
	// wait for master init or healthy
	err = funcutil.WaitForComponentInitOrHealthy(ctx, masterService, "MasterService", 100, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetMasterService(masterService); err != nil {
		panic(err)
	}

	// --- Data service client ---
	log.Debug("DataService", zap.String("Address", Params.DataServiceAddress))
	log.Debug("QueryService Init data service client ...")

	dataService := dsc.NewClient(Params.DataServiceAddress)
	if err = dataService.Init(); err != nil {
		panic(err)
	}
	if err = dataService.Start(); err != nil {
		panic(err)
	}
	err = funcutil.WaitForComponentInitOrHealthy(ctx, dataService, "DataService", 100, time.Millisecond*200)
	if err != nil {
		panic(err)
	}
	if err := s.SetDataService(dataService); err != nil {
		panic(err)
	}

	qs.Params.Init()
	s.impl.UpdateStateCode(internalpb2.StateCode_INITIALIZING)

	if err := s.impl.Init(); err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.wg.Done()

	log.Debug("network", zap.String("port", strconv.Itoa(grpcPort)))
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Debug("GrpcServer:failed to listen:", zap.String("error", err.Error()))
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)))
	querypb.RegisterQueryServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	return s.impl.Start()
}

func (s *Server) Stop() error {
	err := s.impl.Stop()
	s.loopCancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	return err
}

func (s *Server) GetComponentStates(ctx context.Context, req *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.impl.GetComponentStates(ctx)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.impl.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.impl.GetStatisticsChannel(ctx)
}

func (s *Server) SetMasterService(m qs.MasterServiceInterface) error {
	s.impl.SetMasterService(m)
	return nil
}

func (s *Server) SetDataService(d qs.DataServiceInterface) error {
	s.impl.SetDataService(d)
	return nil
}

func (s *Server) RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	return s.impl.RegisterNode(ctx, req)
}

func (s *Server) ShowCollections(ctx context.Context, req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error) {
	return s.impl.ShowCollections(ctx, req)
}

func (s *Server) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.impl.LoadCollection(ctx, req)
}

func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.impl.ReleaseCollection(ctx, req)
}

func (s *Server) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error) {
	return s.impl.ShowPartitions(ctx, req)
}

func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
	return s.impl.GetPartitionStates(ctx, req)
}

func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionRequest) (*commonpb.Status, error) {
	return s.impl.LoadPartitions(ctx, req)
}

func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return s.impl.ReleasePartitions(ctx, req)
}

func (s *Server) CreateQueryChannel(ctx context.Context, req *commonpb.Empty) (*querypb.CreateQueryChannelResponse, error) {
	return s.impl.CreateQueryChannel(ctx)
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return s.impl.GetSegmentInfo(ctx, req)
}

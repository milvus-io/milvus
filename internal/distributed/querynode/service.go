package grpcquerynode

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/retry"

	"github.com/zilliztech/milvus-distributed/internal/types"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice/client"
	isc "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice/client"
	qsc "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice/client"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	qn "github.com/zilliztech/milvus-distributed/internal/querynode"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type Server struct {
	impl        *qn.QueryNode
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	grpcErrChan chan error

	grpcServer *grpc.Server

	dataService   *dsc.Client
	masterService *msc.GrpcClient
	indexService  *isc.Client
	queryService  *qsc.Client

	closer io.Closer
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		impl:        qn.NewQueryNodeWithoutID(ctx, factory),
		grpcErrChan: make(chan error),
	}
	return s, nil
}

func (s *Server) init() error {
	ctx := context.Background()
	Params.Init()
	Params.LoadFromEnv()
	Params.LoadFromArgs()

	// TODO
	cfg := &config.Configuration{
		ServiceName: fmt.Sprintf("query_node ip: %s, port: %d", Params.QueryNodeIP, Params.QueryNodePort),
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

	log.Debug("QueryNode", zap.Int("port", Params.QueryNodePort))
	s.wg.Add(1)
	go s.startGrpcLoop(Params.QueryNodePort)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		return err
	}
	// --- QueryService ---
	log.Debug("QueryService", zap.String("address", Params.QueryServiceAddress))
	log.Debug("Init Query service client ...")
	queryService, err := qsc.NewClient(Params.QueryServiceAddress, 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = queryService.Init(); err != nil {
		panic(err)
	}

	if err = queryService.Start(); err != nil {
		panic(err)
	}

	err = funcutil.WaitForComponentInitOrHealthy(ctx, queryService, "QueryService", 100, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetQueryService(queryService); err != nil {
		panic(err)
	}

	// --- Master Server Client ---
	//ms.Params.Init()
	addr := Params.MasterAddress
	log.Debug("Master service", zap.String("address", addr))
	log.Debug("Init master service client ...")

	masterService, err := msc.NewClient(addr, 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = masterService.Init(); err != nil {
		panic(err)
	}

	if err = masterService.Start(); err != nil {
		panic(err)
	}

	err = funcutil.WaitForComponentHealthy(ctx, masterService, "MasterService", 100, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetMasterService(masterService); err != nil {
		panic(err)
	}

	// --- IndexService ---
	log.Debug("Index service", zap.String("address", Params.IndexServiceAddress))
	indexService := isc.NewClient(Params.IndexServiceAddress)

	if err := indexService.Init(); err != nil {
		panic(err)
	}

	if err := indexService.Start(); err != nil {
		panic(err)
	}
	// wait indexservice healthy
	err = funcutil.WaitForComponentHealthy(ctx, indexService, "IndexService", 100, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetIndexService(indexService); err != nil {
		panic(err)
	}

	// --- DataService ---
	log.Debug("Data service", zap.String("address", Params.DataServiceAddress))
	log.Debug("QueryNode Init data service client ...")

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

	qn.Params.Init()
	qn.Params.QueryNodeIP = Params.QueryNodeIP
	qn.Params.QueryNodePort = int64(Params.QueryNodePort)
	qn.Params.QueryNodeID = Params.QueryNodeID

	s.impl.UpdateStateCode(internalpb2.StateCode_Initializing)

	if err := s.impl.Init(); err != nil {
		log.Error("impl init error: ", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) start() error {
	return s.impl.Start()
}

func (s *Server) startGrpcLoop(grpcPort int) {
	defer s.wg.Done()

	var lis net.Listener
	var err error
	err = retry.Retry(10, 0, func() error {
		addr := ":" + strconv.Itoa(grpcPort)
		lis, err = net.Listen("tcp", addr)
		if err == nil {
			Params.QueryNodePort = lis.Addr().(*net.TCPAddr).Port
		} else {
			// set port=0 to get next available port
			grpcPort = 0
		}
		return err
	})
	if err != nil {
		log.Error("QueryNode GrpcServer:failed to listen", zap.Error(err))
		s.grpcErrChan <- err
		return
	}

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)))
	querypb.RegisterQueryNodeServer(s.grpcServer, s)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Debug("QueryNode Start Grpc Failed!!!!")
		s.grpcErrChan <- err
	}

}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Debug("QueryNode init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Debug("QueryNode start done ...")
	return nil
}

func (s *Server) Stop() error {
	if err := s.closer.Close(); err != nil {
		return err
	}

	s.cancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	err := s.impl.Stop()
	if err != nil {
		return err
	}
	s.wg.Wait()
	return nil
}

func (s *Server) SetMasterService(masterService types.MasterService) error {
	return s.impl.SetMasterService(masterService)
}

func (s *Server) SetQueryService(queryService types.QueryService) error {
	return s.impl.SetQueryService(queryService)
}

func (s *Server) SetIndexService(indexService types.IndexService) error {
	return s.impl.SetIndexService(indexService)
}

func (s *Server) SetDataService(dataService types.DataService) error {
	return s.impl.SetDataService(dataService)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, in *commonpb.Empty) (*milvuspb.StringResponse, error) {
	// ignore ctx and in
	channel, err := s.impl.GetTimeTickChannel()
	if err != nil {
		return nil, err
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: channel,
	}, nil
}

func (s *Server) GetStatsChannel(ctx context.Context, in *commonpb.Empty) (*milvuspb.StringResponse, error) {
	// ignore ctx and in
	channel, err := s.impl.GetStatisticsChannel()
	if err != nil {
		return nil, err
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: channel,
	}, nil
}

func (s *Server) GetComponentStates(ctx context.Context, in *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	// ignore ctx and in
	return s.impl.GetComponentStates()
}

func (s *Server) AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.impl.AddQueryChannel(in)
}

func (s *Server) RemoveQueryChannel(ctx context.Context, in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.impl.RemoveQueryChannel(in)
}

func (s *Server) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.impl.WatchDmChannels(in)
}

func (s *Server) LoadSegments(ctx context.Context, in *querypb.LoadSegmentRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.impl.LoadSegments(in)
}

func (s *Server) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.impl.ReleaseCollection(in)
}

func (s *Server) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.impl.ReleasePartitions(in)
}

func (s *Server) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.impl.ReleaseSegments(in)
}

func (s *Server) GetSegmentInfo(ctx context.Context, in *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return s.impl.GetSegmentInfo(in)
}

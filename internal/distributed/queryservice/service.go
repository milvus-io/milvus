package grpcqueryservice

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice/client"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice/client"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	qs "github.com/zilliztech/milvus-distributed/internal/queryservice"
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
	log.Println("queryservice init done ...")

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	Params.Init()

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	if err := <-s.grpcErrChan; err != nil {
		return err
	}

	// --- Master Server Client ---
	log.Println("Master service address:", Params.MasterAddress)
	log.Println("Init master service client ...")

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
	err = funcutil.WaitForComponentInitOrHealthy(masterService, "MasterService", 100, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetMasterService(masterService); err != nil {
		panic(err)
	}

	// --- Data service client ---
	log.Println("DataService Address:", Params.DataServiceAddress)
	log.Println("QueryService Init data service client ...")

	dataService := dsc.NewClient(Params.DataServiceAddress)
	if err = dataService.Init(); err != nil {
		panic(err)
	}
	if err = dataService.Start(); err != nil {
		panic(err)
	}
	err = funcutil.WaitForComponentInitOrHealthy(dataService, "DataService", 100, time.Millisecond*200)
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

	log.Println("network port: ", grpcPort)
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Printf("GrpcServer:failed to listen: %v", err)
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	s.grpcServer = grpc.NewServer()
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
	componentStates, err := s.impl.GetComponentStates()
	if err != nil {
		return &internalpb2.ComponentStates{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, err
	}

	return componentStates, nil
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *commonpb.Empty) (*milvuspb.StringResponse, error) {
	channel, err := s.impl.GetTimeTickChannel()
	if err != nil {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, err
	}

	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: channel,
	}, nil
}

func (s *Server) GetStatisticsChannel(ctx context.Context, req *commonpb.Empty) (*milvuspb.StringResponse, error) {
	statisticsChannel, err := s.impl.GetStatisticsChannel()
	if err != nil {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, err
	}

	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: statisticsChannel,
	}, nil
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
	return s.impl.RegisterNode(req)
}

func (s *Server) ShowCollections(ctx context.Context, req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error) {
	return s.impl.ShowCollections(req)
}

func (s *Server) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.impl.LoadCollection(req)
}

func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.impl.ReleaseCollection(req)
}

func (s *Server) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error) {
	return s.impl.ShowPartitions(req)
}

func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
	return s.impl.GetPartitionStates(req)
}

func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionRequest) (*commonpb.Status, error) {
	return s.impl.LoadPartitions(req)
}

func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return s.impl.ReleasePartitions(req)
}

func (s *Server) CreateQueryChannel(ctx context.Context, req *commonpb.Empty) (*querypb.CreateQueryChannelResponse, error) {
	return s.impl.CreateQueryChannel()
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return s.impl.GetSegmentInfo(req)
}

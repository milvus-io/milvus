package grpcqueryservice

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/queryservice"
)

type QueryService = queryservice.QueryService

type Server struct {
	grpcServer   *grpc.Server
	queryService *QueryService

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup
}

func (s *Server) Init() error {
	log.Println()
	initParams := queryservice.InitParams{
		Distributed: true,
	}
	s.InitParams(&initParams)
	s.queryService.Init()
	return nil
}

func (s *Server) InitParams(params *queryservice.InitParams) {
	s.queryService.InitParams(params)
}

func (s *Server) Start() error {
	s.Init()
	log.Println("start query service ...")
	s.loopWg.Add(1)
	go s.grpcLoop()
	s.queryService.Start()
	return nil
}

func (s *Server) Stop() error {
	s.queryService.Stop()
	s.loopCancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.loopWg.Wait()
	return nil
}

//func (s *Server) SetDataService(p querynode.DataServiceInterface) error {
//	c, ok := s.queryService
//	if !ok {
//		return errors.Errorf("set data service failed")
//	}
//	return c.SetDataService(p)
//}
//
//func (s *Server) SetIndexService(p querynode.IndexServiceInterface) error {
//	c, ok := s.core.(*cms.Core)
//	if !ok {
//		return errors.Errorf("set index service failed")
//	}
//	return c.SetIndexService(p)
//}

func (s *Server) GetComponentStates(ctx context.Context, req *commonpb.Empty) (*querypb.ComponentStatesResponse, error) {
	componentStates, err := s.queryService.GetComponentStates()
	if err != nil {
		return &querypb.ComponentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, err
	}

	return &querypb.ComponentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		States: componentStates,
	}, nil
}

func (s *Server) GetTimeTickChannel(ctx context.Context, req *commonpb.Empty) (*milvuspb.StringResponse, error) {
	channel, err := s.queryService.GetTimeTickChannel()
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
	statisticsChannel, err := s.queryService.GetStatisticsChannel()
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

func (s *Server) RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	return s.queryService.RegisterNode(req)
}

func (s *Server) ShowCollections(ctx context.Context, req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error) {
	return s.queryService.ShowCollections(req)
}

func (s *Server) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.queryService.LoadCollection(req)
}

func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.queryService.ReleaseCollection(req)
}

func (s *Server) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error) {
	return s.queryService.ShowPartitions(req)
}

func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
	return s.queryService.GetPartitionStates(req)
}

func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionRequest) (*commonpb.Status, error) {
	return s.queryService.LoadPartitions(req)
}

func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return s.queryService.ReleasePartitions(req)
}

func (s *Server) CreateQueryChannel(ctx context.Context, req *commonpb.Empty) (*querypb.CreateQueryChannelResponse, error) {
	return s.queryService.CreateQueryChannel()
}

func (s *Server) NewServer(ctx context.Context) *Server {
	ctx1, cancel := context.WithCancel(ctx)
	serviceInterface, err := queryservice.NewQueryService(ctx)
	if err != nil {
		log.Fatal(errors.New("create QueryService failed"))
	}

	return &Server{
		queryService: serviceInterface.(*QueryService),
		loopCtx:      ctx1,
		loopCancel:   cancel,
	}
}

func (s *Server) grpcLoop() {
	defer s.loopWg.Done()

	log.Println("Starting start query service Server")
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(queryservice.Params.Port))
	if err != nil {
		log.Fatalf("query service grpc server fatal error=%v", err)
	}

	s.grpcServer = grpc.NewServer()
	querypb.RegisterQueryServiceServer(s.grpcServer, s)

	log.Println("queryService's server register finished")
	if err = s.grpcServer.Serve(lis); err != nil {
		log.Fatalf("queryService grpc server fatal error=%v", err)
	}
	log.Println("query service grpc server starting...")
}

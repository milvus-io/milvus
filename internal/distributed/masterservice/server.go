package grpcmasterservice

import (
	"context"
	"log"
	"strconv"
	"time"

	"net"
	"sync"

	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice/client"
	isc "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	psc "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice/client"
	qsc "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice/client"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	cms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"google.golang.org/grpc"
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

	var err error
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
	log.Println("init params done")

	err := s.startGrpc()
	if err != nil {
		return err
	}

	s.core.UpdateStateCode(internalpb2.StateCode_INITIALIZING)

	if s.connectProxyService {
		log.Printf("proxy service address : %s", Params.ProxyServiceAddress)
		proxyService := psc.NewClient(Params.ProxyServiceAddress)
		if err := proxyService.Init(); err != nil {
			panic(err)
		}

		err := funcutil.WaitForComponentInitOrHealthy(proxyService, "ProxyService", 100, 200*time.Millisecond)
		if err != nil {
			panic(err)
		}

		if err = s.core.SetProxyService(proxyService); err != nil {
			panic(err)
		}
	}
	if s.connectDataService {
		log.Printf("data service address : %s", Params.DataServiceAddress)
		dataService := dsc.NewClient(Params.DataServiceAddress)
		if err := dataService.Init(); err != nil {
			panic(err)
		}
		if err := dataService.Start(); err != nil {
			panic(err)
		}
		err := funcutil.WaitForComponentInitOrHealthy(dataService, "DataService", 100, 200*time.Millisecond)
		if err != nil {
			panic(err)
		}

		if err = s.core.SetDataService(dataService); err != nil {
			panic(err)
		}
	}
	if s.connectIndexService {
		log.Printf("index service address : %s", Params.IndexServiceAddress)
		indexService := isc.NewClient(Params.IndexServiceAddress)
		if err := indexService.Init(); err != nil {
			panic(err)
		}

		if err := s.core.SetIndexService(indexService); err != nil {
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
		if err = s.core.SetQueryService(queryService); err != nil {
			panic(err)
		}
	}
	cms.Params.Init()
	log.Println("grpc init done ...")

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

	log.Println("network port: ", grpcPort)
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Printf("GrpcServer:failed to listen: %v", err)
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	s.grpcServer = grpc.NewServer()
	masterpb.RegisterMasterServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) start() error {
	log.Println("Master Core start ...")
	if err := s.core.Start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() error {
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
	return s.core.GetComponentStates()
}

//DDL request
func (s *Server) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.core.CreateCollection(in)
}

func (s *Server) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.core.DropCollection(in)
}

func (s *Server) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.core.HasCollection(in)
}

func (s *Server) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.core.DescribeCollection(in)
}

func (s *Server) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	return s.core.ShowCollections(in)
}

func (s *Server) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.core.CreatePartition(in)
}

func (s *Server) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.core.DropPartition(in)
}

func (s *Server) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.core.HasPartition(in)
}

func (s *Server) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	return s.core.ShowPartitions(in)
}

//index builder service
func (s *Server) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.core.CreateIndex(in)
}

func (s *Server) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return s.core.DropIndex(in)
}

func (s *Server) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.core.DescribeIndex(in)
}

//global timestamp allocator
func (s *Server) AllocTimestamp(ctx context.Context, in *masterpb.TsoRequest) (*masterpb.TsoResponse, error) {
	return s.core.AllocTimestamp(in)
}

func (s *Server) AllocID(ctx context.Context, in *masterpb.IDRequest) (*masterpb.IDResponse, error) {
	return s.core.AllocID(in)
}

//receiver time tick from proxy service, and put it into this channel
func (s *Server) GetTimeTickChannelRPC(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	rsp, err := s.core.GetTimeTickChannel()
	if err != nil {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Value: "",
		}, nil
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: rsp,
	}, nil
}

//receive ddl from rpc and time tick from proxy service, and put them into this channel
func (s *Server) GetDdChannelRPC(ctx context.Context, in *commonpb.Empty) (*milvuspb.StringResponse, error) {
	rsp, err := s.core.GetDdChannel()
	if err != nil {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Value: "",
		}, nil
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: rsp,
	}, nil
}

//just define a channel, not used currently
func (s *Server) GetStatisticsChannelRPC(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	rsp, err := s.core.GetStatisticsChannel()
	if err != nil {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Value: "",
		}, nil
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: rsp,
	}, nil
}

func (s *Server) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return s.core.DescribeSegment(in)
}

func (s *Server) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	return s.core.ShowSegments(in)
}

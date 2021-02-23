package grpcquerynode

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice/client"
	isc "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice/client"
	qsc "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice/client"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	qn "github.com/zilliztech/milvus-distributed/internal/querynode"
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

	Params.Init()
	Params.QueryNodePort = funcutil.GetAvailablePort()
	Params.LoadFromEnv()
	Params.LoadFromArgs()

	log.Println("QueryNode, port:", Params.QueryNodePort)
	s.wg.Add(1)
	go s.startGrpcLoop(Params.QueryNodePort)
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	if err != nil {
		return err
	}
	// --- QueryService ---
	log.Println("QueryService address:", Params.QueryServiceAddress)
	log.Println("Init Query service client ...")
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

	err = funcutil.WaitForComponentInitOrHealthy(queryService, "QueryService", 100, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetQueryService(queryService); err != nil {
		panic(err)
	}

	// --- Master Server Client ---
	//ms.Params.Init()
	addr := Params.MasterAddress
	log.Println("Master service address:", addr)
	log.Println("Init master service client ...")

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

	err = funcutil.WaitForComponentHealthy(masterService, "MasterService", 100, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetMasterService(masterService); err != nil {
		panic(err)
	}

	// --- IndexService ---
	log.Println("Index service address:", Params.IndexServiceAddress)
	indexService := isc.NewClient(Params.IndexServiceAddress)

	if err := indexService.Init(); err != nil {
		panic(err)
	}

	if err := indexService.Start(); err != nil {
		panic(err)
	}
	// wait indexservice healthy
	err = funcutil.WaitForComponentHealthy(indexService, "IndexService", 100, time.Millisecond*200)
	if err != nil {
		panic(err)
	}

	if err := s.SetIndexService(indexService); err != nil {
		panic(err)
	}

	// --- DataService ---
	log.Printf("Data service address: %s", Params.DataServiceAddress)
	log.Println("Querynode Init data service client ...")

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

	qn.Params.Init()
	qn.Params.QueryNodeIP = Params.QueryNodeIP
	qn.Params.QueryNodePort = int64(Params.QueryNodePort)
	qn.Params.QueryNodeID = Params.QueryNodeID

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

func (s *Server) startGrpcLoop(grpcPort int) {
	defer s.wg.Done()

	addr := ":" + strconv.Itoa(grpcPort)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("QueryNode GrpcServer:failed to listen: %v", err)
		s.grpcErrChan <- err
		return
	}
	log.Println("QueryNode:: addr:", addr)

	s.grpcServer = grpc.NewServer()
	querypb.RegisterQueryNodeServer(s.grpcServer, s)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Println("QueryNode Start Grpc Failed!!!!")
		s.grpcErrChan <- err
	}

}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Println("querynode init done ...")

	if err := s.start(); err != nil {
		return err
	}
	log.Println("querynode start done ...")
	return nil
}

func (s *Server) Stop() error {
	s.cancel()
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

func (s *Server) SetMasterService(master qn.MasterServiceInterface) error {
	return s.impl.SetMasterService(master)
}

func (s *Server) SetQueryService(query qn.QueryServiceInterface) error {
	return s.impl.SetQueryService(query)
}

func (s *Server) SetIndexService(index qn.IndexServiceInterface) error {
	return s.impl.SetIndexService(index)
}

func (s *Server) SetDataService(data qn.DataServiceInterface) error {
	return s.impl.SetDataService(data)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, in *commonpb.Empty) (*milvuspb.StringResponse, error) {
	// ignore ctx and in
	channel, err := s.impl.GetTimeTickChannel()
	if err != nil {
		return nil, err
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
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
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: channel,
	}, nil
}

func (s *Server) GetComponentStates(ctx context.Context, in *commonpb.Empty) (*querypb.ComponentStatesResponse, error) {
	// ignore ctx and in
	componentStates, err := s.impl.GetComponentStates()
	if err != nil {
		return nil, err
	}
	return &querypb.ComponentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		States: componentStates,
	}, nil
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

func (s *Server) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	// ignore ctx
	return s.impl.ReleaseSegments(in)
}

func (s *Server) GetSegmentInfo(ctx context.Context, in *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return s.impl.GetSegmentInfo(in)
}

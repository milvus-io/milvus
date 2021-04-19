package grpcdataserviceclient

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice/client"

	"github.com/zilliztech/milvus-distributed/internal/dataservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
)

type Server struct {
	ctx    context.Context
	cancel context.CancelFunc

	grpcErrChan chan error
	wg          sync.WaitGroup

	impl         *dataservice.Server
	grpcServer   *grpc.Server
	masterClient *msc.GrpcClient
}

func NewServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {

	ctx1, cancel := context.WithCancel(ctx)

	s := &Server{
		ctx:         ctx1,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}

	var err error
	s.impl, err = dataservice.CreateServer(s.ctx, factory)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) init() error {
	Params.Init()
	Params.LoadFromEnv()

	s.wg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	if err := <-s.grpcErrChan; err != nil {
		return err
	}

	log.Println("DataService:: MasterServicAddr:", Params.MasterAddress)
	client, err := msc.NewClient(Params.MasterAddress, 10*time.Second)
	if err != nil {
		panic(err)
	}
	log.Println("master client create complete")
	if err = client.Init(); err != nil {
		panic(err)
	}
	if err = client.Start(); err != nil {
		panic(err)
	}
	s.impl.UpdateStateCode(internalpb2.StateCode_INITIALIZING)

	err = funcutil.WaitForComponentInitOrHealthy(client, "MasterService", 100, time.Millisecond*200)

	if err != nil {
		panic(err)
	}
	s.impl.SetMasterClient(client)

	dataservice.Params.Init()
	if err := s.impl.Init(); err != nil {
		log.Println("impl init error: ", err)
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

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	s.grpcServer = grpc.NewServer()
	datapb.RegisterDataServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	return s.impl.Start()
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

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}
	log.Println("dataservice init done ...")

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) GetSegmentInfo(ctx context.Context, request *datapb.SegmentInfoRequest) (*datapb.SegmentInfoResponse, error) {
	return s.impl.GetSegmentInfo(request)
}

func (s *Server) RegisterNode(ctx context.Context, request *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return s.impl.RegisterNode(request)
}

func (s *Server) Flush(ctx context.Context, request *datapb.FlushRequest) (*commonpb.Status, error) {
	return s.impl.Flush(request)
}

func (s *Server) AssignSegmentID(ctx context.Context, request *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	return s.impl.AssignSegmentID(request)
}

func (s *Server) ShowSegments(ctx context.Context, request *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error) {
	return s.impl.ShowSegments(request)
}

func (s *Server) GetSegmentStates(ctx context.Context, request *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	return s.impl.GetSegmentStates(request)
}

func (s *Server) GetInsertBinlogPaths(ctx context.Context, request *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error) {
	return s.impl.GetInsertBinlogPaths(request)
}

func (s *Server) GetInsertChannels(ctx context.Context, request *datapb.InsertChannelRequest) (*internalpb2.StringList, error) {
	return s.impl.GetInsertChannels(request)
}

func (s *Server) GetCollectionStatistics(ctx context.Context, request *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error) {
	return s.impl.GetCollectionStatistics(request)
}

func (s *Server) GetPartitionStatistics(ctx context.Context, request *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error) {
	return s.impl.GetPartitionStatistics(request)
}

func (s *Server) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.impl.GetComponentStates()
}

func (s *Server) GetTimeTickChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.impl.GetTimeTickChannel()
}

func (s *Server) GetStatisticsChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.impl.GetStatisticsChannel()
}

func (s *Server) GetSegmentInfoChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.impl.GetSegmentInfoChannel()
}

func (s *Server) GetCount(ctx context.Context, request *datapb.CollectionCountRequest) (*datapb.CollectionCountResponse, error) {
	return s.impl.GetCount(request)
}

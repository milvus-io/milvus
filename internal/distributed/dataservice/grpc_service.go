package dataservice

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/dataservice"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type Service struct {
	server       *dataservice.Server
	ctx          context.Context
	cancel       context.CancelFunc
	grpcServer   *grpc.Server
	masterClient *masterservice.GrpcClient
}

func NewGrpcService() {
	s := &Service{}
	var err error
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if err = s.connectMaster(); err != nil {
		log.Fatal("connect to master" + err.Error())
	}
	s.server, err = dataservice.CreateServer(s.ctx, s.masterClient)
	if err != nil {
		log.Fatalf("create server error: %s", err.Error())
		return
	}
	s.grpcServer = grpc.NewServer()
	datapb.RegisterDataServiceServer(s.grpcServer, s)
	lis, err := net.Listen("tcp", "localhost:11111") // todo address
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	if err = s.grpcServer.Serve(lis); err != nil {
		log.Fatal(err.Error())
		return
	}
}

func (s *Service) connectMaster() error {
	log.Println("connecting to master")
	master, err := masterservice.NewGrpcClient("localhost:10101", 30*time.Second) // todo address
	if err != nil {
		return err
	}
	if err = master.Init(nil); err != nil {
		return err
	}
	if err = master.Start(); err != nil {
		return err
	}
	s.masterClient = master
	log.Println("connect to master success")
	return nil
}
func (s *Service) Init() error {
	return s.server.Init()
}

func (s *Service) Start() error {
	return s.server.Start()
}

func (s *Service) Stop() error {
	err := s.server.Stop()
	s.grpcServer.GracefulStop()
	s.cancel()
	return err
}

func (s *Service) RegisterNode(ctx context.Context, request *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return s.server.RegisterNode(request)
}

func (s *Service) Flush(ctx context.Context, request *datapb.FlushRequest) (*commonpb.Status, error) {
	return s.server.Flush(request)
}

func (s *Service) AssignSegmentID(ctx context.Context, request *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	return s.server.AssignSegmentID(request)
}

func (s *Service) ShowSegments(ctx context.Context, request *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error) {
	return s.server.ShowSegments(request)
}

func (s *Service) GetSegmentStates(ctx context.Context, request *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	return s.server.GetSegmentStates(request)
}

func (s *Service) GetInsertBinlogPaths(ctx context.Context, request *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error) {
	return s.server.GetInsertBinlogPaths(request)
}

func (s *Service) GetInsertChannels(ctx context.Context, request *datapb.InsertChannelRequest) (*internalpb2.StringList, error) {
	return s.server.GetInsertChannels(request)
}

func (s *Service) GetCollectionStatistics(ctx context.Context, request *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error) {
	return s.server.GetCollectionStatistics(request)
}

func (s *Service) GetPartitionStatistics(ctx context.Context, request *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error) {
	return s.server.GetPartitionStatistics(request)
}

func (s *Service) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.server.GetComponentStates()
}

func (s *Service) GetTimeTickChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.server.GetTimeTickChannel()
}

func (s *Service) GetStatisticsChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.server.GetStatisticsChannel()
}

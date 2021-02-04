package dataservice

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/dataservice"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type Service struct {
	server     *dataservice.Server
	ctx        context.Context
	grpcServer *grpc.Server
}

func (s *Service) GetSegmentInfo(ctx context.Context, request *datapb.SegmentInfoRequest) (*datapb.SegmentInfoResponse, error) {
	return s.server.GetSegmentInfo(request)
}

func NewGrpcService(ctx context.Context) *Service {
	s := &Service{}
	var err error
	s.ctx = ctx
	s.server, err = dataservice.CreateServer(s.ctx)
	if err != nil {
		log.Fatalf("create server error: %s", err.Error())
		return nil
	}
	return s
}

func (s *Service) SetMasterClient(masterClient dataservice.MasterClient) {
	s.server.SetMasterClient(masterClient)
}

func (s *Service) Init() error {
	var err error
	s.grpcServer = grpc.NewServer()
	datapb.RegisterDataServiceServer(s.grpcServer, s)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", dataservice.Params.Address, dataservice.Params.Port))
	if err != nil {
		return nil
	}
	c := make(chan struct{})
	go func() {
		if err2 := s.grpcServer.Serve(lis); err2 != nil {
			close(c)
			err = err2
		}
	}()
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
		break
	case <-c:
		return err
	}
	return s.server.Init()
}

func (s *Service) Start() error {
	return s.server.Start()
}

func (s *Service) Stop() error {
	err := s.server.Stop()
	s.grpcServer.GracefulStop()
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

func (s *Service) GetSegmentInfoChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.server.GetSegmentInfoChannel()
}

func (s *Service) GetCount(ctx context.Context, request *datapb.CollectionCountRequest) (*datapb.CollectionCountResponse, error) {
	return s.server.GetCount(request)
}

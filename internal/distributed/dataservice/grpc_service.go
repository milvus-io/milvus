package dataservice

import (
	"context"
	"fmt"
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
	grpcServer   *grpc.Server
	masterClient *masterservice.GrpcClient
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
		log.Fatal(err.Error())
		return nil
	}
	c := make(chan struct{})
	go func() {
		if err2 := s.grpcServer.Serve(lis); err2 != nil {
			log.Println(err.Error())
			close(c)
			err = err2
		}
	}()
	timer := time.NewTimer(1 * time.Second)
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
	resp := &internalpb2.StringList{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	channels, err := s.server.GetInsertChannels(request)
	if err != nil {
		resp.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	resp.Values = channels
	return resp, nil
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
	resp := &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	channel, err := s.server.GetTimeTickChannel()
	if err != nil {
		resp.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	resp.Value = channel
	return resp, nil
}

func (s *Service) GetStatisticsChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	resp := &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	channel, err := s.server.GetStatisticsChannel()
	if err != nil {
		resp.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	resp.Value = channel
	return resp, nil
}

func (s *Service) GetSegmentInfoChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	resp := &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	channel, err := s.server.GetSegmentInfoChannel()
	if err != nil {
		resp.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	resp.Value = channel
	return resp, nil
}

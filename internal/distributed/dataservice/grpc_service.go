package dataservice

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/dataservice"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type Service struct {
	server *dataservice.Server
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

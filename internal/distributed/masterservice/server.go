package masterservice

import (
	"context"
	"fmt"
	"net"
	"sync"

	cms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"google.golang.org/grpc"
)

// grpc wrapper
type GrpcServer struct {
	core       cms.Interface
	grpcServer *grpc.Server
	grpcError  error
	grpcErrMux sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

func NewGrpcServer() (*GrpcServer, error) {
	s := &GrpcServer{}
	var err error
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if s.core, err = cms.NewCore(s.ctx); err != nil {
		return nil, err
	}
	s.grpcServer = grpc.NewServer()
	s.grpcError = nil
	masterpb.RegisterMasterServiceServer(s.grpcServer, s)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cms.Params.Port))
	if err != nil {
		return nil, err
	}
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			s.grpcErrMux.Lock()
			defer s.grpcErrMux.Unlock()
			s.grpcError = err
		}
	}()

	s.grpcErrMux.Lock()
	err = s.grpcError
	s.grpcErrMux.Unlock()

	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *GrpcServer) Init(params *cms.InitParams) error {
	return s.core.Init(params)
}

func (s *GrpcServer) Start() error {
	return s.core.Start()
}

func (s *GrpcServer) Stop() error {
	err := s.core.Stop()
	s.cancel()
	s.grpcServer.GracefulStop()
	return err
}

func (s *GrpcServer) GetServiceStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ServiceStates, error) {
	return nil, nil
}

//DDL request
func (s *GrpcServer) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.core.CreateCollection(in)
}

func (s *GrpcServer) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.core.DropCollection(in)
}

func (s *GrpcServer) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.core.HasCollection(in)
}

func (s *GrpcServer) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.core.DescribeCollection(in)
}

func (s *GrpcServer) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	return s.core.ShowCollections(in)
}

func (s *GrpcServer) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.core.CreatePartition(in)
}

func (s *GrpcServer) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.core.DropPartition(in)
}

func (s *GrpcServer) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.core.HasPartition(in)
}

func (s *GrpcServer) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	return s.core.ShowPartitions(in)
}

//index builder service
func (s *GrpcServer) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.core.CreateIndex(in)
}

func (s *GrpcServer) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return s.core.DescribeIndex(in)
}

//global timestamp allocator
func (s *GrpcServer) AllocTimestamp(ctx context.Context, in *masterpb.TsoRequest) (*masterpb.TsoResponse, error) {
	return s.core.AllocTimestamp(in)
}

func (s *GrpcServer) AllocID(ctx context.Context, in *masterpb.IDRequest) (*masterpb.IDResponse, error) {
	return s.core.AllocID(in)
}

//receiver time tick from proxy service, and put it into this channel
func (s *GrpcServer) GetTimeTickChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.core.GetTimeTickChannel(empty)
}

//receive ddl from rpc and time tick from proxy service, and put them into this channel
func (s *GrpcServer) GetDdChannel(ctx context.Context, in *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.core.GetDdChannel(in)
}

//just define a channel, not used currently
func (s *GrpcServer) GetStatisticsChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.core.GetStatisticsChannel(empty)
}

func (s *GrpcServer) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return s.core.DescribeSegment(in)
}

func (s *GrpcServer) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	return s.core.ShowSegments(in)
}

//TODO, move to query node
func (s *GrpcServer) GetIndexState(ctx context.Context, request *milvuspb.IndexStateRequest) (*milvuspb.IndexStateResponse, error) {
	panic("implement me")
}

//TODO, move to data service
func (s *GrpcServer) AssignSegmentID(ctx context.Context, request *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	panic("implement me")
}

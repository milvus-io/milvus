package masterservice

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	cms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
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

func (s *GrpcServer) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func NewGrpcServer(ctx context.Context, factory msgstream.Factory) (*GrpcServer, error) {
	s := &GrpcServer{}
	var err error
	s.ctx, s.cancel = context.WithCancel(ctx)
	if s.core, err = cms.NewCore(s.ctx, factory); err != nil {
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

func (s *GrpcServer) Init() error {
	return s.core.Init()
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

func (s *GrpcServer) SetProxyService(p cms.ProxyServiceInterface) error {
	c, ok := s.core.(*cms.Core)
	if !ok {
		return errors.Errorf("set proxy service failed")
	}
	return c.SetProxyService(p)
}

func (s *GrpcServer) SetDataService(p cms.DataServiceInterface) error {
	c, ok := s.core.(*cms.Core)
	if !ok {
		return errors.Errorf("set data service failed")
	}
	return c.SetDataService(p)
}

func (s *GrpcServer) SetIndexService(p cms.IndexServiceInterface) error {
	c, ok := s.core.(*cms.Core)
	if !ok {
		return errors.Errorf("set index service failed")
	}
	return c.SetIndexService(p)
}

func (s *GrpcServer) SetQueryService(q cms.QueryServiceInterface) error {
	c, ok := s.core.(*cms.Core)
	if !ok {
		return errors.Errorf("set query service failed")
	}
	return c.SetQueryService(q)
}

func (s *GrpcServer) GetComponentStatesRPC(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.core.GetComponentStates()
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
func (s *GrpcServer) GetTimeTickChannelRPC(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
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
func (s *GrpcServer) GetDdChannelRPC(ctx context.Context, in *commonpb.Empty) (*milvuspb.StringResponse, error) {
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
func (s *GrpcServer) GetStatisticsChannelRPC(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
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

func (s *GrpcServer) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return s.core.DescribeSegment(in)
}

func (s *GrpcServer) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	return s.core.ShowSegments(in)
}

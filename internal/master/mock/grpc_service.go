package mockmaster

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/master/id"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

func (s *Master) CreateCollection(ctx context.Context, in *internalpb.CreateCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (s *Master) DropCollection(ctx context.Context, in *internalpb.DropCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (s *Master) HasCollection(ctx context.Context, in *internalpb.HasCollectionRequest) (*servicepb.BoolResponse, error) {
	return &servicepb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: false,
	}, nil
}

func (s *Master) DescribeCollection(ctx context.Context, in *internalpb.DescribeCollectionRequest) (*servicepb.CollectionDescription, error) {
	return &servicepb.CollectionDescription{
	}, nil
}

func (s *Master) ShowCollections(ctx context.Context, in *internalpb.ShowCollectionRequest) (*servicepb.StringListResponse, error) {
	return &servicepb.StringListResponse{
	}, nil
}

//////////////////////////////////////////////////////////////////////////
func (s *Master) CreatePartition(ctx context.Context, in *internalpb.CreatePartitionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (s *Master) DropPartition(ctx context.Context, in *internalpb.DropPartitionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (s *Master) HasPartition(ctx context.Context, in *internalpb.HasPartitionRequest) (*servicepb.BoolResponse, error) {

	return &servicepb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}, nil
}

func (s *Master) DescribePartition(ctx context.Context, in *internalpb.DescribePartitionRequest) (*servicepb.PartitionDescription, error) {
	return &servicepb.PartitionDescription{}, nil
}

func (s *Master) ShowPartitions(ctx context.Context,  in *internalpb.ShowPartitionRequest) (*servicepb.StringListResponse, error) {
	return &servicepb.StringListResponse{}, nil
}

//----------------------------------------Internal GRPC Service--------------------------------

func (s *Master) AllocTimestamp(ctx context.Context, request *internalpb.TsoRequest) (*internalpb.TsoResponse, error) {
	count := request.GetCount()
	ts, err := s.tsoAllocator.GenerateTSO(count)

	if err != nil {
		return &internalpb.TsoResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		}, err
	}

	response := &internalpb.TsoResponse{
		Status:    &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		Timestamp: ts,
		Count:     count,
	}

	return response, nil
}

func (s *Master) AllocId(ctx context.Context, request *internalpb.IdRequest) (*internalpb.IdResponse, error) {
	panic("implement me")
	count := request.GetCount()
	ts, err := id.AllocOne()

	if err != nil {
		return &internalpb.IdResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		}, err
	}

	response := &internalpb.IdResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		Id:     ts,
		Count:  count,
	}

	return response, nil
}
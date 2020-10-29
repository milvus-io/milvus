package master

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"time"
)


const slowThreshold = 5 * time.Millisecond



func (ms Master) CreateCollection(ctx context.Context, in *internalpb.CreateCollectionRequest) (*commonpb.Status, error) {
	var t task = &createCollectionTask{
		req: in,
		baseTask: baseTask{
			kvBase: &ms.kvBase,
			mt:     &ms.mt,
			cv:     make(chan int),
		},
	}

	var status = ms.scheduler.Enqueue(&t)
	if status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		err := errors.New("Enqueue failed")
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Enqueue failed",
		}, err
	}

	status = t.WaitToFinish(ctx)
	if status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		err := errors.New("WaitToFinish failed")
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "WaitToFinish failed",
		}, err
	}

	return &status, nil
}

func (ms Master) DropCollection(ctx context.Context, in *internalpb.DropCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (ms Master) HasCollection(ctx context.Context, in *internalpb.HasCollectionRequest) (*servicepb.BoolResponse, error) {
	return &servicepb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
		Value: true,
	}, nil
}

func (ms Master) DescribeCollection(ctx context.Context, in *internalpb.DescribeCollectionRequest) (*servicepb.CollectionDescription, error) {
	return &servicepb.CollectionDescription{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	}, nil
}

func (ms Master) ShowCollections(ctx context.Context, in *internalpb.ShowCollectionRequest) (*servicepb.StringListResponse, error) {
	return &servicepb.StringListResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	}, nil
}

func (ms Master) CreatePartition(ctx context.Context, in *internalpb.CreatePartitionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (ms Master) DropPartition(ctx context.Context, in *internalpb.DropPartitionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (ms Master) HasPartition(ctx context.Context, in *internalpb.HasPartitionRequest) (*servicepb.BoolResponse, error) {
	return &servicepb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
		Value: true,
	}, nil
}

func (ms Master) DescribePartition(ctx context.Context, in *internalpb.DescribePartitionRequest) (*servicepb.PartitionDescription, error) {
	return &servicepb.PartitionDescription{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	}, nil
}

func (ms Master) ShowPartitions(ctx context.Context, in *internalpb.ShowPartitionRequest) (*servicepb.StringListResponse, error) {
	return &servicepb.StringListResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	}, nil
}


//----------------------------------------Internal GRPC Service--------------------------------

// Tso implements gRPC PDServer.
func (ms *Master) Tso(stream masterpb.Master_TsoServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		start := time.Now()

		count := request.GetCount()
		ts, err := ms.tsoAllocator.GenerateTSO(count)
		if err != nil {
			return status.Errorf(codes.Unknown, err.Error())
		}

		elapsed := time.Since(start)
		if elapsed > slowThreshold {
			log.Println("get timestamp too slow", zap.Duration("cost", elapsed))
		}
		response := &internalpb.TsoResponse{
			Status:    &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

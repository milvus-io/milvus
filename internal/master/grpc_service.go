package master

import (
	"context"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

const slowThreshold = 5 * time.Millisecond

func (s *Master) CreateCollection(ctx context.Context, in *internalpb.CreateCollectionRequest) (*commonpb.Status, error) {
	var t task = &createCollectionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
	}

	response := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
	}

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		response.Reason = "Enqueue failed: " + err.Error()
		return response, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		response.Reason = "Create collection failed: " + err.Error()
		return response, nil
	}

	response.ErrorCode = commonpb.ErrorCode_SUCCESS
	return response, nil
}

func (s *Master) DropCollection(ctx context.Context, in *internalpb.DropCollectionRequest) (*commonpb.Status, error) {
	var t task = &dropCollectionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
	}

	response := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
	}

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		response.Reason = "Enqueue failed: " + err.Error()
		return response, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		response.Reason = "Drop collection failed: " + err.Error()
		return response, nil
	}

	response.ErrorCode = commonpb.ErrorCode_SUCCESS
	return response, nil
}

func (s *Master) HasCollection(ctx context.Context, in *internalpb.HasCollectionRequest) (*servicepb.BoolResponse, error) {
	var t task = &hasCollectionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		hasCollection: false,
	}

	st := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
	}

	response := &servicepb.BoolResponse{
		Status: st,
		Value:  false,
	}

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		st.Reason = "Enqueue failed: " + err.Error()
		return response, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		st.Reason = "Has collection failed: " + err.Error()
		return response, nil
	}

	st.ErrorCode = commonpb.ErrorCode_SUCCESS
	response.Value = t.(*hasCollectionTask).hasCollection
	return response, nil
}

func (s *Master) DescribeCollection(ctx context.Context, in *internalpb.DescribeCollectionRequest) (*servicepb.CollectionDescription, error) {
	var t task = &describeCollectionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		description: nil,
	}

	response := &servicepb.CollectionDescription{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
		Schema: nil,
	}

	t.(*describeCollectionTask).description = response

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		response.Status.Reason = "Enqueue failed: " + err.Error()
		return response, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		response.Status.Reason = "Describe collection failed: " + err.Error()
		return response, nil
	}

	response.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	return response, nil
}

func (s *Master) ShowCollections(ctx context.Context, in *internalpb.ShowCollectionRequest) (*servicepb.StringListResponse, error) {
	var t task = &showCollectionsTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		stringListResponse: nil,
	}

	response := &servicepb.StringListResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "",
		},
		Values: nil,
	}

	t.(*showCollectionsTask).stringListResponse = response

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		response.Status.Reason = "Enqueue filed: " + err.Error()
		return response, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		response.Status.Reason = "Show Collections failed: " + err.Error()
		return response, nil
	}

	response.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	return response, nil
}

//////////////////////////////////////////////////////////////////////////
func (s *Master) CreatePartition(ctx context.Context, in *internalpb.CreatePartitionRequest) (*commonpb.Status, error) {
	var t task = &createPartitionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
	}

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Enqueue failed",
		}, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "WaitToFinish failed",
		}, nil
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (s *Master) DropPartition(ctx context.Context, in *internalpb.DropPartitionRequest) (*commonpb.Status, error) {
	var t task = &dropPartitionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
	}

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Enqueue failed",
		}, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "WaitToFinish failed",
		}, nil
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (s *Master) HasPartition(ctx context.Context, in *internalpb.HasPartitionRequest) (*servicepb.BoolResponse, error) {
	var t task = &hasPartitionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		hasPartition: false,
	}

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		return &servicepb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "Enqueue failed",
			},
			Value: t.(*hasPartitionTask).hasPartition,
		}, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		return &servicepb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Value: t.(*hasPartitionTask).hasPartition,
		}, nil
	}

	return &servicepb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: t.(*hasPartitionTask).hasPartition,
	}, nil
}

func (s *Master) DescribePartition(ctx context.Context, in *internalpb.DescribePartitionRequest) (*servicepb.PartitionDescription, error) {
	var t task = &describePartitionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		description: nil,
	}

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		return &servicepb.PartitionDescription{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "Enqueue failed",
			},
			Name:       in.PartitionName,
			Statistics: nil,
		}, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		return &servicepb.PartitionDescription{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "WaitToFinish failed",
			},
			Name:       in.PartitionName,
			Statistics: nil,
		}, nil
	}

	return t.(*describePartitionTask).description, nil
}

func (s *Master) ShowPartitions(ctx context.Context, in *internalpb.ShowPartitionRequest) (*servicepb.StringListResponse, error) {
	var t task = &showPartitionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		stringListResponse: nil,
	}

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		return &servicepb.StringListResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "Enqueue failed",
			},
			Values: nil,
		}, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		return &servicepb.StringListResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "WaitToFinish failed",
			},
			Values: nil,
		}, nil
	}

	return t.(*showPartitionTask).stringListResponse, nil
}

//----------------------------------------Internal GRPC Service--------------------------------

func (s *Master) AllocTimestamp(ctx context.Context, request *internalpb.TsoRequest) (*internalpb.TsoResponse, error) {
	count := request.GetCount()
	ts, err := s.tsoAllocator.Alloc(count)

	if err != nil {
		return &internalpb.TsoResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		}, nil
	}

	response := &internalpb.TsoResponse{
		Status:    &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
		Timestamp: ts,
		Count:     count,
	}

	return response, nil
}

func (s *Master) AllocID(ctx context.Context, request *internalpb.IDRequest) (*internalpb.IDResponse, error) {
	count := request.GetCount()
	ts, err := s.idAllocator.AllocOne()

	if err != nil {
		return &internalpb.IDResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		}, nil
	}

	response := &internalpb.IDResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
		ID:     ts,
		Count:  count,
	}

	return response, nil
}

func (s *Master) AssignSegmentID(ctx context.Context, request *internalpb.AssignSegIDRequest) (*internalpb.AssignSegIDResponse, error) {
	segInfos, err := s.segmentMgr.AssignSegmentID(request.GetPerChannelReq())
	if err != nil {
		return &internalpb.AssignSegIDResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		}, nil
	}
	ts, err := s.tsoAllocator.AllocOne()
	if err != nil {
		return &internalpb.AssignSegIDResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		}, nil
	}
	return &internalpb.AssignSegIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Timestamp:            ts,
		ExpireDuration:       10000,
		PerChannelAssignment: segInfos,
	}, nil
}

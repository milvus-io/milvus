package master

import (
	"context"
	"fmt"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

const slowThreshold = 5 * time.Millisecond

func (s *Master) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
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

func (s *Master) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	var t task = &dropCollectionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		segManager: s.segmentManager,
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

func (s *Master) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
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

	response := &milvuspb.BoolResponse{
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

func (s *Master) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	var t task = &describeCollectionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		description: nil,
	}

	response := &milvuspb.DescribeCollectionResponse{
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

func (s *Master) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	var t task = &showCollectionsTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		stringListResponse: nil,
	}

	response := &milvuspb.ShowCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "",
		},
		CollectionNames: nil,
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
func (s *Master) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
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

func (s *Master) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
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

func (s *Master) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
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
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "Enqueue failed",
			},
			Value: t.(*hasPartitionTask).hasPartition,
		}, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Value: t.(*hasPartitionTask).hasPartition,
		}, nil
	}

	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: t.(*hasPartitionTask).hasPartition,
	}, nil
}

func (s *Master) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	var t task = &showPartitionTask{
		req: in,
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		resp: nil,
	}

	var err = s.scheduler.Enqueue(t)
	if err != nil {
		return &milvuspb.ShowPartitionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "Enqueue failed",
			},
			PartitionNames: nil,
		}, nil
	}

	err = t.WaitToFinish(ctx)
	if err != nil {
		return &milvuspb.ShowPartitionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "WaitToFinish failed",
			},
			PartitionNames: nil,
		}, nil
	}

	return t.(*showPartitionTask).resp, nil
}

//----------------------------------------Internal GRPC Service--------------------------------

func (s *Master) AllocTimestamp(ctx context.Context, request *masterpb.TsoRequest) (*masterpb.TsoResponse, error) {
	count := request.GetCount()
	ts, err := s.tsoAllocator.Alloc(count)

	if err != nil {
		return &masterpb.TsoResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		}, nil
	}

	response := &masterpb.TsoResponse{
		Status:    &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
		Timestamp: ts,
		Count:     count,
	}

	return response, nil
}

func (s *Master) AllocID(ctx context.Context, request *masterpb.IDRequest) (*masterpb.IDResponse, error) {
	count := request.GetCount()
	ts, err := s.idAllocator.AllocOne()

	if err != nil {
		return &masterpb.IDResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		}, nil
	}

	response := &masterpb.IDResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
		ID:     ts,
		Count:  count,
	}

	return response, nil
}

func (s *Master) AssignSegmentID(ctx context.Context, request *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	segInfos, _ := s.segmentManager.AssignSegment(request.SegIDRequests)
	return &datapb.AssignSegIDResponse{
		SegIDAssignments: segInfos,
	}, nil
}

func (s *Master) CreateIndex(ctx context.Context, req *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
	}
	task := &createIndexTask{
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		req:                 req,
		indexBuildScheduler: s.indexBuildSch,
		indexLoadScheduler:  s.indexLoadSch,
		segManager:          s.segmentManager,
	}

	err := s.scheduler.Enqueue(task)
	if err != nil {
		ret.Reason = "Enqueue failed: " + err.Error()
		return ret, nil
	}

	err = task.WaitToFinish(ctx)
	if err != nil {
		ret.Reason = "Create Index error: " + err.Error()
		return ret, nil
	}

	ret.ErrorCode = commonpb.ErrorCode_SUCCESS
	return ret, nil
}

func (s *Master) DescribeIndex(ctx context.Context, req *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	resp := &milvuspb.DescribeIndexResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
		//CollectionName: req.CollectionName,
		//FieldName:      req.FieldName,
	}
	//resp.
	task := &describeIndexTask{
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		req:  req,
		resp: resp,
	}

	if err := s.scheduler.Enqueue(task); err != nil {
		task.resp.Status.Reason = fmt.Sprintf("Enqueue failed: %s", err.Error())
		return task.resp, nil
	}

	if err := task.WaitToFinish(ctx); err != nil {
		task.resp.Status.Reason = fmt.Sprintf("Describe Index failed: %s", err.Error())
		return task.resp, nil
	}

	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	return task.resp, nil

}

func (s *Master) GetIndexState(ctx context.Context, req *milvuspb.IndexStateRequest) (*milvuspb.IndexStateResponse, error) {
	resp := &milvuspb.IndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
		State: commonpb.IndexState_NONE,
	}
	task := &getIndexStateTask{
		baseTask: baseTask{
			sch: s.scheduler,
			mt:  s.metaTable,
			cv:  make(chan error),
		},
		req:          req,
		resp:         resp,
		runtimeStats: s.runtimeStats,
	}

	if err := s.scheduler.Enqueue(task); err != nil {
		task.resp.Status.Reason = "Enqueue failed :" + err.Error()
		return task.resp, nil
	}

	if err := task.WaitToFinish(ctx); err != nil {
		resp.Status.Reason = "Describe index progress failed:" + err.Error()
		return task.resp, nil
	}

	task.resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	return task.resp, nil
}

func (s *Master) GetCollectionStatistics(ctx context.Context, request *milvuspb.CollectionStatsRequest) (*milvuspb.CollectionStatsResponse, error) {
	panic("implement me")
}

func (s *Master) GetPartitionStatistics(ctx context.Context, request *milvuspb.PartitionStatsRequest) (*milvuspb.PartitionStatsResponse, error) {
	panic("implement me")
}

func (s *Master) GetServiceStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (s *Master) GetTimeTickChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (s *Master) GetStatisticsChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (s *Master) DescribeSegment(ctx context.Context, request *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	panic("implement me")
}

func (s *Master) ShowSegments(ctx context.Context, request *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	panic("implement me")
}

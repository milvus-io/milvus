package dataservice

import (
	"fmt"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"golang.org/x/net/context"
)

func (ds *Server) RegisterNode(context.Context, *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return nil, nil
}
func (ds *Server) Flush(context.Context, *datapb.FlushRequest) (*commonpb.Status, error) {
	return nil, nil
}
func (ds *Server) AssignSegmentID(ctx context.Context, request *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	resp := &datapb.AssignSegIDResponse{
		SegIDAssignments: make([]*datapb.SegIDAssignment, 0),
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	task := &allocateTask{
		baseTask: baseTask{
			sch:  ds.scheduler,
			meta: ds.meta,
			cv:   make(chan error),
		},
		req:           request,
		resp:          resp,
		segAllocator:  ds.segAllocator,
		insertCMapper: ds.insertCMapper,
	}

	if err := ds.scheduler.Enqueue(task); err != nil {
		resp.Status.Reason = fmt.Sprintf("enqueue error: %s", err.Error())
		return resp, nil
	}

	if err := task.WaitToFinish(ctx); err != nil {
		resp.Status.Reason = fmt.Sprintf("wait to finish error: %s", err.Error())
		return resp, nil
	}
	return resp, nil
}
func (ds *Server) ShowSegments(context.Context, *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error) {
	return nil, nil

}
func (ds *Server) GetSegmentStates(context.Context, *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	return nil, nil

}
func (ds *Server) GetInsertBinlogPaths(context.Context, *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error) {
	return nil, nil

}
func (ds *Server) GetInsertChannels(context.Context, *datapb.InsertChannelRequest) (*internalpb2.StringList, error) {
	return nil, nil

}

package viewquery

import (
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func searchExecutionScope(tasks []qnview.QNSearchSegmentTask) (*segments.Collection, []segments.Segment, []qnview.SealedSegmentHandle, error) {
	handles := make([]qnview.SealedSegmentHandle, 0, len(tasks))
	for _, task := range tasks {
		handles = append(handles, task.Handle)
	}
	return segmentExecutionScope(handles)
}

func queryExecutionScope(tasks []qnview.QNQuerySegmentTask) (*segments.Collection, []segments.Segment, []qnview.SealedSegmentHandle, error) {
	handles := make([]qnview.SealedSegmentHandle, 0, len(tasks))
	for _, task := range tasks {
		handles = append(handles, task.Handle)
	}
	return segmentExecutionScope(handles)
}

func segmentExecutionScope(handles []qnview.SealedSegmentHandle) (*segments.Collection, []segments.Segment, []qnview.SealedSegmentHandle, error) {
	selected := make([]segments.Segment, 0, len(handles))
	var collection *segments.Collection
	for _, handle := range handles {
		readable, ok := handle.Segment().(qnview.ReadableSealedSegment)
		if !ok {
			return nil, nil, nil, merr.WrapErrServiceInternalMsg("querynode QueryView segment %d is not readable", handle.ID())
		}
		if collection == nil {
			collection = readable.Collection()
		}
		selected = append(selected, readable.QuerySegment())
	}
	return collection, selected, handles, nil
}

func segmentIDsFromHandles(handles []qnview.SealedSegmentHandle) []int64 {
	segmentIDs := make([]int64, 0, len(handles))
	for _, handle := range handles {
		segmentIDs = append(segmentIDs, handle.ID())
	}
	return segmentIDs
}

func dmlChannelsFromHandles(handles []qnview.SealedSegmentHandle) []string {
	seen := make(map[string]struct{}, 1)
	channels := make([]string, 0, 1)
	for _, handle := range handles {
		channel := handle.Segment().VChannel()
		if _, ok := seen[channel]; ok {
			continue
		}
		seen[channel] = struct{}{}
		channels = append(channels, channel)
	}
	return channels
}

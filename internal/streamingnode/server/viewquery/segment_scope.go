package viewquery

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/util/segcore"
)

func searchExecutionScope(tasks []snview.SNSearchSegmentTask) (*segcore.CCollection, []segcore.CSegment, []snview.GrowingSegmentHandle) {
	handles := make([]snview.GrowingSegmentHandle, 0, len(tasks))
	for _, task := range tasks {
		handles = append(handles, task.Handle)
	}
	return segmentExecutionScope(handles)
}

func queryExecutionScope(tasks []snview.SNQuerySegmentTask) (*segcore.CCollection, []segcore.CSegment, []snview.GrowingSegmentHandle) {
	handles := make([]snview.GrowingSegmentHandle, 0, len(tasks))
	for _, task := range tasks {
		handles = append(handles, task.Handle)
	}
	return segmentExecutionScope(handles)
}

func segmentExecutionScope(handles []snview.GrowingSegmentHandle) (*segcore.CCollection, []segcore.CSegment, []snview.GrowingSegmentHandle) {
	selected := make([]segcore.CSegment, 0, len(handles))
	var collection *segcore.CCollection
	for _, handle := range handles {
		if collection == nil {
			collection = handle.Collection()
		}
		selected = append(selected, handle.Segment())
	}
	return collection, selected, handles
}

func segmentIDsFromHandles(handles []snview.GrowingSegmentHandle) []int64 {
	segmentIDs := make([]int64, 0, len(handles))
	for _, handle := range handles {
		segmentIDs = append(segmentIDs, handle.ID())
	}
	return segmentIDs
}

func dmlChannelsFromVChannel(vchannel string) []string {
	if vchannel == "" {
		return nil
	}
	return []string{vchannel}
}

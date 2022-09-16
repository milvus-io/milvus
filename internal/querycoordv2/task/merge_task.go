package task

import (
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type MergeableTask[K comparable, R any] interface {
	ID() K
	Merge(other MergeableTask[K, R])
}

var _ MergeableTask[segmentIndex, *querypb.LoadSegmentsRequest] = (*LoadSegmentsTask)(nil)

type segmentIndex struct {
	NodeID       int64
	CollectionID int64
	Shard        string
}

type LoadSegmentsTask struct {
	tasks []*SegmentTask
	steps []int
	req   *querypb.LoadSegmentsRequest
}

func NewLoadSegmentsTask(task *SegmentTask, step int, req *querypb.LoadSegmentsRequest) *LoadSegmentsTask {
	return &LoadSegmentsTask{
		tasks: []*SegmentTask{task},
		steps: []int{step},
		req:   req,
	}
}

func (task *LoadSegmentsTask) ID() segmentIndex {
	return segmentIndex{
		NodeID:       task.req.GetDstNodeID(),
		CollectionID: task.req.GetCollectionID(),
		Shard:        task.req.GetInfos()[0].GetInsertChannel(),
	}
}

func (task *LoadSegmentsTask) Merge(other MergeableTask[segmentIndex, *querypb.LoadSegmentsRequest]) {
	otherTask := other.(*LoadSegmentsTask)
	task.tasks = append(task.tasks, otherTask.tasks...)
	task.steps = append(task.steps, otherTask.steps...)
	task.req.Infos = append(task.req.Infos, otherTask.req.GetInfos()...)
	deltaPositions := make(map[string]*internalpb.MsgPosition)
	for _, position := range task.req.DeltaPositions {
		deltaPositions[position.GetChannelName()] = position
	}
	for _, position := range otherTask.req.GetDeltaPositions() {
		merged, ok := deltaPositions[position.GetChannelName()]
		if !ok || merged.GetTimestamp() > position.GetTimestamp() {
			merged = position
		}
		deltaPositions[position.GetChannelName()] = merged
	}
	task.req.DeltaPositions = make([]*internalpb.MsgPosition, 0, len(deltaPositions))
	for _, position := range deltaPositions {
		task.req.DeltaPositions = append(task.req.DeltaPositions, position)
	}
}

func (task *LoadSegmentsTask) Result() *querypb.LoadSegmentsRequest {
	return task.req
}

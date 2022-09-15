package session

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var _ typeutil.MergeableTask[segmentIndex, *commonpb.Status] = (*LoadSegmentsTask)(nil)

type LoadSegmentsTask struct {
	doneCh  chan struct{}
	cluster *QueryCluster
	nodeID  int64
	req     *querypb.LoadSegmentsRequest
	result  *commonpb.Status
	err     error
}

func NewLoadSegmentsTask(cluster *QueryCluster, nodeID int64, req *querypb.LoadSegmentsRequest) *LoadSegmentsTask {
	return &LoadSegmentsTask{
		doneCh:  make(chan struct{}),
		cluster: cluster,
		nodeID:  nodeID,
		req:     req,
	}
}

func (task *LoadSegmentsTask) ID() segmentIndex {
	return segmentIndex{
		NodeID:       task.nodeID,
		CollectionID: task.req.GetCollectionID(),
		Shard:        task.req.GetInfos()[0].GetInsertChannel(),
	}
}

func (task *LoadSegmentsTask) Execute() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := task.cluster.loadSegments(ctx, task.nodeID, task.req)
	if err != nil {
		task.err = err
		return err
	}
	task.result = status
	return nil
}

func (task *LoadSegmentsTask) Merge(other typeutil.MergeableTask[segmentIndex, *commonpb.Status]) {
	task.req.Infos = append(task.req.Infos, other.(*LoadSegmentsTask).req.GetInfos()...)
	deltaPositions := make(map[string]*internalpb.MsgPosition)
	for _, position := range task.req.DeltaPositions {
		deltaPositions[position.GetChannelName()] = position
	}
	for _, position := range other.(*LoadSegmentsTask).req.GetDeltaPositions() {
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

func (task *LoadSegmentsTask) SetResult(result *commonpb.Status) {
	task.result = result
}

func (task *LoadSegmentsTask) SetError(err error) {
	task.err = err
}

func (task *LoadSegmentsTask) Done() {
	close(task.doneCh)
}

func (task *LoadSegmentsTask) Wait() (*commonpb.Status, error) {
	<-task.doneCh
	return task.result, task.err
}

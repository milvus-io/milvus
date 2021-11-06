package querynode

import (
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

type deleteNode struct {
	baseNode
	replica ReplicaInterface // historical
}

func (dNode *deleteNode) Name() string {
	return "dNode"
}

func (dNode *deleteNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Error("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	dMsg, ok := in[0].(*deleteMsg)
	if !ok {
		log.Warn("type assertion failed for deleteMsg")
		// TODO: add error handling
	}

	delData := &deleteData{
		deleteIDs:        map[UniqueID][]int64{},
		deleteTimestamps: map[UniqueID][]Timestamp{},
		deleteOffset:     map[UniqueID]int64{},
	}

	if dMsg == nil {
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range dMsg.deleteMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	// 1. filter segment by bloom filter
	for _, delMsg := range dMsg.deleteMessages {
		if dNode.replica.getSegmentNum() != 0 {
			processDeleteMessages(dNode.replica, delMsg, delData)
		}
	}

	// 2. do preDelete
	for segmentID, pks := range delData.deleteIDs {
		segment, err := dNode.replica.getSegmentByID(segmentID)
		if err != nil {
			log.Warn("Cannot find segment in historical replica:", zap.Int64("segmentID", segmentID))
		}
		offset := segment.segmentPreDelete(len(pks))
		delData.deleteOffset[segmentID] = offset
	}

	// 3. do delete
	wg := sync.WaitGroup{}
	for segmentID := range delData.deleteIDs {
		wg.Add(1)
		go dNode.delete(delData, segmentID, &wg)
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		timeRange: dMsg.timeRange,
	}
	for _, sp := range spans {
		sp.Finish()
	}

	return []Msg{res}
}

func (dNode *deleteNode) delete(deleteData *deleteData, segmentID UniqueID, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Debug("QueryNode::dNode::delete", zap.Any("SegmentID", segmentID))
	targetSegment := dNode.getSegmentInReplica(segmentID)
	if targetSegment == nil {
		log.Warn("targetSegment is nil")
		return
	}

	if targetSegment.segmentType != segmentTypeSealed {
		return
	}

	ids := deleteData.deleteIDs[segmentID]
	timestamps := deleteData.deleteTimestamps[segmentID]
	offset := deleteData.deleteOffset[segmentID]

	err := targetSegment.segmentDelete(offset, &ids, &timestamps)
	if err != nil {
		log.Warn("QueryNode: targetSegmentDelete failed", zap.Error(err))
		return
	}

	log.Debug("Do delete done", zap.Int("len", len(deleteData.deleteIDs[segmentID])), zap.Int64("segmentID", segmentID))
}

func (dNode *deleteNode) getSegmentInReplica(segmentID int64) *Segment {
	segment, err := dNode.replica.getSegmentByID(segmentID)
	if err != nil {
	} else {
		return segment
	}
	return nil
}

func newDeleteNode(historicalReplica ReplicaInterface) *deleteNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &deleteNode{
		baseNode: baseNode,
		replica:  historicalReplica,
	}
}

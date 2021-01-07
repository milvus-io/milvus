package querynode

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type insertNode struct {
	BaseNode
	replica collectionReplica
}

type InsertData struct {
	insertContext    map[int64]context.Context
	insertIDs        map[UniqueID][]UniqueID
	insertTimestamps map[UniqueID][]Timestamp
	insertRecords    map[UniqueID][]*commonpb.Blob
	insertOffset     map[UniqueID]int64
}

func (iNode *insertNode) Name() string {
	return "iNode"
}

func (iNode *insertNode) Operate(in []*Msg) []*Msg {
	// fmt.Println("Do insertNode operation")

	if len(in) != 1 {
		log.Println("Invalid operate message input in insertNode, input length = ", len(in))
		// TODO: add error handling
	}

	iMsg, ok := (*in[0]).(*insertMsg)
	if !ok {
		log.Println("type assertion failed for insertMsg")
		// TODO: add error handling
	}

	var childs []opentracing.Span
	tracer := opentracing.GlobalTracer()
	if tracer != nil && iMsg != nil {
		for _, msg := range iMsg.insertMessages {
			if msg.Type() == internalPb.MsgType_kInsert || msg.Type() == internalPb.MsgType_kSearch {
				var child opentracing.Span
				ctx := msg.GetMsgContext()
				if parent := opentracing.SpanFromContext(ctx); parent != nil {
					child = tracer.StartSpan("pass filter node",
						opentracing.FollowsFrom(parent.Context()))
				} else {
					child = tracer.StartSpan("pass filter node")
				}
				child.SetTag("hash keys", msg.HashKeys())
				child.SetTag("start time", msg.BeginTs())
				child.SetTag("end time", msg.EndTs())
				msg.SetMsgContext(opentracing.ContextWithSpan(ctx, child))
				childs = append(childs, child)
			}
		}
	}

	insertData := InsertData{
		insertContext:    make(map[int64]context.Context),
		insertIDs:        make(map[int64][]int64),
		insertTimestamps: make(map[int64][]uint64),
		insertRecords:    make(map[int64][]*commonpb.Blob),
		insertOffset:     make(map[int64]int64),
	}

	// 1. hash insertMessages to insertData
	for _, task := range iMsg.insertMessages {
		insertData.insertContext[task.SegmentID] = task.GetMsgContext()
		insertData.insertIDs[task.SegmentID] = append(insertData.insertIDs[task.SegmentID], task.RowIDs...)
		insertData.insertTimestamps[task.SegmentID] = append(insertData.insertTimestamps[task.SegmentID], task.Timestamps...)
		insertData.insertRecords[task.SegmentID] = append(insertData.insertRecords[task.SegmentID], task.RowData...)

		// check if segment exists, if not, create this segment
		if !iNode.replica.hasSegment(task.SegmentID) {
			collection, err := iNode.replica.getCollectionByName(task.CollectionName)
			if err != nil {
				log.Println(err)
				continue
			}
			err = iNode.replica.addSegment(task.SegmentID, task.PartitionTag, collection.ID())
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}

	// 2. do preInsert
	for segmentID := range insertData.insertRecords {
		var targetSegment, err = iNode.replica.getSegmentByID(segmentID)
		if err != nil {
			log.Println("preInsert failed")
			// TODO: add error handling
		}

		var numOfRecords = len(insertData.insertRecords[segmentID])
		if targetSegment != nil {
			var offset = targetSegment.segmentPreInsert(numOfRecords)
			insertData.insertOffset[segmentID] = offset
		}
	}

	// 3. do insert
	wg := sync.WaitGroup{}
	for segmentID := range insertData.insertRecords {
		wg.Add(1)
		go iNode.insert(insertData.insertContext[segmentID], &insertData, segmentID, &wg)
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		gcRecord:  iMsg.gcRecord,
		timeRange: iMsg.timeRange,
	}
	for _, child := range childs {
		child.Finish()
	}
	return []*Msg{&res}
}

func (iNode *insertNode) insert(ctx context.Context, insertData *InsertData, segmentID int64, wg *sync.WaitGroup) {
	span, _ := opentracing.StartSpanFromContext(ctx, "insert node insert")
	defer span.Finish()
	var targetSegment, err = iNode.replica.getSegmentByID(segmentID)
	if err != nil {
		log.Println("cannot find segment:", segmentID)
		// TODO: add error handling
		wg.Done()
		span.LogFields(oplog.Error(err))
		return
	}

	ids := insertData.insertIDs[segmentID]
	timestamps := insertData.insertTimestamps[segmentID]
	records := insertData.insertRecords[segmentID]
	offsets := insertData.insertOffset[segmentID]

	err = targetSegment.segmentInsert(offsets, &ids, &timestamps, &records)
	if err != nil {
		log.Println(err)
		// TODO: add error handling
		wg.Done()
		span.LogFields(oplog.Error(err))
		return
	}

	fmt.Println("Do insert done, len = ", len(insertData.insertIDs[segmentID]))
	wg.Done()
}

func newInsertNode(replica collectionReplica) *insertNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &insertNode{
		BaseNode: baseNode,
		replica:  replica,
	}
}

package querynode

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
	"go.uber.org/zap"
)

type insertNode struct {
	baseNode
	collectionID UniqueID
	replica      ReplicaInterface
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

func (iNode *insertNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	//log.Debug("Do insertNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in insertNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	iMsg, ok := in[0].(*insertMsg)
	if !ok {
		log.Error("type assertion failed for insertMsg")
		// TODO: add error handling
	}

	insertData := InsertData{
		insertIDs:        make(map[int64][]int64),
		insertTimestamps: make(map[int64][]uint64),
		insertRecords:    make(map[int64][]*commonpb.Blob),
		insertOffset:     make(map[int64]int64),
	}

	if iMsg == nil {
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range iMsg.insertMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	// 1. hash insertMessages to insertData
	for _, task := range iMsg.insertMessages {
		// check if segment exists, if not, create this segment
		if !iNode.replica.hasSegment(task.SegmentID) {
			err := iNode.replica.addSegment(task.SegmentID, task.PartitionID, task.CollectionID, segmentTypeGrowing)
			if err != nil {
				log.Error(err.Error())
				continue
			}
		}

		segment, err := iNode.replica.getSegmentByID(task.SegmentID)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		if segment.enableLoadBinLog {
			continue
		}

		insertData.insertIDs[task.SegmentID] = append(insertData.insertIDs[task.SegmentID], task.RowIDs...)
		insertData.insertTimestamps[task.SegmentID] = append(insertData.insertTimestamps[task.SegmentID], task.Timestamps...)
		insertData.insertRecords[task.SegmentID] = append(insertData.insertRecords[task.SegmentID], task.RowData...)
	}

	// 2. do preInsert
	for segmentID := range insertData.insertRecords {
		var targetSegment, err = iNode.replica.getSegmentByID(segmentID)
		if err != nil {
			log.Error("preInsert failed")
			// TODO: add error handling
		}

		var numOfRecords = len(insertData.insertRecords[segmentID])
		if targetSegment != nil {
			offset, err := targetSegment.segmentPreInsert(numOfRecords)
			if err != nil {
				log.Error(err.Error())
			}
			insertData.insertOffset[segmentID] = offset
			log.Debug("insertNode operator", zap.Int("insert size", numOfRecords), zap.Int64("insert offset", offset), zap.Int64("segment id", segmentID))
		}
	}

	// 3. do insert
	wg := sync.WaitGroup{}
	for segmentID := range insertData.insertRecords {
		wg.Add(1)
		go iNode.insert(&insertData, segmentID, &wg)
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		gcRecord:  iMsg.gcRecord,
		timeRange: iMsg.timeRange,
	}
	for _, sp := range spans {
		sp.Finish()
	}
	return []Msg{res}
}

func (iNode *insertNode) insert(insertData *InsertData, segmentID int64, wg *sync.WaitGroup) {
	var targetSegment, err = iNode.replica.getSegmentByID(segmentID)
	if targetSegment.segmentType != segmentTypeGrowing || targetSegment.enableLoadBinLog {
		wg.Done()
		return
	}
	if err != nil {
		log.Error("cannot find segment:", zap.Int64("segmentID", segmentID))
		// TODO: add error handling
		wg.Done()
		return
	}

	if targetSegment.segmentType != segmentTypeGrowing {
		wg.Done()
		return
	}

	ids := insertData.insertIDs[segmentID]
	timestamps := insertData.insertTimestamps[segmentID]
	records := insertData.insertRecords[segmentID]
	offsets := insertData.insertOffset[segmentID]

	err = targetSegment.segmentInsert(offsets, &ids, &timestamps, &records)
	if err != nil {
		log.Error(err.Error())
		// TODO: add error handling
		wg.Done()
		return
	}

	log.Debug("Do insert done", zap.Int("len", len(insertData.insertIDs[segmentID])),
		zap.Int64("segmentID", segmentID),
		zap.Int64("collectionID", iNode.collectionID))
	wg.Done()
}

func newInsertNode(replica ReplicaInterface, collectionID UniqueID) *insertNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &insertNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		replica:      replica,
	}
}

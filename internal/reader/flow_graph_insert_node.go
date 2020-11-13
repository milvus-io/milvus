package reader

import (
	"fmt"
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type insertNode struct {
	BaseNode
	container *container
}

type InsertData struct {
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

	insertData := InsertData{
		insertIDs:        make(map[int64][]int64),
		insertTimestamps: make(map[int64][]uint64),
		insertRecords:    make(map[int64][]*commonpb.Blob),
		insertOffset:     make(map[int64]int64),
	}

	// 1. hash insertMessages to insertData
	for _, task := range iMsg.insertMessages {
		if len(task.RowIDs) != len(task.Timestamps) || len(task.RowIDs) != len(task.RowData) {
			// TODO: what if the messages are misaligned?
			// Here, we ignore those messages and print error
			log.Println("Error, misaligned messages detected")
			continue
		}
		insertData.insertIDs[task.SegmentID] = append(insertData.insertIDs[task.SegmentID], task.RowIDs...)
		insertData.insertTimestamps[task.SegmentID] = append(insertData.insertTimestamps[task.SegmentID], task.Timestamps...)
		insertData.insertRecords[task.SegmentID] = append(insertData.insertRecords[task.SegmentID], task.RowData...)
	}

	// 2. do preInsert
	for segmentID := range insertData.insertRecords {
		var targetSegment, err = (*iNode.container).getSegmentByID(segmentID)
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
		go iNode.insert(&insertData, segmentID, &wg)
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		timeRange: iMsg.timeRange,
	}
	return []*Msg{&res}
}

func (iNode *insertNode) insert(insertData *InsertData, segmentID int64, wg *sync.WaitGroup) {
	var targetSegment, err = (*iNode.container).getSegmentByID(segmentID)
	if err != nil {
		log.Println("cannot find segment:", segmentID)
		// TODO: add error handling
		return
	}

	ids := insertData.insertIDs[segmentID]
	timestamps := insertData.insertTimestamps[segmentID]
	records := insertData.insertRecords[segmentID]
	offsets := insertData.insertOffset[segmentID]

	err = targetSegment.segmentInsert(offsets, &ids, &timestamps, &records)
	if err != nil {
		log.Println("insert failed")
		// TODO: add error handling
		return
	}

	fmt.Println("Do insert done, len = ", len(insertData.insertIDs[segmentID]))
	wg.Done()
}

func newInsertNode(container *container) *insertNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &insertNode{
		BaseNode:  baseNode,
		container: container,
	}
}

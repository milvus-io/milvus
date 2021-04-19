package reader

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
)

type insertNode struct {
	BaseNode
	SegmentsMap *map[int64]*Segment
	insertMsg   *insertMsg
}

func (iNode *insertNode) Name() string {
	return "iNode"
}

func (iNode *insertNode) Operate(in []*Msg) []*Msg {
	if len(in) != 1 {
		log.Println("Invalid operate message input in insertNode")
		// TODO: add error handling
	}

	insertMsg, ok := (*in[0]).(*insertMsg)
	if !ok {
		log.Println("type assertion failed for insertMsg")
		// TODO: add error handling
	}

	iNode.insertMsg = insertMsg

	var err = iNode.preInsert()
	if err != nil {
		log.Println("preInsert failed")
		// TODO: add error handling
	}

	wg := sync.WaitGroup{}
	for segmentID := range iNode.insertMsg.insertData.insertRecords {
		wg.Add(1)
		go iNode.insert(segmentID, &wg)
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		timeRange: insertMsg.timeRange,
	}
	return []*Msg{&res}
}

func (iNode *insertNode) preInsert() error {
	for segmentID := range iNode.insertMsg.insertData.insertRecords {
		var targetSegment, err = iNode.getSegmentBySegmentID(segmentID)
		if err != nil {
			return err
		}

		var numOfRecords = len(iNode.insertMsg.insertData.insertRecords[segmentID])
		var offset = targetSegment.SegmentPreInsert(numOfRecords)
		iNode.insertMsg.insertData.insertOffset[segmentID] = offset
	}

	return nil
}

func (iNode *insertNode) getSegmentBySegmentID(segmentID int64) (*Segment, error) {
	targetSegment, ok := (*iNode.SegmentsMap)[segmentID]

	if !ok {
		return nil, errors.New("cannot found segment with id = " + strconv.FormatInt(segmentID, 10))
	}

	return targetSegment, nil
}

func (iNode *insertNode) insert(segmentID int64, wg *sync.WaitGroup) {
	var targetSegment, err = iNode.getSegmentBySegmentID(segmentID)
	if err != nil {
		log.Println("insert failed")
		// TODO: add error handling
		return
	}

	ids := iNode.insertMsg.insertData.insertIDs[segmentID]
	timestamps := iNode.insertMsg.insertData.insertTimestamps[segmentID]
	records := iNode.insertMsg.insertData.insertRecords[segmentID]
	offsets := iNode.insertMsg.insertData.insertOffset[segmentID]

	err = targetSegment.SegmentInsert(offsets, &ids, &timestamps, &records)
	fmt.Println("Do insert done, len = ", len(iNode.insertMsg.insertData.insertIDs[segmentID]))

	wg.Done()
}

func newInsertNode() *insertNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &insertNode{
		BaseNode: baseNode,
	}
}

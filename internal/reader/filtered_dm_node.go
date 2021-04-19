package reader

import (
	"log"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type filteredDmNode struct {
	BaseNode
	filteredDmMsg filteredDmMsg
}

func (fdmNode *filteredDmNode) Name() string {
	return "fdmNode"
}

func (fdmNode *filteredDmNode) Operate(in []*Msg) []*Msg {
	// fmt.Println("Do filteredDmNode operation")

	if len(in) != 1 {
		log.Println("Invalid operate message input in filteredDmNode")
		// TODO: add error handling
	}

	fdmMsg, ok := (*in[0]).(*filteredDmMsg)
	if !ok {
		log.Println("type assertion failed for filteredDmMsg")
		// TODO: add error handling
	}

	insertData := InsertData{
		insertIDs:        make(map[int64][]int64),
		insertTimestamps: make(map[int64][]uint64),
		insertRecords:    make(map[int64][]*commonpb.Blob),
		insertOffset:     make(map[int64]int64),
	}

	var iMsg = insertMsg{
		insertData: insertData,
		timeRange:  fdmMsg.timeRange,
	}
	for _, task := range fdmMsg.insertMessages {
		if len(task.RowIds) != len(task.Timestamps) || len(task.RowIds) != len(task.RowData) {
			// TODO: what if the messages are misaligned?
			// Here, we ignore those messages and print error
			log.Println("Error, misaligned messages detected")
			continue
		}
		iMsg.insertData.insertIDs[task.SegmentId] = append(iMsg.insertData.insertIDs[task.SegmentId], task.RowIds...)
		iMsg.insertData.insertTimestamps[task.SegmentId] = append(iMsg.insertData.insertTimestamps[task.SegmentId], task.Timestamps...)
		iMsg.insertData.insertRecords[task.SegmentId] = append(iMsg.insertData.insertRecords[task.SegmentId], task.RowData...)
	}
	var res Msg = &iMsg
	return []*Msg{&res}
}

func newFilteredDmNode() *filteredDmNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filteredDmNode{
		BaseNode: baseNode,
	}
}

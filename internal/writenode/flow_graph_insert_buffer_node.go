package writenode

import (
	"log"

	"github.com/zilliztech/milvus-distributed/internal/storage"
)

type (
	insertBufferNode struct {
		BaseNode
		binLogs map[SegmentID][]*storage.Blob // Binary logs of a segment.
		buffer  *insertBuffer
	}

	insertBufferData struct {
		logIdx      int // TODO What's it for?
		partitionID UniqueID
		segmentID   UniqueID
		data        *storage.InsertData
	}

	insertBuffer struct {
		buffer  []*insertBufferData
		maxSize int //  TODO set from write_node.yaml
	}
)

func (ibNode *insertBufferNode) Name() string {
	return "ibNode"
}

func (ibNode *insertBufferNode) Operate(in []*Msg) []*Msg {
	log.Println("=========== insert buffer Node Operating")

	if len(in) != 1 {
		log.Println("Invalid operate message input in insertBuffertNode, input length = ", len(in))
		// TODO: add error handling
	}

	_, ok := (*in[0]).(*insertMsg)
	if !ok {
		log.Println("type assertion failed for insertMsg")
		// TODO: add error handling
	}

	// iMsg is insertMsg
	//   1. iMsg -> insertBufferData -> insertBuffer
	//   2. Send hardTimeTick msg
	//   3. if insertBuffer full
	//     3.1 insertBuffer -> binLogs
	//     3.2 binLogs -> minIO/S3
	// iMsg is Flush() msg from master
	//   1. insertBuffer(not empty) -> binLogs -> minIO/S3
	// Return

	// log.Println("=========== insertMsg length:", len(iMsg.insertMessages))
	// for _, task := range iMsg.insertMessages {
	//     if len(task.RowIDs) != len(task.Timestamps) || len(task.RowIDs) != len(task.RowData) {
	//         log.Println("Error, misaligned messages detected")
	//         continue
	//     }
	//     log.Println("Timestamp: ", task.Timestamps[0])
	//     log.Printf("t(%d) : %v ", task.Timestamps[0], task.RowData[0])
	// }

	// TODO
	return nil
}

func newInsertBufferNode() *insertBufferNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	// TODO read from yaml
	maxSize := 10
	iBuffer := &insertBuffer{
		buffer:  make([]*insertBufferData, maxSize),
		maxSize: maxSize,
	}

	return &insertBufferNode{
		BaseNode: baseNode,
		binLogs:  make(map[SegmentID][]*storage.Blob),
		buffer:   iBuffer,
	}
}

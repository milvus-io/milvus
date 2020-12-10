package querynode

type key2SegNode struct {
	BaseNode
	key2SegMsg key2SegMsg
}

func (ksNode *key2SegNode) Name() string {
	return "ksNode"
}

func (ksNode *key2SegNode) Operate(in []*Msg) []*Msg {
	return in
}

func newKey2SegNode() *key2SegNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &key2SegNode{
		BaseNode: baseNode,
	}
}

/************************************** util functions ***************************************/
// Function `GetSegmentByEntityId` should return entityIDs, timestamps and segmentIDs
//func (node *QueryNode) GetKey2Segments() (*[]int64, *[]uint64, *[]int64) {
//	var entityIDs = make([]int64, 0)
//	var timestamps = make([]uint64, 0)
//	var segmentIDs = make([]int64, 0)
//
//	var key2SegMsg = node.messageClient.Key2SegMsg
//	for _, msg := range key2SegMsg {
//		if msg.SegmentID == nil {
//			segmentIDs = append(segmentIDs, -1)
//			entityIDs = append(entityIDs, msg.Uid)
//			timestamps = append(timestamps, msg.Timestamp)
//		} else {
//			for _, segmentID := range msg.SegmentID {
//				segmentIDs = append(segmentIDs, segmentID)
//				entityIDs = append(entityIDs, msg.Uid)
//				timestamps = append(timestamps, msg.Timestamp)
//			}
//		}
//	}
//
//	return &entityIDs, &timestamps, &segmentIDs
//}

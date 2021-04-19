// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

type key2SegNode struct {
	baseNode
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

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &key2SegNode{
		baseNode: baseNode,
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

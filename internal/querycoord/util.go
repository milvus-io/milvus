// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoord

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func getCompareMapFromSlice(sliceData []int64) map[int64]struct{} {
	compareMap := make(map[int64]struct{})
	for _, data := range sliceData {
		compareMap[data] = struct{}{}
	}

	return compareMap
}

func getVecFieldIDs(schema *schemapb.CollectionSchema) []int64 {
	var vecFieldIDs []int64
	for _, field := range schema.Fields {
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
			vecFieldIDs = append(vecFieldIDs, field.FieldID)
		}
	}

	return vecFieldIDs
}

func getDstNodeIDByTask(t task) int64 {
	var nodeID int64
	switch t.msgType() {
	case commonpb.MsgType_LoadSegments:
		loadSegment := t.(*loadSegmentTask)
		nodeID = loadSegment.DstNodeID
	case commonpb.MsgType_WatchDmChannels:
		watchDmChannel := t.(*watchDmChannelTask)
		nodeID = watchDmChannel.NodeID
	case commonpb.MsgType_WatchDeltaChannels:
		watchDeltaChannel := t.(*watchDeltaChannelTask)
		nodeID = watchDeltaChannel.NodeID
	case commonpb.MsgType_WatchQueryChannels:
		watchQueryChannel := t.(*watchQueryChannelTask)
		nodeID = watchQueryChannel.NodeID
	case commonpb.MsgType_ReleaseCollection:
		releaseCollection := t.(*releaseCollectionTask)
		nodeID = releaseCollection.NodeID
	case commonpb.MsgType_ReleasePartitions:
		releasePartition := t.(*releasePartitionTask)
		nodeID = releasePartition.NodeID
	case commonpb.MsgType_ReleaseSegments:
		releaseSegment := t.(*releaseSegmentTask)
		nodeID = releaseSegment.NodeID
	default:
		//TODO::
	}

	return nodeID
}

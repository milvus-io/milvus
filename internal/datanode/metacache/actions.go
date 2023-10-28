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

package metacache

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
)

type SegmentFilter func(info *SegmentInfo) bool

func WithPartitionID(partitionID int64) SegmentFilter {
	return func(info *SegmentInfo) bool {
		return info.partitionID == partitionID
	}
}

func WithSegmentID(segmentID int64) SegmentFilter {
	return func(info *SegmentInfo) bool {
		return info.segmentID == segmentID
	}
}

func WithSegmentState(state commonpb.SegmentState) SegmentFilter {
	return func(info *SegmentInfo) bool {
		return info.state == state
	}
}

func WithStartPosNotRecorded() SegmentFilter {
	return func(info *SegmentInfo) bool {
		return !info.startPosRecorded
	}
}

type SegmentAction func(info *SegmentInfo)

func UpdateState(state commonpb.SegmentState) SegmentAction {
	return func(info *SegmentInfo) {
		info.state = state
	}
}

func UpdateCheckpoint(checkpoint *msgpb.MsgPosition) SegmentAction {
	return func(info *SegmentInfo) {
		info.checkpoint = checkpoint
	}
}

func UpdateNumOfRows(numOfRows int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.numOfRows = numOfRows
	}
}

func RollStats() SegmentAction {
	return func(info *SegmentInfo) {
		info.bfs.Roll()
	}
}

// MergeSegmentAction is the util function to merge multiple SegmentActions into one.
func MergeSegmentAction(actions ...SegmentAction) SegmentAction {
	return func(info *SegmentInfo) {
		for _, action := range actions {
			action(info)
		}
	}
}

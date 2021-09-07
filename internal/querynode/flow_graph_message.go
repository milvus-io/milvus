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

import (
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type Msg = flowgraph.Msg
type MsgStreamMsg = flowgraph.MsgStreamMsg

type insertMsg struct {
	insertMessages []*msgstream.InsertMsg
	gcRecord       *gcRecord
	timeRange      TimeRange
}

type serviceTimeMsg struct {
	gcRecord  *gcRecord
	timeRange TimeRange
}

type gcMsg struct {
	gcRecord  *gcRecord
	timeRange TimeRange
}

type DeleteData struct {
	deleteIDs        map[UniqueID][]UniqueID
	deleteTimestamps map[UniqueID][]Timestamp
	deleteOffset     map[UniqueID]int64
}

type DeleteRecord struct {
	entityID  UniqueID
	timestamp Timestamp
	segmentID UniqueID
}

type DeletePreprocessData struct {
	deleteRecords []*DeleteRecord
	count         int32
}

// TODO: delete collection id
type partitionWithID struct {
	partitionID  UniqueID
	collectionID UniqueID
}

type gcRecord struct {
	// collections and partitions to be dropped
	collections []UniqueID
	partitions  []partitionWithID
}

func (iMsg *insertMsg) TimeTick() Timestamp {
	return iMsg.timeRange.timestampMax
}

func (stMsg *serviceTimeMsg) TimeTick() Timestamp {
	return stMsg.timeRange.timestampMax
}

func (gcMsg *gcMsg) TimeTick() Timestamp {
	return gcMsg.timeRange.timestampMax
}

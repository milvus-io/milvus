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

package pipeline

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type (
	// Msg is flowgraph.Msg
	Msg = flowgraph.Msg

	BaseMsg = flowgraph.BaseMsg

	// MsgStreamMsg is flowgraph.MsgStreamMsg
	MsgStreamMsg = flowgraph.MsgStreamMsg

	// InsertData of storage
	InsertData = storage.InsertData

	// DeleteData record deleted IDs and Timestamps
	DeleteData = storage.DeleteData

	// Blob of storage
	Blob = storage.Blob
)

type FlowGraphMsg struct {
	BaseMsg
	InsertMessages []*msgstream.InsertMsg
	InsertData     []*writebuffer.InsertData

	DeleteMessages []*msgstream.DeleteMsg

	TimeRange      util.TimeRange
	StartPositions []*msgpb.MsgPosition
	EndPositions   []*msgpb.MsgPosition

	// segmentsToSync is the signal used by insertBufferNode to notify deleteNode to flush
	segmentsToSync []typeutil.UniqueID
	dropCollection bool
	dropPartitions []typeutil.UniqueID

	updatedSchema *schemapb.CollectionSchema
	schemaVersion uint64
}

func (fgMsg *FlowGraphMsg) TimeTick() typeutil.Timestamp {
	return fgMsg.TimeRange.TimestampMax
}

func (fgMsg *FlowGraphMsg) IsClose() bool {
	return fgMsg.BaseMsg.IsCloseMsg()
}

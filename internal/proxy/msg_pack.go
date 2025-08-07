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

package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func genInsertMsgsByPartition(ctx context.Context,
	segmentID UniqueID,
	partitionID UniqueID,
	partitionName string,
	rowOffsets []int,
	channelName string,
	insertMsg *msgstream.InsertMsg,
) ([]msgstream.TsMsg, error) {
	threshold := Params.PulsarCfg.MaxMessageSize.GetAsInt()

	// create empty insert message
	createInsertMsg := func(segmentID UniqueID, channelName string) *msgstream.InsertMsg {
		insertReq := &msgpb.InsertRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Insert),
				commonpbutil.WithTimeStamp(insertMsg.BeginTimestamp), // entity's timestamp was set to equal it.BeginTimestamp in preExecute()
				commonpbutil.WithSourceID(insertMsg.Base.SourceID),
			),
			CollectionID:   insertMsg.CollectionID,
			PartitionID:    partitionID,
			DbName:         insertMsg.DbName,
			CollectionName: insertMsg.CollectionName,
			PartitionName:  partitionName,
			SegmentID:      segmentID,
			ShardName:      channelName,
			Version:        msgpb.InsertDataVersion_ColumnBased,
			FieldsData:     make([]*schemapb.FieldData, len(insertMsg.GetFieldsData())),
		}
		msg := &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				Ctx: ctx,
			},
			InsertRequest: insertReq,
		}

		return msg
	}

	repackedMsgs := make([]msgstream.TsMsg, 0)
	requestSize := 0
	msg := createInsertMsg(segmentID, channelName)
	for _, offset := range rowOffsets {
		curRowMessageSize, err := typeutil.EstimateEntitySize(insertMsg.GetFieldsData(), offset)
		if err != nil {
			return nil, err
		}

		// if insertMsg's size is greater than the threshold, split into multiple insertMsgs
		if requestSize+curRowMessageSize >= threshold {
			repackedMsgs = append(repackedMsgs, msg)
			msg = createInsertMsg(segmentID, channelName)
			requestSize = 0
		}

		typeutil.AppendFieldData(msg.FieldsData, insertMsg.GetFieldsData(), int64(offset))
		msg.HashValues = append(msg.HashValues, insertMsg.HashValues[offset])
		msg.Timestamps = append(msg.Timestamps, insertMsg.Timestamps[offset])
		msg.RowIDs = append(msg.RowIDs, insertMsg.RowIDs[offset])
		msg.NumRows++
		requestSize += curRowMessageSize
	}
	repackedMsgs = append(repackedMsgs, msg)

	return repackedMsgs, nil
}

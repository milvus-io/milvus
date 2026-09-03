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

package channelmgr

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	streamingutil "github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// GetActiveWALName returns the name of the currently active WAL implementation.
func GetActiveWALName() message.WALName {
	return streamingutil.MustSelectWALName()
}

func getMaxSingleRowSize(walName message.WALName) (int, bool) {
	switch walName {
	case message.WALNamePulsar:
		limit := paramtable.Get().PulsarCfg.MaxMessageSize.GetAsInt()
		return limit, limit > 0
	case message.WALNameKafka:
		limit := paramtable.Get().KafkaCfg.ProducerMessageMaxBytes.GetAsInt()
		return limit, limit > 0
	case message.WALNameRocksmq, message.WALNameWoodpecker:
		// RocksMQ page size and Woodpecker batch size are not hard limits
		// on an individual WAL entry.
		return 0, false
	default:
		return 0, false
	}
}

// GenInsertMsgsByPartition splits the insert payload of a partition into
// per-segment messages, honoring the cross-WAL packing threshold.
func GenInsertMsgsByPartition(ctx context.Context,
	segmentID typeutil.UniqueID,
	partitionID typeutil.UniqueID,
	partitionName string,
	rowOffsets []int,
	channelName string,
	insertMsg *msgstream.InsertMsg,
	walName message.WALName,
) ([]msgstream.TsMsg, [][]int, error) {
	// Keep the existing cross-WAL packing threshold separate from the
	// backend-specific hard limit for a row that cannot be split further.
	splitThreshold := paramtable.Get().PulsarCfg.MaxMessageSize.GetAsInt()
	singleRowLimit, hasSingleRowLimit := getMaxSingleRowSize(walName)

	// create empty insert message
	createInsertMsg := func(segmentID typeutil.UniqueID, channelName string) *msgstream.InsertMsg {
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

	fieldsData := insertMsg.GetFieldsData()
	idxComputer := typeutil.NewFieldDataIdxComputer(fieldsData)

	repackedMsgs := make([]msgstream.TsMsg, 0)
	repackedRowOffsets := make([][]int, 0)
	requestSize := 0
	msg := createInsertMsg(segmentID, channelName)
	msgRowOffsets := make([]int, 0)
	for _, offset := range rowOffsets {
		fieldIdxs := idxComputer.Compute(int64(offset))
		curRowMessageSize, err := typeutil.EstimateEntitySize(fieldsData, offset, fieldIdxs...)
		if err != nil {
			return nil, nil, err
		}
		if hasSingleRowLimit && curRowMessageSize >= singleRowLimit {
			return nil, nil, merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"single row at offset %d is too large to fit in one WAL message: estimated size=%d bytes, limit=%d bytes",
				offset, curRowMessageSize, singleRowLimit,
			))
		}

		// If the insert message size exceeds the threshold, flush the current
		// message first.
		if msg.NumRows > 0 && requestSize+curRowMessageSize >= splitThreshold {
			repackedMsgs = append(repackedMsgs, msg)
			repackedRowOffsets = append(repackedRowOffsets, msgRowOffsets)
			msg = createInsertMsg(segmentID, channelName)
			msgRowOffsets = make([]int, 0)
			requestSize = 0
		}

		typeutil.AppendFieldData(msg.FieldsData, fieldsData, int64(offset), fieldIdxs...)
		msg.HashValues = append(msg.HashValues, insertMsg.HashValues[offset])
		msg.Timestamps = append(msg.Timestamps, insertMsg.Timestamps[offset])
		msg.RowIDs = append(msg.RowIDs, insertMsg.RowIDs[offset])
		msg.NumRows++
		msgRowOffsets = append(msgRowOffsets, offset)
		requestSize += curRowMessageSize
	}
	if msg.NumRows > 0 {
		repackedMsgs = append(repackedMsgs, msg)
		repackedRowOffsets = append(repackedRowOffsets, msgRowOffsets)
	}

	return repackedMsgs, repackedRowOffsets, nil
}

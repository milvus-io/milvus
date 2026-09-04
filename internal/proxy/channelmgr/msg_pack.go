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

type insertMsgBatch struct {
	start int
	end   int
}

func contiguousRowRange(rowOffsets []int) (int, int, bool) {
	if len(rowOffsets) == 0 || rowOffsets[0] < 0 {
		return 0, 0, false
	}

	start := rowOffsets[0]
	for i, offset := range rowOffsets {
		if offset != start+i {
			return 0, 0, false
		}
	}
	return start, start + len(rowOffsets), true
}

func canCreateInsertRangeView(insertMsg *msgstream.InsertMsg, start, end int) bool {
	return start >= 0 && end > start &&
		end <= len(insertMsg.HashValues) &&
		end <= len(insertMsg.GetTimestamps()) &&
		end <= len(insertMsg.GetRowIDs())
}

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
//
// To avoid copying contiguous batches, returned messages may share backing
// storage with insertMsg. Callers must treat insertMsg and the returned
// messages as read-only until the returned messages have been serialized.
func GenInsertMsgsByPartition(ctx context.Context,
	segmentID typeutil.UniqueID,
	partitionID typeutil.UniqueID,
	partitionName string,
	rowOffsets []int,
	channelName string,
	insertMsg *msgstream.InsertMsg,
	walName message.WALName,
) ([]msgstream.TsMsg, error) {
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
	_, _, contiguous := contiguousRowRange(rowOffsets)
	idxComputer := typeutil.NewFieldDataIdxComputer(fieldsData)
	repackedMsgs := make([]msgstream.TsMsg, 0)
	var (
		copiedMsg                 *msgstream.InsertMsg
		firstFieldIdxs            []int64
		singletonRangeIdxComputer *typeutil.FieldDataIdxComputer
	)
	appendRow := func(msg *msgstream.InsertMsg, offset int, fieldIdxs []int64) {
		if msg.FieldsData == nil {
			msg.FieldsData = make([]*schemapb.FieldData, len(fieldsData))
		}
		typeutil.AppendFieldData(msg.FieldsData, fieldsData, int64(offset), fieldIdxs...)
		msg.HashValues = append(msg.HashValues, insertMsg.HashValues[offset])
		msg.Timestamps = append(msg.Timestamps, insertMsg.Timestamps[offset])
		msg.RowIDs = append(msg.RowIDs, insertMsg.RowIDs[offset])
		msg.NumRows++
	}
	emitSparseBatch := func(rowStart int) {
		if copiedMsg != nil {
			repackedMsgs = append(repackedMsgs, copiedMsg)
			copiedMsg = nil
			return
		}

		// A singleton batch can share its row even when the overall selection
		// is sparse. Its starting indexes were saved before sizing the next row.
		msg := createInsertMsg(segmentID, channelName)
		rowEnd := rowStart + 1
		if canCreateInsertRangeView(insertMsg, rowStart, rowEnd) {
			if singletonRangeIdxComputer == nil {
				singletonRangeIdxComputer = typeutil.NewFieldDataIdxComputer(fieldsData)
			}
			dataEnds := singletonRangeIdxComputer.Compute(int64(rowEnd))
			fieldViews, ok := typeutil.CreateFieldDataRangeView(
				fieldsData, int64(rowStart), int64(rowEnd), firstFieldIdxs, dataEnds,
			)
			if ok {
				msg.FieldsData = fieldViews
				msg.HashValues = insertMsg.HashValues[rowStart:rowEnd:rowEnd]
				msg.Timestamps = insertMsg.Timestamps[rowStart:rowEnd:rowEnd]
				msg.RowIDs = insertMsg.RowIDs[rowStart:rowEnd:rowEnd]
				msg.NumRows = 1
				repackedMsgs = append(repackedMsgs, msg)
				return
			}
		}
		appendRow(msg, rowStart, firstFieldIdxs)
		repackedMsgs = append(repackedMsgs, msg)
	}
	batches := make([]insertMsgBatch, 0, 1)
	batchStart := 0
	requestSize := 0
	for i, offset := range rowOffsets {
		fieldIdxs := idxComputer.Compute(int64(offset))
		curRowMessageSize, err := typeutil.EstimateEntitySize(fieldsData, offset, fieldIdxs...)
		if err != nil {
			return nil, err
		}
		if hasSingleRowLimit && curRowMessageSize >= singleRowLimit {
			return nil, merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"single row at offset %d is too large to fit in one WAL message: estimated size=%d bytes, limit=%d bytes",
				offset, curRowMessageSize, singleRowLimit,
			))
		}

		// If the insert message size exceeds the threshold, finish the current
		// batch first. Do not emit an empty batch before adding the first row.
		if i > batchStart && requestSize+curRowMessageSize >= splitThreshold {
			if contiguous {
				batches = append(batches, insertMsgBatch{start: batchStart, end: i})
			} else {
				emitSparseBatch(rowOffsets[batchStart])
			}
			batchStart = i
			requestSize = 0
		}
		requestSize += curRowMessageSize

		if !contiguous {
			if i == batchStart {
				// Defer the first row so singleton batches can use a view. Keep
				// only its indexes because Compute reuses its result buffer.
				if firstFieldIdxs == nil {
					firstFieldIdxs = make([]int64, len(fieldsData))
				}
				copy(firstFieldIdxs, fieldIdxs)
			} else {
				if copiedMsg == nil {
					copiedMsg = createInsertMsg(segmentID, channelName)
					appendRow(copiedMsg, rowOffsets[batchStart], firstFieldIdxs)
				}
				appendRow(copiedMsg, offset, fieldIdxs)
			}
		}
	}
	if !contiguous {
		if batchStart < len(rowOffsets) {
			emitSparseBatch(rowOffsets[batchStart])
		}
		return repackedMsgs, nil
	}
	if batchStart < len(rowOffsets) {
		batches = append(batches, insertMsgBatch{start: batchStart, end: len(rowOffsets)})
	}

	repackedMsgs = make([]msgstream.TsMsg, 0, len(batches))
	var (
		rangeIdxComputer  *typeutil.FieldDataIdxComputer
		appendIdxComputer *typeutil.FieldDataIdxComputer
		dataStarts        []int64
	)
	for _, batch := range batches {
		msg := createInsertMsg(segmentID, channelName)
		batchOffsets := rowOffsets[batch.start:batch.end]
		rowStart := batchOffsets[0]
		rowEnd := rowStart + len(batchOffsets)
		if canCreateInsertRangeView(insertMsg, rowStart, rowEnd) {
			if rangeIdxComputer == nil {
				rangeIdxComputer = typeutil.NewFieldDataIdxComputer(fieldsData)
				dataStarts = make([]int64, len(fieldsData))
			}
			copy(dataStarts, rangeIdxComputer.Compute(int64(rowStart)))
			dataEnds := rangeIdxComputer.Compute(int64(rowEnd))
			fieldViews, ok := typeutil.CreateFieldDataRangeView(
				fieldsData,
				int64(rowStart),
				int64(rowEnd),
				dataStarts,
				dataEnds,
			)
			if ok {
				msg.FieldsData = fieldViews
				msg.HashValues = insertMsg.HashValues[rowStart:rowEnd:rowEnd]
				msg.Timestamps = insertMsg.Timestamps[rowStart:rowEnd:rowEnd]
				msg.RowIDs = insertMsg.RowIDs[rowStart:rowEnd:rowEnd]
				msg.NumRows = uint64(rowEnd - rowStart)
				repackedMsgs = append(repackedMsgs, msg)
				continue
			}
		}

		if appendIdxComputer == nil {
			appendIdxComputer = typeutil.NewFieldDataIdxComputer(fieldsData)
		}
		for _, offset := range batchOffsets {
			fieldIdxs := appendIdxComputer.Compute(int64(offset))
			appendRow(msg, offset, fieldIdxs)
		}
		repackedMsgs = append(repackedMsgs, msg)
	}

	return repackedMsgs, nil
}

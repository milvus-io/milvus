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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// denseVectorReserveBytes returns the bytes PrepareResultFieldData reserves per
// row for dense vector columns. Those reservations are sized dim*capacity and
// do not depend on nullability, unlike EstimateEntitySize which skips null
// rows. Sparse vectors are excluded: they are reserved per row, not per
// dimension.
func denseVectorReserveBytes(fieldsData []*schemapb.FieldData) int {
	total := 0
	for _, fd := range fieldsData {
		dim := int(fd.GetVectors().GetDim())
		if dim <= 0 {
			continue
		}
		switch fd.GetType() {
		case schemapb.DataType_FloatVector:
			total += dim * 4
		case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
			total += dim * 2
		case schemapb.DataType_BinaryVector:
			total += dim / 8
		case schemapb.DataType_Int8Vector:
			total += dim
		}
	}
	return total
}

func genInsertMsgsByPartition(ctx context.Context,
	segmentID UniqueID,
	partitionID UniqueID,
	partitionName string,
	rowOffsets []int,
	channelName string,
	insertMsg *msgstream.InsertMsg,
) ([]msgstream.TsMsg, error) {
	threshold := Params.PulsarCfg.MaxMessageSize.GetAsInt()

	// PrepareResultFieldData sizes a dense vector column's backing array as
	// dim*capacity regardless of nullability, while EstimateEntitySize skips
	// null rows entirely. Deriving capacity from a row whose vectors happen to
	// be null would therefore over-reserve by orders of magnitude -- 10k rows
	// of a null 32768-dim float vector would reserve ~1.22GiB for an empty
	// payload. Use the schema-derived per-row reservation as a floor so the
	// reserved bytes stay bounded by the message threshold either way.
	vectorReserveFloor := denseVectorReserveBytes(insertMsg.GetFieldsData())

	// rowCapacity estimates how many rows will fit into a single message before
	// the size threshold forces a flush, so the destination buffers can be
	// preallocated. Without it the FieldData slices start at zero capacity and
	// Go's ~1.25x growth factor copies the payload roughly five times over.
	rowCapacity := func(rowSize, remaining int) int {
		if rowSize < vectorReserveFloor {
			rowSize = vectorReserveFloor
		}
		if rowSize <= 0 || rowSize >= threshold {
			return 1
		}
		n := threshold / rowSize
		if n > remaining {
			n = remaining
		}
		if n < 1 {
			n = 1
		}
		return n
	}

	// create empty insert message
	createInsertMsg := func(segmentID UniqueID, channelName string, capacity int) *msgstream.InsertMsg {
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
			// PrepareResultFieldData preallocates each column for `capacity`
			// rows. Field types it does not know about get a nil Field, which
			// AppendFieldData already creates lazily, so behavior is unchanged
			// for them.
			FieldsData: typeutil.PrepareResultFieldData(insertMsg.GetFieldsData(), int64(capacity)),
			RowIDs:     make([]int64, 0, capacity),
			Timestamps: make([]uint64, 0, capacity),
		}
		msg := &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				Ctx:        ctx,
				HashValues: make([]uint32, 0, capacity),
			},
			InsertRequest: insertReq,
		}

		return msg
	}

	fieldsData := insertMsg.GetFieldsData()
	idxComputer := typeutil.NewFieldDataIdxComputer(fieldsData)

	repackedMsgs := make([]msgstream.TsMsg, 0)
	requestSize := 0
	// The message is created on the first row rather than up front, so that its
	// capacity can be derived from that row's size.
	var msg *msgstream.InsertMsg
	for i, offset := range rowOffsets {
		fieldIdxs := idxComputer.Compute(int64(offset))
		curRowMessageSize, err := typeutil.EstimateEntitySize(fieldsData, offset, fieldIdxs...)
		if err != nil {
			return nil, err
		}

		// If the insert message size exceeds the threshold, flush the current
		// message first. A single row can be larger than the threshold, so do
		// not emit an empty message before adding that row.
		if msg == nil {
			msg = createInsertMsg(segmentID, channelName,
				rowCapacity(curRowMessageSize, len(rowOffsets)-i))
		} else if msg.NumRows > 0 && requestSize+curRowMessageSize >= threshold {
			repackedMsgs = append(repackedMsgs, msg)
			msg = createInsertMsg(segmentID, channelName,
				rowCapacity(curRowMessageSize, len(rowOffsets)-i))
			requestSize = 0
		}

		typeutil.AppendFieldData(msg.FieldsData, fieldsData, int64(offset), fieldIdxs...)
		msg.HashValues = append(msg.HashValues, insertMsg.HashValues[offset])
		msg.Timestamps = append(msg.Timestamps, insertMsg.Timestamps[offset])
		msg.RowIDs = append(msg.RowIDs, insertMsg.RowIDs[offset])
		msg.NumRows++
		requestSize += curRowMessageSize
	}
	if msg != nil && msg.NumRows > 0 {
		repackedMsgs = append(repackedMsgs, msg)
	}

	return repackedMsgs, nil
}

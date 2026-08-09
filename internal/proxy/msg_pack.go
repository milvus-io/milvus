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
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// visitInsertRowsByMessageSize partitions a monotonically increasing row
// selection and synchronously visits each per-message view. The rows slice
// borrows the backing array of rowOffsets. firstFieldDataIndices is reusable
// scratch and is valid only until visit returns.
//
// Keep the existing entity-size split rule here. This change removes the
// materialized repack buffers without also changing message boundaries.
func visitInsertRowsByMessageSize(
	insertMsg *msgstream.InsertMsg,
	rowOffsets []int,
	visit func(rows []int, firstFieldDataIndices []int64) error,
) error {
	if len(rowOffsets) == 0 {
		return nil
	}

	threshold := Params.PulsarCfg.MaxMessageSize.GetAsInt()
	fieldsData := insertMsg.GetFieldsData()
	idxComputer := typeutil.NewFieldDataIdxComputer(fieldsData)
	firstFieldDataIndices := make([]int64, len(fieldsData))

	start := 0
	requestSize := 0
	for i, offset := range rowOffsets {
		fieldIdxs := idxComputer.Compute(int64(offset))
		rowSize, err := typeutil.EstimateEntitySize(fieldsData, offset, fieldIdxs...)
		if err != nil {
			return err
		}
		if i == start {
			copy(firstFieldDataIndices, fieldIdxs)
		}

		// A single row may be larger than the threshold. Do not emit an empty
		// selection before it; match the previous repack behavior exactly.
		if i > start && requestSize+rowSize >= threshold {
			if err := visit(rowOffsets[start:i], firstFieldDataIndices); err != nil {
				return err
			}
			start = i
			requestSize = 0
			copy(firstFieldDataIndices, fieldIdxs)
		}
		requestSize += rowSize
	}

	return visit(rowOffsets[start:], firstFieldDataIndices)
}

// splitInsertRowsByMessageSize exposes the borrowed row views for focused
// message-boundary tests. Production encoding uses the synchronous visitor
// above so it does not allocate an outer selection slice.
func splitInsertRowsByMessageSize(insertMsg *msgstream.InsertMsg, rowOffsets []int) ([][]int, error) {
	var selections [][]int
	err := visitInsertRowsByMessageSize(insertMsg, rowOffsets, func(rows []int, _ []int64) error {
		selections = append(selections, rows)
		return nil
	})
	return selections, err
}

// genInsertMessagesByPartition builds V1 WAL insert messages directly from
// borrowed row selections. The selected rows are encoded into the final
// protobuf payload without first materializing a second InsertRequest.
func genInsertMessagesByPartition(
	segmentID UniqueID,
	partitionID UniqueID,
	partitionName string,
	rowOffsets []int,
	channelName string,
	insertMsg *msgstream.InsertMsg,
	ez *message.CipherConfig,
	schemaVersion int32,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0, 1)
	err := visitInsertRowsByMessageSize(insertMsg, rowOffsets, func(rows []int, firstFieldDataIndices []int64) error {
		template := &msgpb.InsertRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Insert),
				commonpbutil.WithTimeStamp(insertMsg.BeginTimestamp),
				commonpbutil.WithSourceID(insertMsg.Base.SourceID),
			),
			CollectionID:   insertMsg.CollectionID,
			PartitionID:    partitionID,
			DbName:         insertMsg.DbName,
			CollectionName: insertMsg.CollectionName,
			PartitionName:  partitionName,
			SegmentID:      segmentID,
			ShardName:      channelName,
			NumRows:        uint64(len(rows)),
			Version:        msgpb.InsertDataVersion_ColumnBased,
		}
		encoder, err := fastpb.NewInsertRequestViewEncoderWithFirstFieldIndices(
			template,
			insertMsg.InsertRequest,
			rows,
			firstFieldDataIndices,
		)
		if err != nil {
			return err
		}
		if hasSingleRowLimit && curRowMessageSize >= singleRowLimit {
			return nil, merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"single row at offset %d is too large to fit in one WAL message: estimated size=%d bytes, limit=%d bytes",
				offset, curRowMessageSize, singleRowLimit,
			))
		}

		// BuildMutable consumes the borrowed encoder synchronously. Once it
		// returns, the message owns only the final payload and the splitter may
		// safely reuse firstFieldDataIndices for the next view.
		newMsg, err := message.NewInsertMessageBuilderV1().
			WithVChannel(channelName).
			WithHeader(&message.InsertMessageHeader{
				CollectionId: insertMsg.CollectionID,
				Partitions: []*message.PartitionSegmentAssignment{
					{
						PartitionId: partitionID,
						Rows:        uint64(len(rows)),
						BinarySize:  0, // StreamingNode uses the encoded message size when absent.
					},
				},
				SchemaVersion: &schemaVersion,
			}).
			WithBodyEncoder(encoder).
			WithCipher(ez).
			BuildMutable()
		if err != nil {
			return err
		}
		messages = append(messages, newMsg)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return messages, nil
}

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
)

// insertRequestBodyLimit budgets the plaintext InsertRequest body inside the
// broker-facing message limit. GetMessageSizeLimits normalizes mixed-generation
// refreshable config as a pair; keep the final guard for invalid test-only
// overrides. A single oversized row is still emitted by InsertRequestViewCursor.
func insertRequestBodyLimit() int {
	maxMessageSize, reserve := Params.PulsarCfg.GetMessageSizeLimits()
	if maxMessageSize <= 0 || reserve < 0 || reserve >= maxMessageSize {
		return 1
	}
	return maxMessageSize - reserve
}

// visitInsertRowsByMessageSize partitions a monotonically increasing row
// selection by the exact plaintext InsertRequest protobuf size and
// synchronously visits each per-message encoder. Both the rows slice and the
// encoder borrow reusable cursor scratch and are valid only until visit
// returns.
func visitInsertRowsByMessageSize(
	template *msgpb.InsertRequest,
	insertMsg *msgstream.InsertMsg,
	rowOffsets []int,
	visit func(rows []int, encoder *fastpb.InsertRequestViewEncoder) error,
) error {
	if len(rowOffsets) == 0 {
		return nil
	}

	bodyLimit := insertRequestBodyLimit()
	viewCursor, err := fastpb.NewInsertRequestViewCursor(insertMsg.InsertRequest)
	if err != nil {
		return err
	}
	start := 0
	for start < len(rowOffsets) {
		encoder, consumed, err := viewCursor.NextEncoder(template, rowOffsets[start:], bodyLimit)
		if err != nil {
			return err
		}
		rows := rowOffsets[start : start+consumed]
		if err := visit(rows, encoder); err != nil {
			return err
		}
		start += consumed
	}
	return nil
}

// splitInsertRowsByMessageSize exposes the borrowed row views for focused
// message-boundary tests. Production encoding uses the synchronous visitor
// above so it does not allocate an outer selection slice.
func splitInsertRowsByMessageSize(insertMsg *msgstream.InsertMsg, rowOffsets []int) ([][]int, error) {
	template := &msgpb.InsertRequest{
		Base:           insertMsg.GetBase(),
		ShardName:      insertMsg.GetShardName(),
		DbName:         insertMsg.GetDbName(),
		CollectionName: insertMsg.GetCollectionName(),
		PartitionName:  insertMsg.GetPartitionName(),
		DbID:           insertMsg.GetDbID(),
		CollectionID:   insertMsg.GetCollectionID(),
		PartitionID:    insertMsg.GetPartitionID(),
		SegmentID:      insertMsg.GetSegmentID(),
		Version:        insertMsg.GetVersion(),
		Namespace:      insertMsg.Namespace,
	}
	var selections [][]int
	err := visitInsertRowsByMessageSize(template, insertMsg, rowOffsets, func(rows []int, encoder *fastpb.InsertRequestViewEncoder) error {
		selections = append(selections, rows)
		size, err := encoder.EncodedSize()
		if err != nil {
			return err
		}
		_, err = encoder.MarshalTo(make([]byte, size))
		return err
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
		Version:        msgpb.InsertDataVersion_ColumnBased,
	}
	messages := make([]message.MutableMessage, 0, 1)
	err := visitInsertRowsByMessageSize(template, insertMsg, rowOffsets, func(rows []int, encoder *fastpb.InsertRequestViewEncoder) error {
		// BuildMutable consumes the borrowed encoder synchronously. Once it
		// returns, the message owns only the final payload and the splitter may
		// safely reuse the cursor scratch for the next view.
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

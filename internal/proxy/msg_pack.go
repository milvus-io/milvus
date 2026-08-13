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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	streamingutil "github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func getActiveWALName() message.WALName {
	return streamingutil.MustSelectWALName()
}

// activeWALMessageSize reports the message-size limit the split budget should
// be computed against for walName. Pulsar, Kafka, and Woodpecker each have
// their own hard cap on one message; paramtable owns the name-to-limit
// mapping so every consumer budgets against the same value. RocksMQ has no
// per-entry limit of its own and budgets against the Pulsar-shaped default,
// same as before any backend had its own hard limit checked.
func activeWALMessageSize(walName message.WALName) int {
	return Params.WALMaxMessageSize(walName.String())
}

// messageBodyLimit budgets a plaintext message body inside the active WAL
// backend's broker-facing message limit -- not always Pulsar's, so a message
// packed for Kafka stays under Kafka's own limit instead of Pulsar's. Before
// this, every backend budgeted against pulsar.maxMessageSize regardless of
// which one was actually sending the message: on a deployment where
// kafka.message.max.bytes was set below Pulsar's default, several small rows
// could still be packed past what Kafka would accept, and the broker would
// reject the message on every send (#52413's failure mode, reached from a
// packed message instead of a single oversized row).
//
// GetMessageSizeLimitsFor normalizes mixed-generation refreshable config as a
// pair; keep the final guard for invalid test-only overrides. Insert and
// Delete preserve their existing single-oversized-row behavior.
func messageBodyLimit(walName message.WALName) int {
	maxMessageSize, reserve := Params.PulsarCfg.GetMessageSizeLimitsFor(activeWALMessageSize(walName))
	if maxMessageSize <= 0 || reserve < 0 || reserve >= maxMessageSize {
		return 1
	}
	return maxMessageSize - reserve
}

// walHasSingleMessageLimit reports whether the active WAL backend enforces a
// hard limit on one message at all. Pulsar, Kafka, and Woodpecker each do;
// RocksMQ's page size is not a per-entry limit, so it reports false.
func walHasSingleMessageLimit(walName message.WALName) bool {
	switch walName {
	case message.WALNamePulsar, message.WALNameKafka, message.WALNameWoodpecker:
		return activeWALMessageSize(walName) > 0
	default:
		return false
	}
}

// visitInsertRowsByMessageSize partitions a monotonically increasing row
// selection by the exact plaintext InsertRequest protobuf size and
// synchronously visits each per-message encoder. Both the rows slice and the
// encoder borrow reusable cursor scratch and are valid only until visit
// returns.
//
// A message the active WAL cannot carry is refused here rather than handed to
// the broker. The size compared against that limit is the exact encoded body,
// but that body is still only the plaintext InsertRequest this Proxy builds --
// not the final WAL record StreamingNode later wraps it into with a header,
// properties, and (if enabled) cipher overhead, all of which land on top of
// this measurement after this check runs. bodyLimit, not the backend's raw
// message-size config, is what is compared against here: it already reserves
// pulsar.messageReserveSize bytes off the broker limit for exactly that
// envelope, and that reservation has to apply the same way whether the bytes
// it protects belong to several packed rows or to one row alone -- the
// envelope is added once per message either way, not once per row. A row
// measured just under the raw broker limit but over bodyLimit can still come
// out of StreamingNode's wrapping over that raw limit and be rejected by the
// broker on every send, permanently stalling the DML channel (#52413's
// failure mode) -- reusing bodyLimit here instead of the raw limit is what
// closes that gap.
func visitInsertRowsByMessageSize(
	template *msgpb.InsertRequest,
	insertMsg *msgstream.InsertMsg,
	rowOffsets []int,
	walName message.WALName,
	visit func(rows []int, encoder *fastpb.InsertRequestViewEncoder) error,
) error {
	if len(rowOffsets) == 0 {
		return nil
	}

	bodyLimit := messageBodyLimit(walName)
	checkSingleMessageLimit := walHasSingleMessageLimit(walName)
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
		// Only a message carrying a single row is refused, matching the rule
		// #52420 established: that row cannot be split any further, so there is
		// nothing left to try. A message holding several rows does not need the
		// same check -- bodyLimit is already computed against this walName's own
		// backend limit, so a packed message's body cannot reach it in the first
		// place; only a single row too large to split can still get there.
		if checkSingleMessageLimit && len(rows) == 1 {
			size, err := encoder.EncodedSize()
			if err != nil {
				return err
			}
			if size >= bodyLimit {
				return merr.WrapErrParameterTooLarge(fmt.Sprintf(
					"single row at offset %d is too large to fit in one WAL message: encoded size=%d bytes, limit=%d bytes",
					rows[0], size, bodyLimit))
			}
		}
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
func splitInsertRowsByMessageSize(insertMsg *msgstream.InsertMsg, rowOffsets []int, walName message.WALName) ([][]int, error) {
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
	err := visitInsertRowsByMessageSize(template, insertMsg, rowOffsets, walName, func(rows []int, encoder *fastpb.InsertRequestViewEncoder) error {
		selections = append(selections, rows)
		size, err := encoder.EncodedSize()
		if err != nil {
			return err
		}
		_, err = encoder.MarshalTo(make([]byte, size))
		return err
	})
	if err != nil {
		// Same contract as genInsertMessagesByPartition: a rejected batch
		// produces no selections, not a partial prefix.
		return nil, err
	}
	return selections, nil
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
	walName message.WALName,
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
	err := visitInsertRowsByMessageSize(template, insertMsg, rowOffsets, walName, func(rows []int, encoder *fastpb.InsertRequestViewEncoder) error {
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

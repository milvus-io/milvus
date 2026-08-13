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
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
)

func TestMessageBodyLimit(t *testing.T) {
	paramtable.Init()
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
	defer Params.Reset(Params.PulsarCfg.MessageReserveSize.Key)

	// The default reserve is taken out of the broker-facing limit.
	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "2097152"))
	assert.Equal(t, 2097152-4096, messageBodyLimit(message.WALNamePulsar))

	// A configured reserve replaces the default one.
	require.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "1024"))
	assert.Equal(t, 2097152-1024, messageBodyLimit(message.WALNamePulsar))

	// An oversized reserve falls back to the safe default instead of collapsing
	// the body budget to one byte.
	require.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "4194304"))
	assert.Equal(t, 2097152-4096, messageBodyLimit(message.WALNamePulsar))

	// If the broker limit is then lowered below 4 KiB, the effective reserve is
	// zero rather than max-1, so inserts do not degenerate into one-row messages.
	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "1024"))
	assert.Equal(t, 1024, messageBodyLimit(message.WALNamePulsar))
}

// The body budget now tracks the active WAL's own broker limit instead of
// always Pulsar's, so a message packed for Kafka or Woodpecker stays under
// that backend's own limit even when it differs from Pulsar's -- in either
// direction. RocksMQ has no per-entry limit of its own, so it keeps using
// the Pulsar-shaped default as a general-purpose packing threshold.
func TestMessageBodyLimitIsWALSpecific(t *testing.T) {
	paramtable.Init()
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
	defer Params.Reset(Params.PulsarCfg.MessageReserveSize.Key)
	defer Params.Reset(Params.KafkaCfg.ProducerMessageMaxBytes.Key)

	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "2097152"))
	require.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "4096"))

	t.Run("kafka limit below pulsar's", func(t *testing.T) {
		require.NoError(t, Params.Save(Params.KafkaCfg.ProducerMessageMaxBytes.Key, "524288"))
		assert.Equal(t, 524288-4096, messageBodyLimit(message.WALNameKafka))
		assert.Equal(t, 2097152-4096, messageBodyLimit(message.WALNamePulsar))
	})

	t.Run("kafka limit above pulsar's", func(t *testing.T) {
		require.NoError(t, Params.Save(Params.KafkaCfg.ProducerMessageMaxBytes.Key, "10485760"))
		assert.Equal(t, 10485760-4096, messageBodyLimit(message.WALNameKafka))
	})

	t.Run("woodpecker uses its own limit", func(t *testing.T) {
		require.NoError(t, Params.Save(Params.WoodpeckerCfg.MaxMessageSize.Key, "524288"))
		defer Params.Reset(Params.WoodpeckerCfg.MaxMessageSize.Key)
		assert.Equal(t, 524288-4096, messageBodyLimit(message.WALNameWoodpecker))
	})

	t.Run("woodpecker default limit is 10 MiB", func(t *testing.T) {
		assert.Equal(t, 10485760-4096, messageBodyLimit(message.WALNameWoodpecker))
	})

	t.Run("rocksmq uses the pulsar-shaped default", func(t *testing.T) {
		assert.Equal(t, 2097152-4096, messageBodyLimit(message.WALNameRocksmq))
	})
}

// A row that does not fit under the plaintext body budget is still emitted on
// its own -- there is nothing left to split -- as long as the active WAL has no
// hard limit of its own that it would breach. RocksMQ's page size is not a
// per-entry limit, so it is the one backend this still applies to; Pulsar,
// Kafka, and Woodpecker each enforce their own cap and are covered by the
// rejection tests below.
func TestSplitInsertRowsByMessageSizeSingleOversizedRow(t *testing.T) {
	// One byte above the default reserve, so the plaintext body budget is 1 byte.
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "4097"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
	assert.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "4096"))
	defer Params.Reset(Params.PulsarCfg.MessageReserveSize.Key)

	insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
	selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0}, message.WALNameRocksmq)
	assert.NoError(t, err)
	assert.Equal(t, [][]int{{0}}, selections)
}

// The same row is refused when the WAL would reject the message on the wire:
// the send never succeeds, so retrying it forever stalls the DML channel
// (#52413). The limit is compared against the exact encoded message, which
// includes the envelope, not against a per-row estimate.
func TestSplitInsertRowsByMessageSizeRejectsOversizedMessage(t *testing.T) {
	// Large enough that a small row's message -- envelope included -- fits, so
	// the row that does not can be named.
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "512"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
	assert.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "0"))
	defer Params.Reset(Params.PulsarCfg.MessageReserveSize.Key)

	t.Run("only row", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
		selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0}, message.WALNamePulsar)
		assert.Nil(t, selections)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Contains(t, err.Error(), "single row at offset 0")
		assert.False(t, merr.Status(err).GetRetriable())
	})

	t.Run("later row", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest("small", strings.Repeat("x", 1024))
		selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0, 1}, message.WALNamePulsar)
		assert.Nil(t, selections)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Contains(t, err.Error(), "single row at offset 1")
	})

	t.Run("built messages are refused too", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
		msgs, err := genInsertMessagesByPartition(0, 1, "test_partition", []int{0}, "test_channel",
			insertMsg, nil, 7, nil, message.WALNamePulsar)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})
}

// The single-row hard check is compared against bodyLimit, not the WAL
// backend's raw message-size config, because bodyLimit is what already
// reserves headroom for the header, properties, and cipher overhead
// StreamingNode wraps around this plaintext measurement afterward -- and that
// overhead lands on a solo row's message exactly the same as a packed one's,
// so the same reservation has to apply to both. A row measured just under the
// raw limit but over bodyLimit used to pass this check and could still come
// out of StreamingNode's wrapping over the raw limit, so the broker would
// reject the message on every send and permanently stall the DML channel
// (#52413's failure mode, reached through a row this check was supposed to
// catch instead of the packed-message case #52413 originally covered).
func TestSplitInsertRowsByMessageSizeRejectsRowOverBodyLimitEvenUnderRawLimit(t *testing.T) {
	insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
	// This row's exact plaintext encoded size is 1109 bytes (measured directly
	// against the production encoder, not estimated).
	const encodedSize = 1109

	t.Run("under the raw limit alone is not enough", func(t *testing.T) {
		assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, strconv.Itoa(encodedSize+1)))
		defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
		// A reserve bigger than the one-byte raw headroom: the row fits under
		// the raw limit (1109 < 1110) but not under bodyLimit (1109 >= 1060).
		assert.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "50"))
		defer Params.Reset(Params.PulsarCfg.MessageReserveSize.Key)

		selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0}, message.WALNamePulsar)
		assert.Nil(t, selections)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Contains(t, err.Error(), "single row at offset 0")
	})

	t.Run("passes once it is also under bodyLimit", func(t *testing.T) {
		assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, strconv.Itoa(encodedSize+1)))
		defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
		// Same raw limit as above, but a reserve small enough that bodyLimit
		// (encodedSize+1-1 == encodedSize) still clears the row.
		assert.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "0"))
		defer Params.Reset(Params.PulsarCfg.MessageReserveSize.Key)

		selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0}, message.WALNamePulsar)
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{0}}, selections)
	})
}

// Each backend answers with its own limit, so a row Pulsar would refuse can be
// perfectly sendable on Kafka.
func TestSplitInsertRowsByMessageSizeUsesWALSpecificLimit(t *testing.T) {
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "64"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
	assert.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "0"))
	defer Params.Reset(Params.PulsarCfg.MessageReserveSize.Key)
	assert.NoError(t, Params.Save(Params.KafkaCfg.ProducerMessageMaxBytes.Key, "4096"))
	defer Params.Reset(Params.KafkaCfg.ProducerMessageMaxBytes.Key)

	t.Run("kafka allows a row above the pulsar limit", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
		selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0}, message.WALNameKafka)
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{0}}, selections)
	})

	t.Run("kafka refuses a row above its own limit", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 8192))
		selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0}, message.WALNameKafka)
		assert.Nil(t, selections)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("woodpecker refuses a row above its own limit", func(t *testing.T) {
		assert.NoError(t, Params.Save(Params.WoodpeckerCfg.MaxMessageSize.Key, "512"))
		defer Params.Reset(Params.WoodpeckerCfg.MaxMessageSize.Key)

		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 8192))
		selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0}, message.WALNameWoodpecker)
		assert.Nil(t, selections)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("woodpecker allows a row above the pulsar limit but under its own", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 8192))
		selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0}, message.WALNameWoodpecker)
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{0}}, selections)
	})

	t.Run("rocksmq has no single message limit", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 8192))
		selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0}, message.WALNameRocksmq)
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{0}}, selections)
	})
}

func TestSplitInsertRowsByMessageSizeSplitsMultipleRows(t *testing.T) {
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "512"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
	assert.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "0"))
	defer Params.Reset(Params.PulsarCfg.MessageReserveSize.Key)

	insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 300), strings.Repeat("y", 300))
	selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0, 1}, message.WALNamePulsar)
	assert.NoError(t, err)
	assert.Equal(t, [][]int{{0}, {1}}, selections)
}

// The scenario #52413's original fix left open: several rows, each individually
// well under Pulsar's limit, packed into one message that only Kafka's own
// (smaller) limit would have rejected. Before bodyLimit became WAL-specific,
// splitting used Pulsar's budget regardless of which backend was actually
// sending the message, so this message would have gone out at a size the
// active Kafka broker does not accept, on every retry, forever.
func TestGenInsertMessagesByPartitionKafkaBodyStaysUnderKafkaLimit(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "2097152"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
	assert.NoError(t, Params.Save(Params.PulsarCfg.MessageReserveSize.Key, "0"))
	defer Params.Reset(Params.PulsarCfg.MessageReserveSize.Key)
	assert.NoError(t, Params.Save(Params.KafkaCfg.ProducerMessageMaxBytes.Key, "512"))
	defer Params.Reset(Params.KafkaCfg.ProducerMessageMaxBytes.Key)

	insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 200), strings.Repeat("y", 200), strings.Repeat("z", 200))
	kafkaLimit := int(Params.KafkaCfg.ProducerMessageMaxBytes.GetAsInt32())

	kafkaMsgs, err := genInsertMessagesByPartition(0, 1, "test_partition", []int{0, 1, 2}, "test_channel",
		insertMsg, nil, 7, nil, message.WALNameKafka)
	require.NoError(t, err)
	require.Greater(t, len(kafkaMsgs), 1, "packing against pulsar's much larger budget would wrongly keep this to one message")
	for _, msg := range kafkaMsgs {
		payload := msg.IntoMessageProto().GetPayload()
		assert.Less(t, len(payload), kafkaLimit)
	}

	// Pulsar's own budget is unaffected: the same rows still fit in one message.
	pulsarMsgs, err := genInsertMessagesByPartition(0, 1, "test_partition", []int{0, 1, 2}, "test_channel",
		insertMsg, nil, 7, nil, message.WALNamePulsar)
	require.NoError(t, err)
	require.Len(t, pulsarMsgs, 1)
}

func newVarCharInsertMsgForPackTest(rows ...string) *msgstream.InsertMsg {
	hashValues := make([]uint32, len(rows))
	timestamps := make([]uint64, len(rows))
	rowIDs := make([]int64, len(rows))
	for i := range rows {
		hashValues[i] = 1
		timestamps[i] = 1
		rowIDs[i] = int64(i + 1)
	}

	return &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:        context.Background(),
			HashValues: hashValues,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Insert,
				SourceID: paramtable.GetNodeID(),
			},
			DbName:         "default",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			NumRows:        uint64(len(rows)),
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_VarChar,
					FieldId:   101,
					FieldName: "large_text",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: rows},
							},
						},
					},
				},
			},
			Timestamps: timestamps,
			RowIDs:     rowIDs,
			Version:    msgpb.InsertDataVersion_ColumnBased,
		},
	}
}

func TestRepackInsertData(t *testing.T) {
	nb := 10
	hash := testutils.GenerateHashKeys(nb)
	prefix := "TestRepackInsertData"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	ctx := context.Background()

	mix := NewMixCoordMock()
	defer mix.Close()

	cache := NewMockCache(t)
	globalMetaCache = cache

	idAllocator, err := allocator.NewIDAllocator(ctx, mix, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	t.Run("create collection", func(t *testing.T) {
		resp, err := mix.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		assert.NoError(t, err)

		resp, err = mix.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  paramtable.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		assert.NoError(t, err)
	})

	fieldData := generateFieldData(schemapb.DataType_Int64, testInt64Field, nb)
	insertMsg := &BaseInsertTask{
		BaseMsg: msgstream.BaseMsg{
			HashValues: hash,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Insert,
				MsgID:    0,
				SourceID: paramtable.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
			NumRows:        uint64(nb),
			FieldsData:     []*schemapb.FieldData{fieldData},
			Version:        msgpb.InsertDataVersion_ColumnBased,
		},
	}
	insertMsg.Timestamps = make([]uint64, nb)
	for index := range insertMsg.Timestamps {
		insertMsg.Timestamps[index] = insertMsg.BeginTimestamp
	}
	insertMsg.RowIDs = make([]UniqueID, nb)
	for index := range insertMsg.RowIDs {
		insertMsg.RowIDs[index] = int64(index)
	}
}

func TestRepackInsertDataWithPartitionKey(t *testing.T) {
	nb := 10
	hash := testutils.GenerateHashKeys(nb)
	prefix := "TestRepackInsertData"
	collectionName := prefix + funcutil.GenRandomStr()

	ctx := context.Background()
	dbName := GetCurDBNameFromContextOrDefault(ctx)

	mix := NewMixCoordMock()

	err := InitMetaCache(ctx, mix)
	assert.NoError(t, err)

	idAllocator, err := allocator.NewIDAllocator(ctx, mix, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	fieldName2Types := map[string]schemapb.DataType{
		testInt64Field:    schemapb.DataType_Int64,
		testVarCharField:  schemapb.DataType_VarChar,
		testFloatVecField: schemapb.DataType_FloatVector,
	}

	t.Run("create collection with partition key", func(t *testing.T) {
		schema := ConstructCollectionSchemaWithPartitionKey(collectionName, fieldName2Types, testInt64Field, testVarCharField, false)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		resp, err := mix.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			NumPartitions:  100,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		assert.NoError(t, err)
	})

	fieldNameToDatas := make(map[string]*schemapb.FieldData)
	fieldDatas := make([]*schemapb.FieldData, 0)
	for name, dataType := range fieldName2Types {
		data := generateFieldData(dataType, name, nb)
		fieldNameToDatas[name] = data
		fieldDatas = append(fieldDatas, data)
	}

	insertMsg := &BaseInsertTask{
		BaseMsg: msgstream.BaseMsg{
			HashValues: hash,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Insert,
				MsgID:    0,
				SourceID: paramtable.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			NumRows:        uint64(nb),
			FieldsData:     fieldDatas,
			Version:        msgpb.InsertDataVersion_ColumnBased,
		},
	}
	insertMsg.Timestamps = make([]uint64, nb)
	for index := range insertMsg.Timestamps {
		insertMsg.Timestamps[index] = insertMsg.BeginTimestamp
	}
	insertMsg.RowIDs = make([]UniqueID, nb)
	for index := range insertMsg.RowIDs {
		insertMsg.RowIDs[index] = int64(index)
	}
}

func TestSplitInsertRowsByMessageSize(t *testing.T) {
	paramtable.Init()

	newInsertMsg := func(rows int) *msgstream.InsertMsg {
		ids := make([]int64, rows)
		timestamps := make([]uint64, rows)
		for i := 0; i < rows; i++ {
			ids[i] = int64(i)
			timestamps[i] = uint64(i)
		}
		return &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				NumRows:    uint64(rows),
				RowIDs:     ids,
				Timestamps: timestamps,
				FieldsData: []*schemapb.FieldData{
					{
						Type: schemapb.DataType_Int64, FieldId: 100, FieldName: "pk",
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}},
						}},
					},
				},
			},
		}
	}

	offsets := func(n int) []int {
		out := make([]int, n)
		for i := range out {
			out[i] = i
		}
		return out
	}

	t.Run("all rows borrow one selection", func(t *testing.T) {
		rows := offsets(100)
		selections, err := splitInsertRowsByMessageSize(newInsertMsg(len(rows)), rows, message.WALNameRocksmq)
		assert.NoError(t, err)
		assert.Len(t, selections, 1)
		assert.Equal(t, rows, selections[0])

		// The selection is a view over the caller's offset array, not a copy.
		rows[0] = 99
		assert.Equal(t, 99, selections[0][0])
	})

	t.Run("splitting on the size threshold still works", func(t *testing.T) {
		// Force a tiny body budget so every row lands in its own message.
		require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "4097"))
		defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

		const rows = 5
		rowOffsets := offsets(rows)
		selections, err := splitInsertRowsByMessageSize(newInsertMsg(rows), rowOffsets, message.WALNameRocksmq)
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{0}, {1}, {2}, {3}, {4}}, selections)
	})

	t.Run("no rows produces no messages", func(t *testing.T) {
		selections, err := splitInsertRowsByMessageSize(newInsertMsg(3), nil, message.WALNameRocksmq)
		assert.NoError(t, err)
		assert.Empty(t, selections)
	})
}

func TestGenInsertMessagesByPartitionSplitsByPlaintextBodyLimit(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "1048576"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

	const rows = 3
	value := strings.Repeat("v", 80*1024)
	source := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{BeginTimestamp: 99},
		InsertRequest: &msgpb.InsertRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: 42},
			CollectionID:   100,
			DbName:         strings.Repeat("db", 32),
			CollectionName: strings.Repeat("collection", 16),
			NumRows:        rows,
			RowIDs:         []int64{1, 1, 1},
			Timestamps:     []uint64{1, 1, 1},
			FieldsData: []*schemapb.FieldData{
				{
					Type: schemapb.DataType_VarChar, FieldId: 100, FieldName: strings.Repeat("field", 32),
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{value, value, value}}},
					}},
				},
			},
			Version: msgpb.InsertDataVersion_ColumnBased,
		},
	}
	partitionName := strings.Repeat("partition", 16)
	channelName := strings.Repeat("channel", 16)

	twoRows, err := genInsertMessagesByPartition(200, 300, partitionName, []int{0, 1}, channelName, source, nil, 7, nil, message.WALNameRocksmq)
	require.NoError(t, err)
	require.Len(t, twoRows, 1)
	twoRowRecord := twoRows[0].IntoMessageProto()
	// A limit just below the two-row plaintext body forces one row per message.
	// The reserve shrinks the body budget further, so it stays below it too.
	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, strconv.Itoa(len(twoRowRecord.GetPayload())-1)))
	bodyLimit := messageBodyLimit(message.WALNameRocksmq)

	msgs, err := genInsertMessagesByPartition(200, 300, partitionName, []int{0, 1, 2}, channelName, source, nil, 7, nil, message.WALNameRocksmq)
	require.NoError(t, err)
	require.Len(t, msgs, 3, "each row must land in its own message under this body limit")
	for _, msg := range msgs {
		assert.LessOrEqual(t, len(msg.Payload()), bodyLimit)
		body := message.MustAsMutableInsertMessageV1(msg).MustBody()
		assert.Equal(t, uint64(1), body.GetNumRows())
		assert.Equal(t, commonpb.MsgType_Insert, body.GetBase().GetMsgType())
		assert.Equal(t, uint64(99), body.GetBase().GetTimestamp())
		assert.Equal(t, int64(42), body.GetBase().GetSourceID())
		assert.Equal(t, int64(100), body.GetCollectionID())
		assert.Equal(t, int64(300), body.GetPartitionID())
		assert.Equal(t, int64(200), body.GetSegmentID())
		assert.Equal(t, partitionName, body.GetPartitionName())
		assert.Equal(t, channelName, body.GetShardName())
		assert.Equal(t, source.GetDbName(), body.GetDbName())
		assert.Equal(t, source.GetCollectionName(), body.GetCollectionName())
		assert.Equal(t, msgpb.InsertDataVersion_ColumnBased, body.GetVersion())
		assert.Equal(t, []int64{1}, body.GetRowIDs())
		assert.Equal(t, []uint64{1}, body.GetTimestamps())
		require.Len(t, body.GetFieldsData(), 1)
		field := body.GetFieldsData()[0]
		assert.Equal(t, schemapb.DataType_VarChar, field.GetType())
		assert.Equal(t, int64(100), field.GetFieldId())
		assert.Equal(t, source.GetFieldsData()[0].GetFieldName(), field.GetFieldName())
		assert.Equal(t, []string{value}, field.GetScalars().GetStringData().GetData())
		assert.Equal(t, proto.Size(body), len(msg.Payload()))
	}
}

func TestGenInsertMessagesByPartitionSmallScalars(t *testing.T) {
	paramtable.Init()
	const maxMessageSize = 64 * 1024
	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, strconv.Itoa(maxMessageSize)))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

	const (
		rows       = 1024
		fieldCount = 128
	)
	rowIDs := make([]int64, rows)
	timestamps := make([]uint64, rows)
	selection := make([]int, rows)
	for i := range rows {
		rowIDs[i] = int64(i + 1)
		timestamps[i] = uint64(i + 1)
		selection[i] = i
	}

	fields := make([]*schemapb.FieldData, 0, fieldCount)
	for fieldID := range fieldCount {
		values := make([]int64, rows)
		for row := range values {
			values[row] = 1
		}
		fields = append(fields, &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldId:   int64(1000 + fieldID),
			FieldName: "small_scalar_" + strconv.Itoa(fieldID),
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
			}},
		})
	}

	source := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{BeginTimestamp: 99},
		InsertRequest: &msgpb.InsertRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: 42},
			CollectionID:   100,
			DbName:         "db",
			CollectionName: "collection",
			NumRows:        rows,
			RowIDs:         rowIDs,
			Timestamps:     timestamps,
			FieldsData:     fields,
			Version:        msgpb.InsertDataVersion_ColumnBased,
		},
	}

	msgs, err := genInsertMessagesByPartition(200, 300, "partition", selection, "vchannel", source, nil, 7, nil, message.WALNameRocksmq)
	require.NoError(t, err)
	require.Greater(t, len(msgs), 1)
	require.Less(t, len(msgs), rows/2, "splitting must not degenerate into one WAL message per row")

	totalRows := 0
	for i, msg := range msgs {
		raw := msg.IntoMessageProto()
		assert.LessOrEqual(t, len(raw.GetPayload()), messageBodyLimit(message.WALNameRocksmq))
		body := message.MustAsMutableInsertMessageV1(msg).MustBody()
		if i < len(msgs)-1 {
			assert.Greater(t, body.GetNumRows(), uint64(1), "non-final messages should use the full body budget")
		}
		totalRows += int(body.GetNumRows())
	}
	assert.Equal(t, rows, totalRows)
}

func TestGenInsertMessagesByPartitionEncodesSelectionView(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "1048576"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

	const dim = 2
	source := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{BeginTimestamp: 99},
		InsertRequest: &msgpb.InsertRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: 42},
			CollectionID:   100,
			DbName:         "db",
			CollectionName: "collection",
			PartitionName:  "source-partition",
			NumRows:        4,
			RowIDs:         []int64{10, 11, 12, 13},
			Timestamps:     []uint64{20, 21, 22, 23},
			FieldsData: []*schemapb.FieldData{
				{
					Type: schemapb.DataType_Int64, FieldId: 100, FieldName: "pk",
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{
							Data: []int64{100, 101, 102, 103},
						}},
					}},
				},
				{
					Type: schemapb.DataType_FloatVector, FieldId: 101, FieldName: "vec",
					ValidData: []bool{true, false, true, true},
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
						Dim: dim,
						Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
							Data: []float32{0, 1, 4, 5, 6, 7},
						}},
					}},
				},
			},
			Version: msgpb.InsertDataVersion_ColumnBased,
		},
	}

	selection := []int{1, 3}
	msgs, err := genInsertMessagesByPartition(0, 200, "target-partition", selection, "vchannel", source, nil, 7, nil, message.WALNameRocksmq)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	builtPayload := append([]byte(nil), msgs[0].Payload()...)

	insert := message.MustAsMutableInsertMessageV1(msgs[0])
	header := insert.Header()
	assert.Equal(t, int64(100), header.GetCollectionId())
	assert.Equal(t, int32(7), header.GetSchemaVersion())
	assert.Equal(t, uint64(2), header.GetPartitions()[0].GetRows())

	body := insert.MustBody()
	assert.Equal(t, uint64(2), body.GetNumRows())
	assert.Equal(t, int64(200), body.GetPartitionID())
	assert.Equal(t, "target-partition", body.GetPartitionName())
	assert.Equal(t, "vchannel", body.GetShardName())
	assert.Equal(t, uint64(99), body.GetBase().GetTimestamp())
	assert.Equal(t, int64(42), body.GetBase().GetSourceID())
	assert.Equal(t, []int64{11, 13}, body.GetRowIDs())
	assert.Equal(t, []uint64{21, 23}, body.GetTimestamps())
	assert.Equal(t, []int64{101, 103}, body.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	// #52203 moved row validity to the field-specific location; the encoder
	// writes it there, matching AppendFieldData.
	assert.Equal(t, []bool{false, true}, body.GetFieldsData()[1].GetVectors().GetValidData())
	assert.Equal(t, []float32{6, 7}, body.GetFieldsData()[1].GetVectors().GetFloatVector().GetData())

	// The view encoder must not mutate or compact the source request.
	assert.Equal(t, []int64{10, 11, 12, 13}, source.GetRowIDs())
	assert.Equal(t, []float32{0, 1, 4, 5, 6, 7}, source.GetFieldsData()[1].GetVectors().GetFloatVector().GetData())

	// Force every logical row into a separate message. This pins the cursor's
	// compact-vector prefix state at each later selection boundary; advancing it
	// incorrectly would return the wrong physical vector row.
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "4097"))
	msgs, err = genInsertMessagesByPartition(0, 200, "target-partition", []int{0, 1, 2, 3}, "vchannel", source, nil, 7, nil, message.WALNameRocksmq)
	assert.NoError(t, err)
	require.Len(t, msgs, 4)
	expectedVectors := [][]float32{{0, 1}, nil, {4, 5}, {6, 7}}
	for i, msg := range msgs {
		body := message.MustAsMutableInsertMessageV1(msg).MustBody()
		assert.Equal(t, []int64{int64(10 + i)}, body.GetRowIDs())
		assert.Equal(t, []bool{source.GetFieldsData()[1].GetValidData()[i]}, body.GetFieldsData()[1].GetVectors().GetValidData())
		assert.Equal(t, expectedVectors[i], body.GetFieldsData()[1].GetVectors().GetFloatVector().GetData())
	}

	// BuildMutable consumes the view synchronously. Later changes to either the
	// source request or the borrowed selection cannot affect the WAL payload.
	selection[0] = 0
	source.RowIDs[1] = 999
	source.GetFieldsData()[1].GetVectors().GetFloatVector().Data[4] = 999
	assert.Equal(t, builtPayload, insert.Payload())
	var rebuilt msgpb.InsertRequest
	require.NoError(t, proto.Unmarshal(insert.Payload(), &rebuilt))
	assert.Equal(t, []int64{11, 13}, rebuilt.GetRowIDs())
	assert.Equal(t, []float32{6, 7}, rebuilt.GetFieldsData()[1].GetVectors().GetFloatVector().GetData())
}

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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestGenInsertMsgsByPartitionRejectsSingleOversizedRow(t *testing.T) {
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().PulsarCfg.MaxMessageSize.Key, "64"))
	defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.MaxMessageSize.Key)

	t.Run("only row", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
		msgs, err := GenInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNamePulsar)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Contains(t, err.Error(), "single row at offset 0")
		assert.False(t, merr.Status(err).GetRetriable())
	})

	t.Run("row at limit", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 64))
		msgs, err := GenInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNamePulsar)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("later row", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest("small", strings.Repeat("x", 1024))
		msgs, err := GenInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0, 1}, "test_channel", insertMsg, message.WALNamePulsar)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Contains(t, err.Error(), "single row at offset 1")
	})
}

func TestGenInsertMsgsByPartitionUsesWALSpecificSingleRowLimit(t *testing.T) {
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().PulsarCfg.MaxMessageSize.Key, "64"))
	defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.MaxMessageSize.Key)
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().KafkaCfg.ProducerMessageMaxBytes.Key, "2048"))
	defer paramtable.Get().Reset(paramtable.Get().KafkaCfg.ProducerMessageMaxBytes.Key)

	t.Run("kafka allows row above pulsar split threshold", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
		msgs, err := GenInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNameKafka)
		assert.NoError(t, err)
		assert.Len(t, msgs, 1)
	})

	t.Run("kafka rejects row at its own limit", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 2048))
		msgs, err := GenInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNameKafka)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	for _, walName := range []message.WALName{message.WALNameRocksmq, message.WALNameWoodpecker} {
		t.Run(walName.String()+" has no single row limit", func(t *testing.T) {
			insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
			msgs, err := GenInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, walName)
			assert.NoError(t, err)
			assert.Len(t, msgs, 1)
		})
	}
}

func TestGenInsertMsgsByPartitionSplitsMultipleRows(t *testing.T) {
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().PulsarCfg.MaxMessageSize.Key, "512"))
	defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.MaxMessageSize.Key)

	insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 300), strings.Repeat("y", 300))
	msgs, err := GenInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0, 1}, "test_channel", insertMsg, message.WALNamePulsar)
	assert.NoError(t, err)
	assert.Len(t, msgs, 2)
	for _, msg := range msgs {
		assert.Equal(t, uint64(1), msg.(*msgstream.InsertMsg).GetNumRows())
	}
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

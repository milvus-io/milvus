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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
)

func TestGenInsertMsgsByPartitionRejectsSingleOversizedRow(t *testing.T) {
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "64"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

	t.Run("only row", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
		msgs, _, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNamePulsar)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Contains(t, err.Error(), "single row at offset 0")
		assert.False(t, merr.Status(err).GetRetriable())
	})

	t.Run("row at limit", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 64))
		msgs, _, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNamePulsar)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("later row", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest("small", strings.Repeat("x", 1024))
		msgs, _, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0, 1}, "test_channel", insertMsg, message.WALNamePulsar)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Contains(t, err.Error(), "single row at offset 1")
	})
}

func TestGenInsertMsgsByPartitionUsesWALSpecificSingleRowLimit(t *testing.T) {
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "64"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)
	assert.NoError(t, Params.Save(Params.KafkaCfg.ProducerMessageMaxBytes.Key, "2048"))
	defer Params.Reset(Params.KafkaCfg.ProducerMessageMaxBytes.Key)

	t.Run("kafka allows row above pulsar split threshold", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
		msgs, _, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNameKafka)
		assert.NoError(t, err)
		assert.Len(t, msgs, 1)
	})

	t.Run("kafka rejects row at its own limit", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 2048))
		msgs, _, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNameKafka)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	for _, walName := range []message.WALName{message.WALNameRocksmq, message.WALNameWoodpecker} {
		t.Run(walName.String()+" has no single row limit", func(t *testing.T) {
			insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
			msgs, _, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, walName)
			assert.NoError(t, err)
			assert.Len(t, msgs, 1)
		})
	}
}

func TestGenInsertMsgsByPartitionSplitsMultipleRows(t *testing.T) {
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "512"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

	insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 300), strings.Repeat("y", 300))
	msgs, _, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0, 1}, "test_channel", insertMsg, message.WALNamePulsar)
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
	msgs, msgRowOffsets, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNamePulsar)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	assert.Equal(t, uint64(1), msgs[0].(*msgstream.InsertMsg).GetNumRows())
	assert.Equal(t, [][]int{{0}}, msgRowOffsets)
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

	_, err := initMetaCache(ctx, mix)
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

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

func TestSplitInsertRowsByMessageSizeSingleOversizedRow(t *testing.T) {
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "64"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

	t.Run("only row", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
		msgs, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNamePulsar)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Contains(t, err.Error(), "single row at offset 0")
		assert.False(t, merr.Status(err).GetRetriable())
	})

	t.Run("row at limit", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 64))
		msgs, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNamePulsar)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("later row", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest("small", strings.Repeat("x", 1024))
		msgs, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0, 1}, "test_channel", insertMsg, message.WALNamePulsar)
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
		msgs, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNameKafka)
		assert.NoError(t, err)
		assert.Len(t, msgs, 1)
	})

	t.Run("kafka rejects row at its own limit", func(t *testing.T) {
		insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 2048))
		msgs, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, message.WALNameKafka)
		assert.Nil(t, msgs)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	for _, walName := range []message.WALName{message.WALNameRocksmq, message.WALNameWoodpecker} {
		t.Run(walName.String()+" has no single row limit", func(t *testing.T) {
			insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 1024))
			msgs, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg, walName)
			assert.NoError(t, err)
			assert.Len(t, msgs, 1)
		})
	}
}

func TestGenInsertMsgsByPartitionSplitsMultipleRows(t *testing.T) {
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "512"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

	insertMsg := newVarCharInsertMsgForPackTest(strings.Repeat("x", 300), strings.Repeat("y", 300))
	msgs, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0, 1}, "test_channel", insertMsg, message.WALNamePulsar)
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

	selections, err := splitInsertRowsByMessageSize(insertMsg, []int{0})
	assert.NoError(t, err)
	assert.Equal(t, [][]int{{0}}, selections)
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
		for i := 0; i < rows; i++ {
			ids[i] = int64(i)
		}
		return &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				NumRows: uint64(rows),
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
		selections, err := splitInsertRowsByMessageSize(newInsertMsg(len(rows)), rows)
		assert.NoError(t, err)
		assert.Len(t, selections, 1)
		assert.Equal(t, rows, selections[0])

		// The selection is a view over the caller's offset array, not a copy.
		rows[0] = 99
		assert.Equal(t, 99, selections[0][0])
	})

	t.Run("splitting on the size threshold still works", func(t *testing.T) {
		// Force a tiny threshold so every row lands in its own message.
		Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "1")
		defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

		const rows = 5
		rowOffsets := offsets(rows)
		selections, err := splitInsertRowsByMessageSize(newInsertMsg(rows), rowOffsets)
		assert.NoError(t, err)
		assert.Equal(t, [][]int{{0}, {1}, {2}, {3}, {4}}, selections)
	})

	t.Run("no rows produces no messages", func(t *testing.T) {
		selections, err := splitInsertRowsByMessageSize(newInsertMsg(3), nil)
		assert.NoError(t, err)
		assert.Empty(t, selections)
	})
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
	msgs, err := genInsertMessagesByPartition(0, 200, "target-partition", selection, "vchannel", source, nil, 7)
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
	assert.Equal(t, []bool{false, true}, body.GetFieldsData()[1].GetValidData())
	assert.Equal(t, []float32{6, 7}, body.GetFieldsData()[1].GetVectors().GetFloatVector().GetData())

	// The view encoder must not mutate or compact the source request.
	assert.Equal(t, []int64{10, 11, 12, 13}, source.GetRowIDs())
	assert.Equal(t, []float32{0, 1, 4, 5, 6, 7}, source.GetFieldsData()[1].GetVectors().GetFloatVector().GetData())

	// Force every logical row into a separate message. This pins the physical
	// compact-vector index captured at each later selection boundary; rescanning
	// or reusing the previous message's index would return the wrong vector row.
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "1"))
	msgs, err = genInsertMessagesByPartition(0, 200, "target-partition", []int{0, 1, 2, 3}, "vchannel", source, nil, 7)
	assert.NoError(t, err)
	require.Len(t, msgs, 4)
	expectedVectors := [][]float32{{0, 1}, nil, {4, 5}, {6, 7}}
	for i, msg := range msgs {
		body := message.MustAsMutableInsertMessageV1(msg).MustBody()
		assert.Equal(t, []int64{int64(10 + i)}, body.GetRowIDs())
		assert.Equal(t, []bool{source.GetFieldsData()[1].GetValidData()[i]}, body.GetFieldsData()[1].GetValidData())
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

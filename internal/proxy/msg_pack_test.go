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

func TestGenInsertMsgsByPartitionSingleOversizedRow(t *testing.T) {
	assert.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "64"))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldId:   101,
		FieldName: "large_text",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{strings.Repeat("x", 1024)},
					},
				},
			},
		},
	}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:        context.Background(),
			HashValues: []uint32{1},
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Insert,
				SourceID: paramtable.GetNodeID(),
			},
			DbName:         "default",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			NumRows:        1,
			FieldsData:     []*schemapb.FieldData{fieldData},
			Timestamps:     []uint64{1},
			RowIDs:         []int64{1},
			Version:        msgpb.InsertDataVersion_ColumnBased,
		},
	}

	msgs, err := genInsertMsgsByPartition(context.Background(), 0, 1, "test_partition", []int{0}, "test_channel", insertMsg)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	assert.Equal(t, uint64(1), msgs[0].(*msgstream.InsertMsg).GetNumRows())
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

// genInsertMsgsByPartition used to build each message's FieldData with zero
// capacity and grow it one row at a time, which copies the payload roughly five
// times over. These tests pin the observable behaviour (message splitting and
// row contents) and assert that the destination is preallocated.
func TestGenInsertMsgsByPartitionPreallocates(t *testing.T) {
	paramtable.Init()

	const dim = 8

	// One int64 column plus one float-vector column, both preallocatable.
	newInsertMsg := func(rows int) *msgstream.InsertMsg {
		ids := make([]int64, rows)
		vec := make([]float32, rows*dim)
		ts := make([]uint64, rows)
		rowIDs := make([]int64, rows)
		hash := make([]uint32, rows)
		for i := 0; i < rows; i++ {
			ids[i] = int64(i)
			ts[i] = uint64(i + 1)
			rowIDs[i] = int64(i)
			for d := 0; d < dim; d++ {
				vec[i*dim+d] = float32(i*dim + d)
			}
		}
		return &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{Ctx: context.Background(), HashValues: hash},
			InsertRequest: &msgpb.InsertRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
				DbName:         "default",
				CollectionName: "c",
				PartitionName:  "p",
				NumRows:        uint64(rows),
				Timestamps:     ts,
				RowIDs:         rowIDs,
				Version:        msgpb.InsertDataVersion_ColumnBased,
				FieldsData: []*schemapb.FieldData{
					{
						Type: schemapb.DataType_Int64, FieldId: 100, FieldName: "pk",
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}},
						}},
					},
					{
						Type: schemapb.DataType_FloatVector, FieldId: 101, FieldName: "vec",
						Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
							Dim:  dim,
							Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vec}},
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

	t.Run("all rows in one message, contents preserved", func(t *testing.T) {
		const rows = 100
		src := newInsertMsg(rows)
		msgs, err := genInsertMsgsByPartition(context.Background(), 7, 1, "p", offsets(rows), "ch", src)
		assert.NoError(t, err)
		assert.Len(t, msgs, 1)

		got := msgs[0].(*msgstream.InsertMsg)
		assert.Equal(t, uint64(rows), got.GetNumRows())
		assert.Len(t, got.GetRowIDs(), rows)
		assert.Len(t, got.GetTimestamps(), rows)

		pk := got.GetFieldsData()[0].GetScalars().GetLongData().GetData()
		assert.Len(t, pk, rows)
		for i := 0; i < rows; i++ {
			assert.Equal(t, int64(i), pk[i])
		}
		vec := got.GetFieldsData()[1].GetVectors().GetFloatVector().GetData()
		assert.Len(t, vec, rows*dim)
		assert.Equal(t, float32(rows*dim-1), vec[rows*dim-1])

		// The point of the change: the buffers were sized up front rather than
		// grown row by row. A grown slice would land on a power-of-two-ish
		// capacity well above the exact length.
		assert.Equal(t, rows, cap(pk), "pk column should be preallocated exactly")
		assert.Equal(t, rows*dim, cap(vec), "vector column should be preallocated exactly")
		assert.Equal(t, rows, cap(got.GetRowIDs()))
		assert.Equal(t, rows, cap(got.GetTimestamps()))
	})

	t.Run("splitting on the size threshold still works", func(t *testing.T) {
		// Force a tiny threshold so every row lands in its own message.
		Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "1")
		defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

		const rows = 5
		src := newInsertMsg(rows)
		msgs, err := genInsertMsgsByPartition(context.Background(), 7, 1, "p", offsets(rows), "ch", src)
		assert.NoError(t, err)
		assert.Len(t, msgs, rows)

		for i, m := range msgs {
			im := m.(*msgstream.InsertMsg)
			assert.Equal(t, uint64(1), im.GetNumRows())
			pk := im.GetFieldsData()[0].GetScalars().GetLongData().GetData()
			assert.Equal(t, []int64{int64(i)}, pk)
		}
	})

	// The message is now created lazily on the first row, so the no-rows case
	// must not dereference a nil message.
	t.Run("no rows produces no messages", func(t *testing.T) {
		src := newInsertMsg(3)
		msgs, err := genInsertMsgsByPartition(context.Background(), 7, 1, "p", nil, "ch", src)
		assert.NoError(t, err)
		assert.Empty(t, msgs)
	})
}

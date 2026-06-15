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

// =========================================================================
// Partial-update OCC repack tests (genInsertMsgsByPartitionWithOCC)
// =========================================================================

// buildOCCInsertMsg builds a minimal column-based InsertMsg with `nb` rows
// (single Int64 PK column) suitable for genInsertMsgsByPartitionWithOCC.
func buildOCCInsertMsg(nb int) *msgstream.InsertMsg {
	pks := make([]int64, nb)
	for i := 0; i < nb; i++ {
		pks[i] = int64(i + 1)
	}
	pkField := &schemapb.FieldData{
		FieldId:   100,
		FieldName: "pk",
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
			},
		},
	}
	im := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{HashValues: make([]uint32, nb)},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Insert,
				SourceID: paramtable.GetNodeID(),
			},
			NumRows:    uint64(nb),
			FieldsData: []*schemapb.FieldData{pkField},
			Version:    msgpb.InsertDataVersion_ColumnBased,
		},
	}
	im.Timestamps = make([]uint64, nb)
	im.RowIDs = make([]UniqueID, nb)
	for i := 0; i < nb; i++ {
		im.Timestamps[i] = uint64(i)
		im.RowIDs[i] = int64(i)
	}
	return im
}

func TestGenInsertMsgsByPartitionWithOCC_NilOccInputBackwardCompat(t *testing.T) {
	paramtable.Init()
	nb := 4
	insertMsg := buildOCCInsertMsg(nb)
	rowOffsets := []int{0, 1, 2, 3}

	msgs, occOuts, err := genInsertMsgsByPartitionWithOCC(
		context.Background(), 0, 1, "p1", rowOffsets, "ch", insertMsg, nil,
	)
	assert.NoError(t, err)
	assert.Nil(t, occOuts, "occOuts should be nil when occInput is nil")
	assert.Len(t, msgs, 1)
	im := msgs[0].(*msgstream.InsertMsg)
	assert.EqualValues(t, nb, im.NumRows)
}

func TestGenInsertMsgsByPartitionWithOCC_AlignsOccMeta(t *testing.T) {
	paramtable.Init()
	nb := 3
	insertMsg := buildOCCInsertMsg(nb)
	occInput := &OCCRowMeta{
		PKs: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10, 20, 30}}},
		},
		ExpectedTs:     []uint64{100, 200, 300},
		ExpectedExists: []bool{true, false, true},
	}

	rowOffsets := []int{0, 1, 2}
	msgs, occOuts, err := genInsertMsgsByPartitionWithOCC(
		context.Background(), 0, 1, "p1", rowOffsets, "ch", insertMsg, occInput,
	)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1, "small payload should fit into a single InsertMsg")
	assert.Len(t, occOuts, 1, "occOuts must be aligned 1:1 with msgs")
	out := occOuts[0]
	assert.Equal(t, []int64{10, 20, 30}, out.PKs.GetIntId().GetData())
	assert.Equal(t, []uint64{100, 200, 300}, out.ExpectedTs)
	assert.Equal(t, []bool{true, false, true}, out.ExpectedExists)
}

func TestGenInsertMsgsByPartitionWithOCC_SplitsAcrossThreshold(t *testing.T) {
	paramtable.Init()
	// Force the WAL message-size threshold to 1 byte so every row triggers a
	// new InsertMsg, exercising the split path that also needs to slice
	// OCCRowMeta.
	origVal := Params.PulsarCfg.MaxMessageSize.GetValue()
	paramtable.Get().Save(Params.PulsarCfg.MaxMessageSize.Key, "1")
	defer paramtable.Get().Save(Params.PulsarCfg.MaxMessageSize.Key, origVal)

	nb := 3
	insertMsg := buildOCCInsertMsg(nb)
	occInput := &OCCRowMeta{
		PKs: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}},
		},
		ExpectedTs:     []uint64{11, 22, 33},
		ExpectedExists: []bool{true, true, false},
	}

	rowOffsets := []int{0, 1, 2}
	msgs, occOuts, err := genInsertMsgsByPartitionWithOCC(
		context.Background(), 0, 1, "p1", rowOffsets, "ch", insertMsg, occInput,
	)
	assert.NoError(t, err)
	// One row per output message because threshold=1 forces a flush after
	// each row beyond the first.
	assert.Len(t, msgs, nb)
	assert.Len(t, occOuts, nb)
	for i := 0; i < nb; i++ {
		assert.Equal(t, []int64{int64(i + 1)}, occOuts[i].PKs.GetIntId().GetData())
		assert.Equal(t, []uint64{occInput.ExpectedTs[i]}, occOuts[i].ExpectedTs)
		assert.Equal(t, []bool{occInput.ExpectedExists[i]}, occOuts[i].ExpectedExists)
	}
}

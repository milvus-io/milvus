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

package storage

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
)

const (
	CollectionID        = 1
	PartitionID         = 1
	SegmentID           = 1
	RowIDField          = 0
	TimestampField      = 1
	BoolField           = 100
	Int8Field           = 101
	Int16Field          = 102
	Int32Field          = 103
	Int64Field          = 104
	FloatField          = 105
	DoubleField         = 106
	StringField         = 107
	BinaryVectorField   = 108
	FloatVectorField    = 109
	ArrayField          = 110
	JSONField           = 111
	Float16VectorField  = 112
	BFloat16VectorField = 113
)

func genTestCollectionMeta() *etcdpb.CollectionMeta {
	return &etcdpb.CollectionMeta{
		ID:            CollectionID,
		CreateTime:    1,
		SegmentIDs:    []int64{SegmentID},
		PartitionTags: []string{"partition_0", "partition_1"},
		Schema: &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     RowIDField,
					Name:        "row_id",
					Description: "row_id",
					DataType:    schemapb.DataType_Int64,
				},
				{
					FieldID:     TimestampField,
					Name:        "Timestamp",
					Description: "Timestamp",
					DataType:    schemapb.DataType_Int64,
				},
				{
					FieldID:     BoolField,
					Name:        "field_bool",
					Description: "bool",
					DataType:    schemapb.DataType_Bool,
				},
				{
					FieldID:     Int8Field,
					Name:        "field_int8",
					Description: "int8",
					DataType:    schemapb.DataType_Int8,
				},
				{
					FieldID:     Int16Field,
					Name:        "field_int16",
					Description: "int16",
					DataType:    schemapb.DataType_Int16,
				},
				{
					FieldID:     Int32Field,
					Name:        "field_int32",
					Description: "int32",
					DataType:    schemapb.DataType_Int32,
				},
				{
					FieldID:      Int64Field,
					Name:         "field_int64",
					IsPrimaryKey: true,
					Description:  "int64",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:     FloatField,
					Name:        "field_float",
					Description: "float",
					DataType:    schemapb.DataType_Float,
				},
				{
					FieldID:     DoubleField,
					Name:        "field_double",
					Description: "double",
					DataType:    schemapb.DataType_Double,
				},
				{
					FieldID:     StringField,
					Name:        "field_string",
					Description: "string",
					DataType:    schemapb.DataType_String,
				},
				{
					FieldID:     ArrayField,
					Name:        "field_int32_array",
					Description: "int32 array",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
				},
				{
					FieldID:     JSONField,
					Name:        "field_json",
					Description: "json",
					DataType:    schemapb.DataType_JSON,
				},
				{
					FieldID:     BinaryVectorField,
					Name:        "field_binary_vector",
					Description: "binary_vector",
					DataType:    schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					FieldID:     FloatVectorField,
					Name:        "field_float_vector",
					Description: "float_vector",
					DataType:    schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
				{
					FieldID:     Float16VectorField,
					Name:        "field_float16_vector",
					Description: "float16_vector",
					DataType:    schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
				{
					FieldID:     BFloat16VectorField,
					Name:        "field_bfloat16_vector",
					Description: "bfloat16_vector",
					DataType:    schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
			},
		},
	}
}

func TestInsertCodec(t *testing.T) {
	schema := genTestCollectionMeta()
	insertCodec := NewInsertCodecWithSchema(schema)
	insertData1 := &InsertData{
		Data: map[int64]FieldData{
			RowIDField: &Int64FieldData{
				Data: []int64{3, 4},
			},
			TimestampField: &Int64FieldData{
				Data: []int64{3, 4},
			},
			BoolField: &BoolFieldData{
				Data: []bool{true, false},
			},
			Int8Field: &Int8FieldData{
				Data: []int8{3, 4},
			},
			Int16Field: &Int16FieldData{
				Data: []int16{3, 4},
			},
			Int32Field: &Int32FieldData{
				Data: []int32{3, 4},
			},
			Int64Field: &Int64FieldData{
				Data: []int64{3, 4},
			},
			FloatField: &FloatFieldData{
				Data: []float32{3, 4},
			},
			DoubleField: &DoubleFieldData{
				Data: []float64{3, 4},
			},
			StringField: &StringFieldData{
				Data: []string{"3", "4"},
			},
			BinaryVectorField: &BinaryVectorFieldData{
				Data: []byte{0, 255},
				Dim:  8,
			},
			FloatVectorField: &FloatVectorFieldData{
				Data: []float32{4, 5, 6, 7, 4, 5, 6, 7},
				Dim:  4,
			},
			ArrayField: &ArrayFieldData{
				ElementType: schemapb.DataType_Int32,
				Data: []*schemapb.ScalarField{
					{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{3, 2, 1}},
						},
					},
					{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{6, 5, 4}},
						},
					},
				},
			},
			JSONField: &JSONFieldData{
				Data: [][]byte{
					[]byte(`{"batch":2}`),
					[]byte(`{"key":"world"}`),
				},
			},
			Float16VectorField: &Float16VectorFieldData{
				// length = 2 * Dim * numRows(2) = 16
				Data: []byte{0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255},
				Dim:  4,
			},
			BFloat16VectorField: &BFloat16VectorFieldData{
				// length = 2 * Dim * numRows(2) = 16
				Data: []byte{0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255},
				Dim:  4,
			},
		},
	}

	insertData2 := &InsertData{
		Data: map[int64]FieldData{
			RowIDField: &Int64FieldData{
				Data: []int64{1, 2},
			},
			TimestampField: &Int64FieldData{
				Data: []int64{1, 2},
			},
			BoolField: &BoolFieldData{
				Data: []bool{true, false},
			},
			Int8Field: &Int8FieldData{
				Data: []int8{1, 2},
			},
			Int16Field: &Int16FieldData{
				Data: []int16{1, 2},
			},
			Int32Field: &Int32FieldData{
				Data: []int32{1, 2},
			},
			Int64Field: &Int64FieldData{
				Data: []int64{1, 2},
			},
			FloatField: &FloatFieldData{
				Data: []float32{1, 2},
			},
			DoubleField: &DoubleFieldData{
				Data: []float64{1, 2},
			},
			StringField: &StringFieldData{
				Data: []string{"1", "2"},
			},
			BinaryVectorField: &BinaryVectorFieldData{
				Data: []byte{0, 255},
				Dim:  8,
			},
			FloatVectorField: &FloatVectorFieldData{
				Data: []float32{0, 1, 2, 3, 0, 1, 2, 3},
				Dim:  4,
			},
			Float16VectorField: &Float16VectorFieldData{
				// length = 2 * Dim * numRows(2) = 16
				Data: []byte{0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255},
				Dim:  4,
			},
			BFloat16VectorField: &BFloat16VectorFieldData{
				// length = 2 * Dim * numRows(2) = 16
				Data: []byte{0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255},
				Dim:  4,
			},
			ArrayField: &ArrayFieldData{
				ElementType: schemapb.DataType_Int32,
				Data: []*schemapb.ScalarField{
					{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
						},
					},
					{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{4, 5, 6}},
						},
					},
				},
			},
			JSONField: &JSONFieldData{
				Data: [][]byte{
					[]byte(`{"batch":1}`),
					[]byte(`{"key":"hello"}`),
				},
			},
		},
	}

	insertDataEmpty := &InsertData{
		Data: map[int64]FieldData{
			RowIDField:          &Int64FieldData{[]int64{}},
			TimestampField:      &Int64FieldData{[]int64{}},
			BoolField:           &BoolFieldData{[]bool{}},
			Int8Field:           &Int8FieldData{[]int8{}},
			Int16Field:          &Int16FieldData{[]int16{}},
			Int32Field:          &Int32FieldData{[]int32{}},
			Int64Field:          &Int64FieldData{[]int64{}},
			FloatField:          &FloatFieldData{[]float32{}},
			DoubleField:         &DoubleFieldData{[]float64{}},
			StringField:         &StringFieldData{[]string{}, schemapb.DataType_VarChar},
			BinaryVectorField:   &BinaryVectorFieldData{[]byte{}, 8},
			FloatVectorField:    &FloatVectorFieldData{[]float32{}, 4},
			Float16VectorField:  &Float16VectorFieldData{[]byte{}, 4},
			BFloat16VectorField: &BFloat16VectorFieldData{[]byte{}, 4},
			ArrayField:          &ArrayFieldData{schemapb.DataType_Int32, []*schemapb.ScalarField{}},
			JSONField:           &JSONFieldData{[][]byte{}},
		},
	}
	b, err := insertCodec.Serialize(PartitionID, SegmentID, insertDataEmpty)
	assert.Error(t, err)
	assert.Empty(t, b)

	s, err := insertCodec.SerializePkStatsByData(insertDataEmpty)
	assert.Error(t, err)
	assert.Empty(t, s)

	Blobs1, err := insertCodec.Serialize(PartitionID, SegmentID, insertData1)
	assert.NoError(t, err)
	for _, blob := range Blobs1 {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
		assert.Equal(t, blob.GetKey(), blob.Key)
	}
	Blobs2, err := insertCodec.Serialize(PartitionID, SegmentID, insertData2)
	assert.NoError(t, err)
	for _, blob := range Blobs2 {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 99)
		assert.Equal(t, blob.GetKey(), blob.Key)
	}
	resultBlobs := append(Blobs1, Blobs2...)
	collID, partID, segID, resultData, err := insertCodec.DeserializeAll(resultBlobs)
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(CollectionID), collID)
	assert.Equal(t, UniqueID(PartitionID), partID)
	assert.Equal(t, UniqueID(SegmentID), segID)
	assert.Equal(t, []int64{1, 2, 3, 4}, resultData.Data[RowIDField].(*Int64FieldData).Data)
	assert.Equal(t, []int64{1, 2, 3, 4}, resultData.Data[TimestampField].(*Int64FieldData).Data)
	assert.Equal(t, []bool{true, false, true, false}, resultData.Data[BoolField].(*BoolFieldData).Data)
	assert.Equal(t, []int8{1, 2, 3, 4}, resultData.Data[Int8Field].(*Int8FieldData).Data)
	assert.Equal(t, []int16{1, 2, 3, 4}, resultData.Data[Int16Field].(*Int16FieldData).Data)
	assert.Equal(t, []int32{1, 2, 3, 4}, resultData.Data[Int32Field].(*Int32FieldData).Data)
	assert.Equal(t, []int64{1, 2, 3, 4}, resultData.Data[Int64Field].(*Int64FieldData).Data)
	assert.Equal(t, []float32{1, 2, 3, 4}, resultData.Data[FloatField].(*FloatFieldData).Data)
	assert.Equal(t, []float64{1, 2, 3, 4}, resultData.Data[DoubleField].(*DoubleFieldData).Data)
	assert.Equal(t, []string{"1", "2", "3", "4"}, resultData.Data[StringField].(*StringFieldData).Data)
	assert.Equal(t, []byte{0, 255, 0, 255}, resultData.Data[BinaryVectorField].(*BinaryVectorFieldData).Data)
	assert.Equal(t, []float32{0, 1, 2, 3, 0, 1, 2, 3, 4, 5, 6, 7, 4, 5, 6, 7}, resultData.Data[FloatVectorField].(*FloatVectorFieldData).Data)
	assert.Equal(t, []byte{
		0, 255, 0, 255, 0, 255, 0, 255,
		0, 255, 0, 255, 0, 255, 0, 255,
		0, 255, 0, 255, 0, 255, 0, 255,
		0, 255, 0, 255, 0, 255, 0, 255,
	}, resultData.Data[Float16VectorField].(*Float16VectorFieldData).Data)
	assert.Equal(t, []byte{
		0, 255, 0, 255, 0, 255, 0, 255,
		0, 255, 0, 255, 0, 255, 0, 255,
		0, 255, 0, 255, 0, 255, 0, 255,
		0, 255, 0, 255, 0, 255, 0, 255,
	}, resultData.Data[BFloat16VectorField].(*BFloat16VectorFieldData).Data)

	int32ArrayList := [][]int32{{1, 2, 3}, {4, 5, 6}, {3, 2, 1}, {6, 5, 4}}
	resultArrayList := [][]int32{}
	for _, v := range resultData.Data[ArrayField].(*ArrayFieldData).Data {
		resultArrayList = append(resultArrayList, v.GetIntData().GetData())
	}
	assert.EqualValues(t, int32ArrayList, resultArrayList)

	assert.Equal(t,
		[][]byte{
			[]byte(`{"batch":1}`),
			[]byte(`{"key":"hello"}`),
			[]byte(`{"batch":2}`),
			[]byte(`{"key":"world"}`),
		},
		resultData.Data[JSONField].(*JSONFieldData).Data)

	log.Debug("Data", zap.Any("Data", resultData.Data))
	log.Debug("Infos", zap.Any("Infos", resultData.Infos))

	blobs := []*Blob{}
	_, _, _, err = insertCodec.Deserialize(blobs)
	assert.Error(t, err)
	_, _, _, _, err = insertCodec.DeserializeAll(blobs)
	assert.Error(t, err)

	statsBlob1, err := insertCodec.SerializePkStatsByData(insertData1)
	assert.NoError(t, err)
	_, err = DeserializeStats([]*Blob{statsBlob1})
	assert.NoError(t, err)

	statsBlob2, err := insertCodec.SerializePkStatsByData(insertData2)
	assert.NoError(t, err)
	_, err = DeserializeStats([]*Blob{statsBlob2})
	assert.NoError(t, err)

	_, err = insertCodec.SerializePkStatsList([]*PrimaryKeyStats{}, 0)
	assert.Error(t, err, "SerializePkStatsList zero length pkstats list shall return error")
}

func TestDeleteCodec(t *testing.T) {
	t.Run("int64 pk", func(t *testing.T) {
		deleteCodec := NewDeleteCodec()
		pk1 := &Int64PrimaryKey{
			Value: 1,
		}
		deleteData := &DeleteData{
			Pks:      []PrimaryKey{pk1},
			Tss:      []uint64{43757345},
			RowCount: int64(1),
		}

		pk2 := &Int64PrimaryKey{
			Value: 2,
		}
		deleteData.Append(pk2, 23578294723)
		blob, err := deleteCodec.Serialize(CollectionID, 1, 1, deleteData)
		assert.NoError(t, err)

		pid, sid, data, err := deleteCodec.Deserialize([]*Blob{blob})
		assert.NoError(t, err)
		assert.Equal(t, pid, int64(1))
		assert.Equal(t, sid, int64(1))
		assert.Equal(t, data, deleteData)
	})

	t.Run("string pk", func(t *testing.T) {
		deleteCodec := NewDeleteCodec()
		pk1 := NewVarCharPrimaryKey("test1")
		deleteData := &DeleteData{
			Pks:      []PrimaryKey{pk1},
			Tss:      []uint64{43757345},
			RowCount: int64(1),
		}

		pk2 := NewVarCharPrimaryKey("test2")
		deleteData.Append(pk2, 23578294723)
		blob, err := deleteCodec.Serialize(CollectionID, 1, 1, deleteData)
		assert.NoError(t, err)

		pid, sid, data, err := deleteCodec.Deserialize([]*Blob{blob})
		assert.NoError(t, err)
		assert.Equal(t, pid, int64(1))
		assert.Equal(t, sid, int64(1))
		assert.Equal(t, data, deleteData)
	})

	t.Run("merge", func(t *testing.T) {
		first := &DeleteData{
			Pks:      []PrimaryKey{NewInt64PrimaryKey(1)},
			Tss:      []uint64{100},
			RowCount: 1,
		}

		second := &DeleteData{
			Pks:      []PrimaryKey{NewInt64PrimaryKey(2)},
			Tss:      []uint64{100},
			RowCount: 1,
		}

		first.Merge(second)
		assert.Equal(t, len(first.Pks), 2)
		assert.Equal(t, len(first.Tss), 2)
		assert.Equal(t, first.RowCount, int64(2))
	})
}

func TestUpgradeDeleteLog(t *testing.T) {
	binlogWriter := NewDeleteBinlogWriter(schemapb.DataType_String, CollectionID, 1, 1)
	eventWriter, err := binlogWriter.NextDeleteEventWriter()
	assert.NoError(t, err)

	dData := &DeleteData{
		Pks:      []PrimaryKey{&Int64PrimaryKey{Value: 1}, &Int64PrimaryKey{Value: 2}},
		Tss:      []Timestamp{100, 200},
		RowCount: 2,
	}

	sizeTotal := 0
	for i := int64(0); i < dData.RowCount; i++ {
		int64PkValue := dData.Pks[i].(*Int64PrimaryKey).Value
		ts := dData.Tss[i]
		err = eventWriter.AddOneStringToPayload(fmt.Sprintf("%d,%d", int64PkValue, ts))
		assert.NoError(t, err)
		sizeTotal += binary.Size(int64PkValue)
		sizeTotal += binary.Size(ts)
	}
	eventWriter.SetEventTimestamp(100, 200)
	binlogWriter.SetEventTimeStamp(100, 200)
	binlogWriter.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	err = binlogWriter.Finish()
	assert.NoError(t, err)
	buffer, err := binlogWriter.GetBuffer()
	assert.NoError(t, err)
	blob := &Blob{Value: buffer}

	dCodec := NewDeleteCodec()
	parID, segID, deleteData, err := dCodec.Deserialize([]*Blob{blob})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), parID)
	assert.Equal(t, int64(1), segID)
	assert.ElementsMatch(t, dData.Pks, deleteData.Pks)
	assert.ElementsMatch(t, dData.Tss, deleteData.Tss)
}

func TestDDCodec(t *testing.T) {
	dataDefinitionCodec := NewDataDefinitionCodec(int64(1))
	ts := []Timestamp{1, 2, 3, 4}
	ddRequests := []string{
		"CreateCollection",
		"DropCollection",
		"CreatePartition",
		"DropPartition",
	}
	eventTypeCodes := []EventTypeCode{
		CreateCollectionEventType,
		DropCollectionEventType,
		CreatePartitionEventType,
		DropPartitionEventType,
	}
	blobs, err := dataDefinitionCodec.Serialize(ts, ddRequests, eventTypeCodes)
	assert.NoError(t, err)
	for _, blob := range blobs {
		blob.Key = fmt.Sprintf("1/data_definition/3/4/5/%d", 99)
	}
	resultTs, resultRequests, err := dataDefinitionCodec.Deserialize(blobs)
	assert.NoError(t, err)
	assert.Equal(t, resultTs, ts)
	assert.Equal(t, resultRequests, ddRequests)

	blobs = []*Blob{}
	_, _, err = dataDefinitionCodec.Deserialize(blobs)
	assert.Error(t, err)
}

func TestTsError(t *testing.T) {
	insertData := &InsertData{}
	insertCodec := NewInsertCodecWithSchema(nil)
	blobs, err := insertCodec.Serialize(1, 1, insertData)
	assert.Nil(t, blobs)
	assert.Error(t, err)
}

func TestMemorySize(t *testing.T) {
	insertData1 := &InsertData{
		Data: map[int64]FieldData{
			RowIDField: &Int64FieldData{
				Data: []int64{3},
			},
			TimestampField: &Int64FieldData{
				Data: []int64{3},
			},
			BoolField: &BoolFieldData{
				Data: []bool{true},
			},
			Int8Field: &Int8FieldData{
				Data: []int8{3},
			},
			Int16Field: &Int16FieldData{
				Data: []int16{3},
			},
			Int32Field: &Int32FieldData{
				Data: []int32{3},
			},
			Int64Field: &Int64FieldData{
				Data: []int64{3},
			},
			FloatField: &FloatFieldData{
				Data: []float32{3},
			},
			DoubleField: &DoubleFieldData{
				Data: []float64{3},
			},
			StringField: &StringFieldData{
				Data: []string{"3"},
			},
			BinaryVectorField: &BinaryVectorFieldData{
				Data: []byte{0},
				Dim:  8,
			},
			FloatVectorField: &FloatVectorFieldData{
				Data: []float32{4, 5, 6, 7},
				Dim:  4,
			},
			ArrayField: &ArrayFieldData{
				ElementType: schemapb.DataType_Int32,
				Data: []*schemapb.ScalarField{
					{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
						},
					},
				},
			},
			JSONField: &JSONFieldData{
				Data: [][]byte{
					[]byte(`{"batch":1}`),
				},
			},
		},
	}
	assert.Equal(t, insertData1.Data[RowIDField].GetMemorySize(), 8)
	assert.Equal(t, insertData1.Data[TimestampField].GetMemorySize(), 8)
	assert.Equal(t, insertData1.Data[BoolField].GetMemorySize(), 1)
	assert.Equal(t, insertData1.Data[Int8Field].GetMemorySize(), 1)
	assert.Equal(t, insertData1.Data[Int16Field].GetMemorySize(), 2)
	assert.Equal(t, insertData1.Data[Int32Field].GetMemorySize(), 4)
	assert.Equal(t, insertData1.Data[Int64Field].GetMemorySize(), 8)
	assert.Equal(t, insertData1.Data[FloatField].GetMemorySize(), 4)
	assert.Equal(t, insertData1.Data[DoubleField].GetMemorySize(), 8)
	assert.Equal(t, insertData1.Data[StringField].GetMemorySize(), 17)
	assert.Equal(t, insertData1.Data[BinaryVectorField].GetMemorySize(), 5)
	assert.Equal(t, insertData1.Data[FloatField].GetMemorySize(), 4)
	assert.Equal(t, insertData1.Data[ArrayField].GetMemorySize(), 3*4)
	assert.Equal(t, insertData1.Data[JSONField].GetMemorySize(), len([]byte(`{"batch":1}`))+16)

	insertData2 := &InsertData{
		Data: map[int64]FieldData{
			RowIDField: &Int64FieldData{
				Data: []int64{1, 2},
			},
			TimestampField: &Int64FieldData{
				Data: []int64{1, 2},
			},
			BoolField: &BoolFieldData{
				Data: []bool{true, false},
			},
			Int8Field: &Int8FieldData{
				Data: []int8{1, 2},
			},
			Int16Field: &Int16FieldData{
				Data: []int16{1, 2},
			},
			Int32Field: &Int32FieldData{
				Data: []int32{1, 2},
			},
			Int64Field: &Int64FieldData{
				Data: []int64{1, 2},
			},
			FloatField: &FloatFieldData{
				Data: []float32{1, 2},
			},
			DoubleField: &DoubleFieldData{
				Data: []float64{1, 2},
			},
			StringField: &StringFieldData{
				Data: []string{"1", "23"},
			},
			BinaryVectorField: &BinaryVectorFieldData{
				Data: []byte{0, 255},
				Dim:  8,
			},
			FloatVectorField: &FloatVectorFieldData{
				Data: []float32{0, 1, 2, 3, 0, 1, 2, 3},
				Dim:  4,
			},
		},
	}

	assert.Equal(t, insertData2.Data[RowIDField].GetMemorySize(), 16)
	assert.Equal(t, insertData2.Data[TimestampField].GetMemorySize(), 16)
	assert.Equal(t, insertData2.Data[BoolField].GetMemorySize(), 2)
	assert.Equal(t, insertData2.Data[Int8Field].GetMemorySize(), 2)
	assert.Equal(t, insertData2.Data[Int16Field].GetMemorySize(), 4)
	assert.Equal(t, insertData2.Data[Int32Field].GetMemorySize(), 8)
	assert.Equal(t, insertData2.Data[Int64Field].GetMemorySize(), 16)
	assert.Equal(t, insertData2.Data[FloatField].GetMemorySize(), 8)
	assert.Equal(t, insertData2.Data[DoubleField].GetMemorySize(), 16)
	assert.Equal(t, insertData2.Data[StringField].GetMemorySize(), 35)
	assert.Equal(t, insertData2.Data[BinaryVectorField].GetMemorySize(), 6)
	assert.Equal(t, insertData2.Data[FloatField].GetMemorySize(), 8)

	insertDataEmpty := &InsertData{
		Data: map[int64]FieldData{
			RowIDField:        &Int64FieldData{[]int64{}},
			TimestampField:    &Int64FieldData{[]int64{}},
			BoolField:         &BoolFieldData{[]bool{}},
			Int8Field:         &Int8FieldData{[]int8{}},
			Int16Field:        &Int16FieldData{[]int16{}},
			Int32Field:        &Int32FieldData{[]int32{}},
			Int64Field:        &Int64FieldData{[]int64{}},
			FloatField:        &FloatFieldData{[]float32{}},
			DoubleField:       &DoubleFieldData{[]float64{}},
			StringField:       &StringFieldData{[]string{}, schemapb.DataType_VarChar},
			BinaryVectorField: &BinaryVectorFieldData{[]byte{}, 8},
			FloatVectorField:  &FloatVectorFieldData{[]float32{}, 4},
		},
	}

	assert.Equal(t, insertDataEmpty.Data[RowIDField].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[TimestampField].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[BoolField].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[Int8Field].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[Int16Field].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[Int32Field].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[Int64Field].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[FloatField].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[DoubleField].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[StringField].GetMemorySize(), 0)
	assert.Equal(t, insertDataEmpty.Data[BinaryVectorField].GetMemorySize(), 4)
	assert.Equal(t, insertDataEmpty.Data[FloatVectorField].GetMemorySize(), 4)
}

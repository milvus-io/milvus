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
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	CollectionID           = 1
	PartitionID            = 1
	SegmentID              = 1
	RowIDField             = 0
	TimestampField         = 1
	BoolField              = 100
	Int8Field              = 101
	Int16Field             = 102
	Int32Field             = 103
	Int64Field             = 104
	FloatField             = 105
	DoubleField            = 106
	StringField            = 107
	BinaryVectorField      = 108
	FloatVectorField       = 109
	ArrayField             = 110
	JSONField              = 111
	Float16VectorField     = 112
	BFloat16VectorField    = 113
	SparseFloatVectorField = 114
	Int8VectorField        = 115
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
				{
					FieldID:     SparseFloatVectorField,
					Name:        "field_sparse_float_vector",
					Description: "sparse_float_vector",
					DataType:    schemapb.DataType_SparseFloatVector,
					TypeParams:  []*commonpb.KeyValuePair{},
				},
				{
					FieldID:     Int8VectorField,
					Name:        "field_int8_vector",
					Description: "int8_vector",
					DataType:    schemapb.DataType_Int8Vector,
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

func TestInsertCodecFailed(t *testing.T) {
	t.Run("vector field not support null", func(t *testing.T) {
		tests := []struct {
			description string
			dataType    schemapb.DataType
		}{
			{"nullable FloatVector field", schemapb.DataType_FloatVector},
			{"nullable Float16Vector field", schemapb.DataType_Float16Vector},
			{"nullable BinaryVector field", schemapb.DataType_BinaryVector},
			{"nullable BFloat16Vector field", schemapb.DataType_BFloat16Vector},
			{"nullable SparseFloatVector field", schemapb.DataType_SparseFloatVector},
			{"nullable Int8Vector field", schemapb.DataType_Int8Vector},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				schema := &etcdpb.CollectionMeta{
					ID:            CollectionID,
					CreateTime:    1,
					SegmentIDs:    []int64{SegmentID},
					PartitionTags: []string{"partition_0", "partition_1"},
					Schema: &schemapb.CollectionSchema{
						Name:        "schema",
						Description: "schema",
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
								DataType: test.dataType,
							},
						},
					},
				}
				insertCodec := NewInsertCodecWithSchema(schema)
				insertDataEmpty := &InsertData{
					Data: map[int64]FieldData{
						RowIDField:     &Int64FieldData{[]int64{}, nil, false},
						TimestampField: &Int64FieldData{[]int64{}, nil, false},
					},
				}
				_, err := insertCodec.Serialize(PartitionID, SegmentID, insertDataEmpty)
				assert.Error(t, err)
			})
		}
	})
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
			SparseFloatVectorField: &SparseFloatVectorFieldData{
				SparseFloatArray: schemapb.SparseFloatArray{
					Dim: 600,
					Contents: [][]byte{
						typeutil.CreateSparseFloatRow([]uint32{0, 1, 2}, []float32{1.1, 1.2, 1.3}),
						typeutil.CreateSparseFloatRow([]uint32{10, 20, 30}, []float32{2.1, 2.2, 2.3}),
						typeutil.CreateSparseFloatRow([]uint32{100, 200, 599}, []float32{3.1, 3.2, 3.3}),
					},
				},
			},
			Int8VectorField: &Int8VectorFieldData{
				Data: []int8{-4, -5, -6, -7, -4, -5, -6, -7},
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
			SparseFloatVectorField: &SparseFloatVectorFieldData{
				SparseFloatArray: schemapb.SparseFloatArray{
					Dim: 300,
					Contents: [][]byte{
						typeutil.CreateSparseFloatRow([]uint32{5, 6, 7}, []float32{1.1, 1.2, 1.3}),
						typeutil.CreateSparseFloatRow([]uint32{15, 26, 37}, []float32{2.1, 2.2, 2.3}),
						typeutil.CreateSparseFloatRow([]uint32{105, 207, 299}, []float32{3.1, 3.2, 3.3}),
					},
				},
			},
			Int8VectorField: &Int8VectorFieldData{
				Data: []int8{0, 1, 2, 3, 0, 1, 2, 3},
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
			RowIDField:          &Int64FieldData{[]int64{}, nil, false},
			TimestampField:      &Int64FieldData{[]int64{}, nil, false},
			BoolField:           &BoolFieldData{[]bool{}, nil, false},
			Int8Field:           &Int8FieldData{[]int8{}, nil, false},
			Int16Field:          &Int16FieldData{[]int16{}, nil, false},
			Int32Field:          &Int32FieldData{[]int32{}, nil, false},
			Int64Field:          &Int64FieldData{[]int64{}, nil, false},
			FloatField:          &FloatFieldData{[]float32{}, nil, false},
			DoubleField:         &DoubleFieldData{[]float64{}, nil, false},
			StringField:         &StringFieldData{[]string{}, schemapb.DataType_VarChar, nil, false},
			BinaryVectorField:   &BinaryVectorFieldData{[]byte{}, 8},
			FloatVectorField:    &FloatVectorFieldData{[]float32{}, 4},
			Float16VectorField:  &Float16VectorFieldData{[]byte{}, 4},
			BFloat16VectorField: &BFloat16VectorFieldData{[]byte{}, 4},
			SparseFloatVectorField: &SparseFloatVectorFieldData{
				SparseFloatArray: schemapb.SparseFloatArray{
					Dim:      0,
					Contents: [][]byte{},
				},
			},
			Int8VectorField: &Int8VectorFieldData{[]int8{}, 4},
			ArrayField:      &ArrayFieldData{schemapb.DataType_Int32, []*schemapb.ScalarField{}, nil, false},
			JSONField:       &JSONFieldData{[][]byte{}, nil, false},
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

	assert.EqualExportedValues(t, &schemapb.SparseFloatArray{
		// merged dim should be max of all dims
		Dim: 600,
		Contents: [][]byte{
			typeutil.CreateSparseFloatRow([]uint32{5, 6, 7}, []float32{1.1, 1.2, 1.3}),
			typeutil.CreateSparseFloatRow([]uint32{15, 26, 37}, []float32{2.1, 2.2, 2.3}),
			typeutil.CreateSparseFloatRow([]uint32{105, 207, 299}, []float32{3.1, 3.2, 3.3}),
			typeutil.CreateSparseFloatRow([]uint32{0, 1, 2}, []float32{1.1, 1.2, 1.3}),
			typeutil.CreateSparseFloatRow([]uint32{10, 20, 30}, []float32{2.1, 2.2, 2.3}),
			typeutil.CreateSparseFloatRow([]uint32{100, 200, 599}, []float32{3.1, 3.2, 3.3}),
		},
	}, &resultData.Data[SparseFloatVectorField].(*SparseFloatVectorFieldData).SparseFloatArray)
	assert.Equal(t, []int8{0, 1, 2, 3, 0, 1, 2, 3, -4, -5, -6, -7, -4, -5, -6, -7}, resultData.Data[Int8VectorField].(*Int8VectorFieldData).Data)

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
		deleteData := NewDeleteData([]PrimaryKey{pk1}, []uint64{43757345})

		pk2 := &Int64PrimaryKey{
			Value: 2,
		}
		deleteData.Append(pk2, 23578294723)
		blob, err := deleteCodec.Serialize(CollectionID, 1, 1, deleteData)
		assert.NoError(t, err)

		pid, sid, data, err := deleteCodec.Deserialize([]*Blob{blob})
		assert.NoError(t, err)
		intPks, ok := data.DeletePks().(*Int64PrimaryKeys)
		require.True(t, ok)
		assert.Equal(t, pid, int64(1))
		assert.Equal(t, sid, int64(1))
		assert.Equal(t, []int64{1, 2}, intPks.values)
	})

	t.Run("string pk", func(t *testing.T) {
		deleteCodec := NewDeleteCodec()
		pk1 := NewVarCharPrimaryKey("test1")
		deleteData := NewDeleteData([]PrimaryKey{pk1}, []uint64{43757345})

		pk2 := NewVarCharPrimaryKey("test2")
		deleteData.Append(pk2, 23578294723)
		blob, err := deleteCodec.Serialize(CollectionID, 1, 1, deleteData)
		assert.NoError(t, err)

		pid, sid, data, err := deleteCodec.Deserialize([]*Blob{blob})
		assert.NoError(t, err)
		strPks, ok := data.DeletePks().(*VarcharPrimaryKeys)
		require.True(t, ok)
		assert.Equal(t, pid, int64(1))
		assert.Equal(t, sid, int64(1))
		assert.Equal(t, []string{"test1", "test2"}, strPks.values)
	})
}

func TestUpgradeDeleteLog(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
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
			err = eventWriter.AddOneStringToPayload(fmt.Sprintf("%d,%d", int64PkValue, ts), true)
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
		intPks, ok := deleteData.DeletePks().(*Int64PrimaryKeys)
		require.True(t, ok)
		assert.ElementsMatch(t, []int64{1, 2}, intPks.values)
		assert.ElementsMatch(t, dData.Tss, deleteData.DeleteTimestamps())
	})

	t.Run("with split lenth error", func(t *testing.T) {
		binlogWriter := NewDeleteBinlogWriter(schemapb.DataType_String, CollectionID, 1, 1)
		eventWriter, err := binlogWriter.NextDeleteEventWriter()
		assert.NoError(t, err)

		dData := &DeleteData{
			Pks:      []PrimaryKey{&Int64PrimaryKey{Value: 1}, &Int64PrimaryKey{Value: 2}},
			Tss:      []Timestamp{100, 200},
			RowCount: 2,
		}

		for i := int64(0); i < dData.RowCount; i++ {
			int64PkValue := dData.Pks[i].(*Int64PrimaryKey).Value
			ts := dData.Tss[i]
			err = eventWriter.AddOneStringToPayload(fmt.Sprintf("%d,%d,?", int64PkValue, ts), true)
			assert.NoError(t, err)
		}
		eventWriter.SetEventTimestamp(100, 200)
		binlogWriter.SetEventTimeStamp(100, 200)
		binlogWriter.AddExtra(originalSizeKey, fmt.Sprintf("%v", 0))

		err = binlogWriter.Finish()
		assert.NoError(t, err)
		buffer, err := binlogWriter.GetBuffer()
		assert.NoError(t, err)
		blob := &Blob{Value: buffer}

		dCodec := NewDeleteCodec()
		_, _, _, err = dCodec.Deserialize([]*Blob{blob})
		assert.Error(t, err)
	})

	t.Run("with parse int error", func(t *testing.T) {
		binlogWriter := NewDeleteBinlogWriter(schemapb.DataType_String, CollectionID, 1, 1)
		eventWriter, err := binlogWriter.NextDeleteEventWriter()
		assert.NoError(t, err)

		dData := &DeleteData{
			Pks:      []PrimaryKey{&Int64PrimaryKey{Value: 1}, &Int64PrimaryKey{Value: 2}},
			Tss:      []Timestamp{100, 200},
			RowCount: 2,
		}

		for i := int64(0); i < dData.RowCount; i++ {
			ts := dData.Tss[i]
			err = eventWriter.AddOneStringToPayload(fmt.Sprintf("abc,%d", ts), true)
			assert.NoError(t, err)
		}
		eventWriter.SetEventTimestamp(100, 200)
		binlogWriter.SetEventTimeStamp(100, 200)
		binlogWriter.AddExtra(originalSizeKey, fmt.Sprintf("%v", 0))

		err = binlogWriter.Finish()
		assert.NoError(t, err)
		buffer, err := binlogWriter.GetBuffer()
		assert.NoError(t, err)
		blob := &Blob{Value: buffer}

		dCodec := NewDeleteCodec()
		_, _, _, err = dCodec.Deserialize([]*Blob{blob})
		assert.Error(t, err)
	})

	t.Run("with parse ts uint error", func(t *testing.T) {
		binlogWriter := NewDeleteBinlogWriter(schemapb.DataType_String, CollectionID, 1, 1)
		eventWriter, err := binlogWriter.NextDeleteEventWriter()
		assert.NoError(t, err)

		dData := &DeleteData{
			Pks:      []PrimaryKey{&Int64PrimaryKey{Value: 1}, &Int64PrimaryKey{Value: 2}},
			Tss:      []Timestamp{100, 200},
			RowCount: 2,
		}

		for i := int64(0); i < dData.RowCount; i++ {
			int64PkValue := dData.Pks[i].(*Int64PrimaryKey).Value
			err = eventWriter.AddOneStringToPayload(fmt.Sprintf("%d,abc", int64PkValue), true)
			assert.NoError(t, err)
		}
		eventWriter.SetEventTimestamp(100, 200)
		binlogWriter.SetEventTimeStamp(100, 200)
		binlogWriter.AddExtra(originalSizeKey, fmt.Sprintf("%v", 0))

		err = binlogWriter.Finish()
		assert.NoError(t, err)
		buffer, err := binlogWriter.GetBuffer()
		assert.NoError(t, err)
		blob := &Blob{Value: buffer}

		dCodec := NewDeleteCodec()
		_, _, _, err = dCodec.Deserialize([]*Blob{blob})
		assert.Error(t, err)
	})
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
			Float16VectorField: &Float16VectorFieldData{
				Data: []byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7},
				Dim:  4,
			},
			BFloat16VectorField: &BFloat16VectorFieldData{
				Data: []byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7},
				Dim:  4,
			},
			Int8VectorField: &Int8VectorFieldData{
				Data: []int8{4, 5, 6, 7},
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
	assert.Equal(t, insertData1.Data[RowIDField].GetMemorySize(), 9)
	assert.Equal(t, insertData1.Data[TimestampField].GetMemorySize(), 9)
	assert.Equal(t, insertData1.Data[BoolField].GetMemorySize(), 2)
	assert.Equal(t, insertData1.Data[Int8Field].GetMemorySize(), 2)
	assert.Equal(t, insertData1.Data[Int16Field].GetMemorySize(), 3)
	assert.Equal(t, insertData1.Data[Int32Field].GetMemorySize(), 5)
	assert.Equal(t, insertData1.Data[Int64Field].GetMemorySize(), 9)
	assert.Equal(t, insertData1.Data[FloatField].GetMemorySize(), 5)
	assert.Equal(t, insertData1.Data[DoubleField].GetMemorySize(), 9)
	assert.Equal(t, insertData1.Data[StringField].GetMemorySize(), 18)
	assert.Equal(t, insertData1.Data[BinaryVectorField].GetMemorySize(), 5)
	assert.Equal(t, insertData1.Data[FloatVectorField].GetMemorySize(), 20)
	assert.Equal(t, insertData1.Data[Float16VectorField].GetMemorySize(), 12)
	assert.Equal(t, insertData1.Data[BFloat16VectorField].GetMemorySize(), 12)
	assert.Equal(t, insertData1.Data[Int8VectorField].GetMemorySize(), 8)
	assert.Equal(t, insertData1.Data[ArrayField].GetMemorySize(), 3*4+1)
	assert.Equal(t, insertData1.Data[JSONField].GetMemorySize(), len([]byte(`{"batch":1}`))+16+1)

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
			Float16VectorField: &Float16VectorFieldData{
				Data: []byte{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
				Dim:  4,
			},
			BFloat16VectorField: &BFloat16VectorFieldData{
				Data: []byte{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
				Dim:  4,
			},
			Int8VectorField: &Int8VectorFieldData{
				Data: []int8{0, 1, 2, 3, 0, 1, 2, 3},
				Dim:  4,
			},
		},
	}

	assert.Equal(t, insertData2.Data[RowIDField].GetMemorySize(), 17)
	assert.Equal(t, insertData2.Data[TimestampField].GetMemorySize(), 17)
	assert.Equal(t, insertData2.Data[BoolField].GetMemorySize(), 3)
	assert.Equal(t, insertData2.Data[Int8Field].GetMemorySize(), 3)
	assert.Equal(t, insertData2.Data[Int16Field].GetMemorySize(), 5)
	assert.Equal(t, insertData2.Data[Int32Field].GetMemorySize(), 9)
	assert.Equal(t, insertData2.Data[Int64Field].GetMemorySize(), 17)
	assert.Equal(t, insertData2.Data[FloatField].GetMemorySize(), 9)
	assert.Equal(t, insertData2.Data[DoubleField].GetMemorySize(), 17)
	assert.Equal(t, insertData2.Data[StringField].GetMemorySize(), 36)
	assert.Equal(t, insertData2.Data[BinaryVectorField].GetMemorySize(), 6)
	assert.Equal(t, insertData2.Data[FloatVectorField].GetMemorySize(), 36)
	assert.Equal(t, insertData2.Data[Float16VectorField].GetMemorySize(), 20)
	assert.Equal(t, insertData2.Data[BFloat16VectorField].GetMemorySize(), 20)
	assert.Equal(t, insertData2.Data[Int8VectorField].GetMemorySize(), 12)

	insertDataEmpty := &InsertData{
		Data: map[int64]FieldData{
			RowIDField:          &Int64FieldData{[]int64{}, nil, false},
			TimestampField:      &Int64FieldData{[]int64{}, nil, false},
			BoolField:           &BoolFieldData{[]bool{}, nil, false},
			Int8Field:           &Int8FieldData{[]int8{}, nil, false},
			Int16Field:          &Int16FieldData{[]int16{}, nil, false},
			Int32Field:          &Int32FieldData{[]int32{}, nil, false},
			Int64Field:          &Int64FieldData{[]int64{}, nil, false},
			FloatField:          &FloatFieldData{[]float32{}, nil, false},
			DoubleField:         &DoubleFieldData{[]float64{}, nil, false},
			StringField:         &StringFieldData{[]string{}, schemapb.DataType_VarChar, nil, false},
			BinaryVectorField:   &BinaryVectorFieldData{[]byte{}, 8},
			FloatVectorField:    &FloatVectorFieldData{[]float32{}, 4},
			Float16VectorField:  &Float16VectorFieldData{[]byte{}, 4},
			BFloat16VectorField: &BFloat16VectorFieldData{[]byte{}, 4},
			Int8VectorField:     &Int8VectorFieldData{[]int8{}, 4},
		},
	}

	assert.Equal(t, insertDataEmpty.Data[RowIDField].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[TimestampField].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[BoolField].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[Int8Field].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[Int16Field].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[Int32Field].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[Int64Field].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[FloatField].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[DoubleField].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[StringField].GetMemorySize(), 1)
	assert.Equal(t, insertDataEmpty.Data[BinaryVectorField].GetMemorySize(), 4)
	assert.Equal(t, insertDataEmpty.Data[FloatVectorField].GetMemorySize(), 4)
	assert.Equal(t, insertDataEmpty.Data[Float16VectorField].GetMemorySize(), 4)
	assert.Equal(t, insertDataEmpty.Data[BFloat16VectorField].GetMemorySize(), 4)
	assert.Equal(t, insertDataEmpty.Data[Int8VectorField].GetMemorySize(), 4)
}

func TestDeleteData(t *testing.T) {
	pks, err := GenInt64PrimaryKeys(1, 2, 3)
	require.NoError(t, err)

	pks2, err := GenInt64PrimaryKeys(4, 5, 6)
	require.NoError(t, err)

	t.Run("merge", func(t *testing.T) {
		first := NewDeleteData(pks, []Timestamp{100, 101, 102})
		second := NewDeleteData(pks2, []Timestamp{103, 104, 105})
		require.EqualValues(t, first.RowCount, second.RowCount)
		require.EqualValues(t, first.Size(), second.Size())
		require.EqualValues(t, 3, first.RowCount)
		require.EqualValues(t, 72, first.Size())

		first.Merge(second)
		assert.Equal(t, len(first.Pks), 6)
		assert.Equal(t, len(first.Tss), 6)
		assert.EqualValues(t, first.RowCount, 6)
		assert.EqualValues(t, first.Size(), 144)
		assert.ElementsMatch(t, first.Pks, append(pks, pks2...))
		assert.ElementsMatch(t, first.Tss, []Timestamp{100, 101, 102, 103, 104, 105})

		assert.NotNil(t, second)
		assert.EqualValues(t, 0, second.RowCount)
		assert.EqualValues(t, 0, second.Size())
	})

	t.Run("append", func(t *testing.T) {
		dData := NewDeleteData(nil, nil)
		dData.Append(pks[0], 100)

		assert.EqualValues(t, dData.RowCount, 1)
		assert.EqualValues(t, dData.Size(), 24)
	})

	t.Run("append batch", func(t *testing.T) {
		dData := NewDeleteData(nil, nil)
		dData.AppendBatch(pks, []Timestamp{100, 101, 102})

		assert.EqualValues(t, dData.RowCount, 3)
		assert.EqualValues(t, dData.Size(), 72)
	})
}

func TestAddFieldDataToPayload(t *testing.T) {
	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40, false)
	e, _ := w.NextInsertEventWriter()
	var err error
	err = AddFieldDataToPayload(e, schemapb.DataType_Bool, &BoolFieldData{[]bool{}, nil, false})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_Int8, &Int8FieldData{[]int8{}, nil, false})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_Int16, &Int16FieldData{[]int16{}, nil, false})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_Int32, &Int32FieldData{[]int32{}, nil, false})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_Int64, &Int64FieldData{[]int64{}, nil, false})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_Float, &FloatFieldData{[]float32{}, nil, false})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_Double, &DoubleFieldData{[]float64{}, nil, false})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_String, &StringFieldData{[]string{"test"}, schemapb.DataType_VarChar, nil, false})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_Array, &ArrayFieldData{
		ElementType: schemapb.DataType_VarChar,
		Data: []*schemapb.ScalarField{{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
			},
		}},
	})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_JSON, &JSONFieldData{[][]byte{[]byte(`"batch":2}`)}, nil, false})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_BinaryVector, &BinaryVectorFieldData{[]byte{}, 8})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_FloatVector, &FloatVectorFieldData{[]float32{}, 4})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_Float16Vector, &Float16VectorFieldData{[]byte{}, 4})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_BFloat16Vector, &BFloat16VectorFieldData{[]byte{}, 8})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_SparseFloatVector, &SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Dim:      0,
			Contents: [][]byte{},
		},
	})
	assert.Error(t, err)
	err = AddFieldDataToPayload(e, schemapb.DataType_Int8Vector, &Int8VectorFieldData{[]int8{}, 4})
	assert.Error(t, err)
}

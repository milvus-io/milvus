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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDataSorter(t *testing.T) {
	schema := &etcdpb.CollectionMeta{
		ID:            1,
		CreateTime:    1,
		SegmentIDs:    []int64{0, 1},
		PartitionTags: []string{"partition_0", "partition_1"},
		Schema: &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      0,
					Name:         "row_id",
					IsPrimaryKey: false,
					Description:  "row_id",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      1,
					Name:         "Ts",
					IsPrimaryKey: false,
					Description:  "Ts",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      100,
					Name:         "field_bool",
					IsPrimaryKey: false,
					Description:  "description_2",
					DataType:     schemapb.DataType_Bool,
				},
				{
					FieldID:      101,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "description_3",
					DataType:     schemapb.DataType_Int8,
				},
				{
					FieldID:      102,
					Name:         "field_int16",
					IsPrimaryKey: false,
					Description:  "description_4",
					DataType:     schemapb.DataType_Int16,
				},
				{
					FieldID:      103,
					Name:         "field_int32",
					IsPrimaryKey: false,
					Description:  "description_5",
					DataType:     schemapb.DataType_Int32,
				},
				{
					FieldID:      104,
					Name:         "field_int64",
					IsPrimaryKey: false,
					Description:  "description_6",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      105,
					Name:         "field_float",
					IsPrimaryKey: false,
					Description:  "description_7",
					DataType:     schemapb.DataType_Float,
				},
				{
					FieldID:      106,
					Name:         "field_double",
					IsPrimaryKey: false,
					Description:  "description_8",
					DataType:     schemapb.DataType_Double,
				},
				{
					FieldID:      107,
					Name:         "field_string",
					IsPrimaryKey: false,
					Description:  "description_9",
					DataType:     schemapb.DataType_String,
				},
				{
					FieldID:      108,
					Name:         "field_binary_vector",
					IsPrimaryKey: false,
					Description:  "description_10",
					DataType:     schemapb.DataType_BinaryVector,
				},
				{
					FieldID:      109,
					Name:         "field_float_vector",
					IsPrimaryKey: false,
					Description:  "description_11",
					DataType:     schemapb.DataType_FloatVector,
				},
				{
					FieldID:      110,
					Name:         "field_float16_vector",
					IsPrimaryKey: false,
					Description:  "description_12",
					DataType:     schemapb.DataType_Float16Vector,
				},
				{
					FieldID:      111,
					Name:         "field_bfloat16_vector",
					IsPrimaryKey: false,
					Description:  "description_13",
					DataType:     schemapb.DataType_BFloat16Vector,
				},
				{
					FieldID:      112,
					Name:         "field_sparse_float_vector",
					IsPrimaryKey: false,
					Description:  "description_14",
					DataType:     schemapb.DataType_SparseFloatVector,
				},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 113,
					Name:    "field_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     114,
							Name:        "field_sturct_float_vector",
							Description: "float",
							DataType:    schemapb.DataType_ArrayOfVector,
							ElementType: schemapb.DataType_FloatVector,
						},
					},
				},
			},
		},
	}

	insertCodec := NewInsertCodecWithSchema(schema)
	insertDataFirst := &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				Data: []int64{3, 4, 2},
			},
			1: &Int64FieldData{
				Data: []int64{3, 4, 5},
			},
			100: &BoolFieldData{
				Data: []bool{true, false, true},
			},
			101: &Int8FieldData{
				Data: []int8{3, 4, 5},
			},
			102: &Int16FieldData{
				Data: []int16{3, 4, 5},
			},
			103: &Int32FieldData{
				Data: []int32{3, 4, 5},
			},
			104: &Int64FieldData{
				Data: []int64{3, 4, 5},
			},
			105: &FloatFieldData{
				Data: []float32{3, 4, 5},
			},
			106: &DoubleFieldData{
				Data: []float64{3, 4, 5},
			},
			107: &StringFieldData{
				Data: []string{"3", "4", "5"},
			},
			108: &BinaryVectorFieldData{
				Data: []byte{0, 255, 128},
				Dim:  8,
			},
			109: &FloatVectorFieldData{
				Data: []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23},
				Dim:  8,
			},
			110: &Float16VectorFieldData{
				Data: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23},
				Dim:  4,
			},
			111: &BFloat16VectorFieldData{
				Data: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23},
				Dim:  4,
			},
			112: &SparseFloatVectorFieldData{
				SparseFloatArray: schemapb.SparseFloatArray{
					Dim: 600,
					Contents: [][]byte{
						typeutil.CreateSparseFloatRow([]uint32{0, 1, 2}, []float32{1.1, 1.2, 1.3}),
						typeutil.CreateSparseFloatRow([]uint32{10, 20, 30}, []float32{2.1, 2.2, 2.3}),
						typeutil.CreateSparseFloatRow([]uint32{100, 200, 599}, []float32{3.1, 3.2, 3.3}),
					},
				},
			},
			114: &VectorArrayFieldData{
				Dim:         2,
				ElementType: schemapb.DataType_FloatVector,
				Data: []*schemapb.VectorField{
					{
						Dim: 2,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}},
						},
					},
					{
						Dim: 2,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{Data: []float32{5, 6, 7, 8, 9, 10}},
						},
					},
					{
						Dim: 2,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{Data: []float32{11, 12, 13, 14, 15, 16}},
						},
					},
				},
			},
		},
	}

	dataSorter := &DataSorter{
		InsertCodec: insertCodec,
		InsertData:  insertDataFirst,
	}

	sort.Sort(dataSorter)

	// for _, field := range insertCodec.Schema.Schema.Fields {
	// 	singleData := insertDataFirst.Data[field.FieldID]
	// 	switch field.DataType {
	// 	case schemapb.DataType_Bool:
	// 		fmt.Println("BoolField:")
	// 		fmt.Println(singleData.(*BoolFieldData).Data)
	// 	case schemapb.DataType_Int8:
	// 		fmt.Println("Int8Field:")
	// 		fmt.Println(singleData.(*Int8FieldData).Data)
	// 	case schemapb.DataType_Int16:
	// 		fmt.Println("Int16Field:")
	// 		fmt.Println(singleData.(*Int16FieldData).Data)
	// 	case schemapb.DataType_Int32:
	// 		fmt.Println("Int32Field:")
	// 		fmt.Println(singleData.(*Int32FieldData).Data)
	// 	case schemapb.DataType_Int64:
	// 		fmt.Println("Int64Field:")
	// 		fmt.Println(singleData.(*Int64FieldData).Data)
	// 	case schemapb.DataType_Float:
	// 		fmt.Println("FloatField:")
	// 		fmt.Println(singleData.(*FloatFieldData).Data)
	// 	case schemapb.DataType_Double:
	// 		fmt.Println("DoubleField:")
	// 		fmt.Println(singleData.(*DoubleFieldData).Data)
	// 	case schemapb.DataType_String:
	// 		fmt.Println("StringField:")
	// 		for _, singleString := range singleData.(*StringFieldData).Data {
	// 			fmt.Println(singleString)
	// 		}
	// 	case schemapb.DataType_BinaryVector:
	// 		fmt.Println("BinaryVectorField:")
	// 		fmt.Println(singleData.(*BinaryVectorFieldData).Data)
	// 	case schemapb.DataType_FloatVector:
	// 		fmt.Println("FloatVectorField:")
	// 		fmt.Println(singleData.(*FloatVectorFieldData).Data)
	// 	default:
	// 	}
	// }

	// last row should be moved to the first row
	assert.Equal(t, []int64{2, 3, 4}, dataSorter.InsertData.Data[0].(*Int64FieldData).Data)
	assert.Equal(t, []int64{5, 3, 4}, dataSorter.InsertData.Data[1].(*Int64FieldData).Data)
	assert.Equal(t, []bool{true, true, false}, dataSorter.InsertData.Data[100].(*BoolFieldData).Data)
	assert.Equal(t, []int8{5, 3, 4}, dataSorter.InsertData.Data[101].(*Int8FieldData).Data)
	assert.Equal(t, []int16{5, 3, 4}, dataSorter.InsertData.Data[102].(*Int16FieldData).Data)
	assert.Equal(t, []int32{5, 3, 4}, dataSorter.InsertData.Data[103].(*Int32FieldData).Data)
	assert.Equal(t, []int64{5, 3, 4}, dataSorter.InsertData.Data[104].(*Int64FieldData).Data)
	assert.Equal(t, []float32{5, 3, 4}, dataSorter.InsertData.Data[105].(*FloatFieldData).Data)
	assert.Equal(t, []float64{5, 3, 4}, dataSorter.InsertData.Data[106].(*DoubleFieldData).Data)
	assert.Equal(t, []string{"5", "3", "4"}, dataSorter.InsertData.Data[107].(*StringFieldData).Data)
	assert.Equal(t, []byte{128, 0, 255}, dataSorter.InsertData.Data[108].(*BinaryVectorFieldData).Data)
	assert.Equal(t, []float32{16, 17, 18, 19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, dataSorter.InsertData.Data[109].(*FloatVectorFieldData).Data)
	assert.Equal(t, []byte{16, 17, 18, 19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, dataSorter.InsertData.Data[110].(*Float16VectorFieldData).Data)
	assert.Equal(t, []byte{16, 17, 18, 19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, dataSorter.InsertData.Data[111].(*BFloat16VectorFieldData).Data)
	assert.EqualExportedValues(t, &schemapb.SparseFloatArray{
		Dim: 600,
		Contents: [][]byte{
			typeutil.CreateSparseFloatRow([]uint32{100, 200, 599}, []float32{3.1, 3.2, 3.3}),
			typeutil.CreateSparseFloatRow([]uint32{0, 1, 2}, []float32{1.1, 1.2, 1.3}),
			typeutil.CreateSparseFloatRow([]uint32{10, 20, 30}, []float32{2.1, 2.2, 2.3}),
		},
	}, &dataSorter.InsertData.Data[112].(*SparseFloatVectorFieldData).SparseFloatArray)

	assert.Equal(t, []float32{11, 12, 13, 14, 15, 16},
		dataSorter.InsertData.Data[114].(*VectorArrayFieldData).Data[0].Data.(*schemapb.VectorField_FloatVector).FloatVector.Data)
	assert.Equal(t, []float32{1, 2, 3, 4},
		dataSorter.InsertData.Data[114].(*VectorArrayFieldData).Data[1].Data.(*schemapb.VectorField_FloatVector).FloatVector.Data)
	assert.Equal(t, []float32{5, 6, 7, 8, 9, 10},
		dataSorter.InsertData.Data[114].(*VectorArrayFieldData).Data[2].Data.(*schemapb.VectorField_FloatVector).FloatVector.Data)
}

func TestDataSorterNullableCompactVectors(t *testing.T) {
	schema := &etcdpb.CollectionMeta{
		ID:         1,
		CreateTime: 1,
		Schema: &schemapb.CollectionSchema{
			Name:   "schema",
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
				{FieldID: 200, Name: "nullable_int64", DataType: schemapb.DataType_Int64, Nullable: true},
				{FieldID: 201, Name: "binary_vector", DataType: schemapb.DataType_BinaryVector, Nullable: true},
				{FieldID: 202, Name: "float_vector", DataType: schemapb.DataType_FloatVector, Nullable: true},
				{FieldID: 203, Name: "float16_vector", DataType: schemapb.DataType_Float16Vector, Nullable: true},
				{FieldID: 204, Name: "bfloat16_vector", DataType: schemapb.DataType_BFloat16Vector, Nullable: true},
				{FieldID: 205, Name: "sparse_vector", DataType: schemapb.DataType_SparseFloatVector, Nullable: true},
				{FieldID: 206, Name: "int8_vector", DataType: schemapb.DataType_Int8Vector, Nullable: true},
			},
		},
	}

	binaryVector := &BinaryVectorFieldData{Dim: 8, Nullable: true}
	assert.NoError(t, binaryVector.AppendRow([]byte{0x11}))
	assert.NoError(t, binaryVector.AppendRow(nil))
	assert.NoError(t, binaryVector.AppendRow([]byte{0x33}))

	floatVector := &FloatVectorFieldData{Dim: 2, Nullable: true}
	assert.NoError(t, floatVector.AppendRow([]float32{1, 2}))
	assert.NoError(t, floatVector.AppendRow(nil))
	assert.NoError(t, floatVector.AppendRow([]float32{5, 6}))

	float16Vector := &Float16VectorFieldData{Dim: 2, Nullable: true}
	assert.NoError(t, float16Vector.AppendRow([]byte{1, 2, 3, 4}))
	assert.NoError(t, float16Vector.AppendRow(nil))
	assert.NoError(t, float16Vector.AppendRow([]byte{5, 6, 7, 8}))

	bfloat16Vector := &BFloat16VectorFieldData{Dim: 2, Nullable: true}
	assert.NoError(t, bfloat16Vector.AppendRow([]byte{11, 12, 13, 14}))
	assert.NoError(t, bfloat16Vector.AppendRow(nil))
	assert.NoError(t, bfloat16Vector.AppendRow([]byte{15, 16, 17, 18}))

	sparseRow0 := typeutil.CreateSparseFloatRow([]uint32{1}, []float32{1})
	sparseRow2 := typeutil.CreateSparseFloatRow([]uint32{3}, []float32{3})
	sparseVector := &SparseFloatVectorFieldData{SparseFloatArray: schemapb.SparseFloatArray{Dim: 8}, Nullable: true}
	assert.NoError(t, sparseVector.AppendRow(sparseRow0))
	assert.NoError(t, sparseVector.AppendRow(nil))
	assert.NoError(t, sparseVector.AppendRow(sparseRow2))

	int8Vector := &Int8VectorFieldData{Dim: 2, Nullable: true}
	assert.NoError(t, int8Vector.AppendRow([]int8{21, 22}))
	assert.NoError(t, int8Vector.AppendRow(nil))
	assert.NoError(t, int8Vector.AppendRow([]int8{25, 26}))

	insertData := &InsertData{
		Data: map[int64]FieldData{
			0:   &Int64FieldData{Data: []int64{20, 10, 30}},
			200: &Int64FieldData{Data: []int64{200, 0, 300}, ValidData: []bool{true, false, true}, Nullable: true},
			201: binaryVector,
			202: floatVector,
			203: float16Vector,
			204: bfloat16Vector,
			205: sparseVector,
			206: int8Vector,
		},
	}

	dataSorter := &DataSorter{
		InsertCodec: NewInsertCodecWithSchema(schema),
		InsertData:  insertData,
	}

	sort.Sort(dataSorter)

	assert.Equal(t, []int64{10, 20, 30}, insertData.Data[0].(*Int64FieldData).Data)

	int64Field := insertData.Data[200].(*Int64FieldData)
	assert.Equal(t, []bool{false, true, true}, int64Field.ValidData)
	assert.Equal(t, []int64{0, 200, 300}, int64Field.Data)
	assert.Nil(t, int64Field.GetRow(0))
	assert.Equal(t, int64(200), int64Field.GetRow(1))

	assert.Equal(t, []bool{false, true, true}, binaryVector.ValidData)
	assert.Equal(t, []byte{0x11, 0x33}, binaryVector.Data)
	assert.Nil(t, binaryVector.GetRow(0))
	assert.Equal(t, []byte{0x11}, binaryVector.GetRow(1))
	assert.Equal(t, []byte{0x33}, binaryVector.GetRow(2))

	assert.Equal(t, []bool{false, true, true}, floatVector.ValidData)
	assert.Equal(t, []float32{1, 2, 5, 6}, floatVector.Data)
	assert.Nil(t, floatVector.GetRow(0))
	assert.Equal(t, []float32{1, 2}, floatVector.GetRow(1))
	assert.Equal(t, []float32{5, 6}, floatVector.GetRow(2))

	assert.Equal(t, []bool{false, true, true}, float16Vector.ValidData)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, float16Vector.Data)
	assert.Nil(t, float16Vector.GetRow(0))
	assert.Equal(t, []byte{1, 2, 3, 4}, float16Vector.GetRow(1))
	assert.Equal(t, []byte{5, 6, 7, 8}, float16Vector.GetRow(2))

	assert.Equal(t, []bool{false, true, true}, bfloat16Vector.ValidData)
	assert.Equal(t, []byte{11, 12, 13, 14, 15, 16, 17, 18}, bfloat16Vector.Data)
	assert.Nil(t, bfloat16Vector.GetRow(0))
	assert.Equal(t, []byte{11, 12, 13, 14}, bfloat16Vector.GetRow(1))
	assert.Equal(t, []byte{15, 16, 17, 18}, bfloat16Vector.GetRow(2))

	assert.Equal(t, []bool{false, true, true}, sparseVector.ValidData)
	assert.Equal(t, [][]byte{sparseRow0, sparseRow2}, sparseVector.Contents)
	assert.Nil(t, sparseVector.GetRow(0))
	assert.Equal(t, sparseRow0, sparseVector.GetRow(1))
	assert.Equal(t, sparseRow2, sparseVector.GetRow(2))

	assert.Equal(t, []bool{false, true, true}, int8Vector.ValidData)
	assert.Equal(t, []int8{21, 22, 25, 26}, int8Vector.Data)
	assert.Nil(t, int8Vector.GetRow(0))
	assert.Equal(t, []int8{21, 22}, int8Vector.GetRow(1))
	assert.Equal(t, []int8{25, 26}, int8Vector.GetRow(2))
}

func TestDataSorterNullableCompactVectorsInterleavedRows(t *testing.T) {
	schema := &etcdpb.CollectionMeta{
		ID:         1,
		CreateTime: 1,
		Schema: &schemapb.CollectionSchema{
			Name:   "schema",
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
				{FieldID: 201, Name: "float_vector", DataType: schemapb.DataType_FloatVector, Nullable: true},
				{FieldID: 202, Name: "sparse_vector", DataType: schemapb.DataType_SparseFloatVector, Nullable: true},
			},
		},
	}

	floatVector := &FloatVectorFieldData{Dim: 1, Nullable: true}
	sparseVector := &SparseFloatVectorFieldData{SparseFloatArray: schemapb.SparseFloatArray{Dim: 8}, Nullable: true}
	rows := []struct {
		rowID int64
		valid bool
		value float32
	}{
		{40, true, 40},
		{10, false, 0},
		{30, true, 30},
		{20, false, 0},
		{50, true, 50},
	}
	for _, row := range rows {
		if row.valid {
			assert.NoError(t, floatVector.AppendRow([]float32{row.value}))
			assert.NoError(t, sparseVector.AppendRow(typeutil.CreateSparseFloatRow([]uint32{uint32(row.value)}, []float32{row.value})))
		} else {
			assert.NoError(t, floatVector.AppendRow(nil))
			assert.NoError(t, sparseVector.AppendRow(nil))
		}
	}

	insertData := &InsertData{
		Data: map[int64]FieldData{
			0:   &Int64FieldData{Data: []int64{40, 10, 30, 20, 50}},
			201: floatVector,
			202: sparseVector,
		},
	}
	sort.Sort(&DataSorter{
		InsertCodec: NewInsertCodecWithSchema(schema),
		InsertData:  insertData,
	})

	assert.Equal(t, []int64{10, 20, 30, 40, 50}, insertData.Data[0].(*Int64FieldData).Data)
	assert.Equal(t, []bool{false, false, true, true, true}, floatVector.ValidData)
	assert.Equal(t, []float32{30, 40, 50}, floatVector.Data)
	assert.Nil(t, floatVector.GetRow(0))
	assert.Nil(t, floatVector.GetRow(1))
	assert.Equal(t, []float32{30}, floatVector.GetRow(2))
	assert.Equal(t, []float32{40}, floatVector.GetRow(3))
	assert.Equal(t, []float32{50}, floatVector.GetRow(4))

	assert.Equal(t, []bool{false, false, true, true, true}, sparseVector.ValidData)
	assert.Len(t, sparseVector.Contents, 3)
	assert.Nil(t, sparseVector.GetRow(0))
	assert.Nil(t, sparseVector.GetRow(1))
	assert.Equal(t, typeutil.CreateSparseFloatRow([]uint32{30}, []float32{30}), sparseVector.GetRow(2))
	assert.Equal(t, typeutil.CreateSparseFloatRow([]uint32{40}, []float32{40}), sparseVector.GetRow(3))
	assert.Equal(t, typeutil.CreateSparseFloatRow([]uint32{50}, []float32{50}), sparseVector.GetRow(4))
}

func BenchmarkDataSorterNullableCompactVectorSort(b *testing.B) {
	const (
		rowCount = 4096
		dim      = 8
	)
	schema := &etcdpb.CollectionMeta{
		ID:         1,
		CreateTime: 1,
		Schema: &schemapb.CollectionSchema{
			Name:   "schema",
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
				{FieldID: 201, Name: "float_vector", DataType: schemapb.DataType_FloatVector, Nullable: true},
			},
		},
	}

	makeInsertData := func() *InsertData {
		rowIDs := make([]int64, rowCount)
		vector := &FloatVectorFieldData{Dim: dim, Nullable: true}
		for i := 0; i < rowCount; i++ {
			rowIDs[i] = int64(rowCount - i)
			if i%3 == 0 {
				assert.NoError(b, vector.AppendRow(nil))
				continue
			}
			row := make([]float32, dim)
			for j := range row {
				row[j] = float32(i*dim + j)
			}
			assert.NoError(b, vector.AppendRow(row))
		}
		return &InsertData{
			Data: map[int64]FieldData{
				0:   &Int64FieldData{Data: rowIDs},
				201: vector,
			},
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		insertData := makeInsertData()
		dataSorter := &DataSorter{
			InsertCodec: NewInsertCodecWithSchema(schema),
			InsertData:  insertData,
		}
		b.StartTimer()
		sort.Sort(dataSorter)
		b.StopTimer()
		if got := insertData.Data[0].(*Int64FieldData).Data[0]; got != 1 {
			b.Fatalf("first row id = %d, want 1", got)
		}
		if got := insertData.Data[0].(*Int64FieldData).Data[rowCount-1]; got != rowCount {
			b.Fatalf("last row id = %d, want %d", got, rowCount)
		}
		if got := insertData.Data[201].(*FloatVectorFieldData).RowNum(); got != rowCount {
			b.Fatalf("vector row count = %d, want %d", got, rowCount)
		}
	}
}

func TestDataSorter_Len(t *testing.T) {
	insertData := &InsertData{
		Data: map[int64]FieldData{
			1: &Int64FieldData{
				Data: []int64{6, 4},
			},
		},
	}

	dataSorter := &DataSorter{
		InsertCodec: nil,
		InsertData:  insertData,
	}

	n := dataSorter.Len()
	assert.Equal(t, n, 0)

	insertData = &InsertData{
		Data: map[int64]FieldData{
			0: &Int8FieldData{
				Data: []int8{3, 4},
			},
		},
	}

	dataSorter = &DataSorter{
		InsertCodec: nil,
		InsertData:  insertData,
	}

	n = dataSorter.Len()
	assert.Equal(t, n, 0)
}

func TestDataSorter_Less(t *testing.T) {
	insertData := &InsertData{
		Data: map[int64]FieldData{
			1: &Int64FieldData{
				Data: []int64{6, 4},
			},
		},
	}

	dataSorter := &DataSorter{
		InsertCodec: nil,
		InsertData:  insertData,
	}

	res := dataSorter.Less(1, 2)
	assert.True(t, res)

	insertData = &InsertData{
		Data: map[int64]FieldData{
			0: &Int8FieldData{
				Data: []int8{3, 4},
			},
		},
	}

	dataSorter = &DataSorter{
		InsertCodec: nil,
		InsertData:  insertData,
	}

	res = dataSorter.Less(1, 2)
	assert.True(t, res)

	insertData = &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				Data: []int64{6, 4},
			},
		},
	}

	dataSorter = &DataSorter{
		InsertCodec: nil,
		InsertData:  insertData,
	}

	res = dataSorter.Less(-1, -2)
	assert.True(t, res)
}

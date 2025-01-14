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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

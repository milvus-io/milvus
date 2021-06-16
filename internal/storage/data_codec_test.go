// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestInsertCodec(t *testing.T) {
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
			},
		},
	}
	insertCodec := NewInsertCodec(schema)
	insertDataFirst := &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{3, 4},
			},
			1: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{3, 4},
			},
			100: &BoolFieldData{
				NumRows: 2,
				Data:    []bool{true, false},
			},
			101: &Int8FieldData{
				NumRows: 2,
				Data:    []int8{3, 4},
			},
			102: &Int16FieldData{
				NumRows: 2,
				Data:    []int16{3, 4},
			},
			103: &Int32FieldData{
				NumRows: 2,
				Data:    []int32{3, 4},
			},
			104: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{3, 4},
			},
			105: &FloatFieldData{
				NumRows: 2,
				Data:    []float32{3, 4},
			},
			106: &DoubleFieldData{
				NumRows: 2,
				Data:    []float64{3, 4},
			},
			107: &StringFieldData{
				NumRows: 2,
				Data:    []string{"3", "4"},
			},
			108: &BinaryVectorFieldData{
				NumRows: 2,
				Data:    []byte{0, 255},
				Dim:     8,
			},
			109: &FloatVectorFieldData{
				NumRows: 2,
				Data:    []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
				Dim:     8,
			},
		},
	}

	insertDataSecond := &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{1, 2},
			},
			1: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{1, 2},
			},
			100: &BoolFieldData{
				NumRows: 2,
				Data:    []bool{true, false},
			},
			101: &Int8FieldData{
				NumRows: 2,
				Data:    []int8{1, 2},
			},
			102: &Int16FieldData{
				NumRows: 2,
				Data:    []int16{1, 2},
			},
			103: &Int32FieldData{
				NumRows: 2,
				Data:    []int32{1, 2},
			},
			104: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{1, 2},
			},
			105: &FloatFieldData{
				NumRows: 2,
				Data:    []float32{1, 2},
			},
			106: &DoubleFieldData{
				NumRows: 2,
				Data:    []float64{1, 2},
			},
			107: &StringFieldData{
				NumRows: 2,
				Data:    []string{"1", "2"},
			},
			108: &BinaryVectorFieldData{
				NumRows: 2,
				Data:    []byte{0, 255},
				Dim:     8,
			},
			109: &FloatVectorFieldData{
				NumRows: 2,
				Data:    []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
				Dim:     8,
			},
		},
	}
	firstBlobs, _, err := insertCodec.Serialize(1, 1, insertDataFirst)
	assert.Nil(t, err)
	for _, blob := range firstBlobs {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
		assert.Equal(t, blob.GetKey(), blob.Key)
	}
	secondBlobs, _, err := insertCodec.Serialize(1, 1, insertDataSecond)
	assert.Nil(t, err)
	for _, blob := range secondBlobs {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 99)
		assert.Equal(t, blob.GetKey(), blob.Key)
	}
	resultBlobs := append(firstBlobs, secondBlobs...)
	partitionID, segmentID, resultData, err := insertCodec.Deserialize(resultBlobs)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), partitionID)
	assert.Equal(t, int64(1), segmentID)
	assert.Equal(t, 4, resultData.Data[0].(*Int64FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[1].(*Int64FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[100].(*BoolFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[101].(*Int8FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[102].(*Int16FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[103].(*Int32FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[104].(*Int64FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[105].(*FloatFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[106].(*DoubleFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[107].(*StringFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[108].(*BinaryVectorFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[109].(*FloatVectorFieldData).NumRows)
	assert.Equal(t, []int64{1, 2, 3, 4}, resultData.Data[0].(*Int64FieldData).Data)
	assert.Equal(t, []int64{1, 2, 3, 4}, resultData.Data[1].(*Int64FieldData).Data)
	assert.Equal(t, []bool{true, false, true, false}, resultData.Data[100].(*BoolFieldData).Data)
	assert.Equal(t, []int8{1, 2, 3, 4}, resultData.Data[101].(*Int8FieldData).Data)
	assert.Equal(t, []int16{1, 2, 3, 4}, resultData.Data[102].(*Int16FieldData).Data)
	assert.Equal(t, []int32{1, 2, 3, 4}, resultData.Data[103].(*Int32FieldData).Data)
	assert.Equal(t, []int64{1, 2, 3, 4}, resultData.Data[104].(*Int64FieldData).Data)
	assert.Equal(t, []float32{1, 2, 3, 4}, resultData.Data[105].(*FloatFieldData).Data)
	assert.Equal(t, []float64{1, 2, 3, 4}, resultData.Data[106].(*DoubleFieldData).Data)
	assert.Equal(t, []string{"1", "2", "3", "4"}, resultData.Data[107].(*StringFieldData).Data)
	assert.Equal(t, []byte{0, 255, 0, 255}, resultData.Data[108].(*BinaryVectorFieldData).Data)
	assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
		resultData.Data[109].(*FloatVectorFieldData).Data)
	assert.Nil(t, insertCodec.Close())
	log.Debug("Data", zap.Any("Data", resultData.Data))
	log.Debug("Infos", zap.Any("Infos", resultData.Infos))

	blobs := []*Blob{}
	_, _, _, err = insertCodec.Deserialize(blobs)
	assert.NotNil(t, err)
}
func TestDDCodec(t *testing.T) {
	dataDefinitionCodec := NewDataDefinitionCodec(int64(1))
	ts := []Timestamp{
		1,
		2,
		3,
		4,
	}
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
	assert.Nil(t, err)
	for _, blob := range blobs {
		blob.Key = fmt.Sprintf("1/data_definition/3/4/5/%d", 99)
	}
	resultTs, resultRequests, err := dataDefinitionCodec.Deserialize(blobs)
	assert.Nil(t, err)
	assert.Equal(t, resultTs, ts)
	assert.Equal(t, resultRequests, ddRequests)
	assert.Nil(t, dataDefinitionCodec.Close())

	blobs = []*Blob{}
	_, _, err = dataDefinitionCodec.Deserialize(blobs)
	assert.NotNil(t, err)
}

func TestIndexCodec(t *testing.T) {
	indexCodec := NewIndexCodec()
	blobs := []*Blob{
		{
			"12345",
			[]byte{1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7},
		},
		{
			"6666",
			[]byte{6, 6, 6, 6, 6, 1, 2, 3, 4, 5, 6, 7},
		},
		{
			"8885",
			[]byte{8, 8, 8, 8, 8, 8, 8, 8, 2, 3, 4, 5, 6, 7},
		},
	}
	indexParams := map[string]string{
		"k1": "v1", "k2": "v2",
	}
	blobsInput, err := indexCodec.Serialize(blobs, indexParams, "index_test_name", 1234)
	assert.Nil(t, err)
	assert.EqualValues(t, 4, len(blobsInput))
	assert.EqualValues(t, IndexParamsFile, blobsInput[3].Key)
	blobsOutput, indexParamsOutput, indexName, indexID, err := indexCodec.Deserialize(blobsInput)
	assert.Nil(t, err)
	assert.EqualValues(t, 3, len(blobsOutput))
	for i := 0; i < 3; i++ {
		assert.EqualValues(t, blobs[i], blobsOutput[i])
	}
	assert.EqualValues(t, indexParams, indexParamsOutput)
	assert.EqualValues(t, "index_test_name", indexName)
	assert.EqualValues(t, 1234, indexID)

	blobs = []*Blob{}
	_, _, _, _, err = indexCodec.Deserialize(blobs)
	assert.NotNil(t, err)
}

func TestTsError(t *testing.T) {
	insertData := &InsertData{}
	insertCodec := NewInsertCodec(nil)
	blobs, _, err := insertCodec.Serialize(1, 1, insertData)
	assert.Nil(t, blobs)
	assert.NotNil(t, err)
}

func TestSchemaError(t *testing.T) {
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
					DataType:     999,
				},
			},
		},
	}
	insertData := &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{3, 4},
			},
			1: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{3, 4},
			},
			100: &BoolFieldData{
				NumRows: 2,
				Data:    []bool{true, false},
			},
		},
	}
	insertCodec := NewInsertCodec(schema)
	blobs, _, err := insertCodec.Serialize(1, 1, insertData)
	assert.Nil(t, blobs)
	assert.NotNil(t, err)
}

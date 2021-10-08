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

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

const (
	CollectionID      = 1
	PartitionID       = 1
	SegmentID         = 1
	RowIDField        = 0
	TimestampField    = 1
	BoolField         = 100
	Int8Field         = 101
	Int16Field        = 102
	Int32Field        = 103
	Int64Field        = 104
	FloatField        = 105
	DoubleField       = 106
	StringField       = 107
	BinaryVectorField = 108
	FloatVectorField  = 109
)

func TestInsertCodec(t *testing.T) {
	schema := &etcdpb.CollectionMeta{
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
					FieldID:      RowIDField,
					Name:         "row_id",
					IsPrimaryKey: false,
					Description:  "row_id",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      TimestampField,
					Name:         "Timestamp",
					IsPrimaryKey: false,
					Description:  "Timestamp",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      BoolField,
					Name:         "field_bool",
					IsPrimaryKey: false,
					Description:  "bool",
					DataType:     schemapb.DataType_Bool,
				},
				{
					FieldID:      Int8Field,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "int8",
					DataType:     schemapb.DataType_Int8,
				},
				{
					FieldID:      Int16Field,
					Name:         "field_int16",
					IsPrimaryKey: false,
					Description:  "int16",
					DataType:     schemapb.DataType_Int16,
				},
				{
					FieldID:      Int32Field,
					Name:         "field_int32",
					IsPrimaryKey: false,
					Description:  "int32",
					DataType:     schemapb.DataType_Int32,
				},
				{
					FieldID:      Int64Field,
					Name:         "field_int64",
					IsPrimaryKey: false,
					Description:  "int64",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      FloatField,
					Name:         "field_float",
					IsPrimaryKey: false,
					Description:  "float",
					DataType:     schemapb.DataType_Float,
				},
				{
					FieldID:      DoubleField,
					Name:         "field_double",
					IsPrimaryKey: false,
					Description:  "double",
					DataType:     schemapb.DataType_Double,
				},
				{
					FieldID:      StringField,
					Name:         "field_string",
					IsPrimaryKey: false,
					Description:  "string",
					DataType:     schemapb.DataType_String,
				},
				{
					FieldID:      BinaryVectorField,
					Name:         "field_binary_vector",
					IsPrimaryKey: false,
					Description:  "binary_vector",
					DataType:     schemapb.DataType_BinaryVector,
				},
				{
					FieldID:      FloatVectorField,
					Name:         "field_float_vector",
					IsPrimaryKey: false,
					Description:  "float_vector",
					DataType:     schemapb.DataType_FloatVector,
				},
			},
		},
	}
	insertCodec := NewInsertCodec(schema)
	insertData1 := &InsertData{
		Data: map[int64]FieldData{
			RowIDField: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{3, 4},
			},
			TimestampField: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{3, 4},
			},
			BoolField: &BoolFieldData{
				NumRows: []int64{2},
				Data:    []bool{true, false},
			},
			Int8Field: &Int8FieldData{
				NumRows: []int64{2},
				Data:    []int8{3, 4},
			},
			Int16Field: &Int16FieldData{
				NumRows: []int64{2},
				Data:    []int16{3, 4},
			},
			Int32Field: &Int32FieldData{
				NumRows: []int64{2},
				Data:    []int32{3, 4},
			},
			Int64Field: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{3, 4},
			},
			FloatField: &FloatFieldData{
				NumRows: []int64{2},
				Data:    []float32{3, 4},
			},
			DoubleField: &DoubleFieldData{
				NumRows: []int64{2},
				Data:    []float64{3, 4},
			},
			StringField: &StringFieldData{
				NumRows: []int64{2},
				Data:    []string{"3", "4"},
			},
			BinaryVectorField: &BinaryVectorFieldData{
				NumRows: []int64{2},
				Data:    []byte{0, 255},
				Dim:     8,
			},
			FloatVectorField: &FloatVectorFieldData{
				NumRows: []int64{2},
				Data:    []float32{4, 5, 6, 7, 4, 5, 6, 7},
				Dim:     4,
			},
		},
	}

	insertData2 := &InsertData{
		Data: map[int64]FieldData{
			RowIDField: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{1, 2},
			},
			TimestampField: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{1, 2},
			},
			BoolField: &BoolFieldData{
				NumRows: []int64{2},
				Data:    []bool{true, false},
			},
			Int8Field: &Int8FieldData{
				NumRows: []int64{2},
				Data:    []int8{1, 2},
			},
			Int16Field: &Int16FieldData{
				NumRows: []int64{2},
				Data:    []int16{1, 2},
			},
			Int32Field: &Int32FieldData{
				NumRows: []int64{2},
				Data:    []int32{1, 2},
			},
			Int64Field: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{1, 2},
			},
			FloatField: &FloatFieldData{
				NumRows: []int64{2},
				Data:    []float32{1, 2},
			},
			DoubleField: &DoubleFieldData{
				NumRows: []int64{2},
				Data:    []float64{1, 2},
			},
			StringField: &StringFieldData{
				NumRows: []int64{2},
				Data:    []string{"1", "2"},
			},
			BinaryVectorField: &BinaryVectorFieldData{
				NumRows: []int64{2},
				Data:    []byte{0, 255},
				Dim:     8,
			},
			FloatVectorField: &FloatVectorFieldData{
				NumRows: []int64{2},
				Data:    []float32{0, 1, 2, 3, 0, 1, 2, 3},
				Dim:     4,
			},
		},
	}
	Blobs1, _, err := insertCodec.Serialize(PartitionID, SegmentID, insertData1)
	assert.Nil(t, err)
	for _, blob := range Blobs1 {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
		assert.Equal(t, blob.GetKey(), blob.Key)
	}
	Blobs2, _, err := insertCodec.Serialize(PartitionID, SegmentID, insertData2)
	assert.Nil(t, err)
	for _, blob := range Blobs2 {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 99)
		assert.Equal(t, blob.GetKey(), blob.Key)
	}
	resultBlobs := append(Blobs1, Blobs2...)
	collID, partID, segID, resultData, err := insertCodec.DeserializeAll(resultBlobs)
	assert.Nil(t, err)
	assert.Equal(t, UniqueID(CollectionID), collID)
	assert.Equal(t, UniqueID(PartitionID), partID)
	assert.Equal(t, UniqueID(SegmentID), segID)
	assert.Equal(t, []int64{2, 2}, resultData.Data[RowIDField].(*Int64FieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[TimestampField].(*Int64FieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[BoolField].(*BoolFieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[Int8Field].(*Int8FieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[Int16Field].(*Int16FieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[Int32Field].(*Int32FieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[Int64Field].(*Int64FieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[FloatField].(*FloatFieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[DoubleField].(*DoubleFieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[StringField].(*StringFieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[BinaryVectorField].(*BinaryVectorFieldData).NumRows)
	assert.Equal(t, []int64{2, 2}, resultData.Data[FloatVectorField].(*FloatVectorFieldData).NumRows)
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
	assert.Nil(t, insertCodec.Close())
	log.Debug("Data", zap.Any("Data", resultData.Data))
	log.Debug("Infos", zap.Any("Infos", resultData.Infos))

	blobs := []*Blob{}
	_, _, _, err = insertCodec.Deserialize(blobs)
	assert.NotNil(t, err)
	_, _, _, _, err = insertCodec.DeserializeAll(blobs)
	assert.NotNil(t, err)
}

func TestDeleteCodec(t *testing.T) {
	schema := &etcdpb.CollectionMeta{
		ID: CollectionID,
	}
	deleteCodec := NewDeleteCodec(schema)
	deleteData := &DeleteData{
		Data: map[string]int64{"1": 43757345, "2": 23578294723},
	}
	blob, err := deleteCodec.Serialize(1, 1, deleteData)
	assert.Nil(t, err)

	pid, sid, data, err := deleteCodec.Deserialize(blob)
	assert.Nil(t, err)
	assert.Equal(t, pid, int64(1))
	assert.Equal(t, sid, int64(1))
	assert.Equal(t, data, deleteData)
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

func TestIndexFileBinlogCodec(t *testing.T) {
	indexBuildID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	version := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	collectionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	partitionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	segmentID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	fieldID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexName := funcutil.GenRandomStr()
	indexID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_FLAT"
	datas := []*Blob{
		{
			Key:   "ivf1",
			Value: []byte{1, 2, 3},
		},
		{
			Key:   "ivf2",
			Value: []byte{4, 5, 6},
		},
		{
			Key:   "large",
			Value: []byte(funcutil.RandomString(maxLengthPerRowOfIndexFile + 1)),
		},
	}

	codec := NewIndexFileBinlogCodec()

	serializedBlobs, err := codec.Serialize(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexParams, indexName, indexID, datas)
	assert.Nil(t, err)

	idxBuildID, v, collID, parID, segID, fID, params, idxName, idxID, blobs, err := codec.DeserializeImpl(serializedBlobs)
	assert.Nil(t, err)
	assert.Equal(t, indexBuildID, idxBuildID)
	assert.Equal(t, version, v)
	assert.Equal(t, collectionID, collID)
	assert.Equal(t, partitionID, parID)
	assert.Equal(t, segmentID, segID)
	assert.Equal(t, fieldID, fID)
	assert.Equal(t, len(indexParams), len(params))
	for key, value := range indexParams {
		assert.Equal(t, value, params[key])
	}
	assert.Equal(t, indexName, idxName)
	assert.Equal(t, indexID, idxID)
	assert.ElementsMatch(t, datas, blobs)

	blobs, indexParams, indexName, indexID, err = codec.Deserialize(serializedBlobs)
	assert.Nil(t, err)
	assert.ElementsMatch(t, datas, blobs)
	for key, value := range indexParams {
		assert.Equal(t, value, params[key])
	}
	assert.Equal(t, indexName, idxName)
	assert.Equal(t, indexID, idxID)

	err = codec.Close()
	assert.Nil(t, err)

	// empty
	_, _, _, _, _, _, _, _, _, _, err = codec.DeserializeImpl(nil)
	assert.NotNil(t, err)
}

func TestIndexFileBinlogCodecError(t *testing.T) {
	var err error

	// failed to read binlog
	codec := NewIndexFileBinlogCodec()
	_, _, _, _, err = codec.Deserialize([]*Blob{{Key: "key", Value: []byte("not in binlog format")}})
	assert.NotNil(t, err)

	indexBuildID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	version := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	collectionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	partitionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	segmentID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	fieldID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexName := funcutil.GenRandomStr()
	indexID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_FLAT"
	datas := []*Blob{
		{
			Key:   "ivf1",
			Value: []byte{1, 2, 3},
		},
	}

	_, err = codec.Serialize(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexParams, indexName, indexID, datas)
	assert.Nil(t, err)
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
					FieldID:      RowIDField,
					Name:         "row_id",
					IsPrimaryKey: false,
					Description:  "row_id",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      TimestampField,
					Name:         "Timestamp",
					IsPrimaryKey: false,
					Description:  "Timestamp",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      BoolField,
					Name:         "field_bool",
					IsPrimaryKey: false,
					Description:  "bool",
					DataType:     999,
				},
			},
		},
	}
	insertData := &InsertData{
		Data: map[int64]FieldData{
			RowIDField: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{3, 4},
			},
			TimestampField: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{3, 4},
			},
			BoolField: &BoolFieldData{
				NumRows: []int64{2},
				Data:    []bool{true, false},
			},
		},
	}
	insertCodec := NewInsertCodec(schema)
	blobs, _, err := insertCodec.Serialize(PartitionID, SegmentID, insertData)
	assert.Nil(t, blobs)
	assert.NotNil(t, err)
}

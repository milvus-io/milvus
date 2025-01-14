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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/uniquegenerator"
)

func TestPrintBinlogFilesInt64(t *testing.T) {
	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40, false)

	curTS := time.Now().UnixNano() / int64(time.Millisecond)

	e1, err := w.NextInsertEventWriter()
	assert.NoError(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3}, nil)
	assert.NoError(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6}, nil)
	assert.Error(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6}, nil)
	assert.NoError(t, err)
	e1.SetEventTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0), tsoutil.ComposeTS(curTS+20*60*1000, 0))

	e2, err := w.NextInsertEventWriter()
	assert.NoError(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9}, nil)
	assert.NoError(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true}, nil)
	assert.Error(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12}, nil)
	assert.NoError(t, err)
	e2.SetEventTimestamp(tsoutil.ComposeTS(curTS+30*60*1000, 0), tsoutil.ComposeTS(curTS+40*60*1000, 0))

	w.SetEventTimeStamp(tsoutil.ComposeTS(curTS, 0), tsoutil.ComposeTS(curTS+3600*1000, 0))

	_, err = w.GetBuffer()
	assert.Error(t, err)
	sizeTotal := 20000000
	w.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))
	err = w.Finish()
	assert.NoError(t, err)
	buf, err := w.GetBuffer()
	assert.NoError(t, err)
	w.Close()

	fd, err := os.CreateTemp("", "binlog_int64.db")
	defer os.RemoveAll(fd.Name())
	assert.NoError(t, err)
	num, err := fd.Write(buf)
	assert.NoError(t, err)
	assert.Equal(t, num, len(buf))
	err = fd.Close()
	assert.NoError(t, err)
}

func TestPrintBinlogFiles(t *testing.T) {
	Schema := &etcdpb.CollectionMeta{
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
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "8"},
					},
				},
				{
					FieldID:      109,
					Name:         "field_float_vector",
					IsPrimaryKey: false,
					Description:  "description_11",
					DataType:     schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "8"},
					},
				},
				{
					FieldID:      110,
					Name:         "field_json",
					IsPrimaryKey: false,
					Description:  "description_12",
					DataType:     schemapb.DataType_JSON,
				},
				{
					FieldID:      111,
					Name:         "field_bfloat16_vector",
					IsPrimaryKey: false,
					Description:  "description_13",
					DataType:     schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
					},
				},
				{
					FieldID:      112,
					Name:         "field_float16_vector",
					IsPrimaryKey: false,
					Description:  "description_14",
					DataType:     schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
					},
				},
			},
		},
	}
	insertCodec := NewInsertCodecWithSchema(Schema)
	insertDataFirst := &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				Data: []int64{3, 4},
			},
			1: &Int64FieldData{
				Data: []int64{3, 4},
			},
			100: &BoolFieldData{
				Data: []bool{true, false},
			},
			101: &Int8FieldData{
				Data: []int8{3, 4},
			},
			102: &Int16FieldData{
				Data: []int16{3, 4},
			},
			103: &Int32FieldData{
				Data: []int32{3, 4},
			},
			104: &Int64FieldData{
				Data: []int64{3, 4},
			},
			105: &FloatFieldData{
				Data: []float32{3, 4},
			},
			106: &DoubleFieldData{
				Data: []float64{3, 4},
			},
			107: &StringFieldData{
				Data: []string{"3", "4"},
			},
			108: &BinaryVectorFieldData{
				Data: []byte{0, 255},
				Dim:  8,
			},
			109: &FloatVectorFieldData{
				Data: []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
				Dim:  8,
			},
			110: &JSONFieldData{
				Data: [][]byte{
					[]byte(`{}`),
					[]byte(`{"key":"hello"}`),
				},
			},
			111: &BFloat16VectorFieldData{
				Data: []byte("12345678"),
				Dim:  4,
			},
			112: &Float16VectorFieldData{
				Data: []byte("12345678"),
				Dim:  4,
			},
		},
	}

	insertDataSecond := &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				Data: []int64{1, 2},
			},
			1: &Int64FieldData{
				Data: []int64{1, 2},
			},
			100: &BoolFieldData{
				Data: []bool{true, false},
			},
			101: &Int8FieldData{
				Data: []int8{1, 2},
			},
			102: &Int16FieldData{
				Data: []int16{1, 2},
			},
			103: &Int32FieldData{
				Data: []int32{1, 2},
			},
			104: &Int64FieldData{
				Data: []int64{1, 2},
			},
			105: &FloatFieldData{
				Data: []float32{1, 2},
			},
			106: &DoubleFieldData{
				Data: []float64{1, 2},
			},
			107: &StringFieldData{
				Data: []string{"1", "2"},
			},
			108: &BinaryVectorFieldData{
				Data: []byte{0, 255},
				Dim:  8,
			},
			109: &FloatVectorFieldData{
				Data: []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
				Dim:  8,
			},
			110: &JSONFieldData{
				Data: [][]byte{
					[]byte(`{}`),
					[]byte(`{"key":"world"}`),
				},
			},
			111: &BFloat16VectorFieldData{
				Data: []byte("abcdefgh"),
				Dim:  4,
			},
			112: &Float16VectorFieldData{
				Data: []byte("abcdefgh"),
				Dim:  4,
			},
		},
	}
	firstBlobs, err := insertCodec.Serialize(1, 1, insertDataFirst)
	assert.NoError(t, err)
	var binlogFiles []string
	for index, blob := range firstBlobs {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
		fileName := fmt.Sprintf("/tmp/firstblob_%d.db", index)
		binlogFiles = append(binlogFiles, fileName)
		fd, err := os.Create(fileName)
		assert.NoError(t, err)
		num, err := fd.Write(blob.GetValue())
		assert.NoError(t, err)
		assert.Equal(t, num, len(blob.GetValue()))
		err = fd.Close()
		assert.NoError(t, err)
	}
	secondBlobs, err := insertCodec.Serialize(1, 1, insertDataSecond)
	assert.NoError(t, err)
	for index, blob := range secondBlobs {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 99)
		fileName := fmt.Sprintf("/tmp/secondblob_%d.db", index)
		binlogFiles = append(binlogFiles, fileName)
		fd, err := os.Create(fileName)
		assert.NoError(t, err)
		num, err := fd.Write(blob.GetValue())
		assert.NoError(t, err)
		assert.Equal(t, num, len(blob.GetValue()))
		err = fd.Close()
		assert.NoError(t, err)
	}
	binlogFiles = append(binlogFiles, "test")

	PrintBinlogFiles(binlogFiles)
	for _, file := range binlogFiles {
		_ = os.RemoveAll(file)
	}
}

func TestPrintDDFiles(t *testing.T) {
	dataDefinitionCodec := NewDataDefinitionCodec(int64(1))
	ts := []Timestamp{
		1,
		2,
		3,
		4,
	}
	collID := int64(1)
	partitionID := int64(1)
	collName := "test"
	partitionName := "test"
	createCollReq := msgpb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			MsgID:     1,
			Timestamp: 1,
			SourceID:  1,
		},
		CollectionID:   collID,
		Schema:         make([]byte, 0),
		CollectionName: collName,
		DbName:         "DbName",
		DbID:           UniqueID(0),
	}
	createCollString, err := proto.Marshal(&createCollReq)
	assert.NoError(t, err)

	dropCollReq := msgpb.DropCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     2,
			Timestamp: 2,
			SourceID:  2,
		},
		CollectionID:   collID,
		CollectionName: collName,
		DbName:         "DbName",
		DbID:           UniqueID(0),
	}
	dropCollString, err := proto.Marshal(&dropCollReq)
	assert.NoError(t, err)

	createPartitionReq := msgpb.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreatePartition,
			MsgID:     3,
			Timestamp: 3,
			SourceID:  3,
		},
		CollectionID:   collID,
		PartitionID:    partitionID,
		CollectionName: collName,
		PartitionName:  partitionName,
		DbName:         "DbName",
		DbID:           UniqueID(0),
	}
	createPartitionString, err := proto.Marshal(&createPartitionReq)
	assert.NoError(t, err)

	dropPartitionReq := msgpb.DropPartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropPartition,
			MsgID:     4,
			Timestamp: 4,
			SourceID:  4,
		},
		CollectionID:   collID,
		PartitionID:    partitionID,
		CollectionName: collName,
		PartitionName:  partitionName,
		DbName:         "DbName",
		DbID:           UniqueID(0),
	}
	dropPartitionString, err := proto.Marshal(&dropPartitionReq)
	assert.NoError(t, err)
	ddRequests := []string{
		string(createCollString),
		string(dropCollString),
		string(createPartitionString),
		string(dropPartitionString),
	}
	eventTypeCodes := []EventTypeCode{
		CreateCollectionEventType,
		DropCollectionEventType,
		CreatePartitionEventType,
		DropPartitionEventType,
	}
	blobs, err := dataDefinitionCodec.Serialize(ts, ddRequests, eventTypeCodes)
	assert.NoError(t, err)
	var binlogFiles []string
	for index, blob := range blobs {
		blob.Key = fmt.Sprintf("1/data_definition/3/4/5/%d", 99)
		fileName := fmt.Sprintf("/tmp/ddblob_%d.db", index)
		binlogFiles = append(binlogFiles, fileName)
		fd, err := os.Create(fileName)
		assert.NoError(t, err)
		num, err := fd.Write(blob.GetValue())
		assert.NoError(t, err)
		assert.Equal(t, num, len(blob.GetValue()))
		err = fd.Close()
		assert.NoError(t, err)
	}
	resultTs, resultRequests, err := dataDefinitionCodec.Deserialize(blobs)
	assert.NoError(t, err)
	assert.Equal(t, resultTs, ts)
	assert.Equal(t, resultRequests, ddRequests)

	PrintBinlogFiles(binlogFiles)

	for _, file := range binlogFiles {
		_ = os.RemoveAll(file)
	}
}

func TestPrintIndexFile(t *testing.T) {
	indexBuildID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	version := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	collectionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	partitionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	segmentID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	fieldID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexName := funcutil.GenRandomStr()
	indexID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexParams := make(map[string]string)
	indexParams[common.IndexTypeKey] = "IVF_FLAT"
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
			Key:   "SLICE_META",
			Value: []byte(`"{"meta":[{"name":"IVF","slice_num":5,"total_len":20047555},{"name":"RAW_DATA","slice_num":20,"total_len":80025824}]}"`),
		},
	}

	codec := NewIndexFileBinlogCodec()

	serializedBlobs, err := codec.Serialize(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexParams, indexName, indexID, datas)
	assert.NoError(t, err)

	var binlogFiles []string
	for index, blob := range serializedBlobs {
		fileName := fmt.Sprintf("/tmp/index_blob_%d.binlog", index)
		binlogFiles = append(binlogFiles, fileName)
		fd, err := os.Create(fileName)
		assert.NoError(t, err)
		num, err := fd.Write(blob.GetValue())
		assert.NoError(t, err)
		assert.Equal(t, num, len(blob.GetValue()))
		err = fd.Close()
		assert.NoError(t, err)
	}

	err = PrintBinlogFiles(binlogFiles)
	assert.NoError(t, err)

	// remove tmp files
	for _, file := range binlogFiles {
		_ = os.RemoveAll(file)
	}
}

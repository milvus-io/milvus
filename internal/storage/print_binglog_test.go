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
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

func TestPrintBinlogFilesInt64(t *testing.T) {
	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)

	curTS := time.Now().UnixNano() / int64(time.Millisecond)

	e1, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
	e1.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

	e2, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetStartTimestamp(tsoutil.ComposeTS(curTS+30*60*1000, 0))
	e2.SetEndTimestamp(tsoutil.ComposeTS(curTS+40*60*1000, 0))

	w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
	w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

	_, err = w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.Nil(t, err)
	buf, err := w.GetBuffer()
	assert.Nil(t, err)

	fd, err := os.Create("/tmp/binlog_int64.db")
	assert.Nil(t, err)
	num, err := fd.Write(buf)
	assert.Nil(t, err)
	assert.Equal(t, num, len(buf))
	err = fd.Close()
	assert.Nil(t, err)

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
	insertCodec := NewInsertCodec(Schema)
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
	var binlogFiles []string
	for index, blob := range firstBlobs {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
		fileName := fmt.Sprintf("/tmp/firstblob_%d.db", index)
		binlogFiles = append(binlogFiles, fileName)
		fd, err := os.Create(fileName)
		assert.Nil(t, err)
		num, err := fd.Write(blob.GetValue())
		assert.Nil(t, err)
		assert.Equal(t, num, len(blob.GetValue()))
		err = fd.Close()
		assert.Nil(t, err)
	}
	secondBlobs, _, err := insertCodec.Serialize(1, 1, insertDataSecond)
	assert.Nil(t, err)
	for index, blob := range secondBlobs {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 99)
		fileName := fmt.Sprintf("/tmp/secondblob_%d.db", index)
		binlogFiles = append(binlogFiles, fileName)
		fd, err := os.Create(fileName)
		assert.Nil(t, err)
		num, err := fd.Write(blob.GetValue())
		assert.Nil(t, err)
		assert.Equal(t, num, len(blob.GetValue()))
		err = fd.Close()
		assert.Nil(t, err)
	}
	binlogFiles = append(binlogFiles, "test")

	PrintBinlogFiles(binlogFiles)
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
	createCollReq := internalpb.CreateCollectionRequest{
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
	assert.Nil(t, err)

	dropCollReq := internalpb.DropCollectionRequest{
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
	assert.Nil(t, err)

	createPartitionReq := internalpb.CreatePartitionRequest{
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
	assert.Nil(t, err)

	dropPartitionReq := internalpb.DropPartitionRequest{
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
	assert.Nil(t, err)
	ddRequests := []string{
		string(createCollString[:]),
		string(dropCollString[:]),
		string(createPartitionString[:]),
		string(dropPartitionString[:]),
	}
	eventTypeCodes := []EventTypeCode{
		CreateCollectionEventType,
		DropCollectionEventType,
		CreatePartitionEventType,
		DropPartitionEventType,
	}
	blobs, err := dataDefinitionCodec.Serialize(ts, ddRequests, eventTypeCodes)
	assert.Nil(t, err)
	var binlogFiles []string
	for index, blob := range blobs {
		blob.Key = fmt.Sprintf("1/data_definition/3/4/5/%d", 99)
		fileName := fmt.Sprintf("/tmp/ddblob_%d.db", index)
		binlogFiles = append(binlogFiles, fileName)
		fd, err := os.Create(fileName)
		assert.Nil(t, err)
		num, err := fd.Write(blob.GetValue())
		assert.Nil(t, err)
		assert.Equal(t, num, len(blob.GetValue()))
		err = fd.Close()
		assert.Nil(t, err)
	}
	resultTs, resultRequests, err := dataDefinitionCodec.Deserialize(blobs)
	assert.Nil(t, err)
	assert.Equal(t, resultTs, ts)
	assert.Equal(t, resultRequests, ddRequests)
	assert.Nil(t, dataDefinitionCodec.Close())

	PrintBinlogFiles(binlogFiles)
}

func TestCompressBinlogFilesInt64(t *testing.T) {
	t.Run("BinlogFilesInt64_case1", func(t *testing.T) {

		// Uncompressed

		w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_UNCOMPRESSED)

		curTS := time.Now().UnixNano() / int64(time.Millisecond)

		e1, err := w.NextInsertEventWriter()
		assert.Nil(t, err)
		err = e1.AddDataToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)
		err = e1.AddDataToPayload([]int32{4, 5, 6})
		assert.NotNil(t, err)
		err = e1.AddDataToPayload([]int64{4, 5, 6})
		assert.Nil(t, err)
		e1.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e1.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

		e2, err := w.NextInsertEventWriter()
		assert.Nil(t, err)
		err = e2.AddDataToPayload([]int64{7, 8, 9})
		assert.Nil(t, err)
		err = e2.AddDataToPayload([]bool{true, false, true})
		assert.NotNil(t, err)
		err = e2.AddDataToPayload([]int64{10, 11, 12})
		assert.Nil(t, err)
		e2.SetStartTimestamp(tsoutil.ComposeTS(curTS+30*60*1000, 0))
		e2.SetEndTimestamp(tsoutil.ComposeTS(curTS+40*60*1000, 0))

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err := w.GetBuffer()
		assert.Nil(t, err)

		fd, err := os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err := fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err := os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		uncompress_size := fi.Size()

		// Snappy

		w = NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_SNAPPY)

		curTS = time.Now().UnixNano() / int64(time.Millisecond)

		e1, err = w.NextInsertEventWriter()
		assert.Nil(t, err)
		err = e1.AddDataToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)
		err = e1.AddDataToPayload([]int32{4, 5, 6})
		assert.NotNil(t, err)
		err = e1.AddDataToPayload([]int64{4, 5, 6})
		assert.Nil(t, err)
		e1.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e1.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

		e2, err = w.NextInsertEventWriter()
		assert.Nil(t, err)
		err = e2.AddDataToPayload([]int64{7, 8, 9})
		assert.Nil(t, err)
		err = e2.AddDataToPayload([]bool{true, false, true})
		assert.NotNil(t, err)
		err = e2.AddDataToPayload([]int64{10, 11, 12})
		assert.Nil(t, err)
		e2.SetStartTimestamp(tsoutil.ComposeTS(curTS+30*60*1000, 0))
		e2.SetEndTimestamp(tsoutil.ComposeTS(curTS+40*60*1000, 0))

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err = w.GetBuffer()
		assert.Nil(t, err)

		fd, err = os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err = fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err = os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		compress_size := fi.Size()

		ratio, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(compress_size)/float64(uncompress_size)), 64)
		fmt.Println("[BinlogFilesInt64_case1] Snappy / Uncompressed = ", compress_size, "/", uncompress_size, " = ", ratio)

		// Gzip
		w = NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_GZIP)

		curTS = time.Now().UnixNano() / int64(time.Millisecond)

		e1, err = w.NextInsertEventWriter()
		assert.Nil(t, err)
		err = e1.AddDataToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)
		err = e1.AddDataToPayload([]int32{4, 5, 6})
		assert.NotNil(t, err)
		err = e1.AddDataToPayload([]int64{4, 5, 6})
		assert.Nil(t, err)
		e1.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e1.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

		e2, err = w.NextInsertEventWriter()
		assert.Nil(t, err)
		err = e2.AddDataToPayload([]int64{7, 8, 9})
		assert.Nil(t, err)
		err = e2.AddDataToPayload([]bool{true, false, true})
		assert.NotNil(t, err)
		err = e2.AddDataToPayload([]int64{10, 11, 12})
		assert.Nil(t, err)
		e2.SetStartTimestamp(tsoutil.ComposeTS(curTS+30*60*1000, 0))
		e2.SetEndTimestamp(tsoutil.ComposeTS(curTS+40*60*1000, 0))

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err = w.GetBuffer()
		assert.Nil(t, err)

		fd, err = os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err = fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err = os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		compress_size = fi.Size()

		ratio, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(compress_size)/float64(uncompress_size)), 64)
		fmt.Println("[BinlogFilesInt64_case1] Gzip / Uncompressed = ", compress_size, "/", uncompress_size, " = ", ratio)
	})
	t.Run("BinlogFilesInt64_case2", func(t *testing.T) {

		// Uncompressed

		w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_UNCOMPRESSED)

		curTS := time.Now().UnixNano() / int64(time.Millisecond)

		for i := 0; i < 50000; i++ {
			e, err := w.NextInsertEventWriter()
			assert.Nil(t, err)
			err = e.AddDataToPayload([]int64{int64(rand.Int()), int64(rand.Int()), int64(rand.Int())})
			assert.Nil(t, err)
			err = e.AddDataToPayload([]int32{int32(rand.Int()), int32(rand.Int()), int32(rand.Int())})
			assert.NotNil(t, err)
			err = e.AddDataToPayload([]int64{int64(rand.Int()), int64(rand.Int()), int64(rand.Int())})
			assert.Nil(t, err)
			e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
			e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))
		}

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err := w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err := w.GetBuffer()
		assert.Nil(t, err)

		fd, err := os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err := fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err := os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		uncompress_size := fi.Size()

		// Snappy

		w = NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_SNAPPY)

		curTS = time.Now().UnixNano() / int64(time.Millisecond)

		for i := 0; i < 50000; i++ {
			e, err := w.NextInsertEventWriter()
			assert.Nil(t, err)
			err = e.AddDataToPayload([]int64{int64(rand.Int()), int64(rand.Int()), int64(rand.Int())})
			assert.Nil(t, err)
			err = e.AddDataToPayload([]int32{int32(rand.Int()), int32(rand.Int()), int32(rand.Int())})
			assert.NotNil(t, err)
			err = e.AddDataToPayload([]int64{int64(rand.Int()), int64(rand.Int()), int64(rand.Int())})
			assert.Nil(t, err)
			e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
			e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))
		}

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err = w.GetBuffer()
		assert.Nil(t, err)

		fd, err = os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err = fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err = os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		compress_size := fi.Size()

		ratio, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(compress_size)/float64(uncompress_size)), 64)
		fmt.Println("[BinlogFilesInt64_case2] Snappy / Uncompressed = ", compress_size, "/", uncompress_size, " = ", ratio)

		// Gzip
		w = NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_GZIP)

		curTS = time.Now().UnixNano() / int64(time.Millisecond)

		for i := 0; i < 50000; i++ {
			e, err := w.NextInsertEventWriter()
			assert.Nil(t, err)
			err = e.AddDataToPayload([]int64{int64(rand.Int()), int64(rand.Int()), int64(rand.Int())})
			assert.Nil(t, err)
			err = e.AddDataToPayload([]int32{int32(rand.Int()), int32(rand.Int()), int32(rand.Int())})
			assert.NotNil(t, err)
			err = e.AddDataToPayload([]int64{int64(rand.Int()), int64(rand.Int()), int64(rand.Int())})
			assert.Nil(t, err)
			e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
			e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))
		}

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err = w.GetBuffer()
		assert.Nil(t, err)

		fd, err = os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err = fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err = os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		compress_size = fi.Size()

		ratio, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(compress_size)/float64(uncompress_size)), 64)
		fmt.Println("[BinlogFilesInt64_case2] Gzip / Uncompressed = ", compress_size, "/", uncompress_size, " = ", ratio)
	})
	t.Run("BinlogFilesInt64_case3", func(t *testing.T) {

		rand.Seed(time.Now().Unix())

		// Uncompressed

		w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_UNCOMPRESSED)

		curTS := time.Now().UnixNano() / int64(time.Millisecond)

		e, err := w.NextInsertEventWriter()
		assert.Nil(t, err)

		for i := 0; i < 50000; i++ {
			err = e.AddDataToPayload([]int64{int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1))})
			assert.Nil(t, err)
			err = e.AddDataToPayload([]int32{int32(rand.Intn(i + 1)), int32(rand.Intn(i + 1)), int32(rand.Intn(i + 1))})
			assert.NotNil(t, err)
			err = e.AddDataToPayload([]int64{int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1))})
			assert.Nil(t, err)
		}
		e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err := w.GetBuffer()
		assert.Nil(t, err)

		fd, err := os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err := fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err := os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		uncompress_size := fi.Size()

		// Snappy

		w = NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_SNAPPY)

		curTS = time.Now().UnixNano() / int64(time.Millisecond)

		e, err = w.NextInsertEventWriter()
		assert.Nil(t, err)

		for i := 0; i < 50000; i++ {
			err = e.AddDataToPayload([]int64{int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1))})
			assert.Nil(t, err)
			err = e.AddDataToPayload([]int32{int32(rand.Intn(i + 1)), int32(rand.Intn(i + 1)), int32(rand.Intn(i + 1))})
			assert.NotNil(t, err)
			err = e.AddDataToPayload([]int64{int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1))})
			assert.Nil(t, err)
		}
		e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err = w.GetBuffer()
		assert.Nil(t, err)

		fd, err = os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err = fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err = os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		compress_size := fi.Size()

		ratio, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(compress_size)/float64(uncompress_size)), 64)
		fmt.Println("[BinlogFilesInt64_case3] Snappy / Uncompressed = ", compress_size, "/", uncompress_size, " = ", ratio)

		// Gzip
		w = NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_GZIP)

		curTS = time.Now().UnixNano() / int64(time.Millisecond)

		e, err = w.NextInsertEventWriter()
		assert.Nil(t, err)

		for i := 0; i < 50000; i++ {
			err = e.AddDataToPayload([]int64{int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1))})
			assert.Nil(t, err)
			err = e.AddDataToPayload([]int32{int32(rand.Intn(i + 1)), int32(rand.Intn(i + 1)), int32(rand.Intn(i + 1))})
			assert.NotNil(t, err)
			err = e.AddDataToPayload([]int64{int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1)), int64(rand.Intn(i + 1))})
			assert.Nil(t, err)
		}
		e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err = w.GetBuffer()
		assert.Nil(t, err)

		fd, err = os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err = fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err = os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		compress_size = fi.Size()

		ratio, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(compress_size)/float64(uncompress_size)), 64)
		fmt.Println("[BinlogFilesInt64_case3] Gzip / Uncompressed = ", compress_size, "/", uncompress_size, " = ", ratio)
	})
	t.Run("BinlogFilesInt64_case4", func(t *testing.T) {

		rand.Seed(time.Now().Unix())

		// Uncompressed

		w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_UNCOMPRESSED)

		curTS := time.Now().UnixNano() / int64(time.Millisecond)

		e, err := w.NextInsertEventWriter()
		assert.Nil(t, err)

		var payload []int64

		for i := 0; i < 50000; i++ {
			payload = append(payload, int64(rand.Intn(i+1)))
		}

		err = e.AddDataToPayload(payload)
		assert.Nil(t, err)

		e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err := w.GetBuffer()
		assert.Nil(t, err)

		fd, err := os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err := fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err := os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		uncompress_size := fi.Size()

		// Snappy

		w = NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_SNAPPY)

		curTS = time.Now().UnixNano() / int64(time.Millisecond)

		e, err = w.NextInsertEventWriter()
		assert.Nil(t, err)

		payload = payload[0:0]
		for i := 0; i < 50000; i++ {
			payload = append(payload, int64(rand.Intn(i+1)))
		}
		err = e.AddDataToPayload(payload)
		assert.Nil(t, err)
		e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err = w.GetBuffer()
		assert.Nil(t, err)

		fd, err = os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err = fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err = os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		compress_size := fi.Size()

		ratio, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(compress_size)/float64(uncompress_size)), 64)
		fmt.Println("[BinlogFilesInt64_case4] Snappy / Uncompressed = ", compress_size, "/", uncompress_size, " = ", ratio)

		// Gzip
		w = NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
		w.setCompressType(CompressType_GZIP)

		curTS = time.Now().UnixNano() / int64(time.Millisecond)

		e, err = w.NextInsertEventWriter()
		assert.Nil(t, err)

		payload = payload[0:0]
		for i := 0; i < 50000; i++ {
			payload = append(payload, int64(rand.Intn(i+1)))
		}
		err = e.AddDataToPayload(payload)
		assert.Nil(t, err)

		e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

		w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
		w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

		_, err = w.GetBuffer()
		assert.NotNil(t, err)
		err = w.Close()
		assert.Nil(t, err)
		buf, err = w.GetBuffer()
		assert.Nil(t, err)

		fd, err = os.Create("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		num, err = fd.Write(buf)
		assert.Nil(t, err)
		assert.Equal(t, num, len(buf))
		err = fd.Close()
		assert.Nil(t, err)

		fi, err = os.Stat("/tmp/binlog_int64.db")
		assert.Nil(t, err)
		compress_size = fi.Size()

		ratio, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(compress_size)/float64(uncompress_size)), 64)
		fmt.Println("[BinlogFilesInt64_case4] Gzip / Uncompressed = ", compress_size, "/", uncompress_size, " = ", ratio)
	})

	t.Run("BinlogFiles", func(t *testing.T) {

		var data_int64 []int64
		var data_int64_ts []int64
		var data_int8 []int8
		var data_int16 []int16
		var data_int32 []int32
		var data_bool []bool
		var data_float32 []float32
		var data_float64 []float64
		var data_string []string

		for i := 0; i < 10000; i++ {
			data_int64 = append(data_int64, int64(i))
			data_int64_ts = append(data_int64_ts, int64(1))
			data_bool = append(data_bool, true)
			data_int8 = append(data_int8, int8(i))
			data_int16 = append(data_int16, int16(i))
			data_int32 = append(data_int32, int32(i))
			data_float32 = append(data_float32, float32(i))
			data_float64 = append(data_float64, float64(i))
			data_string = append(data_string, strconv.FormatInt(int64(i), 10))
		}

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
		insertCodec := NewInsertCodec(Schema)
		insertDataFirst := &InsertData{
			Data: map[int64]FieldData{
				0: &Int64FieldData{
					NumRows: 10000,
					Data:    data_int64,
				},
				1: &Int64FieldData{
					NumRows: 10000,
					Data:    data_int64_ts,
				},
				100: &BoolFieldData{
					NumRows: 10000,
					Data:    data_bool,
				},
				101: &Int8FieldData{
					NumRows: 10000,
					Data:    data_int8,
				},
				102: &Int16FieldData{
					NumRows: 10000,
					Data:    data_int16,
				},
				103: &Int32FieldData{
					NumRows: 10000,
					Data:    data_int32,
				},
				104: &Int64FieldData{
					NumRows: 10000,
					Data:    data_int64,
				},
				105: &FloatFieldData{
					NumRows: 10000,
					Data:    data_float32,
				},
				106: &DoubleFieldData{
					NumRows: 10000,
					Data:    data_float64,
				},
				107: &StringFieldData{
					NumRows: 10000,
					Data:    data_string,
				},
				108: &BinaryVectorFieldData{
					NumRows: 10000,
					Data:    []byte{0, 255},
					Dim:     8,
				},
				109: &FloatVectorFieldData{
					NumRows: 10000,
					Data:    data_float32,
					Dim:     1,
				},
			},
		}

		insertDataSecond := &InsertData{
			Data: map[int64]FieldData{
				0: &Int64FieldData{
					NumRows: 10000,
					Data:    data_int64,
				},
				1: &Int64FieldData{
					NumRows: 10000,
					Data:    data_int64_ts,
				},
				100: &BoolFieldData{
					NumRows: 10000,
					Data:    data_bool,
				},
				101: &Int8FieldData{
					NumRows: 10000,
					Data:    data_int8,
				},
				102: &Int16FieldData{
					NumRows: 10000,
					Data:    data_int16,
				},
				103: &Int32FieldData{
					NumRows: 10000,
					Data:    data_int32,
				},
				104: &Int64FieldData{
					NumRows: 10000,
					Data:    data_int64,
				},
				105: &FloatFieldData{
					NumRows: 10000,
					Data:    data_float32,
				},
				106: &DoubleFieldData{
					NumRows: 10000,
					Data:    data_float64,
				},
				107: &StringFieldData{
					NumRows: 10000,
					Data:    data_string,
				},
				108: &BinaryVectorFieldData{
					NumRows: 10000,
					Data:    []byte{0, 255},
					Dim:     8,
				},
				109: &FloatVectorFieldData{
					NumRows: 10000,
					Data:    data_float32,
					Dim:     1,
				},
			},
		}

		// Uncompressed
		insertCodec.setCompressType(CompressType_UNCOMPRESSED)
		firstBlobs, _, err := insertCodec.Serialize(1, 1, insertDataFirst)
		assert.Nil(t, err)
		var uncompressedSize []int64
		for index, blob := range firstBlobs {
			blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
			fileName := fmt.Sprintf("/tmp/firstblob_%d.db", index)
			fd, err := os.Create(fileName)
			assert.Nil(t, err)
			num, err := fd.Write(blob.GetValue())
			assert.Nil(t, err)
			assert.Equal(t, num, len(blob.GetValue()))
			err = fd.Close()
			assert.Nil(t, err)
			fi, err := os.Stat(fileName)
			assert.Nil(t, err)
			uncompressedSize = append(uncompressedSize, fi.Size())
		}
		secondBlobs, _, err := insertCodec.Serialize(1, 1, insertDataSecond)
		assert.Nil(t, err)
		for index, blob := range secondBlobs {
			blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 99)
			fileName := fmt.Sprintf("/tmp/secondblob_%d.db", index)
			fd, err := os.Create(fileName)
			assert.Nil(t, err)
			num, err := fd.Write(blob.GetValue())
			assert.Nil(t, err)
			assert.Equal(t, num, len(blob.GetValue()))
			err = fd.Close()
			assert.Nil(t, err)
			fi, err := os.Stat(fileName)
			assert.Nil(t, err)
			uncompressedSize = append(uncompressedSize, fi.Size())
		}

		// Snappy
		insertCodec.setCompressType(CompressType_SNAPPY)
		firstBlobs, _, err = insertCodec.Serialize(1, 1, insertDataFirst)
		assert.Nil(t, err)
		var snappySize []int64
		for index, blob := range firstBlobs {
			blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
			fileName := fmt.Sprintf("/tmp/firstblob_%d.db", index)
			fd, err := os.Create(fileName)
			assert.Nil(t, err)
			num, err := fd.Write(blob.GetValue())
			assert.Nil(t, err)
			assert.Equal(t, num, len(blob.GetValue()))
			err = fd.Close()
			assert.Nil(t, err)
			fi, err := os.Stat(fileName)
			assert.Nil(t, err)
			snappySize = append(snappySize, fi.Size())
		}
		secondBlobs, _, err = insertCodec.Serialize(1, 1, insertDataSecond)
		assert.Nil(t, err)
		for index, blob := range secondBlobs {
			blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 99)
			fileName := fmt.Sprintf("/tmp/secondblob_%d.db", index)
			fd, err := os.Create(fileName)
			assert.Nil(t, err)
			num, err := fd.Write(blob.GetValue())
			assert.Nil(t, err)
			assert.Equal(t, num, len(blob.GetValue()))
			err = fd.Close()
			assert.Nil(t, err)
			fi, err := os.Stat(fileName)
			assert.Nil(t, err)
			snappySize = append(snappySize, fi.Size())
		}
		for index, value := range snappySize {
			ratio, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(snappySize[index])/float64(uncompressedSize[index])), 64)
			fmt.Println("[BinlogFiles]", index, " Snappy / Uncompressed = ", value, "/", uncompressedSize[index], " = ", ratio)
		}

		// Gzip
		insertCodec.setCompressType(CompressType_GZIP)
		firstBlobs, _, err = insertCodec.Serialize(1, 1, insertDataFirst)
		assert.Nil(t, err)
		var gzipSize []int64
		for index, blob := range firstBlobs {
			blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
			fileName := fmt.Sprintf("/tmp/firstblob_%d.db", index)
			fd, err := os.Create(fileName)
			assert.Nil(t, err)
			num, err := fd.Write(blob.GetValue())
			assert.Nil(t, err)
			assert.Equal(t, num, len(blob.GetValue()))
			err = fd.Close()
			assert.Nil(t, err)
			fi, err := os.Stat(fileName)
			assert.Nil(t, err)
			gzipSize = append(gzipSize, fi.Size())
		}
		secondBlobs, _, err = insertCodec.Serialize(1, 1, insertDataSecond)
		assert.Nil(t, err)
		for index, blob := range secondBlobs {
			blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 99)
			fileName := fmt.Sprintf("/tmp/secondblob_%d.db", index)
			fd, err := os.Create(fileName)
			assert.Nil(t, err)
			num, err := fd.Write(blob.GetValue())
			assert.Nil(t, err)
			assert.Equal(t, num, len(blob.GetValue()))
			err = fd.Close()
			assert.Nil(t, err)
			fi, err := os.Stat(fileName)
			assert.Nil(t, err)
			gzipSize = append(gzipSize, fi.Size())
		}
		for index, value := range gzipSize {
			ratio, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(gzipSize[index])/float64(uncompressedSize[index])), 64)
			fmt.Println("[BinlogFiles]", index, " Gzip / Uncompressed = ", value, "/", uncompressedSize[index], " = ", ratio)
		}
	})

	t.Run("DDLFiles", func(t *testing.T) {
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
		createCollReq := internalpb.CreateCollectionRequest{
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
		assert.Nil(t, err)

		dropCollReq := internalpb.DropCollectionRequest{
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
		assert.Nil(t, err)

		createPartitionReq := internalpb.CreatePartitionRequest{
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
		assert.Nil(t, err)

		dropPartitionReq := internalpb.DropPartitionRequest{
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
		assert.Nil(t, err)
		ddRequests := []string{
			string(createCollString[:]),
			string(dropCollString[:]),
			string(createPartitionString[:]),
			string(dropPartitionString[:]),
		}
		eventTypeCodes := []EventTypeCode{
			CreateCollectionEventType,
			DropCollectionEventType,
			CreatePartitionEventType,
			DropPartitionEventType,
		}

		// Uncompressed
		dataDefinitionCodec.setCompressType(CompressType_UNCOMPRESSED)
		blobs, err := dataDefinitionCodec.Serialize(ts, ddRequests, eventTypeCodes)
		assert.Nil(t, err)
		var uncompressedSize []int64
		for index, blob := range blobs {
			blob.Key = fmt.Sprintf("1/data_definition/3/4/5/%d", 99)
			fileName := fmt.Sprintf("/tmp/ddblob_%d.db", index)
			fd, err := os.Create(fileName)
			assert.Nil(t, err)
			num, err := fd.Write(blob.GetValue())
			assert.Nil(t, err)
			assert.Equal(t, num, len(blob.GetValue()))
			err = fd.Close()
			assert.Nil(t, err)
			fi, err := os.Stat(fileName)
			assert.Nil(t, err)
			uncompressedSize = append(uncompressedSize, fi.Size())
		}
		resultTs, resultRequests, err := dataDefinitionCodec.Deserialize(blobs)
		assert.Nil(t, err)
		assert.Equal(t, resultTs, ts)
		assert.Equal(t, resultRequests, ddRequests)
		assert.Nil(t, dataDefinitionCodec.Close())

		// Snappy
		dataDefinitionCodec.setCompressType(CompressType_SNAPPY)
		blobs, err = dataDefinitionCodec.Serialize(ts, ddRequests, eventTypeCodes)
		assert.Nil(t, err)
		var snappySize []int64
		for index, blob := range blobs {
			blob.Key = fmt.Sprintf("1/data_definition/3/4/5/%d", 99)
			fileName := fmt.Sprintf("/tmp/ddblob_%d.db", index)
			fd, err := os.Create(fileName)
			assert.Nil(t, err)
			num, err := fd.Write(blob.GetValue())
			assert.Nil(t, err)
			assert.Equal(t, num, len(blob.GetValue()))
			err = fd.Close()
			assert.Nil(t, err)
			fi, err := os.Stat(fileName)
			assert.Nil(t, err)
			snappySize = append(snappySize, fi.Size())
		}
		resultTs, resultRequests, err = dataDefinitionCodec.Deserialize(blobs)
		assert.Nil(t, err)
		assert.Equal(t, resultTs, ts)
		assert.Equal(t, resultRequests, ddRequests)
		assert.Nil(t, dataDefinitionCodec.Close())
		for index, value := range snappySize {
			ratio, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(snappySize[index])/float64(uncompressedSize[index])), 64)
			fmt.Println("[DDLFiles]", index, " Snappy / Uncompressed = ", value, "/", uncompressedSize[index], " = ", ratio)
		}

		// Gzip
		dataDefinitionCodec.setCompressType(CompressType_GZIP)
		blobs, err = dataDefinitionCodec.Serialize(ts, ddRequests, eventTypeCodes)
		assert.Nil(t, err)
		var gzipSize []int64
		for index, blob := range blobs {
			blob.Key = fmt.Sprintf("1/data_definition/3/4/5/%d", 99)
			fileName := fmt.Sprintf("/tmp/ddblob_%d.db", index)
			fd, err := os.Create(fileName)
			assert.Nil(t, err)
			num, err := fd.Write(blob.GetValue())
			assert.Nil(t, err)
			assert.Equal(t, num, len(blob.GetValue()))
			err = fd.Close()
			assert.Nil(t, err)
			fi, err := os.Stat(fileName)
			assert.Nil(t, err)
			gzipSize = append(gzipSize, fi.Size())
		}
		resultTs, resultRequests, err = dataDefinitionCodec.Deserialize(blobs)
		assert.Nil(t, err)
		assert.Equal(t, resultTs, ts)
		assert.Equal(t, resultRequests, ddRequests)
		assert.Nil(t, dataDefinitionCodec.Close())
		for index, value := range gzipSize {
			ratio, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(gzipSize[index])/float64(uncompressedSize[index])), 64)
			fmt.Println("[DDLFiles]", index, " Gzip / Uncompressed = ", value, "/", uncompressedSize[index], " = ", ratio)
		}
	})
}

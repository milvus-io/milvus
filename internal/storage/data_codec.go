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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	// Ts is blob key "ts"
	Ts = "ts"
	// DDL is blob key "ddl"
	DDL = "ddl"
	// IndexParamsKey is blob key "indexParams"
	IndexParamsKey = "indexParams"
)

// when the blob of index file is too large, we can split blob into several rows,
// fortunately, the blob has no other semantics which differs from other binlog type,
// we then assemble these several rows into a whole blob when deserialize index binlog.
// num rows = math.Ceil(len(blob) / maxLengthPerRowOfIndexFile)
// There is only a string row in the past version index file which is a subset case of splitting into several rows.
// So splitting index file won't introduce incompatibility with past version.
const maxLengthPerRowOfIndexFile = 4 * 1024 * 1024

type (
	UniqueID  = typeutil.UniqueID
	FieldID   = typeutil.UniqueID
	Timestamp = typeutil.Timestamp
)

const InvalidUniqueID = UniqueID(-1)

type Blob struct {
	Key   string
	Value []byte
}

type BlobList []*Blob

func (s BlobList) Len() int {
	return len(s)
}

func (s BlobList) Less(i, j int) bool {
	leftValues := strings.Split(s[i].Key, "/")
	rightValues := strings.Split(s[j].Key, "/")
	left, _ := strconv.ParseInt(leftValues[len(leftValues)-1], 0, 10)
	right, _ := strconv.ParseInt(rightValues[len(rightValues)-1], 0, 10)
	return left < right
}

func (s BlobList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (b Blob) GetKey() string {
	return b.Key
}

func (b Blob) GetValue() []byte {
	return b.Value
}

type FieldData interface {
}

type BoolFieldData struct {
	NumRows []int64
	Data    []bool
}
type Int8FieldData struct {
	NumRows []int64
	Data    []int8
}
type Int16FieldData struct {
	NumRows []int64
	Data    []int16
}
type Int32FieldData struct {
	NumRows []int64
	Data    []int32
}
type Int64FieldData struct {
	NumRows []int64
	Data    []int64
}
type FloatFieldData struct {
	NumRows []int64
	Data    []float32
}
type DoubleFieldData struct {
	NumRows []int64
	Data    []float64
}
type StringFieldData struct {
	NumRows []int64
	Data    []string
}
type BinaryVectorFieldData struct {
	NumRows []int64
	Data    []byte
	Dim     int
}
type FloatVectorFieldData struct {
	NumRows []int64
	Data    []float32
	Dim     int
}

// why not binary.Size(data) directly? binary.Size(data) return -1
// binary.Size returns how many bytes Write would generate to encode the value v, which
// must be a fixed-size value or a slice of fixed-size values, or a pointer to such data.
// If v is neither of these, binary.Size returns -1.

func (data *BoolFieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data)
}

func (data *Int8FieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data)
}

func (data *Int16FieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data)
}

func (data *Int32FieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data)
}

func (data *Int64FieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data)
}

func (data *FloatFieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data)
}

func (data *DoubleFieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data)
}

func (data *StringFieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data)
}

func (data *BinaryVectorFieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data) + binary.Size(data.Dim)
}

func (data *FloatVectorFieldData) GetMemorySize() int {
	return binary.Size(data.NumRows) + binary.Size(data.Data) + binary.Size(data.Dim)
}

// system filed id:
// 0: unique row id
// 1: timestamp
// 100: first user field id
// 101: second user field id
// 102: ...

// TODO: fill it
// info for each blob
type BlobInfo struct {
	Length int
}

// example row_schema: {float_field, int_field, float_vector_field, string_field}
// Data {<0, row_id>, <1, timestamp>, <100, float_field>, <101, int_field>, <102, float_vector_field>, <103, string_field>}
type InsertData struct {
	Data  map[FieldID]FieldData // field id to field data
	Infos []BlobInfo
}

// InsertCodec serializes and deserializes the insert data
// Blob key example:
// ${tenant}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/${log_idx}
type InsertCodec struct {
	Schema          *etcdpb.CollectionMeta
	readerCloseFunc []func() error
}

func NewInsertCodec(schema *etcdpb.CollectionMeta) *InsertCodec {
	return &InsertCodec{Schema: schema}
}

// Serialize transfer insert data to blob. It will sort insert data by timestamp.
// From schema, it get all fields.
// For each field, it will create a binlog writer, and write a event to the binlog.
// It returns binlog buffer in the end.
func (insertCodec *InsertCodec) Serialize(partitionID UniqueID, segmentID UniqueID, data *InsertData) ([]*Blob, []*Blob, error) {
	var blobs []*Blob
	var statsBlobs []*Blob
	var writer *InsertBinlogWriter
	timeFieldData, ok := data.Data[rootcoord.TimeStampField]
	if !ok {
		return nil, nil, fmt.Errorf("data doesn't contains timestamp field")
	}
	ts := timeFieldData.(*Int64FieldData).Data
	startTs := ts[0]
	endTs := ts[len(ts)-1]

	dataSorter := &DataSorter{
		InsertCodec: insertCodec,
		InsertData:  data,
	}
	sort.Sort(dataSorter)

	for _, field := range insertCodec.Schema.Schema.Fields {
		singleData := data.Data[field.FieldID]

		// encode fields
		writer = NewInsertBinlogWriter(field.DataType, insertCodec.Schema.ID, partitionID, segmentID, field.FieldID)
		eventWriter, err := writer.NextInsertEventWriter()
		if err != nil {
			return nil, nil, err
		}

		eventWriter.SetEventTimestamp(typeutil.Timestamp(startTs), typeutil.Timestamp(endTs))
		switch field.DataType {
		case schemapb.DataType_Bool:
			err = eventWriter.AddBoolToPayload(singleData.(*BoolFieldData).Data)
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*BoolFieldData).GetMemorySize()))
		case schemapb.DataType_Int8:
			err = eventWriter.AddInt8ToPayload(singleData.(*Int8FieldData).Data)
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*Int8FieldData).GetMemorySize()))
		case schemapb.DataType_Int16:
			err = eventWriter.AddInt16ToPayload(singleData.(*Int16FieldData).Data)
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*Int16FieldData).GetMemorySize()))
		case schemapb.DataType_Int32:
			err = eventWriter.AddInt32ToPayload(singleData.(*Int32FieldData).Data)
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*Int32FieldData).GetMemorySize()))
		case schemapb.DataType_Int64:
			err = eventWriter.AddInt64ToPayload(singleData.(*Int64FieldData).Data)
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*Int64FieldData).GetMemorySize()))
		case schemapb.DataType_Float:
			err = eventWriter.AddFloatToPayload(singleData.(*FloatFieldData).Data)
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*FloatFieldData).GetMemorySize()))
		case schemapb.DataType_Double:
			err = eventWriter.AddDoubleToPayload(singleData.(*DoubleFieldData).Data)
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*DoubleFieldData).GetMemorySize()))
		case schemapb.DataType_String:
			for _, singleString := range singleData.(*StringFieldData).Data {
				err = eventWriter.AddOneStringToPayload(singleString)
				if err != nil {
					return nil, nil, err
				}
			}
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*StringFieldData).GetMemorySize()))
		case schemapb.DataType_BinaryVector:
			err = eventWriter.AddBinaryVectorToPayload(singleData.(*BinaryVectorFieldData).Data, singleData.(*BinaryVectorFieldData).Dim)
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*BinaryVectorFieldData).GetMemorySize()))
		case schemapb.DataType_FloatVector:
			err = eventWriter.AddFloatVectorToPayload(singleData.(*FloatVectorFieldData).Data, singleData.(*FloatVectorFieldData).Dim)
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", singleData.(*FloatVectorFieldData).GetMemorySize()))
		default:
			return nil, nil, fmt.Errorf("undefined data type %d", field.DataType)
		}
		if err != nil {
			return nil, nil, err
		}
		writer.SetEventTimeStamp(typeutil.Timestamp(startTs), typeutil.Timestamp(endTs))

		err = writer.Close()
		if err != nil {
			return nil, nil, err
		}

		buffer, err := writer.GetBuffer()
		if err != nil {
			return nil, nil, err
		}
		blobKey := fmt.Sprintf("%d", field.FieldID)
		blobs = append(blobs, &Blob{
			Key:   blobKey,
			Value: buffer,
		})

		// stats fields
		statsWriter := &StatsWriter{}
		switch field.DataType {
		case schemapb.DataType_Int64:
			err = statsWriter.StatsInt64(field.FieldID, singleData.(*Int64FieldData).Data)
		}
		if err != nil {
			return nil, nil, err
		}
		statsBuffer := statsWriter.GetBuffer()
		statsBlobs = append(statsBlobs, &Blob{
			Key:   blobKey,
			Value: statsBuffer,
		})
	}

	return blobs, statsBlobs, nil
}

func (insertCodec *InsertCodec) DeserializeAll(blobs []*Blob) (
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	data *InsertData,
	err error,
) {
	if len(blobs) == 0 {
		return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, fmt.Errorf("blobs is empty")
	}
	readerClose := func(reader *BinlogReader) func() error {
		return func() error { return reader.Close() }
	}

	var blobList BlobList = blobs
	sort.Sort(blobList)

	var cID UniqueID
	var pID UniqueID
	var sID UniqueID
	resultData := &InsertData{}
	resultData.Data = make(map[FieldID]FieldData)
	for _, blob := range blobList {
		binlogReader, err := NewBinlogReader(blob.Value)
		if err != nil {
			return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
		}

		// read partitionID and SegmentID
		cID, pID, sID = binlogReader.CollectionID, binlogReader.PartitionID, binlogReader.SegmentID

		dataType := binlogReader.PayloadDataType
		fieldID := binlogReader.FieldID
		totalLength := 0
		for {
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
			}
			if eventReader == nil {
				break
			}
			switch dataType {
			case schemapb.DataType_Bool:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &BoolFieldData{}
				}
				boolFieldData := resultData.Data[fieldID].(*BoolFieldData)
				singleData, err := eventReader.GetBoolFromPayload()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				boolFieldData.Data = append(boolFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				boolFieldData.NumRows = append(boolFieldData.NumRows, int64(length))
				resultData.Data[fieldID] = boolFieldData
			case schemapb.DataType_Int8:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &Int8FieldData{}
				}
				int8FieldData := resultData.Data[fieldID].(*Int8FieldData)
				singleData, err := eventReader.GetInt8FromPayload()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				int8FieldData.Data = append(int8FieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				int8FieldData.NumRows = append(int8FieldData.NumRows, int64(length))
				resultData.Data[fieldID] = int8FieldData
			case schemapb.DataType_Int16:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &Int16FieldData{}
				}
				int16FieldData := resultData.Data[fieldID].(*Int16FieldData)
				singleData, err := eventReader.GetInt16FromPayload()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				int16FieldData.Data = append(int16FieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				int16FieldData.NumRows = append(int16FieldData.NumRows, int64(length))
				resultData.Data[fieldID] = int16FieldData
			case schemapb.DataType_Int32:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &Int32FieldData{}
				}
				int32FieldData := resultData.Data[fieldID].(*Int32FieldData)
				singleData, err := eventReader.GetInt32FromPayload()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				int32FieldData.Data = append(int32FieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				int32FieldData.NumRows = append(int32FieldData.NumRows, int64(length))
				resultData.Data[fieldID] = int32FieldData
			case schemapb.DataType_Int64:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &Int64FieldData{}
				}
				int64FieldData := resultData.Data[fieldID].(*Int64FieldData)
				singleData, err := eventReader.GetInt64FromPayload()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				int64FieldData.Data = append(int64FieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				int64FieldData.NumRows = append(int64FieldData.NumRows, int64(length))
				resultData.Data[fieldID] = int64FieldData
			case schemapb.DataType_Float:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &FloatFieldData{}
				}
				floatFieldData := resultData.Data[fieldID].(*FloatFieldData)
				singleData, err := eventReader.GetFloatFromPayload()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				floatFieldData.Data = append(floatFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				floatFieldData.NumRows = append(floatFieldData.NumRows, int64(length))
				resultData.Data[fieldID] = floatFieldData
			case schemapb.DataType_Double:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &DoubleFieldData{}
				}
				doubleFieldData := resultData.Data[fieldID].(*DoubleFieldData)
				singleData, err := eventReader.GetDoubleFromPayload()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				doubleFieldData.Data = append(doubleFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				doubleFieldData.NumRows = append(doubleFieldData.NumRows, int64(length))
				resultData.Data[fieldID] = doubleFieldData
			case schemapb.DataType_String:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &StringFieldData{}
				}
				stringFieldData := resultData.Data[fieldID].(*StringFieldData)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				stringFieldData.NumRows = append(stringFieldData.NumRows, int64(length))
				for i := 0; i < length; i++ {
					singleString, err := eventReader.GetOneStringFromPayload(i)
					if err != nil {
						return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
					}
					stringFieldData.Data = append(stringFieldData.Data, singleString)
				}
				resultData.Data[fieldID] = stringFieldData
			case schemapb.DataType_BinaryVector:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &BinaryVectorFieldData{}
				}
				binaryVectorFieldData := resultData.Data[fieldID].(*BinaryVectorFieldData)
				var singleData []byte
				singleData, binaryVectorFieldData.Dim, err = eventReader.GetBinaryVectorFromPayload()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				binaryVectorFieldData.Data = append(binaryVectorFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				binaryVectorFieldData.NumRows = append(binaryVectorFieldData.NumRows, int64(length))
				resultData.Data[fieldID] = binaryVectorFieldData
			case schemapb.DataType_FloatVector:
				if resultData.Data[fieldID] == nil {
					resultData.Data[fieldID] = &FloatVectorFieldData{}
				}
				floatVectorFieldData := resultData.Data[fieldID].(*FloatVectorFieldData)
				var singleData []float32
				singleData, floatVectorFieldData.Dim, err = eventReader.GetFloatVectorFromPayload()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				floatVectorFieldData.Data = append(floatVectorFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				floatVectorFieldData.NumRows = append(floatVectorFieldData.NumRows, int64(length))
				resultData.Data[fieldID] = floatVectorFieldData
			default:
				return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, fmt.Errorf("undefined data type %d", dataType)
			}
		}
		if fieldID == rootcoord.TimeStampField {
			blobInfo := BlobInfo{
				Length: totalLength,
			}
			resultData.Infos = append(resultData.Infos, blobInfo)
		}
		insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
	}

	return cID, pID, sID, resultData, nil
}

// Deserialize transfer blob back to insert data.
// From schema, it get all fields.
// For each field, it will create a binlog reader, and read all event to the buffer.
// It returns origin @InsertData in the end.
func (insertCodec *InsertCodec) Deserialize(blobs []*Blob) (partitionID UniqueID, segmentID UniqueID, data *InsertData, err error) {
	_, partitionID, segmentID, data, err = insertCodec.DeserializeAll(blobs)
	return partitionID, segmentID, data, err
}

func (insertCodec *InsertCodec) Close() error {
	for _, closeFunc := range insertCodec.readerCloseFunc {
		err := closeFunc()
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteData saves each entity delete message represented as <primarykey,timestamp> map.
// timestamp represents the time when this instance was deleted
type DeleteData struct {
	Data map[string]int64 // primary key to timestamp
}

// DeleteCodec serializes and deserializes the delete data
type DeleteCodec struct {
	Schema          *etcdpb.CollectionMeta
	readerCloseFunc []func() error
}

// NewDeleteCodec returns a DeleteCodec
func NewDeleteCodec(schema *etcdpb.CollectionMeta) *DeleteCodec {
	return &DeleteCodec{Schema: schema}
}

// Serialize transfer delete data to blob. .
// For each delete message, it will save "pk,ts" string to binlog.
func (deleteCodec *DeleteCodec) Serialize(partitionID UniqueID, segmentID UniqueID, data *DeleteData) (*Blob, error) {
	binlogWriter := NewDeleteBinlogWriter(schemapb.DataType_String, deleteCodec.Schema.ID, partitionID, segmentID)
	eventWriter, err := binlogWriter.NextDeleteEventWriter()
	if err != nil {
		return nil, err
	}
	sizeTotal := 0
	startTs, endTs := math.MaxInt64, math.MinInt64
	for key, value := range data.Data {
		if value < int64(startTs) {
			startTs = int(value)
		}
		if value > int64(endTs) {
			endTs = int(value)
		}
		err := eventWriter.AddOneStringToPayload(fmt.Sprintf("%s,%d", key, value))
		if err != nil {
			return nil, err
		}
		sizeTotal += len(key)
		sizeTotal += binary.Size(value)
	}
	eventWriter.SetEventTimestamp(uint64(startTs), uint64(endTs))
	binlogWriter.SetEventTimeStamp(uint64(startTs), uint64(endTs))

	// https://github.com/milvus-io/milvus/issues/9620
	// It's a little complicated to count the memory size of a map.
	// See: https://stackoverflow.com/questions/31847549/computing-the-memory-footprint-or-byte-length-of-a-map
	// Since the implementation of golang map may differ from version, so we'd better not to use this magic method.
	binlogWriter.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	err = binlogWriter.Close()
	if err != nil {
		return nil, err
	}
	buffer, err := binlogWriter.GetBuffer()
	if err != nil {
		return nil, err
	}
	blob := &Blob{
		Value: buffer,
	}
	return blob, nil

}

func (deleteCodec *DeleteCodec) Deserialize(blob *Blob) (partitionID UniqueID, segmentID UniqueID, data *DeleteData, err error) {
	if blob == nil {
		return InvalidUniqueID, InvalidUniqueID, nil, fmt.Errorf("blobs is empty")
	}
	readerClose := func(reader *BinlogReader) func() error {
		return func() error { return reader.Close() }
	}
	binlogReader, err := NewBinlogReader(blob.Value)
	if err != nil {
		return InvalidUniqueID, InvalidUniqueID, nil, err
	}
	pid, sid := binlogReader.PartitionID, binlogReader.SegmentID
	eventReader, err := binlogReader.NextEventReader()
	if err != nil {
		return InvalidUniqueID, InvalidUniqueID, nil, err
	}
	result := &DeleteData{
		Data: make(map[string]int64),
	}
	length, err := eventReader.GetPayloadLengthFromReader()
	for i := 0; i < length; i++ {
		singleString, err := eventReader.GetOneStringFromPayload(i)
		if err != nil {
			return InvalidUniqueID, InvalidUniqueID, nil, err
		}
		splits := strings.Split(singleString, ",")
		if len(splits) != 2 {
			return InvalidUniqueID, InvalidUniqueID, nil, fmt.Errorf("the format of delta log is incorrect")
		}
		ts, err := strconv.ParseInt(splits[1], 10, 64)
		if err != nil {
			return -1, -1, nil, err
		}
		result.Data[splits[0]] = ts
	}
	deleteCodec.readerCloseFunc = append(deleteCodec.readerCloseFunc, readerClose(binlogReader))
	return pid, sid, result, nil

}

// Blob key example:
// ${tenant}/data_definition_log/${collection_id}/ts/${log_idx}
// ${tenant}/data_definition_log/${collection_id}/ddl/${log_idx}
type DataDefinitionCodec struct {
	collectionID    int64
	readerCloseFunc []func() error
}

func NewDataDefinitionCodec(collectionID int64) *DataDefinitionCodec {
	return &DataDefinitionCodec{collectionID: collectionID}
}

// Serialize transfer @ts and @ddRequsts to blob.
// From schema, it get all fields.
// For each field, it will create a binlog writer, and write specific event according
// to the dataDefinition type.
// It returns blobs in the end.
func (dataDefinitionCodec *DataDefinitionCodec) Serialize(ts []Timestamp, ddRequests []string, eventTypes []EventTypeCode) ([]*Blob, error) {
	writer := NewDDLBinlogWriter(schemapb.DataType_Int64, dataDefinitionCodec.collectionID)

	var blobs []*Blob

	eventWriter, err := writer.NextCreateCollectionEventWriter()
	if err != nil {
		return nil, err
	}
	var int64Ts []int64
	for _, singleTs := range ts {
		int64Ts = append(int64Ts, int64(singleTs))
	}
	err = eventWriter.AddInt64ToPayload(int64Ts)
	if err != nil {
		return nil, err
	}
	eventWriter.SetEventTimestamp(ts[0], ts[len(ts)-1])
	writer.SetEventTimeStamp(ts[0], ts[len(ts)-1])

	// https://github.com/milvus-io/milvus/issues/9620
	writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", binary.Size(int64Ts)))

	err = writer.Close()

	if err != nil {
		return nil, err
	}
	buffer, err := writer.GetBuffer()
	if err != nil {
		return nil, err
	}
	blobs = append(blobs, &Blob{
		Key:   Ts,
		Value: buffer,
	})

	writer = NewDDLBinlogWriter(schemapb.DataType_String, dataDefinitionCodec.collectionID)

	sizeTotal := 0
	for pos, req := range ddRequests {
		sizeTotal += len(req)
		switch eventTypes[pos] {
		case CreateCollectionEventType:
			eventWriter, err := writer.NextCreateCollectionEventWriter()
			if err != nil {
				return nil, err
			}
			err = eventWriter.AddOneStringToPayload(req)
			if err != nil {
				return nil, err
			}
			eventWriter.SetEventTimestamp(ts[pos], ts[pos])
		case DropCollectionEventType:
			eventWriter, err := writer.NextDropCollectionEventWriter()
			if err != nil {
				return nil, err
			}
			err = eventWriter.AddOneStringToPayload(req)
			if err != nil {
				return nil, err
			}
			eventWriter.SetEventTimestamp(ts[pos], ts[pos])
		case CreatePartitionEventType:
			eventWriter, err := writer.NextCreatePartitionEventWriter()
			if err != nil {
				return nil, err
			}
			err = eventWriter.AddOneStringToPayload(req)
			if err != nil {
				return nil, err
			}
			eventWriter.SetEventTimestamp(ts[pos], ts[pos])
		case DropPartitionEventType:
			eventWriter, err := writer.NextDropPartitionEventWriter()
			if err != nil {
				return nil, err
			}
			err = eventWriter.AddOneStringToPayload(req)
			if err != nil {
				return nil, err
			}
			eventWriter.SetEventTimestamp(ts[pos], ts[pos])
		}
	}
	writer.SetEventTimeStamp(ts[0], ts[len(ts)-1])

	// https://github.com/milvus-io/milvus/issues/9620
	writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	err = writer.Close()
	if err != nil {
		return nil, err
	}
	buffer, err = writer.GetBuffer()
	if err != nil {
		return nil, err
	}
	blobs = append(blobs, &Blob{
		Key:   DDL,
		Value: buffer,
	})

	return blobs, nil
}

// Deserialize transfer blob back to data definition data.
// From schema, it get all fields.
// It will sort blob by blob key for blob logid is increasing by time.
// For each field, it will create a binlog reader, and read all event to the buffer.
// It returns origin @ts and @ddRequests in the end.
func (dataDefinitionCodec *DataDefinitionCodec) Deserialize(blobs []*Blob) (ts []Timestamp, ddRequests []string, err error) {
	if len(blobs) == 0 {
		return nil, nil, fmt.Errorf("blobs is empty")
	}
	readerClose := func(reader *BinlogReader) func() error {
		return func() error { return reader.Close() }
	}
	var requestsStrings []string
	var resultTs []Timestamp

	var blobList BlobList = blobs
	sort.Sort(blobList)

	for _, blob := range blobList {
		binlogReader, err := NewBinlogReader(blob.Value)
		if err != nil {
			return nil, nil, err
		}
		dataType := binlogReader.PayloadDataType

		for {
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return nil, nil, err
			}
			if eventReader == nil {
				break
			}
			switch dataType {
			case schemapb.DataType_Int64:
				int64Ts, err := eventReader.GetInt64FromPayload()
				if err != nil {
					return nil, nil, err
				}
				for _, singleTs := range int64Ts {
					resultTs = append(resultTs, Timestamp(singleTs))
				}
			case schemapb.DataType_String:
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return nil, nil, err
				}
				for i := 0; i < length; i++ {
					singleString, err := eventReader.GetOneStringFromPayload(i)
					if err != nil {
						return nil, nil, err
					}
					requestsStrings = append(requestsStrings, singleString)
				}
			}
		}

		dataDefinitionCodec.readerCloseFunc = append(dataDefinitionCodec.readerCloseFunc, readerClose(binlogReader))
	}

	return resultTs, requestsStrings, nil
}

func (dataDefinitionCodec *DataDefinitionCodec) Close() error {
	for _, closeFunc := range dataDefinitionCodec.readerCloseFunc {
		err := closeFunc()
		if err != nil {
			return err
		}
	}
	return nil
}

type IndexFileBinlogCodec struct {
	readerCloseFuncs []func() error
}

func NewIndexFileBinlogCodec() *IndexFileBinlogCodec {
	return &IndexFileBinlogCodec{
		readerCloseFuncs: make([]func() error, 0),
	}
}

func (codec *IndexFileBinlogCodec) Serialize(
	indexBuildID UniqueID,
	version int64,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	fieldID UniqueID,
	indexParams map[string]string,
	indexName string,
	indexID UniqueID,
	datas []*Blob,
) ([]*Blob, error) {

	var err error

	var blobs []*Blob

	ts := Timestamp(time.Now().UnixNano())

	for pos := range datas {
		writer := NewIndexFileBinlogWriter(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexName, indexID, datas[pos].Key)

		// https://github.com/milvus-io/milvus/issues/9449
		// store index parameters to extra, in bytes format.
		params, _ := json.Marshal(indexParams)
		writer.descriptorEvent.AddExtra(IndexParamsKey, params)

		eventWriter, err := writer.NextIndexFileEventWriter()
		if err != nil {
			return nil, err
		}

		length := (len(datas[pos].Value) + maxLengthPerRowOfIndexFile - 1) / maxLengthPerRowOfIndexFile
		for i := 0; i < length; i++ {
			start := i * maxLengthPerRowOfIndexFile
			end := (i + 1) * maxLengthPerRowOfIndexFile
			if end > len(datas[pos].Value) {
				end = len(datas[pos].Value)
			}
			err = eventWriter.AddOneStringToPayload(string(datas[pos].Value[start:end]))
			if err != nil {
				return nil, err
			}
		}

		eventWriter.SetEventTimestamp(ts, ts)

		writer.SetEventTimeStamp(ts, ts)

		// https://github.com/milvus-io/milvus/issues/9620
		writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", len(datas[pos].Value)))

		err = writer.Close()
		if err != nil {
			return nil, err
		}
		buffer, err := writer.GetBuffer()
		if err != nil {
			return nil, err
		}

		blobs = append(blobs, &Blob{
			Key: datas[pos].Key,
			//Key:   strconv.Itoa(pos),
			Value: buffer,
		})
	}

	// save index params
	writer := NewIndexFileBinlogWriter(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexName, indexID, IndexParamsKey)

	eventWriter, err := writer.NextIndexFileEventWriter()
	if err != nil {
		return nil, err
	}

	params, _ := json.Marshal(indexParams)
	length := (len(params) + maxLengthPerRowOfIndexFile - 1) / maxLengthPerRowOfIndexFile
	for i := 0; i < length; i++ {
		start := i * maxLengthPerRowOfIndexFile
		end := (i + 1) * maxLengthPerRowOfIndexFile
		if end > len(params) {
			end = len(params)
		}
		err = eventWriter.AddOneStringToPayload(string(params[start:end]))
		if err != nil {
			return nil, err
		}
	}

	eventWriter.SetEventTimestamp(ts, ts)

	writer.SetEventTimeStamp(ts, ts)

	// https://github.com/milvus-io/milvus/issues/9620
	// len(params) is also not accurate, indexParams is a map
	writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", len(params)))

	err = writer.Close()
	if err != nil {
		return nil, err
	}
	buffer, err := writer.GetBuffer()
	if err != nil {
		return nil, err
	}

	blobs = append(blobs, &Blob{
		Key: IndexParamsKey,
		//Key:   strconv.Itoa(len(datas)),
		Value: buffer,
	})

	return blobs, nil
}

func (codec *IndexFileBinlogCodec) DeserializeImpl(blobs []*Blob) (
	indexBuildID UniqueID,
	version int64,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	fieldID UniqueID,
	indexParams map[string]string,
	indexName string,
	indexID UniqueID,
	datas []*Blob,
	err error,
) {
	if len(blobs) == 0 {
		return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, errors.New("blobs is empty")
	}
	readerClose := func(reader *BinlogReader) func() error {
		return func() error { return reader.Close() }
	}

	indexParams = make(map[string]string)
	datas = make([]*Blob, 0)

	for _, blob := range blobs {
		binlogReader, err := NewBinlogReader(blob.Value)
		if err != nil {
			log.Warn("failed to read binlog",
				zap.Error(err))
			return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
		}
		dataType := binlogReader.PayloadDataType

		//desc, err := binlogReader.readDescriptorEvent()
		//if err != nil {
		//	log.Warn("failed to read descriptor event",
		//		zap.Error(err))
		//	return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
		//}
		desc := binlogReader.descriptorEvent
		extraBytes := desc.ExtraBytes
		extra := make(map[string]interface{})
		_ = json.Unmarshal(extraBytes, &extra)

		var value int

		value, _ = strconv.Atoi(extra["indexBuildID"].(string))
		indexBuildID = UniqueID(value)

		value, _ = strconv.Atoi(extra["version"].(string))
		version = int64(value)

		collectionID = desc.CollectionID
		partitionID = desc.PartitionID
		segmentID = desc.SegmentID
		fieldID = desc.FieldID

		indexName = extra["indexName"].(string)

		value, _ = strconv.Atoi(extra["indexID"].(string))
		indexID = UniqueID(value)

		key := extra["key"].(string)

		for {
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				log.Warn("failed to get next event reader",
					zap.Error(err))
				return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
			}
			if eventReader == nil {
				break
			}
			switch dataType {
			case schemapb.DataType_String:
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					log.Warn("failed to get payload length",
						zap.Error(err))
					return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
				}

				var content []byte
				for i := 0; i < length; i++ {
					singleString, err := eventReader.GetOneStringFromPayload(i)
					if err != nil {
						log.Warn("failed to get string from payload",
							zap.Error(err))
						return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
					}

					content = append(content, []byte(singleString)...)
				}

				if key == IndexParamsKey {
					_ = json.Unmarshal(content, &indexParams)
				} else {
					datas = append(datas, &Blob{
						Key:   key,
						Value: content,
					})
				}
			}
		}

		codec.readerCloseFuncs = append(codec.readerCloseFuncs, readerClose(binlogReader))
	}

	return indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexParams, indexName, indexID, datas, nil
}

func (codec *IndexFileBinlogCodec) Deserialize(blobs []*Blob) (
	datas []*Blob,
	indexParams map[string]string,
	indexName string,
	indexID UniqueID,
	err error,
) {
	_, _, _, _, _, _, indexParams, indexName, indexID, datas, err = codec.DeserializeImpl(blobs)
	return datas, indexParams, indexName, indexID, err
}

func (codec *IndexFileBinlogCodec) Close() error {
	for _, closeFunc := range codec.readerCloseFuncs {
		err := closeFunc()
		if err != nil {
			return err
		}
	}
	return nil
}

type IndexCodec struct {
}

func NewIndexCodec() *IndexCodec {
	return &IndexCodec{}
}

func (indexCodec *IndexCodec) Serialize(blobs []*Blob, params map[string]string, indexName string, indexID UniqueID) ([]*Blob, error) {
	paramsBytes, err := json.Marshal(struct {
		Params    map[string]string
		IndexName string
		IndexID   UniqueID
	}{
		Params:    params,
		IndexName: indexName,
		IndexID:   indexID,
	})
	if err != nil {
		return nil, err
	}
	blobs = append(blobs, &Blob{Key: IndexParamsKey, Value: paramsBytes})
	return blobs, nil
}

func (indexCodec *IndexCodec) Deserialize(blobs []*Blob) ([]*Blob, map[string]string, string, UniqueID, error) {
	var file *Blob
	for i := 0; i < len(blobs); i++ {
		if blobs[i].Key != IndexParamsKey {
			continue
		}
		file = blobs[i]
		blobs = append(blobs[:i], blobs[i+1:]...)
		break
	}
	if file == nil {
		return nil, nil, "", InvalidUniqueID, fmt.Errorf("can not find params blob")
	}
	info := struct {
		Params    map[string]string
		IndexName string
		IndexID   UniqueID
	}{}
	if err := json.Unmarshal(file.Value, &info); err != nil {
		return nil, nil, "", InvalidUniqueID, fmt.Errorf("json unmarshal error: %s", err.Error())
	}

	return blobs, info.Params, info.IndexName, info.IndexID, nil
}

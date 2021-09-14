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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	Ts              = "ts"
	DDL             = "ddl"
	IndexParamsFile = "indexParams"
)

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

type FieldData interface{}

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
		case schemapb.DataType_Int8:
			err = eventWriter.AddInt8ToPayload(singleData.(*Int8FieldData).Data)
		case schemapb.DataType_Int16:
			err = eventWriter.AddInt16ToPayload(singleData.(*Int16FieldData).Data)
		case schemapb.DataType_Int32:
			err = eventWriter.AddInt32ToPayload(singleData.(*Int32FieldData).Data)
		case schemapb.DataType_Int64:
			err = eventWriter.AddInt64ToPayload(singleData.(*Int64FieldData).Data)
		case schemapb.DataType_Float:
			err = eventWriter.AddFloatToPayload(singleData.(*FloatFieldData).Data)
		case schemapb.DataType_Double:
			err = eventWriter.AddDoubleToPayload(singleData.(*DoubleFieldData).Data)
		case schemapb.DataType_String:
			for _, singleString := range singleData.(*StringFieldData).Data {
				err = eventWriter.AddOneStringToPayload(singleString)
				if err != nil {
					return nil, nil, err
				}
			}
		case schemapb.DataType_BinaryVector:
			err = eventWriter.AddBinaryVectorToPayload(singleData.(*BinaryVectorFieldData).Data, singleData.(*BinaryVectorFieldData).Dim)
		case schemapb.DataType_FloatVector:
			err = eventWriter.AddFloatVectorToPayload(singleData.(*FloatVectorFieldData).Data, singleData.(*FloatVectorFieldData).Dim)
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
			err = statsWriter.StatsInt64(singleData.(*Int64FieldData).Data)
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

func (insertCodec *InsertCodec) Deserialize(blobs []*Blob) (partitionID UniqueID, segmentID UniqueID, data *InsertData, err error) {
	if len(blobs) == 0 {
		return InvalidUniqueID, InvalidUniqueID, nil, fmt.Errorf("blobs is empty")
	}
	readerClose := func(reader *BinlogReader) func() error {
		return func() error { return reader.Close() }
	}

	var blobList BlobList = blobs
	sort.Sort(blobList)

	var pID UniqueID
	var sID UniqueID
	resultData := &InsertData{}
	resultData.Data = make(map[FieldID]FieldData)
	for _, blob := range blobList {
		binlogReader, err := NewBinlogReader(blob.Value)
		if err != nil {
			return InvalidUniqueID, InvalidUniqueID, nil, err
		}

		// read partitionID and SegmentID
		pID, sID = binlogReader.PartitionID, binlogReader.SegmentID

		dataType := binlogReader.PayloadDataType
		fieldID := binlogReader.FieldID
		totalLength := 0
		for {
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				boolFieldData.Data = append(boolFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				int8FieldData.Data = append(int8FieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				int16FieldData.Data = append(int16FieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				int32FieldData.Data = append(int32FieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				int64FieldData.Data = append(int64FieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				floatFieldData.Data = append(floatFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				doubleFieldData.Data = append(doubleFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				stringFieldData.NumRows = append(stringFieldData.NumRows, int64(length))
				for i := 0; i < length; i++ {
					singleString, err := eventReader.GetOneStringFromPayload(i)
					if err != nil {
						return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				binaryVectorFieldData.Data = append(binaryVectorFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, nil, err
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
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				floatVectorFieldData.Data = append(floatVectorFieldData.Data, singleData...)
				length, err := eventReader.GetPayloadLengthFromReader()
				if err != nil {
					return InvalidUniqueID, InvalidUniqueID, nil, err
				}
				totalLength += length
				floatVectorFieldData.NumRows = append(floatVectorFieldData.NumRows, int64(length))
				resultData.Data[fieldID] = floatVectorFieldData
			default:
				return InvalidUniqueID, InvalidUniqueID, nil, fmt.Errorf("undefined data type %d", dataType)
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

	return pID, sID, resultData, nil
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

	for pos, req := range ddRequests {
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

//type IndexCodec struct {
//	Base
//	readerCloseFunc []func() error
//}
//
////func (builder *IndexBuilder) Build(fieldData FieldData, typeParams map[string]string, indexParams map[string]string) ([]*Blob, error) {}
//func (indexCodec *IndexCodec) Serialize(indexSlices []*Blob) ([]*Blob, error) {}
//
//// TODO: describe inputs and return
//func (indexCodec *IndexCodec) Deserialize(blobs []*Blob) ([]*Blob, error) {}

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
	blobs = append(blobs, &Blob{Key: IndexParamsFile, Value: paramsBytes})
	return blobs, nil
}

func (indexCodec *IndexCodec) Deserialize(blobs []*Blob) ([]*Blob, map[string]string, string, UniqueID, error) {
	var file *Blob
	for i := 0; i < len(blobs); i++ {
		if blobs[i].Key != IndexParamsFile {
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

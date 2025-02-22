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
	"math"
	"sort"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	// UniqueID is type alias of typeutil.UniqueID
	UniqueID = typeutil.UniqueID

	// FieldID represent the identity number of field in collection and its type is UniqueID
	FieldID = typeutil.UniqueID

	// Timestamp is type alias of typeutil.Timestamp
	Timestamp = typeutil.Timestamp
)

// InvalidUniqueID is used when the UniqueID is not set (like in return with err)
const InvalidUniqueID = UniqueID(-1)

// Blob is a pack of key&value
type Blob struct {
	Key        string
	Value      []byte
	MemorySize int64
	RowNum     int64
}

// BlobList implements sort.Interface for a list of Blob
type BlobList []*Blob

// Len implements Len in sort.Interface
func (s BlobList) Len() int {
	return len(s)
}

// Less implements Less in sort.Interface
func (s BlobList) Less(i, j int) bool {
	_, _, _, _, iLog, ok := metautil.ParseInsertLogPath(s[i].Key)
	if !ok {
		return false
	}
	_, _, _, _, jLog, ok := metautil.ParseInsertLogPath(s[j].Key)
	if !ok {
		return false
	}
	return iLog < jLog
}

// Swap implements Swap in sort.Interface
func (s BlobList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// GetKey returns the key of blob
func (b Blob) GetKey() string {
	return b.Key
}

// GetValue returns the value of blob
func (b Blob) GetValue() []byte {
	return b.Value
}

// GetMemorySize returns the memory size of blob
func (b Blob) GetMemorySize() int64 {
	return b.MemorySize
}

// InsertCodec serializes and deserializes the insert data
// Blob key example:
// ${tenant}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/${log_idx}
type InsertCodec struct {
	Schema *etcdpb.CollectionMeta
}

// NewInsertCodec creates an InsertCodec
func NewInsertCodec() *InsertCodec {
	return &InsertCodec{}
}

// NewInsertCodecWithSchema creates an InsertCodec with provided collection meta
func NewInsertCodecWithSchema(schema *etcdpb.CollectionMeta) *InsertCodec {
	return &InsertCodec{Schema: schema}
}

// Serialize Pk stats log
func (insertCodec *InsertCodec) SerializePkStats(stats *PrimaryKeyStats, rowNum int64) (*Blob, error) {
	if stats == nil || stats.BF == nil {
		return nil, fmt.Errorf("sericalize empty pk stats")
	}

	// Serialize by pk stats
	blobKey := fmt.Sprintf("%d", stats.FieldID)
	statsWriter := &StatsWriter{}
	err := statsWriter.Generate(stats)
	if err != nil {
		return nil, err
	}

	buffer := statsWriter.GetBuffer()
	return &Blob{
		Key:    blobKey,
		Value:  buffer,
		RowNum: rowNum,
	}, nil
}

// Serialize Pk stats list to one blob
func (insertCodec *InsertCodec) SerializePkStatsList(stats []*PrimaryKeyStats, rowNum int64) (*Blob, error) {
	if len(stats) == 0 {
		return nil, merr.WrapErrServiceInternal("shall not serialize zero length statslog list")
	}

	blobKey := fmt.Sprintf("%d", stats[0].FieldID)
	statsWriter := &StatsWriter{}
	err := statsWriter.GenerateList(stats)
	if err != nil {
		return nil, err
	}

	buffer := statsWriter.GetBuffer()
	return &Blob{
		Key:    blobKey,
		Value:  buffer,
		RowNum: rowNum,
	}, nil
}

// Serialize Pk stats log by insert data
func (insertCodec *InsertCodec) SerializePkStatsByData(data *InsertData) (*Blob, error) {
	timeFieldData, ok := data.Data[common.TimeStampField]
	if !ok {
		return nil, fmt.Errorf("data doesn't contains timestamp field")
	}
	if timeFieldData.RowNum() <= 0 {
		return nil, fmt.Errorf("there's no data in InsertData")
	}
	rowNum := int64(timeFieldData.RowNum())

	for _, field := range insertCodec.Schema.Schema.Fields {
		// stats fields
		if !field.GetIsPrimaryKey() {
			continue
		}
		singleData := data.Data[field.FieldID]
		blobKey := fmt.Sprintf("%d", field.FieldID)
		statsWriter := &StatsWriter{}
		err := statsWriter.GenerateByData(field.FieldID, field.DataType, singleData)
		if err != nil {
			return nil, err
		}
		buffer := statsWriter.GetBuffer()
		return &Blob{
			Key:    blobKey,
			Value:  buffer,
			RowNum: rowNum,
		}, nil
	}
	return nil, fmt.Errorf("there is no pk field")
}

// Serialize transforms insert data to blob. It will sort insert data by timestamp.
// From schema, it gets all fields.
// For each field, it will create a binlog writer, and write an event to the binlog.
// It returns binlog buffer in the end.
func (insertCodec *InsertCodec) Serialize(partitionID UniqueID, segmentID UniqueID, data ...*InsertData) ([]*Blob, error) {
	blobs := make([]*Blob, 0)
	var writer *InsertBinlogWriter
	if insertCodec.Schema == nil {
		return nil, fmt.Errorf("schema is not set")
	}

	var rowNum int64
	var startTs, endTs Timestamp
	startTs, endTs = math.MaxUint64, 0

	for _, block := range data {
		timeFieldData, ok := block.Data[common.TimeStampField]
		if !ok {
			return nil, fmt.Errorf("data doesn't contains timestamp field")
		}

		rowNum += int64(timeFieldData.RowNum())

		ts := timeFieldData.(*Int64FieldData).Data

		for _, t := range ts {
			if uint64(t) > endTs {
				endTs = uint64(t)
			}

			if uint64(t) < startTs {
				startTs = uint64(t)
			}
		}
	}

	for _, field := range insertCodec.Schema.Schema.Fields {
		// encode fields
		writer = NewInsertBinlogWriter(field.DataType, insertCodec.Schema.ID, partitionID, segmentID, field.FieldID, field.GetNullable())

		// get payload writing configs, including nullable and fallback encoding method
		opts := []PayloadWriterOptions{WithNullable(field.GetNullable()), WithWriterProps(getFieldWriterProps(field))}

		if typeutil.IsVectorType(field.DataType) && !typeutil.IsSparseFloatVectorType(field.DataType) {
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			opts = append(opts, WithDim(int(dim)))
		}
		eventWriter, err := writer.NextInsertEventWriter(opts...)
		if err != nil {
			writer.Close()
			return nil, err
		}
		eventWriter.SetEventTimestamp(startTs, endTs)
		eventWriter.Reserve(int(rowNum))

		var memorySize int64
		for _, block := range data {
			singleData := block.Data[field.FieldID]

			blockMemorySize := singleData.GetMemorySize()
			memorySize += int64(blockMemorySize)
			if err = AddFieldDataToPayload(eventWriter, field.DataType, singleData); err != nil {
				eventWriter.Close()
				writer.Close()
				return nil, err
			}
			writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", blockMemorySize))
			writer.SetEventTimeStamp(startTs, endTs)
		}

		err = writer.Finish()
		if err != nil {
			eventWriter.Close()
			writer.Close()
			return nil, err
		}

		buffer, err := writer.GetBuffer()
		if err != nil {
			eventWriter.Close()
			writer.Close()
			return nil, err
		}
		blobKey := fmt.Sprintf("%d", field.FieldID)
		blobs = append(blobs, &Blob{
			Key:        blobKey,
			Value:      buffer,
			RowNum:     rowNum,
			MemorySize: memorySize,
		})
		eventWriter.Close()
		writer.Close()
	}

	return blobs, nil
}

func AddFieldDataToPayload(eventWriter *insertEventWriter, dataType schemapb.DataType, singleData FieldData) error {
	var err error
	switch dataType {
	case schemapb.DataType_Bool:
		if err = eventWriter.AddBoolToPayload(singleData.(*BoolFieldData).Data, singleData.(*BoolFieldData).ValidData); err != nil {
			return err
		}
	case schemapb.DataType_Int8:
		if err = eventWriter.AddInt8ToPayload(singleData.(*Int8FieldData).Data, singleData.(*Int8FieldData).ValidData); err != nil {
			return err
		}
	case schemapb.DataType_Int16:
		if err = eventWriter.AddInt16ToPayload(singleData.(*Int16FieldData).Data, singleData.(*Int16FieldData).ValidData); err != nil {
			return err
		}
	case schemapb.DataType_Int32:
		if err = eventWriter.AddInt32ToPayload(singleData.(*Int32FieldData).Data, singleData.(*Int32FieldData).ValidData); err != nil {
			return err
		}
	case schemapb.DataType_Int64:
		if err = eventWriter.AddInt64ToPayload(singleData.(*Int64FieldData).Data, singleData.(*Int64FieldData).ValidData); err != nil {
			return err
		}
	case schemapb.DataType_Float:
		if err = eventWriter.AddFloatToPayload(singleData.(*FloatFieldData).Data, singleData.(*FloatFieldData).ValidData); err != nil {
			return err
		}
	case schemapb.DataType_Double:
		if err = eventWriter.AddDoubleToPayload(singleData.(*DoubleFieldData).Data, singleData.(*DoubleFieldData).ValidData); err != nil {
			return err
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		for i, singleString := range singleData.(*StringFieldData).Data {
			isValid := true
			if len(singleData.(*StringFieldData).ValidData) != 0 {
				isValid = singleData.(*StringFieldData).ValidData[i]
			}
			if err = eventWriter.AddOneStringToPayload(singleString, isValid); err != nil {
				return err
			}
		}
	case schemapb.DataType_Array:
		for i, singleArray := range singleData.(*ArrayFieldData).Data {
			isValid := true
			if len(singleData.(*ArrayFieldData).ValidData) != 0 {
				isValid = singleData.(*ArrayFieldData).ValidData[i]
			}
			if err = eventWriter.AddOneArrayToPayload(singleArray, isValid); err != nil {
				return err
			}
		}
	case schemapb.DataType_JSON:
		for i, singleJSON := range singleData.(*JSONFieldData).Data {
			isValid := true
			if len(singleData.(*JSONFieldData).ValidData) != 0 {
				isValid = singleData.(*JSONFieldData).ValidData[i]
			}
			if err = eventWriter.AddOneJSONToPayload(singleJSON, isValid); err != nil {
				return err
			}
		}
	case schemapb.DataType_BinaryVector:
		if err = eventWriter.AddBinaryVectorToPayload(singleData.(*BinaryVectorFieldData).Data, singleData.(*BinaryVectorFieldData).Dim); err != nil {
			return err
		}
	case schemapb.DataType_FloatVector:
		if err = eventWriter.AddFloatVectorToPayload(singleData.(*FloatVectorFieldData).Data, singleData.(*FloatVectorFieldData).Dim); err != nil {
			return err
		}
	case schemapb.DataType_Float16Vector:
		if err = eventWriter.AddFloat16VectorToPayload(singleData.(*Float16VectorFieldData).Data, singleData.(*Float16VectorFieldData).Dim); err != nil {
			return err
		}
	case schemapb.DataType_BFloat16Vector:
		if err = eventWriter.AddBFloat16VectorToPayload(singleData.(*BFloat16VectorFieldData).Data, singleData.(*BFloat16VectorFieldData).Dim); err != nil {
			return err
		}
	case schemapb.DataType_SparseFloatVector:
		if err = eventWriter.AddSparseFloatVectorToPayload(singleData.(*SparseFloatVectorFieldData)); err != nil {
			return err
		}
	default:
		return fmt.Errorf("undefined data type %d", dataType)
	}
	return nil
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

	var blobList BlobList = blobs
	sort.Sort(blobList)

	data = &InsertData{
		Data: make(map[FieldID]FieldData),
	}
	if collectionID, partitionID, segmentID, err = insertCodec.DeserializeInto(blobs, 0, data); err != nil {
		return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, nil, err
	}

	return
}

func (insertCodec *InsertCodec) DeserializeInto(fieldBinlogs []*Blob, rowNum int, insertData *InsertData) (
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	err error,
) {
	for _, blob := range fieldBinlogs {
		binlogReader, err := NewBinlogReader(blob.Value)
		if err != nil {
			return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, err
		}

		// read partitionID and SegmentID
		collectionID, partitionID, segmentID = binlogReader.CollectionID, binlogReader.PartitionID, binlogReader.SegmentID

		dataType := binlogReader.PayloadDataType
		fieldID := binlogReader.FieldID
		totalLength := 0

		for {
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				binlogReader.Close()
				return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, err
			}
			if eventReader == nil {
				break
			}
			data, validData, dim, err := eventReader.GetDataFromPayload()
			if err != nil {
				eventReader.Close()
				binlogReader.Close()
				return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, err
			}
			length, err := AddInsertData(dataType, data, insertData, fieldID, rowNum, eventReader, dim, validData)
			if err != nil {
				eventReader.Close()
				binlogReader.Close()
				return InvalidUniqueID, InvalidUniqueID, InvalidUniqueID, err
			}
			totalLength += length
			eventReader.Close()
		}

		if rowNum <= 0 {
			rowNum = totalLength
		}

		if fieldID == common.TimeStampField {
			blobInfo := BlobInfo{
				Length: totalLength,
			}
			insertData.Infos = append(insertData.Infos, blobInfo)
		}
		binlogReader.Close()
	}

	return collectionID, partitionID, segmentID, nil
}

func AddInsertData(dataType schemapb.DataType, data interface{}, insertData *InsertData, fieldID int64, rowNum int, eventReader *EventReader, dim int, validData []bool) (dataLength int, err error) {
	fieldData := insertData.Data[fieldID]
	switch dataType {
	case schemapb.DataType_Bool:
		singleData := data.([]bool)
		if fieldData == nil {
			fieldData = &BoolFieldData{Data: make([]bool, 0, rowNum)}
		}
		boolFieldData := fieldData.(*BoolFieldData)

		boolFieldData.Data = append(boolFieldData.Data, singleData...)
		boolFieldData.ValidData = append(boolFieldData.ValidData, validData...)
		insertData.Data[fieldID] = boolFieldData
		return len(singleData), nil

	case schemapb.DataType_Int8:
		singleData := data.([]int8)
		if fieldData == nil {
			fieldData = &Int8FieldData{Data: make([]int8, 0, rowNum)}
		}
		int8FieldData := fieldData.(*Int8FieldData)

		int8FieldData.Data = append(int8FieldData.Data, singleData...)
		int8FieldData.ValidData = append(int8FieldData.ValidData, validData...)
		insertData.Data[fieldID] = int8FieldData
		return len(singleData), nil

	case schemapb.DataType_Int16:
		singleData := data.([]int16)
		if fieldData == nil {
			fieldData = &Int16FieldData{Data: make([]int16, 0, rowNum)}
		}
		int16FieldData := fieldData.(*Int16FieldData)

		int16FieldData.Data = append(int16FieldData.Data, singleData...)
		int16FieldData.ValidData = append(int16FieldData.ValidData, validData...)
		insertData.Data[fieldID] = int16FieldData
		return len(singleData), nil

	case schemapb.DataType_Int32:
		singleData := data.([]int32)
		if fieldData == nil {
			fieldData = &Int32FieldData{Data: make([]int32, 0, rowNum)}
		}
		int32FieldData := fieldData.(*Int32FieldData)

		int32FieldData.Data = append(int32FieldData.Data, singleData...)
		int32FieldData.ValidData = append(int32FieldData.ValidData, validData...)
		insertData.Data[fieldID] = int32FieldData
		return len(singleData), nil

	case schemapb.DataType_Int64:
		singleData := data.([]int64)
		if fieldData == nil {
			fieldData = &Int64FieldData{Data: make([]int64, 0, rowNum)}
		}
		int64FieldData := fieldData.(*Int64FieldData)

		int64FieldData.Data = append(int64FieldData.Data, singleData...)
		int64FieldData.ValidData = append(int64FieldData.ValidData, validData...)
		insertData.Data[fieldID] = int64FieldData
		return len(singleData), nil

	case schemapb.DataType_Float:
		singleData := data.([]float32)
		if fieldData == nil {
			fieldData = &FloatFieldData{Data: make([]float32, 0, rowNum)}
		}
		floatFieldData := fieldData.(*FloatFieldData)

		floatFieldData.Data = append(floatFieldData.Data, singleData...)
		floatFieldData.ValidData = append(floatFieldData.ValidData, validData...)
		insertData.Data[fieldID] = floatFieldData
		return len(singleData), nil

	case schemapb.DataType_Double:
		singleData := data.([]float64)
		if fieldData == nil {
			fieldData = &DoubleFieldData{Data: make([]float64, 0, rowNum)}
		}
		doubleFieldData := fieldData.(*DoubleFieldData)

		doubleFieldData.Data = append(doubleFieldData.Data, singleData...)
		doubleFieldData.ValidData = append(doubleFieldData.ValidData, validData...)
		insertData.Data[fieldID] = doubleFieldData
		return len(singleData), nil

	case schemapb.DataType_String, schemapb.DataType_VarChar:
		singleData := data.([]string)
		if fieldData == nil {
			fieldData = &StringFieldData{Data: make([]string, 0, rowNum)}
		}
		stringFieldData := fieldData.(*StringFieldData)

		stringFieldData.Data = append(stringFieldData.Data, singleData...)
		stringFieldData.ValidData = append(stringFieldData.ValidData, validData...)
		stringFieldData.DataType = dataType
		insertData.Data[fieldID] = stringFieldData
		return len(singleData), nil

	case schemapb.DataType_Array:
		singleData := data.([]*schemapb.ScalarField)
		if fieldData == nil {
			fieldData = &ArrayFieldData{Data: make([]*schemapb.ScalarField, 0, rowNum)}
		}
		arrayFieldData := fieldData.(*ArrayFieldData)

		arrayFieldData.Data = append(arrayFieldData.Data, singleData...)
		arrayFieldData.ValidData = append(arrayFieldData.ValidData, validData...)
		insertData.Data[fieldID] = arrayFieldData
		return len(singleData), nil

	case schemapb.DataType_JSON:
		singleData := data.([][]byte)
		if fieldData == nil {
			fieldData = &JSONFieldData{Data: make([][]byte, 0, rowNum)}
		}
		jsonFieldData := fieldData.(*JSONFieldData)

		jsonFieldData.Data = append(jsonFieldData.Data, singleData...)
		jsonFieldData.ValidData = append(jsonFieldData.ValidData, validData...)
		insertData.Data[fieldID] = jsonFieldData
		return len(singleData), nil

	case schemapb.DataType_BinaryVector:
		singleData := data.([]byte)
		if fieldData == nil {
			fieldData = &BinaryVectorFieldData{Data: make([]byte, 0, rowNum*dim)}
		}
		binaryVectorFieldData := fieldData.(*BinaryVectorFieldData)

		binaryVectorFieldData.Data = append(binaryVectorFieldData.Data, singleData...)
		length, err := eventReader.GetPayloadLengthFromReader()
		if err != nil {
			return length, err
		}
		binaryVectorFieldData.Dim = dim
		insertData.Data[fieldID] = binaryVectorFieldData
		return length, nil

	case schemapb.DataType_Float16Vector:
		singleData := data.([]byte)
		if fieldData == nil {
			fieldData = &Float16VectorFieldData{Data: make([]byte, 0, rowNum*dim)}
		}
		float16VectorFieldData := fieldData.(*Float16VectorFieldData)

		float16VectorFieldData.Data = append(float16VectorFieldData.Data, singleData...)
		length, err := eventReader.GetPayloadLengthFromReader()
		if err != nil {
			return length, err
		}
		float16VectorFieldData.Dim = dim
		insertData.Data[fieldID] = float16VectorFieldData
		return length, nil

	case schemapb.DataType_BFloat16Vector:
		singleData := data.([]byte)
		if fieldData == nil {
			fieldData = &BFloat16VectorFieldData{Data: make([]byte, 0, rowNum*dim)}
		}
		bfloat16VectorFieldData := fieldData.(*BFloat16VectorFieldData)

		bfloat16VectorFieldData.Data = append(bfloat16VectorFieldData.Data, singleData...)
		length, err := eventReader.GetPayloadLengthFromReader()
		if err != nil {
			return length, err
		}
		bfloat16VectorFieldData.Dim = dim
		insertData.Data[fieldID] = bfloat16VectorFieldData
		return length, nil

	case schemapb.DataType_FloatVector:
		singleData := data.([]float32)
		if fieldData == nil {
			fieldData = &FloatVectorFieldData{Data: make([]float32, 0, rowNum*dim)}
		}
		floatVectorFieldData := fieldData.(*FloatVectorFieldData)

		floatVectorFieldData.Data = append(floatVectorFieldData.Data, singleData...)
		length, err := eventReader.GetPayloadLengthFromReader()
		if err != nil {
			return 0, err
		}
		floatVectorFieldData.Dim = dim
		insertData.Data[fieldID] = floatVectorFieldData
		return length, nil

	case schemapb.DataType_SparseFloatVector:
		singleData := data.(*SparseFloatVectorFieldData)
		if fieldData == nil {
			fieldData = &SparseFloatVectorFieldData{}
		}
		vec := fieldData.(*SparseFloatVectorFieldData)
		vec.AppendAllRows(singleData)
		insertData.Data[fieldID] = vec
		return singleData.RowNum(), nil

	default:
		return 0, fmt.Errorf("undefined data type %d", dataType)
	}
}

// Deserialize transfer blob back to insert data.
// From schema, it get all fields.
// For each field, it will create a binlog reader, and read all event to the buffer.
// It returns origin @InsertData in the end.
func (insertCodec *InsertCodec) Deserialize(blobs []*Blob) (partitionID UniqueID, segmentID UniqueID, data *InsertData, err error) {
	_, partitionID, segmentID, data, err = insertCodec.DeserializeAll(blobs)
	return partitionID, segmentID, data, err
}

// DeleteCodec serializes and deserializes the delete data
type DeleteCodec struct{}

// NewDeleteCodec returns a DeleteCodec
func NewDeleteCodec() *DeleteCodec {
	return &DeleteCodec{}
}

// Serialize transfer delete data to blob. .
// For each delete message, it will save "pk,ts" string to binlog.
func (deleteCodec *DeleteCodec) Serialize(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, data *DeleteData) (*Blob, error) {
	binlogWriter := NewDeleteBinlogWriter(schemapb.DataType_String, collectionID, partitionID, segmentID)
	field := &schemapb.FieldSchema{IsPrimaryKey: true, DataType: schemapb.DataType_String}
	opts := []PayloadWriterOptions{WithWriterProps(getFieldWriterProps(field))}
	eventWriter, err := binlogWriter.NextDeleteEventWriter(opts...)
	if err != nil {
		binlogWriter.Close()
		return nil, err
	}
	defer binlogWriter.Close()
	defer eventWriter.Close()
	length := len(data.Pks)
	if length != len(data.Tss) {
		return nil, fmt.Errorf("the length of pks, and TimeStamps is not equal")
	}

	sizeTotal := 0
	var startTs, endTs Timestamp
	startTs, endTs = math.MaxUint64, 0
	for i := 0; i < length; i++ {
		ts := data.Tss[i]
		if ts < startTs {
			startTs = ts
		}
		if ts > endTs {
			endTs = ts
		}

		deleteLog := NewDeleteLog(data.Pks[i], ts)
		serializedPayload, err := json.Marshal(deleteLog)
		if err != nil {
			return nil, err
		}
		err = eventWriter.AddOneStringToPayload(string(serializedPayload), true)
		if err != nil {
			return nil, err
		}
		sizeTotal += binary.Size(serializedPayload)
	}
	eventWriter.SetEventTimestamp(startTs, endTs)
	binlogWriter.SetEventTimeStamp(startTs, endTs)

	// https://github.com/milvus-io/milvus/issues/9620
	// It's a little complicated to count the memory size of a map.
	// See: https://stackoverflow.com/questions/31847549/computing-the-memory-footprint-or-byte-length-of-a-map
	// Since the implementation of golang map may differ from version, so we'd better not to use this magic method.
	binlogWriter.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	err = binlogWriter.Finish()
	if err != nil {
		return nil, err
	}
	buffer, err := binlogWriter.GetBuffer()
	if err != nil {
		return nil, err
	}
	blob := &Blob{
		Value:      buffer,
		MemorySize: data.Size(),
	}
	return blob, nil
}

// Deserialize deserializes the deltalog blobs into DeleteData
func (deleteCodec *DeleteCodec) Deserialize(blobs []*Blob) (partitionID UniqueID, segmentID UniqueID, data *DeltaData, err error) {
	if len(blobs) == 0 {
		return InvalidUniqueID, InvalidUniqueID, nil, fmt.Errorf("blobs is empty")
	}

	rowNum := lo.SumBy(blobs, func(blob *Blob) int64 {
		return blob.RowNum
	})

	var pid, sid UniqueID
	result := NewDeltaData(rowNum)

	deserializeBlob := func(blob *Blob) error {
		binlogReader, err := NewBinlogReader(blob.Value)
		if err != nil {
			return err
		}
		defer binlogReader.Close()

		pid, sid = binlogReader.PartitionID, binlogReader.SegmentID
		eventReader, err := binlogReader.NextEventReader()
		if err != nil {
			return err
		}
		defer eventReader.Close()

		rr, err := eventReader.GetArrowRecordReader()
		if err != nil {
			return err
		}
		defer rr.Release()
		deleteLog := &DeleteLog{}

		handleRecord := func() error {
			rec := rr.Record()
			defer rec.Release()
			column := rec.Column(0)
			for i := 0; i < column.Len(); i++ {
				strVal := column.ValueStr(i)

				err := deleteLog.Parse(strVal)
				if err != nil {
					return err
				}
				err = result.Append(deleteLog.Pk, deleteLog.Ts)
				if err != nil {
					return err
				}
			}
			return nil
		}

		for rr.Next() {
			err := handleRecord()
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, blob := range blobs {
		if err := deserializeBlob(blob); err != nil {
			return InvalidUniqueID, InvalidUniqueID, nil, err
		}
	}

	return pid, sid, result, nil
}

// DataDefinitionCodec serializes and deserializes the data definition
// Blob key example:
// ${tenant}/data_definition_log/${collection_id}/ts/${log_idx}
// ${tenant}/data_definition_log/${collection_id}/ddl/${log_idx}
type DataDefinitionCodec struct {
	collectionID int64
}

// NewDataDefinitionCodec is constructor for DataDefinitionCodec
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
	eventWriter, err := writer.NextCreateCollectionEventWriter()
	if err != nil {
		writer.Close()
		return nil, err
	}
	defer writer.Close()
	defer eventWriter.Close()

	var blobs []*Blob

	var int64Ts []int64
	for _, singleTs := range ts {
		int64Ts = append(int64Ts, int64(singleTs))
	}
	err = eventWriter.AddInt64ToPayload(int64Ts, nil)
	if err != nil {
		return nil, err
	}
	eventWriter.SetEventTimestamp(ts[0], ts[len(ts)-1])
	writer.SetEventTimeStamp(ts[0], ts[len(ts)-1])

	// https://github.com/milvus-io/milvus/issues/9620
	writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", binary.Size(int64Ts)))

	err = writer.Finish()
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
	eventWriter.Close()
	writer.Close()

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
			if err = eventWriter.AddOneStringToPayload(req, true); err != nil {
				return nil, err
			}
			eventWriter.SetEventTimestamp(ts[pos], ts[pos])
		case DropCollectionEventType:
			eventWriter, err := writer.NextDropCollectionEventWriter()
			if err != nil {
				return nil, err
			}
			if err = eventWriter.AddOneStringToPayload(req, true); err != nil {
				return nil, err
			}
			eventWriter.SetEventTimestamp(ts[pos], ts[pos])
		case CreatePartitionEventType:
			eventWriter, err := writer.NextCreatePartitionEventWriter()
			if err != nil {
				return nil, err
			}
			if err = eventWriter.AddOneStringToPayload(req, true); err != nil {
				return nil, err
			}
			eventWriter.SetEventTimestamp(ts[pos], ts[pos])
		case DropPartitionEventType:
			eventWriter, err := writer.NextDropPartitionEventWriter()
			if err != nil {
				return nil, err
			}
			if err = eventWriter.AddOneStringToPayload(req, true); err != nil {
				return nil, err
			}
			eventWriter.SetEventTimestamp(ts[pos], ts[pos])
		}
	}
	writer.SetEventTimeStamp(ts[0], ts[len(ts)-1])

	// https://github.com/milvus-io/milvus/issues/9620
	writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	if err = writer.Finish(); err != nil {
		return nil, err
	}
	if buffer, err = writer.GetBuffer(); err != nil {
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
				binlogReader.Close()
				return nil, nil, err
			}
			if eventReader == nil {
				break
			}
			switch dataType {
			case schemapb.DataType_Int64:
				int64Ts, _, err := eventReader.GetInt64FromPayload()
				if err != nil {
					eventReader.Close()
					binlogReader.Close()
					return nil, nil, err
				}
				for _, singleTs := range int64Ts {
					resultTs = append(resultTs, Timestamp(singleTs))
				}
			case schemapb.DataType_String:
				stringPayload, _, err := eventReader.GetStringFromPayload()
				if err != nil {
					eventReader.Close()
					binlogReader.Close()
					return nil, nil, err
				}
				requestsStrings = append(requestsStrings, stringPayload...)
			}
			eventReader.Close()
		}
		binlogReader.Close()
	}

	return resultTs, requestsStrings, nil
}

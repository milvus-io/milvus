package storage

import (
	"fmt"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	ms "github.com/zilliztech/milvus-distributed/internal/master"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type (
	UniqueID  = typeutil.UniqueID
	FieldID   = typeutil.UniqueID
	Timestamp = typeutil.Timestamp
)

type Blob struct {
	key   string
	value []byte
}

type Base struct {
	Version  int
	CommitID int
	TenantID UniqueID
	Schema   *etcdpb.CollectionMeta
}

type FieldData interface{}

type BoolFieldData struct {
	NumRows int
	Data    []bool
}
type Int8FieldData struct {
	NumRows int
	Data    []int8
}
type Int16FieldData struct {
	NumRows int
	Data    []int16
}
type Int32FieldData struct {
	NumRows int
	Data    []int32
}
type Int64FieldData struct {
	NumRows int
	Data    []int64
}
type FloatFieldData struct {
	NumRows int
	Data    []float32
}
type DoubleFieldData struct {
	NumRows int
	Data    []float64
}
type StringFieldData struct {
	NumRows int
	Data    []string
}
type BinaryVectorFieldData struct {
	NumRows int
	Data    []byte
	Dim     int
}
type FloatVectorFieldData struct {
	NumRows int
	Data    []float32
	Dim     int
}

// system filed id:
// 0: unique row id
// 1: timestamp
// 100: first user field id
// 101: second user field id
// 102: ...

// example row_schema: {float_field, int_field, float_vector_field, string_field}
// Data {<0, row_id>, <1, timestamp>, <100, float_field>, <101, int_field>, <102, float_vector_field>, <103, string_field>}
type InsertData struct {
	Data map[FieldID]FieldData // field id to field data
}

// Blob key example:
// ${tenant}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/${log_idx}
type InsertCodec struct {
	Base
	readerCloseFunc []func() error
}

func (insertCodec *InsertCodec) Serialize(partitionID UniqueID, segmentID UniqueID, data *InsertData) ([]*Blob, error) {
	var blobs []*Blob
	var writer *InsertBinlogWriter
	var err error
	timeFieldData, ok := data.Data[ms.TimeStampField]
	if !ok {
		return nil, errors.New("data doesn't contains timestamp field")
	}
	ts := timeFieldData.(Int64FieldData).Data

	for _, field := range insertCodec.Schema.Schema.Fields {
		singleData := data.Data[field.FieldID]
		writer, err = NewInsertBinlogWriter(field.DataType, insertCodec.Schema.ID, partitionID, segmentID, field.FieldID)
		if err != nil {
			return nil, err
		}
		eventWriter, err := writer.NextInsertEventWriter()
		if err != nil {
			return nil, err
		}
		eventWriter.SetStartTimestamp(typeutil.Timestamp(ts[0]))
		eventWriter.SetEndTimestamp(typeutil.Timestamp(ts[len(ts)-1]))
		switch field.DataType {
		case schemapb.DataType_BOOL:
			err = eventWriter.AddBoolToPayload(singleData.(BoolFieldData).Data)
		case schemapb.DataType_INT8:
			err = eventWriter.AddInt8ToPayload(singleData.(Int8FieldData).Data)
		case schemapb.DataType_INT16:
			err = eventWriter.AddInt16ToPayload(singleData.(Int16FieldData).Data)
		case schemapb.DataType_INT32:
			err = eventWriter.AddInt32ToPayload(singleData.(Int32FieldData).Data)
		case schemapb.DataType_INT64:
			err = eventWriter.AddInt64ToPayload(singleData.(Int64FieldData).Data)
		case schemapb.DataType_FLOAT:
			err = eventWriter.AddFloatToPayload(singleData.(FloatFieldData).Data)
		case schemapb.DataType_DOUBLE:
			err = eventWriter.AddDoubleToPayload(singleData.(DoubleFieldData).Data)
		case schemapb.DataType_STRING:
			for _, singleString := range singleData.(StringFieldData).Data {
				err = eventWriter.AddOneStringToPayload(singleString)
				if err != nil {
					return nil, err
				}
			}
		case schemapb.DataType_VECTOR_BINARY:
			err = eventWriter.AddBinaryVectorToPayload(singleData.(BinaryVectorFieldData).Data, singleData.(BinaryVectorFieldData).Dim)
		case schemapb.DataType_VECTOR_FLOAT:
			err = eventWriter.AddFloatVectorToPayload(singleData.(FloatVectorFieldData).Data, singleData.(FloatVectorFieldData).Dim)
		default:
			return nil, errors.Errorf("undefined data type %d", field.DataType)
		}
		if err != nil {
			return nil, err
		}
		if writer == nil {
			return nil, errors.New("binlog writer is nil")
		}
		writer.SetStartTimeStamp(typeutil.Timestamp(ts[0]))
		writer.SetEndTimeStamp(typeutil.Timestamp(ts[len(ts)-1]))

		err = writer.Close()
		if err != nil {
			return nil, err
		}

		buffer, err := writer.GetBuffer()
		if err != nil {
			return nil, err
		}
		blobKey := fmt.Sprintf("%d", field.FieldID)
		blobs = append(blobs, &Blob{
			key:   blobKey,
			value: buffer,
		})

	}
	return blobs, nil

}
func (insertCodec *InsertCodec) Deserialize(blobs []*Blob) (partitionID UniqueID, segmentID UniqueID, data *InsertData, err error) {
	if len(blobs) == 0 {
		return -1, -1, nil, errors.New("blobs is empty")
	}
	readerClose := func(reader *BinlogReader) func() error {
		return func() error { return reader.Close() }
	}
	var resultData InsertData
	var pID UniqueID
	var sID UniqueID
	resultData.Data = make(map[FieldID]FieldData)
	for _, blob := range blobs {
		binlogReader, err := NewBinlogReader(blob.value)
		if err != nil {
			return -1, -1, nil, err
		}

		// read partitionID and SegmentID
		pID, sID = binlogReader.PartitionID, binlogReader.SegmentID

		dataType := binlogReader.PayloadDataType
		fieldID := binlogReader.FieldID
		switch dataType {
		case schemapb.DataType_BOOL:
			var boolFieldData BoolFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			boolFieldData.Data, err = eventReader.GetBoolFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			boolFieldData.NumRows = len(boolFieldData.Data)
			resultData.Data[fieldID] = boolFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_INT8:
			var int8FieldData Int8FieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			int8FieldData.Data, err = eventReader.GetInt8FromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			int8FieldData.NumRows = len(int8FieldData.Data)
			resultData.Data[fieldID] = int8FieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_INT16:
			var int16FieldData Int16FieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			int16FieldData.Data, err = eventReader.GetInt16FromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			int16FieldData.NumRows = len(int16FieldData.Data)
			resultData.Data[fieldID] = int16FieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_INT32:
			var int32FieldData Int32FieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			int32FieldData.Data, err = eventReader.GetInt32FromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			int32FieldData.NumRows = len(int32FieldData.Data)
			resultData.Data[fieldID] = int32FieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_INT64:
			var int64FieldData Int64FieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			int64FieldData.Data, err = eventReader.GetInt64FromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			int64FieldData.NumRows = len(int64FieldData.Data)
			resultData.Data[fieldID] = int64FieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_FLOAT:
			var floatFieldData FloatFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			floatFieldData.Data, err = eventReader.GetFloatFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			floatFieldData.NumRows = len(floatFieldData.Data)
			resultData.Data[fieldID] = floatFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_DOUBLE:
			var doubleFieldData DoubleFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			doubleFieldData.Data, err = eventReader.GetDoubleFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			doubleFieldData.NumRows = len(doubleFieldData.Data)
			resultData.Data[fieldID] = doubleFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_STRING:
			var stringFieldData StringFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			length, err := eventReader.GetPayloadLengthFromReader()
			stringFieldData.NumRows = length
			if err != nil {
				return -1, -1, nil, err
			}
			for i := 0; i < length; i++ {
				singleString, err := eventReader.GetOneStringFromPayload(i)
				if err != nil {
					return -1, -1, nil, err
				}
				stringFieldData.Data = append(stringFieldData.Data, singleString)
			}
			resultData.Data[fieldID] = stringFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_VECTOR_BINARY:
			var binaryVectorFieldData BinaryVectorFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			binaryVectorFieldData.Data, binaryVectorFieldData.Dim, err = eventReader.GetBinaryVectorFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			binaryVectorFieldData.NumRows = len(binaryVectorFieldData.Data)
			resultData.Data[fieldID] = binaryVectorFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_VECTOR_FLOAT:
			var floatVectorFieldData FloatVectorFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			floatVectorFieldData.Data, floatVectorFieldData.Dim, err = eventReader.GetFloatVectorFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			floatVectorFieldData.NumRows = len(floatVectorFieldData.Data) / 8
			resultData.Data[fieldID] = floatVectorFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		default:
			return -1, -1, nil, errors.Errorf("undefined data type %d", dataType)
		}
	}

	return pID, sID, &resultData, nil
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
// ${tenant}/data_definition_log/${collection_id}/${field_type}/${log_idx}
type DataDefinitionCodec struct {
	Base
	readerCloseFunc []func() error
}

func (dataDefinitionCodec *DataDefinitionCodec) Serialize(ts []Timestamp, ddRequests []string, eventTypes []EventTypeCode) ([]*Blob, error) {
	writer, err := NewDDLBinlogWriter(schemapb.DataType_STRING, dataDefinitionCodec.Schema.ID)
	if err != nil {
		return nil, err
	}

	var blobs []*Blob

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
			eventWriter.SetStartTimestamp(ts[pos])
			eventWriter.SetEndTimestamp(ts[pos])
		case DropCollectionEventType:
			eventWriter, err := writer.NextDropCollectionEventWriter()
			if err != nil {
				return nil, err
			}
			err = eventWriter.AddOneStringToPayload(req)
			eventWriter.SetStartTimestamp(ts[pos])
			eventWriter.SetEndTimestamp(ts[pos])
			if err != nil {
				return nil, err
			}
		case CreatePartitionEventType:
			eventWriter, err := writer.NextCreatePartitionEventWriter()
			if err != nil {
				return nil, err
			}
			err = eventWriter.AddOneStringToPayload(req)
			eventWriter.SetStartTimestamp(ts[pos])
			eventWriter.SetEndTimestamp(ts[pos])
			if err != nil {
				return nil, err
			}
		case DropPartitionEventType:
			eventWriter, err := writer.NextDropPartitionEventWriter()
			if err != nil {
				return nil, err
			}
			err = eventWriter.AddOneStringToPayload(req)
			eventWriter.SetStartTimestamp(ts[pos])
			eventWriter.SetEndTimestamp(ts[pos])
			if err != nil {
				return nil, err
			}
		}
	}
	writer.SetStartTimeStamp(ts[0])
	writer.SetEndTimeStamp(ts[len(ts)-1])
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	buffer, err := writer.GetBuffer()
	if err != nil {
		return nil, err
	}
	blobs = append(blobs, &Blob{
		key:   "",
		value: buffer,
	})

	writer, err = NewDDLBinlogWriter(schemapb.DataType_INT64, dataDefinitionCodec.Schema.ID)
	if err != nil {
		return nil, err
	}

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
	eventWriter.SetStartTimestamp(ts[0])
	eventWriter.SetEndTimestamp(ts[len(ts)-1])
	writer.SetStartTimeStamp(ts[0])
	writer.SetEndTimeStamp(ts[len(ts)-1])
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	buffer, err = writer.GetBuffer()
	if err != nil {
		return nil, err
	}
	blobs = append(blobs, &Blob{
		key:   "",
		value: buffer,
	})

	return blobs, nil

}

func (dataDefinitionCodec *DataDefinitionCodec) Deserialize(blobs []*Blob) (ts []Timestamp, ddRequests []string, err error) {
	if len(blobs) == 0 {
		return nil, nil, errors.New("blobs is empty")
	}
	readerClose := func(reader *BinlogReader) func() error {
		return func() error { return reader.Close() }
	}
	var requestsStrings []string
	var resultTs []Timestamp
	for _, blob := range blobs {
		binlogReader, err := NewBinlogReader(blob.value)
		if err != nil {
			return nil, nil, err
		}
		dataType := binlogReader.PayloadDataType

		switch dataType {
		case schemapb.DataType_INT64:
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return nil, nil, err
			}
			int64Ts, err := eventReader.GetInt64FromPayload()
			if err != nil {
				return nil, nil, err
			}
			for _, singleTs := range int64Ts {
				resultTs = append(resultTs, Timestamp(singleTs))
			}
			dataDefinitionCodec.readerCloseFunc = append(dataDefinitionCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_STRING:
			binlogReader, err := NewBinlogReader(blob.value)
			if err != nil {
				return nil, nil, err
			}
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return nil, nil, err
			}
			for eventReader != nil {
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
				eventReader, err = binlogReader.NextEventReader()
				if err != nil {
					return nil, nil, err
				}
			}
			dataDefinitionCodec.readerCloseFunc = append(dataDefinitionCodec.readerCloseFunc, readerClose(binlogReader))
		}

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
	Base
}

func (indexCodec *IndexCodec) Serialize(blobs []*Blob) ([]*Blob, error) {
	return blobs, nil
}

func (indexCodec *IndexCodec) Deserialize(blobs []*Blob) ([]*Blob, error) {
	return blobs, nil
}

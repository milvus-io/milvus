package storage

import (
	"fmt"

	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const (
	TsField  int64 = 1
	DDLField int64 = 2
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
	data    []bool
}
type Int8FieldData struct {
	NumRows int
	data    []int8
}
type Int16FieldData struct {
	NumRows int
	data    []int16
}
type Int32FieldData struct {
	NumRows int
	data    []int32
}
type Int64FieldData struct {
	NumRows int
	data    []int64
}
type FloatFieldData struct {
	NumRows int
	data    []float32
}
type DoubleFieldData struct {
	NumRows int
	data    []float64
}
type StringFieldData struct {
	NumRows int
	data    []string
}
type BinaryVectorFieldData struct {
	NumRows int
	data    []byte
	dim     int
}
type FloatVectorFieldData struct {
	NumRows int
	data    []float32
	dim     int
}

// system filed id:
// 0: unique row id
// 1: timestamp
// 2: ddl
// 100: first user field id
// 101: second user field id
// 102: ...

// example row_schema: {float_field, int_field, float_vector_field, string_field}
// Data {<0, row_id>, <1, timestamp>, <100, float_field>, <101, int_field>, <102, float_vector_field>, <103, string_field>}
type InsertData struct {
	Data map[FieldID]FieldData // field id to field data
}

// Blob key example:
// ${segment_id}/${field_id}
type InsertCodec struct {
	Base
	readerCloseFunc []func() error
}

func (insertCodec *InsertCodec) Serialize(partitionID UniqueID, segmentID UniqueID, data *InsertData) ([]*Blob, error) {
	var blobs []*Blob
	var writer *InsertBinlogWriter
	var err error
	ts := (data.Data[1]).(Int64FieldData).data

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
			err = eventWriter.AddBoolToPayload(singleData.(BoolFieldData).data)
		case schemapb.DataType_INT8:
			err = eventWriter.AddInt8ToPayload(singleData.(Int8FieldData).data)
		case schemapb.DataType_INT16:
			err = eventWriter.AddInt16ToPayload(singleData.(Int16FieldData).data)
		case schemapb.DataType_INT32:
			err = eventWriter.AddInt32ToPayload(singleData.(Int32FieldData).data)
		case schemapb.DataType_INT64:
			err = eventWriter.AddInt64ToPayload(singleData.(Int64FieldData).data)
		case schemapb.DataType_FLOAT:
			err = eventWriter.AddFloatToPayload(singleData.(FloatFieldData).data)
		case schemapb.DataType_DOUBLE:
			err = eventWriter.AddDoubleToPayload(singleData.(DoubleFieldData).data)
		case schemapb.DataType_STRING:
			for _, singleString := range singleData.(StringFieldData).data {
				err = eventWriter.AddOneStringToPayload(singleString)
				if err != nil {
					return nil, err
				}
			}
		case schemapb.DataType_VECTOR_BINARY:
			err = eventWriter.AddBinaryVectorToPayload(singleData.(BinaryVectorFieldData).data, singleData.(BinaryVectorFieldData).dim)
		case schemapb.DataType_VECTOR_FLOAT:
			err = eventWriter.AddFloatVectorToPayload(singleData.(FloatVectorFieldData).data, singleData.(FloatVectorFieldData).dim)
		}
		if err != nil {
			return nil, err
		}
		if writer == nil {
			return nil, fmt.Errorf("binlog writer is nil")
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
		blobKey := fmt.Sprintf("%d/%d", segmentID, field.FieldID)
		blobs = append(blobs, &Blob{
			key:   blobKey,
			value: buffer,
		})

	}
	return blobs, nil

}
func (insertCodec *InsertCodec) Deserialize(blobs []*Blob) (partitionID UniqueID, segmentID UniqueID, data *InsertData, err error) {
	if len(blobs) == 0 {
		return -1, -1, nil, fmt.Errorf("blobs is empty")
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
			boolFieldData.data, err = eventReader.GetBoolFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			boolFieldData.NumRows = len(boolFieldData.data)
			resultData.Data[fieldID] = boolFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_INT8:
			var int8FieldData Int8FieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			int8FieldData.data, err = eventReader.GetInt8FromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			int8FieldData.NumRows = len(int8FieldData.data)
			resultData.Data[fieldID] = int8FieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_INT16:
			var int16FieldData Int16FieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			int16FieldData.data, err = eventReader.GetInt16FromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			int16FieldData.NumRows = len(int16FieldData.data)
			resultData.Data[fieldID] = int16FieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_INT32:
			var int32FieldData Int32FieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			int32FieldData.data, err = eventReader.GetInt32FromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			int32FieldData.NumRows = len(int32FieldData.data)
			resultData.Data[fieldID] = int32FieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_INT64:
			var int64FieldData Int64FieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			int64FieldData.data, err = eventReader.GetInt64FromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			int64FieldData.NumRows = len(int64FieldData.data)
			resultData.Data[fieldID] = int64FieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_FLOAT:
			var floatFieldData FloatFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			floatFieldData.data, err = eventReader.GetFloatFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			floatFieldData.NumRows = len(floatFieldData.data)
			resultData.Data[fieldID] = floatFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_DOUBLE:
			var doubleFieldData DoubleFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			doubleFieldData.data, err = eventReader.GetDoubleFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			doubleFieldData.NumRows = len(doubleFieldData.data)
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
				stringFieldData.data = append(stringFieldData.data, singleString)
			}
			resultData.Data[fieldID] = stringFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_VECTOR_BINARY:
			var binaryVectorFieldData BinaryVectorFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			binaryVectorFieldData.data, binaryVectorFieldData.dim, err = eventReader.GetBinaryVectorFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			binaryVectorFieldData.NumRows = len(binaryVectorFieldData.data)
			resultData.Data[fieldID] = binaryVectorFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
		case schemapb.DataType_VECTOR_FLOAT:
			var floatVectorFieldData FloatVectorFieldData
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				return -1, -1, nil, err
			}
			floatVectorFieldData.data, floatVectorFieldData.dim, err = eventReader.GetFloatVectorFromPayload()
			if err != nil {
				return -1, -1, nil, err
			}
			floatVectorFieldData.NumRows = len(floatVectorFieldData.data) / 8
			resultData.Data[fieldID] = floatVectorFieldData
			insertCodec.readerCloseFunc = append(insertCodec.readerCloseFunc, readerClose(binlogReader))
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
// ${collection_id}
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
	writer.FieldID = DDLField
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
	blobKey := fmt.Sprintf("%d/%d", dataDefinitionCodec.Schema.ID, DDLField)
	blobs = append(blobs, &Blob{
		key:   blobKey,
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
	writer.FieldID = TsField
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	buffer, err = writer.GetBuffer()
	if err != nil {
		return nil, err
	}
	blobKey = fmt.Sprintf("%d/%d", dataDefinitionCodec.Schema.ID, TsField)
	blobs = append(blobs, &Blob{
		key:   blobKey,
		value: buffer,
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
	for _, blob := range blobs {
		binlogReader, err := NewBinlogReader(blob.value)
		if err != nil {
			return nil, nil, err
		}
		fieldID := binlogReader.FieldID

		switch fieldID {
		case TsField:
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
		case DDLField:
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

type IndexCodec struct {
	Base
}

func (indexCodec *IndexCodec) Serialize(blobs []*Blob) ([]*Blob, error) {
	return blobs, nil
}

func (indexCodec *IndexCodec) Deserialize(blobs []*Blob) ([]*Blob, error) {
	return blobs, nil
}

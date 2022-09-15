package storage

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/file"

	"github.com/milvus-io/milvus/api/schemapb"
)

// PayloadReader reads data from payload
type PayloadReader struct {
	reader  *file.Reader
	colType schemapb.DataType
	numRows int64
}

func NewPayloadReader(colType schemapb.DataType, buf []byte) (*PayloadReader, error) {
	if len(buf) == 0 {
		return nil, errors.New("create Payload reader failed, buffer is empty")
	}
	parquetReader, err := file.NewParquetReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	return &PayloadReader{reader: parquetReader, colType: colType, numRows: parquetReader.NumRows()}, nil
}

// GetDataFromPayload returns data,length from payload, returns err if failed
// Return:
//      `interface{}`: all types.
//      `int`: dim, only meaningful to FLOAT/BINARY VECTOR type.
//      `error`: error.
func (r *PayloadReader) GetDataFromPayload() (interface{}, int, error) {
	switch r.colType {
	case schemapb.DataType_Bool:
		val, err := r.GetBoolFromPayload()
		return val, 0, err
	case schemapb.DataType_Int8:
		val, err := r.GetInt8FromPayload()
		return val, 0, err
	case schemapb.DataType_Int16:
		val, err := r.GetInt16FromPayload()
		return val, 0, err
	case schemapb.DataType_Int32:
		val, err := r.GetInt32FromPayload()
		return val, 0, err
	case schemapb.DataType_Int64:
		val, err := r.GetInt64FromPayload()
		return val, 0, err
	case schemapb.DataType_Float:
		val, err := r.GetFloatFromPayload()
		return val, 0, err
	case schemapb.DataType_Double:
		val, err := r.GetDoubleFromPayload()
		return val, 0, err
	case schemapb.DataType_BinaryVector:
		return r.GetBinaryVectorFromPayload()
	case schemapb.DataType_FloatVector:
		return r.GetFloatVectorFromPayload()
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		val, err := r.GetStringFromPayload()
		return val, 0, err
	default:
		return nil, 0, errors.New("unknown type")
	}
}

// ReleasePayloadReader release payload reader.
func (r *PayloadReader) ReleasePayloadReader() {
	r.Close()
}

// GetBoolFromPayload returns bool slice from payload.
func (r *PayloadReader) GetBoolFromPayload() ([]bool, error) {
	if r.colType != schemapb.DataType_Bool {
		return nil, fmt.Errorf("failed to get bool from datatype %v", r.colType.String())
	}
	reader, ok := r.reader.RowGroup(0).Column(0).(*file.BooleanColumnChunkReader)
	if !ok {
		return nil, fmt.Errorf("expect type *file.BooleanColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}

	values := make([]bool, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}
	return values, nil
}

// GetByteFromPayload returns byte slice from payload
func (r *PayloadReader) GetByteFromPayload() ([]byte, error) {
	if r.colType != schemapb.DataType_Int8 {
		return nil, fmt.Errorf("failed to get byte from datatype %v", r.colType.String())
	}
	reader, ok := r.reader.RowGroup(0).Column(0).(*file.Int32ColumnChunkReader)
	if !ok {
		return nil, fmt.Errorf("expect type *file.Int32ColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}

	values := make([]int32, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}

	ret := make([]byte, r.numRows)
	for i := int64(0); i < r.numRows; i++ {
		ret[i] = byte(values[i])
	}
	return ret, nil
}

// GetInt8FromPayload returns int8 slice from payload
func (r *PayloadReader) GetInt8FromPayload() ([]int8, error) {
	if r.colType != schemapb.DataType_Int8 {
		return nil, fmt.Errorf("failed to get int8 from datatype %v", r.colType.String())
	}

	reader, ok := r.reader.RowGroup(0).Column(0).(*file.Int32ColumnChunkReader)
	if !ok {
		return nil, fmt.Errorf("expect type *file.Int32ColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}
	values := make([]int32, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}

	ret := make([]int8, r.numRows)
	for i := int64(0); i < r.numRows; i++ {
		ret[i] = int8(values[i])
	}
	return ret, nil
}

func (r *PayloadReader) GetInt16FromPayload() ([]int16, error) {
	if r.colType != schemapb.DataType_Int16 {
		return nil, fmt.Errorf("failed to get int16 from datatype %v", r.colType.String())
	}

	reader, ok := r.reader.RowGroup(0).Column(0).(*file.Int32ColumnChunkReader)
	if !ok {
		return nil, fmt.Errorf("expect type *file.Int32ColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}
	values := make([]int32, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}

	ret := make([]int16, r.numRows)
	for i := int64(0); i < r.numRows; i++ {
		ret[i] = int16(values[i])
	}
	return ret, nil
}

func (r *PayloadReader) GetInt32FromPayload() ([]int32, error) {
	if r.colType != schemapb.DataType_Int32 {
		return nil, fmt.Errorf("failed to get int32 from datatype %v", r.colType.String())
	}

	reader, ok := r.reader.RowGroup(0).Column(0).(*file.Int32ColumnChunkReader)
	if !ok {
		return nil, fmt.Errorf("expect type *file.Int32ColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}
	values := make([]int32, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}

	return values, nil
}

func (r *PayloadReader) GetInt64FromPayload() ([]int64, error) {
	if r.colType != schemapb.DataType_Int64 {
		return nil, fmt.Errorf("failed to get int64 from datatype %v", r.colType.String())
	}
	reader, ok := r.reader.RowGroup(0).Column(0).(*file.Int64ColumnChunkReader)
	if !ok {
		return nil, fmt.Errorf("expect type *file.Int64ColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}

	values := make([]int64, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}
	return values, nil
}

func (r *PayloadReader) GetFloatFromPayload() ([]float32, error) {
	if r.colType != schemapb.DataType_Float {
		return nil, fmt.Errorf("failed to get float32 from datatype %v", r.colType.String())
	}
	reader, ok := r.reader.RowGroup(0).Column(0).(*file.Float32ColumnChunkReader)
	if !ok {
		return nil, fmt.Errorf("expect type *file.Float32ColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}

	values := make([]float32, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}
	return values, nil
}

func (r *PayloadReader) GetDoubleFromPayload() ([]float64, error) {
	if r.colType != schemapb.DataType_Double {
		return nil, fmt.Errorf("failed to get float32 from datatype %v", r.colType.String())
	}
	reader, ok := r.reader.RowGroup(0).Column(0).(*file.Float64ColumnChunkReader)
	if !ok {
		return nil, fmt.Errorf("expect type *file.Float64ColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}

	values := make([]float64, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}
	return values, nil
}

func (r *PayloadReader) GetStringFromPayload() ([]string, error) {
	if r.colType != schemapb.DataType_String && r.colType != schemapb.DataType_VarChar {
		return nil, fmt.Errorf("failed to get string from datatype %v", r.colType.String())
	}

	reader, ok := r.reader.RowGroup(0).Column(0).(*file.ByteArrayColumnChunkReader)
	if !ok {
		return nil, fmt.Errorf("expect type *file.ByteArrayColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}
	values := make([]parquet.ByteArray, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}

	ret := make([]string, r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		ret[i] = values[i].String()
	}
	return ret, nil
}

// GetBinaryVectorFromPayload returns vector, dimension, error
func (r *PayloadReader) GetBinaryVectorFromPayload() ([]byte, int, error) {
	if r.colType != schemapb.DataType_BinaryVector {
		return nil, -1, fmt.Errorf("failed to get binary vector from datatype %v", r.colType.String())
	}

	reader, ok := r.reader.RowGroup(0).Column(0).(*file.FixedLenByteArrayColumnChunkReader)
	if !ok {
		return nil, -1, fmt.Errorf("expect type *file.FixedLenByteArrayColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}

	dim := r.reader.RowGroup(0).Column(0).Descriptor().TypeLength()
	values := make([]parquet.FixedLenByteArray, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, -1, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, -1, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}
	ret := make([]byte, int64(dim)*r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		copy(ret[i*dim:(i+1)*dim], values[i])
	}
	return ret, dim * 8, nil
}

// GetFloatVectorFromPayload returns vector, dimension, error
func (r *PayloadReader) GetFloatVectorFromPayload() ([]float32, int, error) {
	if r.colType != schemapb.DataType_FloatVector {
		return nil, -1, fmt.Errorf("failed to get float vector from datatype %v", r.colType.String())
	}
	reader, ok := r.reader.RowGroup(0).Column(0).(*file.FixedLenByteArrayColumnChunkReader)
	if !ok {
		return nil, -1, fmt.Errorf("expect type *file.FixedLenByteArrayColumnChunkReader, but got %T", r.reader.RowGroup(0).Column(0))
	}

	dim := r.reader.RowGroup(0).Column(0).Descriptor().TypeLength() / 4
	values := make([]parquet.FixedLenByteArray, r.numRows)
	total, valuesRead, err := reader.ReadBatch(r.numRows, values, nil, nil)
	if err != nil {
		return nil, -1, err
	}
	if total != r.numRows || int64(valuesRead) != r.numRows {
		return nil, -1, fmt.Errorf("expect %d rows, but got total = %d and valuesRead = %d", r.numRows, total, valuesRead)
	}
	ret := make([]float32, int64(dim)*r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		copy(arrow.Float32Traits.CastToBytes(ret[i*dim:(i+1)*dim]), values[i])
	}
	return ret, dim, nil
}

func (r *PayloadReader) GetPayloadLengthFromReader() (int, error) {
	return int(r.numRows), nil
}

// Close closes the payload reader
func (r *PayloadReader) Close() {
	r.reader.Close()
}

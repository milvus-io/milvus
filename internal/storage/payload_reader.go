package storage

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
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
	case schemapb.DataType_String:
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
	dumper, err := r.createDumper()
	if err != nil {
		return nil, err
	}
	ret := make([]bool, r.numRows)
	var i int64
	for i = 0; i < r.numRows; i++ {
		v, hasValue := dumper.Next()
		if !hasValue {
			return nil, fmt.Errorf("unmatched row number: expect %v, actual %v", r.numRows, i)
		}
		ret[i] = v.(bool)
	}
	return ret, nil
}

// GetByteFromPayload returns byte slice from payload
func (r *PayloadReader) GetByteFromPayload() ([]byte, error) {
	if r.colType != schemapb.DataType_Int8 {
		return nil, fmt.Errorf("failed to get byte from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, err
	}
	ret := make([]byte, r.numRows)
	var i int64
	for i = 0; i < r.numRows; i++ {
		v, hasValue := dumper.Next()
		if !hasValue {
			return nil, fmt.Errorf("unmatched row number: expect %v, actual %v", r.numRows, i)
		}
		ret[i] = byte(v.(int32))
	}
	return ret, nil
}

// GetInt8FromPayload returns int8 slice from payload
func (r *PayloadReader) GetInt8FromPayload() ([]int8, error) {
	if r.colType != schemapb.DataType_Int8 {
		return nil, fmt.Errorf("failed to get int8 from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, err
	}
	ret := make([]int8, r.numRows)
	var i int64
	for i = 0; i < r.numRows; i++ {
		v, hasValue := dumper.Next()
		if !hasValue {
			return nil, fmt.Errorf("unmatched row number: expect %v, actual %v", r.numRows, i)
		}
		// need to trasfer because parquet didn't support int8
		ret[i] = int8(v.(int32))
	}
	return ret, nil
}

func (r *PayloadReader) GetInt16FromPayload() ([]int16, error) {
	if r.colType != schemapb.DataType_Int16 {
		return nil, fmt.Errorf("failed to get int16 from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, err
	}
	ret := make([]int16, r.numRows)
	var i int64
	for i = 0; i < r.numRows; i++ {
		v, hasValue := dumper.Next()
		if !hasValue {
			return nil, fmt.Errorf("unmatched row number: expect %v, actual %v", r.numRows, i)
		}
		// need to trasfer because parquet didn't support int16
		ret[i] = int16(v.(int32))
	}
	return ret, nil
}

func (r *PayloadReader) GetInt32FromPayload() ([]int32, error) {
	if r.colType != schemapb.DataType_Int32 {
		return nil, fmt.Errorf("failed to get int32 from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, err
	}
	ret := make([]int32, r.numRows)
	var i int64
	for i = 0; i < r.numRows; i++ {
		v, hasValue := dumper.Next()
		if !hasValue {
			return nil, fmt.Errorf("unmatched row number: expect %v, actual %v", r.numRows, i)
		}
		ret[i] = v.(int32)
	}
	return ret, nil
}

func (r *PayloadReader) GetInt64FromPayload() ([]int64, error) {
	if r.colType != schemapb.DataType_Int64 {
		return nil, fmt.Errorf("failed to get int64 from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, err
	}
	ret := make([]int64, r.numRows)
	var i int64
	for i = 0; i < r.numRows; i++ {
		v, hasValue := dumper.Next()
		if !hasValue {
			return nil, fmt.Errorf("unmatched row number: expect %v, actual %v", r.numRows, i)
		}
		ret[i] = v.(int64)
	}
	return ret, nil
}

func (r *PayloadReader) GetFloatFromPayload() ([]float32, error) {
	if r.colType != schemapb.DataType_Float {
		return nil, fmt.Errorf("failed to get float32 from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, err
	}
	ret := make([]float32, r.numRows)
	var i int64
	for i = 0; i < r.numRows; i++ {
		v, hasValue := dumper.Next()
		if !hasValue {
			return nil, fmt.Errorf("unmatched row number: expect %v, actual %v", r.numRows, i)
		}
		ret[i] = v.(float32)
	}
	return ret, nil
}

func (r *PayloadReader) GetDoubleFromPayload() ([]float64, error) {
	if r.colType != schemapb.DataType_Double {
		return nil, fmt.Errorf("failed to get double from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, err
	}
	ret := make([]float64, r.numRows)
	var i int64
	for i = 0; i < r.numRows; i++ {
		v, hasValue := dumper.Next()
		if !hasValue {
			return nil, fmt.Errorf("unmatched row number: expect %v, actual %v", r.numRows, i)
		}
		ret[i] = v.(float64)
	}
	return ret, nil
}

func (r *PayloadReader) GetStringFromPayload() ([]string, error) {
	if r.colType != schemapb.DataType_String && r.colType != schemapb.DataType_VarChar {
		return nil, fmt.Errorf("failed to get string from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, err
	}
	ret := make([]string, r.numRows)
	var i int64
	for i = 0; i < r.numRows; i++ {
		v, hasValue := dumper.Next()
		if !hasValue {
			return nil, fmt.Errorf("unmatched row number: expect %v, actual %v", r.numRows, i)
		}
		ret[i] = v.(parquet.ByteArray).String()
	}
	return ret, nil
}

// GetBinaryVectorFromPayload returns vector, dimension, error
func (r *PayloadReader) GetBinaryVectorFromPayload() ([]byte, int, error) {
	if r.colType != schemapb.DataType_BinaryVector {
		return nil, -1, fmt.Errorf("failed to get binary vector from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, -1, err
	}

	dim := r.reader.RowGroup(0).Column(0).Descriptor().TypeLength()
	ret := make([]byte, int64(dim)*r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		v, ok := dumper.Next()
		if !ok {
			return nil, -1, fmt.Errorf("unmatched row number: row %v, dim %v", r.numRows, dim)
		}
		parquetArray := v.(parquet.FixedLenByteArray)
		copy(ret[i*dim:(i+1)*dim], parquetArray)
	}
	return ret, dim * 8, nil
}

// GetFloatVectorFromPayload returns vector, dimension, error
func (r *PayloadReader) GetFloatVectorFromPayload() ([]float32, int, error) {
	if r.colType != schemapb.DataType_FloatVector {
		return nil, -1, fmt.Errorf("failed to get float vector from datatype %v", r.colType.String())
	}
	dumper, err := r.createDumper()
	if err != nil {
		return nil, -1, err
	}

	dim := r.reader.RowGroup(0).Column(0).Descriptor().TypeLength() / 4
	ret := make([]float32, int64(dim)*r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		v, ok := dumper.Next()
		if !ok {
			return nil, -1, fmt.Errorf("unmatched row number: row %v, dim %v", r.numRows, dim)
		}
		parquetArray := v.(parquet.FixedLenByteArray)
		copy(arrow.Float32Traits.CastToBytes(ret[i*dim:(i+1)*dim]), parquetArray)
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

type Dumper struct {
	reader         file.ColumnChunkReader
	batchSize      int64
	valueOffset    int
	valuesBuffered int

	levelOffset    int64
	levelsBuffered int64
	defLevels      []int16
	repLevels      []int16

	valueBuffer interface{}
}

func (r *PayloadReader) createDumper() (*Dumper, error) {
	var valueBuffer interface{}
	switch r.reader.RowGroup(0).Column(0).(type) {
	case *file.BooleanColumnChunkReader:
		if r.colType != schemapb.DataType_Bool {
			return nil, errors.New("incorrect data type")
		}
		valueBuffer = make([]bool, r.numRows)
	case *file.Int32ColumnChunkReader:
		if r.colType != schemapb.DataType_Int32 && r.colType != schemapb.DataType_Int16 && r.colType != schemapb.DataType_Int8 {
			return nil, fmt.Errorf("incorrect data type, expect int32/int16/int8 but find %v", r.colType.String())
		}
		valueBuffer = make([]int32, r.numRows)
	case *file.Int64ColumnChunkReader:
		if r.colType != schemapb.DataType_Int64 {
			return nil, fmt.Errorf("incorrect data type, expect int64 but find %v", r.colType.String())
		}
		valueBuffer = make([]int64, r.numRows)
	case *file.Float32ColumnChunkReader:
		if r.colType != schemapb.DataType_Float {
			return nil, fmt.Errorf("incorrect data type, expect float32 but find %v", r.colType.String())
		}
		valueBuffer = make([]float32, r.numRows)
	case *file.Float64ColumnChunkReader:
		if r.colType != schemapb.DataType_Double {
			return nil, fmt.Errorf("incorrect data type, expect float64 but find %v", r.colType.String())
		}
		valueBuffer = make([]float64, r.numRows)
	case *file.ByteArrayColumnChunkReader:
		if r.colType != schemapb.DataType_String && r.colType != schemapb.DataType_VarChar {
			return nil, fmt.Errorf("incorrect data type, expect string/varchar but find %v", r.colType.String())
		}
		valueBuffer = make([]parquet.ByteArray, r.numRows)
	case *file.FixedLenByteArrayColumnChunkReader:
		if r.colType != schemapb.DataType_FloatVector && r.colType != schemapb.DataType_BinaryVector {
			return nil, fmt.Errorf("incorrect data type, expect floavector/binaryvector but find %v", r.colType.String())
		}
		valueBuffer = make([]parquet.FixedLenByteArray, r.numRows)
	}

	return &Dumper{
		reader:      r.reader.RowGroup(0).Column(0),
		batchSize:   r.numRows,
		defLevels:   make([]int16, r.numRows),
		repLevels:   make([]int16, r.numRows),
		valueBuffer: valueBuffer,
	}, nil
}

func (dump *Dumper) readNextBatch() {
	switch reader := dump.reader.(type) {
	case *file.BooleanColumnChunkReader:
		values := dump.valueBuffer.([]bool)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int32ColumnChunkReader:
		values := dump.valueBuffer.([]int32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int64ColumnChunkReader:
		values := dump.valueBuffer.([]int64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float32ColumnChunkReader:
		values := dump.valueBuffer.([]float32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float64ColumnChunkReader:
		values := dump.valueBuffer.([]float64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int96ColumnChunkReader:
		values := dump.valueBuffer.([]parquet.Int96)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.ByteArrayColumnChunkReader:
		values := dump.valueBuffer.([]parquet.ByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.FixedLenByteArrayColumnChunkReader:
		values := dump.valueBuffer.([]parquet.FixedLenByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	}

	dump.valueOffset = 0
	dump.levelOffset = 0
}

func (dump *Dumper) hasNext() bool {
	return dump.levelOffset < dump.levelsBuffered || dump.reader.HasNext()
}

func (dump *Dumper) Next() (interface{}, bool) {
	if dump.levelOffset == dump.levelsBuffered {
		if !dump.hasNext() {
			return nil, false
		}
		dump.readNextBatch()
		if dump.levelsBuffered == 0 {
			return nil, false
		}
	}

	defLevel := dump.defLevels[int(dump.levelOffset)]
	// repLevel := dump.repLevels[int(dump.levelOffset)]
	dump.levelOffset++

	if defLevel < dump.reader.Descriptor().MaxDefinitionLevel() {
		return nil, true
	}

	vb := reflect.ValueOf(dump.valueBuffer)
	v := vb.Index(dump.valueOffset).Interface()
	dump.valueOffset++

	return v, true
}

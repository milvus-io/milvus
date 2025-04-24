package storage

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// PayloadReader reads data from payload
type PayloadReader struct {
	reader   *file.Reader
	colType  schemapb.DataType
	numRows  int64
	nullable bool
}

var _ PayloadReaderInterface = (*PayloadReader)(nil)

func NewPayloadReader(colType schemapb.DataType, buf []byte, nullable bool) (*PayloadReader, error) {
	if len(buf) == 0 {
		return nil, errors.New("create Payload reader failed, buffer is empty")
	}
	parquetReader, err := file.NewParquetReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	return &PayloadReader{reader: parquetReader, colType: colType, numRows: parquetReader.NumRows(), nullable: nullable}, nil
}

// GetDataFromPayload returns data,length from payload, returns err if failed
// Return:
//

//		`interface{}`: all types.
//	 `[]bool`: validData, only meaningful to ScalarField.
//		`int`: dim, only meaningful to FLOAT/BINARY VECTOR type.
//		`error`: error.
func (r *PayloadReader) GetDataFromPayload() (interface{}, []bool, int, error) {
	switch r.colType {
	case schemapb.DataType_Bool:
		val, validData, err := r.GetBoolFromPayload()
		return val, validData, 0, err
	case schemapb.DataType_Int8:
		val, validData, err := r.GetInt8FromPayload()
		return val, validData, 0, err
	case schemapb.DataType_Int16:
		val, validData, err := r.GetInt16FromPayload()
		return val, validData, 0, err
	case schemapb.DataType_Int32:
		val, validData, err := r.GetInt32FromPayload()
		return val, validData, 0, err
	case schemapb.DataType_Int64:
		val, validData, err := r.GetInt64FromPayload()
		return val, validData, 0, err
	case schemapb.DataType_Float:
		val, validData, err := r.GetFloatFromPayload()
		return val, validData, 0, err
	case schemapb.DataType_Double:
		val, validData, err := r.GetDoubleFromPayload()
		return val, validData, 0, err
	case schemapb.DataType_BinaryVector:
		val, dim, err := r.GetBinaryVectorFromPayload()
		return val, nil, dim, err
	case schemapb.DataType_FloatVector:
		val, dim, err := r.GetFloatVectorFromPayload()
		return val, nil, dim, err
	case schemapb.DataType_Float16Vector:
		val, dim, err := r.GetFloat16VectorFromPayload()
		return val, nil, dim, err
	case schemapb.DataType_BFloat16Vector:
		val, dim, err := r.GetBFloat16VectorFromPayload()
		return val, nil, dim, err
	case schemapb.DataType_SparseFloatVector:
		val, dim, err := r.GetSparseFloatVectorFromPayload()
		return val, nil, dim, err
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		val, validData, err := r.GetStringFromPayload()
		return val, validData, 0, err
	case schemapb.DataType_Array:
		val, validData, err := r.GetArrayFromPayload()
		return val, validData, 0, err
	case schemapb.DataType_JSON:
		val, validData, err := r.GetJSONFromPayload()
		return val, validData, 0, err
	default:
		return nil, nil, 0, merr.WrapErrParameterInvalidMsg("unknown type")
	}
}

// ReleasePayloadReader release payload reader.
func (r *PayloadReader) ReleasePayloadReader() error {
	return r.Close()
}

// GetBoolFromPayload returns bool slice from payload.
func (r *PayloadReader) GetBoolFromPayload() ([]bool, []bool, error) {
	if r.colType != schemapb.DataType_Bool {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get bool from datatype %v", r.colType.String()))
	}

	values := make([]bool, r.numRows)

	if r.nullable {
		validData := make([]bool, r.numRows)
		valuesRead, err := ReadData[bool, *array.Boolean](r.reader, values, validData, r.numRows)
		if err != nil {
			return nil, nil, err
		}
		if valuesRead != r.numRows {
			return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
		}
		return values, validData, nil
	}
	valuesRead, err := ReadDataFromAllRowGroups[bool, *file.BooleanColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, nil, err
	}
	if valuesRead != r.numRows {
		return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
	}
	return values, nil, nil
}

// GetByteFromPayload returns byte slice from payload
func (r *PayloadReader) GetByteFromPayload() ([]byte, []bool, error) {
	if r.colType != schemapb.DataType_Int8 {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get byte from datatype %v", r.colType.String()))
	}

	if r.nullable {
		values := make([]int32, r.numRows)
		validData := make([]bool, r.numRows)
		valuesRead, err := ReadData[int32, *array.Int32](r.reader, values, validData, r.numRows)
		if err != nil {
			return nil, nil, err
		}
		if valuesRead != r.numRows {
			return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
		}
		ret := make([]byte, r.numRows)
		for i := int64(0); i < r.numRows; i++ {
			ret[i] = byte(values[i])
		}
		return ret, validData, nil
	}
	values := make([]int32, r.numRows)
	valuesRead, err := ReadDataFromAllRowGroups[int32, *file.Int32ColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, nil, err
	}

	if valuesRead != r.numRows {
		return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
	}

	ret := make([]byte, r.numRows)
	for i := int64(0); i < r.numRows; i++ {
		ret[i] = byte(values[i])
	}
	return ret, nil, nil
}

func (r *PayloadReader) GetInt8FromPayload() ([]int8, []bool, error) {
	if r.colType != schemapb.DataType_Int8 {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get int8 from datatype %v", r.colType.String()))
	}

	if r.nullable {
		values := make([]int8, r.numRows)
		validData := make([]bool, r.numRows)
		valuesRead, err := ReadData[int8, *array.Int8](r.reader, values, validData, r.numRows)
		if err != nil {
			return nil, nil, err
		}

		if valuesRead != r.numRows {
			return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
		}

		return values, validData, nil
	}
	values := make([]int32, r.numRows)
	valuesRead, err := ReadDataFromAllRowGroups[int32, *file.Int32ColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, nil, err
	}

	if valuesRead != r.numRows {
		return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
	}

	ret := make([]int8, r.numRows)
	for i := int64(0); i < r.numRows; i++ {
		ret[i] = int8(values[i])
	}
	return ret, nil, nil
}

func (r *PayloadReader) GetInt16FromPayload() ([]int16, []bool, error) {
	if r.colType != schemapb.DataType_Int16 {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get int16 from datatype %v", r.colType.String()))
	}

	if r.nullable {
		values := make([]int16, r.numRows)
		validData := make([]bool, r.numRows)
		valuesRead, err := ReadData[int16, *array.Int16](r.reader, values, validData, r.numRows)
		if err != nil {
			return nil, nil, err
		}

		if valuesRead != r.numRows {
			return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
		}
		return values, validData, nil
	}
	values := make([]int32, r.numRows)
	valuesRead, err := ReadDataFromAllRowGroups[int32, *file.Int32ColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, nil, err
	}

	if valuesRead != r.numRows {
		return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
	}

	ret := make([]int16, r.numRows)
	for i := int64(0); i < r.numRows; i++ {
		ret[i] = int16(values[i])
	}
	return ret, nil, nil
}

func (r *PayloadReader) GetInt32FromPayload() ([]int32, []bool, error) {
	if r.colType != schemapb.DataType_Int32 {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get int32 from datatype %v", r.colType.String()))
	}

	values := make([]int32, r.numRows)
	if r.nullable {
		validData := make([]bool, r.numRows)
		valuesRead, err := ReadData[int32, *array.Int32](r.reader, values, validData, r.numRows)
		if err != nil {
			return nil, nil, err
		}

		if valuesRead != r.numRows {
			return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
		}
		return values, validData, nil
	}
	valuesRead, err := ReadDataFromAllRowGroups[int32, *file.Int32ColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, nil, err
	}

	if valuesRead != r.numRows {
		return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
	}
	return values, nil, nil
}

func (r *PayloadReader) GetInt64FromPayload() ([]int64, []bool, error) {
	if r.colType != schemapb.DataType_Int64 {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get int64 from datatype %v", r.colType.String()))
	}

	values := make([]int64, r.numRows)
	if r.nullable {
		validData := make([]bool, r.numRows)
		valuesRead, err := ReadData[int64, *array.Int64](r.reader, values, validData, r.numRows)
		if err != nil {
			return nil, nil, err
		}

		if valuesRead != r.numRows {
			return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
		}

		return values, validData, nil
	}
	valuesRead, err := ReadDataFromAllRowGroups[int64, *file.Int64ColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, nil, err
	}

	if valuesRead != r.numRows {
		return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
	}

	return values, nil, nil
}

func (r *PayloadReader) GetFloatFromPayload() ([]float32, []bool, error) {
	if r.colType != schemapb.DataType_Float {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get float32 from datatype %v", r.colType.String()))
	}

	values := make([]float32, r.numRows)
	if r.nullable {
		validData := make([]bool, r.numRows)
		valuesRead, err := ReadData[float32, *array.Float32](r.reader, values, validData, r.numRows)
		if err != nil {
			return nil, nil, err
		}
		if valuesRead != r.numRows {
			return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
		}
		return values, validData, nil
	}
	valuesRead, err := ReadDataFromAllRowGroups[float32, *file.Float32ColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, nil, err
	}
	if valuesRead != r.numRows {
		return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
	}
	return values, nil, nil
}

func (r *PayloadReader) GetDoubleFromPayload() ([]float64, []bool, error) {
	if r.colType != schemapb.DataType_Double {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get double from datatype %v", r.colType.String()))
	}

	values := make([]float64, r.numRows)
	if r.nullable {
		validData := make([]bool, r.numRows)
		valuesRead, err := ReadData[float64, *array.Float64](r.reader, values, validData, r.numRows)
		if err != nil {
			return nil, nil, err
		}

		if valuesRead != r.numRows {
			return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
		}
		return values, validData, nil
	}
	valuesRead, err := ReadDataFromAllRowGroups[float64, *file.Float64ColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, nil, err
	}

	if valuesRead != r.numRows {
		return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
	}
	return values, nil, nil
}

func (r *PayloadReader) GetStringFromPayload() ([]string, []bool, error) {
	if r.colType != schemapb.DataType_String && r.colType != schemapb.DataType_VarChar {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get string from datatype %v", r.colType.String()))
	}

	if r.nullable {
		values := make([]string, r.numRows)
		validData := make([]bool, r.numRows)
		valuesRead, err := ReadData[string, *array.String](r.reader, values, validData, r.numRows)
		if err != nil {
			return nil, nil, err
		}

		if valuesRead != r.numRows {
			return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
		}
		return values, validData, nil
	}
	value, err := readByteAndConvert(r, func(bytes parquet.ByteArray) string {
		return bytes.String()
	})
	if err != nil {
		return nil, nil, err
	}
	return value, nil, nil
}

func (r *PayloadReader) GetArrayFromPayload() ([]*schemapb.ScalarField, []bool, error) {
	if r.colType != schemapb.DataType_Array {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get array from datatype %v", r.colType.String()))
	}

	if r.nullable {
		return readNullableByteAndConvert(r, func(bytes []byte) *schemapb.ScalarField {
			v := &schemapb.ScalarField{}
			proto.Unmarshal(bytes, v)
			return v
		})
	}
	value, err := readByteAndConvert(r, func(bytes parquet.ByteArray) *schemapb.ScalarField {
		v := &schemapb.ScalarField{}
		proto.Unmarshal(bytes, v)
		return v
	})
	if err != nil {
		return nil, nil, err
	}
	return value, nil, nil
}

func (r *PayloadReader) GetJSONFromPayload() ([][]byte, []bool, error) {
	if r.colType != schemapb.DataType_JSON {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("failed to get json from datatype %v", r.colType.String()))
	}

	if r.nullable {
		return readNullableByteAndConvert(r, func(bytes []byte) []byte {
			return bytes
		})
	}
	value, err := readByteAndConvert(r, func(bytes parquet.ByteArray) []byte {
		return bytes
	})
	if err != nil {
		return nil, nil, err
	}
	return value, nil, nil
}

func (r *PayloadReader) GetByteArrayDataSet() (*DataSet[parquet.ByteArray, *file.ByteArrayColumnChunkReader], error) {
	if r.colType != schemapb.DataType_String && r.colType != schemapb.DataType_VarChar {
		return nil, fmt.Errorf("failed to get string from datatype %v", r.colType.String())
	}

	return NewDataSet[parquet.ByteArray, *file.ByteArrayColumnChunkReader](r.reader, 0, r.numRows), nil
}

func (r *PayloadReader) GetArrowRecordReader() (pqarrow.RecordReader, error) {
	arrowReader, err := pqarrow.NewFileReader(r.reader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}

	rr, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return nil, err
	}
	return rr, nil
}

func readNullableByteAndConvert[T any](r *PayloadReader, convert func([]byte) T) ([]T, []bool, error) {
	values := make([][]byte, r.numRows)
	validData := make([]bool, r.numRows)
	valuesRead, err := ReadData[[]byte, *array.Binary](r.reader, values, validData, r.numRows)
	if err != nil {
		return nil, nil, err
	}

	if valuesRead != r.numRows {
		return nil, nil, merr.WrapErrParameterInvalid(r.numRows, valuesRead, "valuesRead is not equal to rows")
	}

	ret := make([]T, r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		ret[i] = convert(values[i])
	}
	return ret, validData, nil
}

func readByteAndConvert[T any](r *PayloadReader, convert func(parquet.ByteArray) T) ([]T, error) {
	values := make([]parquet.ByteArray, r.numRows)
	valuesRead, err := ReadDataFromAllRowGroups[parquet.ByteArray, *file.ByteArrayColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, err
	}

	if valuesRead != r.numRows {
		return nil, fmt.Errorf("expect %d rows, but got valuesRead = %d", r.numRows, valuesRead)
	}

	ret := make([]T, r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		ret[i] = convert(values[i])
	}
	return ret, nil
}

// GetBinaryVectorFromPayload returns vector, dimension, error
func (r *PayloadReader) GetBinaryVectorFromPayload() ([]byte, int, error) {
	if r.colType != schemapb.DataType_BinaryVector {
		return nil, -1, fmt.Errorf("failed to get binary vector from datatype %v", r.colType.String())
	}

	col, err := r.reader.RowGroup(0).Column(0)
	if err != nil {
		return nil, -1, err
	}
	dim := col.Descriptor().TypeLength()
	values := make([]parquet.FixedLenByteArray, r.numRows)
	valuesRead, err := ReadDataFromAllRowGroups[parquet.FixedLenByteArray, *file.FixedLenByteArrayColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, -1, err
	}

	if valuesRead != r.numRows {
		return nil, -1, fmt.Errorf("expect %d rows, but got valuesRead = %d", r.numRows, valuesRead)
	}

	ret := make([]byte, int64(dim)*r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		copy(ret[i*dim:(i+1)*dim], values[i])
	}
	return ret, dim * 8, nil
}

// GetFloat16VectorFromPayload returns vector, dimension, error
func (r *PayloadReader) GetFloat16VectorFromPayload() ([]byte, int, error) {
	if r.colType != schemapb.DataType_Float16Vector {
		return nil, -1, fmt.Errorf("failed to get float vector from datatype %v", r.colType.String())
	}
	col, err := r.reader.RowGroup(0).Column(0)
	if err != nil {
		return nil, -1, err
	}
	dim := col.Descriptor().TypeLength() / 2
	values := make([]parquet.FixedLenByteArray, r.numRows)
	valuesRead, err := ReadDataFromAllRowGroups[parquet.FixedLenByteArray, *file.FixedLenByteArrayColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, -1, err
	}

	if valuesRead != r.numRows {
		return nil, -1, fmt.Errorf("expect %d rows, but got valuesRead = %d", r.numRows, valuesRead)
	}

	ret := make([]byte, int64(dim*2)*r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		copy(ret[i*dim*2:(i+1)*dim*2], values[i])
	}
	return ret, dim, nil
}

// GetBFloat16VectorFromPayload returns vector, dimension, error
func (r *PayloadReader) GetBFloat16VectorFromPayload() ([]byte, int, error) {
	if r.colType != schemapb.DataType_BFloat16Vector {
		return nil, -1, fmt.Errorf("failed to get float vector from datatype %v", r.colType.String())
	}
	col, err := r.reader.RowGroup(0).Column(0)
	if err != nil {
		return nil, -1, err
	}
	dim := col.Descriptor().TypeLength() / 2
	values := make([]parquet.FixedLenByteArray, r.numRows)
	valuesRead, err := ReadDataFromAllRowGroups[parquet.FixedLenByteArray, *file.FixedLenByteArrayColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, -1, err
	}

	if valuesRead != r.numRows {
		return nil, -1, fmt.Errorf("expect %d rows, but got valuesRead = %d", r.numRows, valuesRead)
	}

	ret := make([]byte, int64(dim*2)*r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		copy(ret[i*dim*2:(i+1)*dim*2], values[i])
	}
	return ret, dim, nil
}

// GetFloatVectorFromPayload returns vector, dimension, error
func (r *PayloadReader) GetFloatVectorFromPayload() ([]float32, int, error) {
	if r.colType != schemapb.DataType_FloatVector {
		return nil, -1, fmt.Errorf("failed to get float vector from datatype %v", r.colType.String())
	}
	col, err := r.reader.RowGroup(0).Column(0)
	if err != nil {
		return nil, -1, err
	}

	dim := col.Descriptor().TypeLength() / 4

	values := make([]parquet.FixedLenByteArray, r.numRows)
	valuesRead, err := ReadDataFromAllRowGroups[parquet.FixedLenByteArray, *file.FixedLenByteArrayColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, -1, err
	}

	if valuesRead != r.numRows {
		return nil, -1, fmt.Errorf("expect %d rows, but got valuesRead = %d", r.numRows, valuesRead)
	}

	ret := make([]float32, int64(dim)*r.numRows)
	for i := 0; i < int(r.numRows); i++ {
		copy(arrow.Float32Traits.CastToBytes(ret[i*dim:(i+1)*dim]), values[i])
	}
	return ret, dim, nil
}

func (r *PayloadReader) GetSparseFloatVectorFromPayload() (*SparseFloatVectorFieldData, int, error) {
	if !typeutil.IsSparseFloatVectorType(r.colType) {
		return nil, -1, fmt.Errorf("failed to get sparse float vector from datatype %v", r.colType.String())
	}
	values := make([]parquet.ByteArray, r.numRows)
	valuesRead, err := ReadDataFromAllRowGroups[parquet.ByteArray, *file.ByteArrayColumnChunkReader](r.reader, values, 0, r.numRows)
	if err != nil {
		return nil, -1, err
	}
	if valuesRead != r.numRows {
		return nil, -1, fmt.Errorf("expect %d binary, but got = %d", r.numRows, valuesRead)
	}

	fieldData := &SparseFloatVectorFieldData{}

	for _, value := range values {
		if len(value)%8 != 0 {
			return nil, -1, errors.New("invalid bytesData length")
		}

		fieldData.Contents = append(fieldData.Contents, value)
		rowDim := typeutil.SparseFloatRowDim(value)
		if rowDim > fieldData.Dim {
			fieldData.Dim = rowDim
		}
	}

	return fieldData, int(fieldData.Dim), nil
}

func (r *PayloadReader) GetPayloadLengthFromReader() (int, error) {
	return int(r.numRows), nil
}

// Close closes the payload reader
func (r *PayloadReader) Close() error {
	return r.reader.Close()
}

// ReadDataFromAllRowGroups iterates all row groups of file.Reader, and convert column to E.
// then calls ReadBatch with provided parameters.
func ReadDataFromAllRowGroups[T any, E interface {
	ReadBatch(int64, []T, []int16, []int16) (int64, int, error)
}](reader *file.Reader, values []T, columnIdx int, numRows int64) (int64, error) {
	var offset int64

	for i := 0; i < reader.NumRowGroups(); i++ {
		if columnIdx >= reader.RowGroup(i).NumColumns() {
			return -1, fmt.Errorf("try to fetch %d-th column of reader but row group has only %d column(s)", columnIdx, reader.RowGroup(i).NumColumns())
		}
		column, err := reader.RowGroup(i).Column(columnIdx)
		if err != nil {
			return -1, err
		}

		cReader, ok := column.(E)
		if !ok {
			return -1, fmt.Errorf("expect type %T, but got %T", *new(E), column)
		}

		_, valuesRead, err := cReader.ReadBatch(numRows, values[offset:], nil, nil)
		if err != nil {
			return -1, err
		}

		offset += int64(valuesRead)
	}

	return offset, nil
}

type DataSet[T any, E interface {
	ReadBatch(int64, []T, []int16, []int16) (int64, int, error)
}] struct {
	reader  *file.Reader
	cReader E

	cnt, numRows       int64
	groupID, columnIdx int
}

func NewDataSet[T any, E interface {
	ReadBatch(int64, []T, []int16, []int16) (int64, int, error)
}](reader *file.Reader, columnIdx int, numRows int64) *DataSet[T, E] {
	return &DataSet[T, E]{
		reader:    reader,
		columnIdx: columnIdx,
		numRows:   numRows,
	}
}

func (s *DataSet[T, E]) nextGroup() error {
	s.cnt = 0
	column, err := s.reader.RowGroup(s.groupID).Column(s.columnIdx)
	if err != nil {
		return err
	}

	cReader, ok := column.(E)
	if !ok {
		return fmt.Errorf("expect type %T, but got %T", *new(E), column)
	}
	s.groupID++
	s.cReader = cReader
	return nil
}

func (s *DataSet[T, E]) HasNext() bool {
	if s.groupID > s.reader.NumRowGroups() || (s.groupID == s.reader.NumRowGroups() && s.cnt >= s.numRows) || s.numRows == 0 {
		return false
	}
	return true
}

func (s *DataSet[T, E]) NextBatch(batch int64) ([]T, error) {
	if s.groupID > s.reader.NumRowGroups() || (s.groupID == s.reader.NumRowGroups() && s.cnt >= s.numRows) || s.numRows == 0 {
		return nil, errors.New("has no more data")
	}

	if s.groupID == 0 || s.cnt >= s.numRows {
		err := s.nextGroup()
		if err != nil {
			return nil, err
		}
	}

	batch = Min(batch, s.numRows-s.cnt)
	result := make([]T, batch)
	_, _, err := s.cReader.ReadBatch(batch, result, nil, nil)
	if err != nil {
		return nil, err
	}

	s.cnt += batch
	return result, nil
}

func ReadData[T any, E interface {
	Value(int) T
	NullBitmapBytes() []byte
}](reader *file.Reader, value []T, validData []bool, numRows int64) (int64, error) {
	var offset int
	fileReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	// defer fileReader.ParquetReader().Close()
	if err != nil {
		log.Warn("create arrow parquet file reader failed", zap.Error(err))
		return -1, err
	}
	schema, err := fileReader.Schema()
	if err != nil {
		log.Warn("can't schema from file", zap.Error(err))
		return -1, err
	}
	for i, field := range schema.Fields() {
		// Spawn a new context to ignore cancellation from parental context.
		newCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		columnReader, err := fileReader.GetColumn(newCtx, i)
		if err != nil {
			log.Warn("get column reader failed", zap.String("fieldName", field.Name), zap.Error(err))
			return -1, err
		}
		chunked, err := columnReader.NextBatch(numRows)
		if err != nil {
			return -1, err
		}
		for _, chunk := range chunked.Chunks() {
			dataNums := chunk.Data().Len()
			reader, ok := chunk.(E)
			if !ok {
				log.Warn("the column data in parquet is not equal to field", zap.String("fieldName", field.Name), zap.String("actual type", chunk.DataType().Name()))
				return -1, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not equal to field: %s, but: %s", field.Name, chunk.DataType().Name()))
			}
			nullBitset := bytesToBoolArray(dataNums, reader.NullBitmapBytes())
			for i := 0; i < dataNums; i++ {
				value[offset] = reader.Value(i)
				validData[offset] = nullBitset[i]
				offset++
			}
		}
	}
	return int64(offset), nil
}

// todo(smellthemoon): use byte to store valid_data
func bytesToBoolArray(length int, bytes []byte) []bool {
	bools := make([]bool, 0, length)

	for i := 0; i < length; i++ {
		bit := (bytes[uint(i)/8] & BitMask[byte(i)%8]) != 0
		bools = append(bools, bit)
	}

	return bools
}

var (
	BitMask        = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
	FlippedBitMask = [8]byte{254, 253, 251, 247, 239, 223, 191, 127}
)

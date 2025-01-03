// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"fmt"
	"math"
	"sync"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ PayloadWriterInterface = (*NativePayloadWriter)(nil)

type PayloadWriterOptions func(*NativePayloadWriter)

func WithNullable(nullable bool) PayloadWriterOptions {
	return func(w *NativePayloadWriter) {
		w.nullable = nullable
	}
}

func WithWriterProps(writerProps *parquet.WriterProperties) PayloadWriterOptions {
	return func(w *NativePayloadWriter) {
		w.writerProps = writerProps
	}
}

func WithDim(dim int) PayloadWriterOptions {
	return func(w *NativePayloadWriter) {
		w.dim = NewNullableInt(dim)
	}
}

type NativePayloadWriter struct {
	dataType    schemapb.DataType
	arrowType   arrow.DataType
	builder     array.Builder
	finished    bool
	flushedRows int
	output      *bytes.Buffer
	releaseOnce sync.Once
	dim         *NullableInt
	nullable    bool
	writerProps *parquet.WriterProperties
}

func NewPayloadWriter(colType schemapb.DataType, options ...PayloadWriterOptions) (PayloadWriterInterface, error) {
	w := &NativePayloadWriter{
		dataType:    colType,
		finished:    false,
		flushedRows: 0,
		output:      new(bytes.Buffer),
		nullable:    false,
		writerProps: parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Zstd),
			parquet.WithCompressionLevel(3),
		),
		dim: &NullableInt{},
	}
	for _, o := range options {
		o(w)
	}

	// writer for sparse float vector doesn't require dim
	if typeutil.IsVectorType(colType) && !typeutil.IsSparseFloatVectorType(colType) {
		if w.dim.IsNull() {
			return nil, merr.WrapErrParameterInvalidMsg("incorrect input numbers")
		}
		if w.nullable {
			return nil, merr.WrapErrParameterInvalidMsg("vector type does not support nullable")
		}
	} else {
		w.dim = NewNullableInt(1)
	}
	w.arrowType = milvusDataTypeToArrowType(colType, *w.dim.Value)
	w.builder = array.NewBuilder(memory.DefaultAllocator, w.arrowType)
	return w, nil
}

func (w *NativePayloadWriter) AddDataToPayload(data interface{}, validData []bool) error {
	switch w.dataType {
	case schemapb.DataType_Bool:
		val, ok := data.([]bool)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddBoolToPayload(val, validData)
	case schemapb.DataType_Int8:
		val, ok := data.([]int8)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddInt8ToPayload(val, validData)
	case schemapb.DataType_Int16:
		val, ok := data.([]int16)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddInt16ToPayload(val, validData)
	case schemapb.DataType_Int32:
		val, ok := data.([]int32)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddInt32ToPayload(val, validData)
	case schemapb.DataType_Int64:
		val, ok := data.([]int64)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddInt64ToPayload(val, validData)
	case schemapb.DataType_Float:
		val, ok := data.([]float32)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddFloatToPayload(val, validData)
	case schemapb.DataType_Double:
		val, ok := data.([]float64)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddDoubleToPayload(val, validData)
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		val, ok := data.(string)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		isValid := true
		if len(validData) > 1 {
			return merr.WrapErrParameterInvalidMsg("wrong input length when add data to payload")
		}
		if len(validData) == 0 && w.nullable {
			return merr.WrapErrParameterInvalidMsg("need pass valid_data when nullable==true")
		}
		if len(validData) == 1 {
			if !w.nullable {
				return merr.WrapErrParameterInvalidMsg("no need pass valid_data when nullable==false")
			}
			isValid = validData[0]
		}
		return w.AddOneStringToPayload(val, isValid)
	case schemapb.DataType_Array:
		val, ok := data.(*schemapb.ScalarField)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		isValid := true
		if len(validData) > 1 {
			return merr.WrapErrParameterInvalidMsg("wrong input length when add data to payload")
		}
		if len(validData) == 0 && w.nullable {
			return merr.WrapErrParameterInvalidMsg("need pass valid_data when nullable==true")
		}
		if len(validData) == 1 {
			if !w.nullable {
				return merr.WrapErrParameterInvalidMsg("no need pass valid_data when nullable==false")
			}
			isValid = validData[0]
		}
		return w.AddOneArrayToPayload(val, isValid)
	case schemapb.DataType_JSON:
		val, ok := data.([]byte)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		isValid := true
		if len(validData) > 1 {
			return merr.WrapErrParameterInvalidMsg("wrong input length when add data to payload")
		}
		if len(validData) == 0 && w.nullable {
			return merr.WrapErrParameterInvalidMsg("need pass valid_data when nullable==true")
		}
		if len(validData) == 1 {
			if !w.nullable {
				return merr.WrapErrParameterInvalidMsg("no need pass valid_data when nullable==false")
			}
			isValid = validData[0]
		}
		return w.AddOneJSONToPayload(val, isValid)
	case schemapb.DataType_BinaryVector:
		val, ok := data.([]byte)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddBinaryVectorToPayload(val, w.dim.GetValue())
	case schemapb.DataType_FloatVector:
		val, ok := data.([]float32)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddFloatVectorToPayload(val, w.dim.GetValue())
	case schemapb.DataType_Float16Vector:
		val, ok := data.([]byte)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddFloat16VectorToPayload(val, w.dim.GetValue())
	case schemapb.DataType_BFloat16Vector:
		val, ok := data.([]byte)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddBFloat16VectorToPayload(val, w.dim.GetValue())
	case schemapb.DataType_SparseFloatVector:
		val, ok := data.(*SparseFloatVectorFieldData)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddSparseFloatVectorToPayload(val)
	case schemapb.DataType_Int8Vector:
		val, ok := data.([]int8)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("incorrect data type")
		}
		return w.AddInt8VectorToPayload(val, w.dim.GetValue())
	default:
		return errors.New("unsupported datatype")
	}
}

func (w *NativePayloadWriter) AddBoolToPayload(data []bool, validData []bool) error {
	if w.finished {
		return errors.New("can't append data to finished bool payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into bool payload")
	}

	if !w.nullable && len(validData) != 0 {
		msg := fmt.Sprintf("length of validData(%d) must be 0 when not nullable", len(validData))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	if w.nullable && len(data) != len(validData) {
		msg := fmt.Sprintf("length of validData(%d) must equal to data(%d) when nullable", len(validData), len(data))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	builder, ok := w.builder.(*array.BooleanBuilder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}
	builder.AppendValues(data, validData)

	return nil
}

func (w *NativePayloadWriter) AddByteToPayload(data []byte, validData []bool) error {
	if w.finished {
		return errors.New("can't append data to finished byte payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into byte payload")
	}

	if !w.nullable && len(validData) != 0 {
		msg := fmt.Sprintf("length of validData(%d) must be 0 when not nullable", len(validData))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	if w.nullable && len(data) != len(validData) {
		msg := fmt.Sprintf("length of validData(%d) must equal to data(%d) when nullable", len(validData), len(data))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	builder, ok := w.builder.(*array.Int8Builder)
	if !ok {
		return errors.New("failed to cast ByteBuilder")
	}

	builder.Reserve(len(data))
	for i := range data {
		builder.Append(int8(data[i]))
		if w.nullable && !validData[i] {
			builder.AppendNull()
		}
	}

	return nil
}

func (w *NativePayloadWriter) AddInt8ToPayload(data []int8, validData []bool) error {
	if w.finished {
		return errors.New("can't append data to finished int8 payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into int8 payload")
	}

	if !w.nullable && len(validData) != 0 {
		msg := fmt.Sprintf("length of validData(%d) must be 0 when not nullable", len(validData))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	if w.nullable && len(data) != len(validData) {
		msg := fmt.Sprintf("length of validData(%d) must equal to data(%d) when nullable", len(validData), len(data))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	builder, ok := w.builder.(*array.Int8Builder)
	if !ok {
		return errors.New("failed to cast Int8Builder")
	}
	builder.AppendValues(data, validData)

	return nil
}

func (w *NativePayloadWriter) AddInt16ToPayload(data []int16, validData []bool) error {
	if w.finished {
		return errors.New("can't append data to finished int16 payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into int16 payload")
	}

	if !w.nullable && len(validData) != 0 {
		msg := fmt.Sprintf("length of validData(%d) must be 0 when not nullable", len(validData))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	if w.nullable && len(data) != len(validData) {
		msg := fmt.Sprintf("length of validData(%d) must equal to data(%d) when nullable", len(validData), len(data))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	builder, ok := w.builder.(*array.Int16Builder)
	if !ok {
		return errors.New("failed to cast Int16Builder")
	}
	builder.AppendValues(data, validData)

	return nil
}

func (w *NativePayloadWriter) AddInt32ToPayload(data []int32, validData []bool) error {
	if w.finished {
		return errors.New("can't append data to finished int32 payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into int32 payload")
	}

	if !w.nullable && len(validData) != 0 {
		msg := fmt.Sprintf("length of validData(%d) must be 0 when not nullable", len(validData))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	if w.nullable && len(data) != len(validData) {
		msg := fmt.Sprintf("length of validData(%d) must equal to data(%d) when nullable", len(validData), len(data))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	builder, ok := w.builder.(*array.Int32Builder)
	if !ok {
		return errors.New("failed to cast Int32Builder")
	}
	builder.AppendValues(data, validData)

	return nil
}

func (w *NativePayloadWriter) AddInt64ToPayload(data []int64, validData []bool) error {
	if w.finished {
		return errors.New("can't append data to finished int64 payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into int64 payload")
	}

	if !w.nullable && len(validData) != 0 {
		msg := fmt.Sprintf("length of validData(%d) must be 0 when not nullable", len(validData))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	if w.nullable && len(data) != len(validData) {
		msg := fmt.Sprintf("length of validData(%d) must equal to data(%d) when nullable", len(validData), len(data))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	builder, ok := w.builder.(*array.Int64Builder)
	if !ok {
		return errors.New("failed to cast Int64Builder")
	}
	builder.AppendValues(data, validData)

	return nil
}

func (w *NativePayloadWriter) AddFloatToPayload(data []float32, validData []bool) error {
	if w.finished {
		return errors.New("can't append data to finished float payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into float payload")
	}

	if !w.nullable && len(validData) != 0 {
		msg := fmt.Sprintf("length of validData(%d) must be 0 when not nullable", len(validData))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	if w.nullable && len(data) != len(validData) {
		msg := fmt.Sprintf("length of validData(%d) must equal to data(%d) when nullable", len(validData), len(data))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	builder, ok := w.builder.(*array.Float32Builder)
	if !ok {
		return errors.New("failed to cast FloatBuilder")
	}
	builder.AppendValues(data, validData)

	return nil
}

func (w *NativePayloadWriter) AddDoubleToPayload(data []float64, validData []bool) error {
	if w.finished {
		return errors.New("can't append data to finished double payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into double payload")
	}

	if !w.nullable && len(validData) != 0 {
		msg := fmt.Sprintf("length of validData(%d) must be 0 when not nullable", len(validData))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	if w.nullable && len(data) != len(validData) {
		msg := fmt.Sprintf("length of validData(%d) must equal to data(%d) when nullable", len(validData), len(data))
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	builder, ok := w.builder.(*array.Float64Builder)
	if !ok {
		return errors.New("failed to cast DoubleBuilder")
	}
	builder.AppendValues(data, validData)

	return nil
}

func (w *NativePayloadWriter) AddOneStringToPayload(data string, isValid bool) error {
	if w.finished {
		return errors.New("can't append data to finished string payload")
	}

	if !w.nullable && !isValid {
		return merr.WrapErrParameterInvalidMsg("not support null when nullable is false")
	}

	builder, ok := w.builder.(*array.StringBuilder)
	if !ok {
		return errors.New("failed to cast StringBuilder")
	}

	if !isValid {
		builder.AppendNull()
	} else {
		builder.Append(data)
	}

	return nil
}

func (w *NativePayloadWriter) AddOneArrayToPayload(data *schemapb.ScalarField, isValid bool) error {
	if w.finished {
		return errors.New("can't append data to finished array payload")
	}

	if !w.nullable && !isValid {
		return merr.WrapErrParameterInvalidMsg("not support null when nullable is false")
	}

	bytes, err := proto.Marshal(data)
	if err != nil {
		return errors.New("Marshal ListValue failed")
	}

	builder, ok := w.builder.(*array.BinaryBuilder)
	if !ok {
		return errors.New("failed to cast BinaryBuilder")
	}

	if !isValid {
		builder.AppendNull()
	} else {
		builder.Append(bytes)
	}

	return nil
}

func (w *NativePayloadWriter) AddOneJSONToPayload(data []byte, isValid bool) error {
	if w.finished {
		return errors.New("can't append data to finished json payload")
	}

	if !w.nullable && !isValid {
		return merr.WrapErrParameterInvalidMsg("not support null when nullable is false")
	}

	builder, ok := w.builder.(*array.BinaryBuilder)
	if !ok {
		return errors.New("failed to cast JsonBuilder")
	}

	if !isValid {
		builder.AppendNull()
	} else {
		builder.Append(data)
	}

	return nil
}

func (w *NativePayloadWriter) AddBinaryVectorToPayload(data []byte, dim int) error {
	if w.finished {
		return errors.New("can't append data to finished binary vector payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into binary vector payload")
	}

	builder, ok := w.builder.(*array.FixedSizeBinaryBuilder)
	if !ok {
		return errors.New("failed to cast BinaryVectorBuilder")
	}

	byteLength := dim / 8
	length := len(data) / byteLength
	builder.Reserve(length)
	for i := 0; i < length; i++ {
		builder.Append(data[i*byteLength : (i+1)*byteLength])
	}

	return nil
}

func (w *NativePayloadWriter) AddFloatVectorToPayload(data []float32, dim int) error {
	if w.finished {
		return errors.New("can't append data to finished float vector payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into float vector payload")
	}

	builder, ok := w.builder.(*array.FixedSizeBinaryBuilder)
	if !ok {
		return errors.New("failed to cast FloatVectorBuilder")
	}

	byteLength := dim * 4
	length := len(data) / dim

	builder.Reserve(length)
	bytesData := make([]byte, byteLength)
	for i := 0; i < length; i++ {
		vec := data[i*dim : (i+1)*dim]
		for j := range vec {
			bytes := math.Float32bits(vec[j])
			common.Endian.PutUint32(bytesData[j*4:], bytes)
		}
		builder.Append(bytesData)
	}

	return nil
}

func (w *NativePayloadWriter) AddFloat16VectorToPayload(data []byte, dim int) error {
	if w.finished {
		return errors.New("can't append data to finished float16 payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into float16 payload")
	}

	builder, ok := w.builder.(*array.FixedSizeBinaryBuilder)
	if !ok {
		return errors.New("failed to cast Float16Builder")
	}

	byteLength := dim * 2
	length := len(data) / byteLength

	builder.Reserve(length)
	for i := 0; i < length; i++ {
		builder.Append(data[i*byteLength : (i+1)*byteLength])
	}

	return nil
}

func (w *NativePayloadWriter) AddBFloat16VectorToPayload(data []byte, dim int) error {
	if w.finished {
		return errors.New("can't append data to finished BFloat16 payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into BFloat16 payload")
	}

	builder, ok := w.builder.(*array.FixedSizeBinaryBuilder)
	if !ok {
		return errors.New("failed to cast BFloat16Builder")
	}

	byteLength := dim * 2
	length := len(data) / byteLength

	builder.Reserve(length)
	for i := 0; i < length; i++ {
		builder.Append(data[i*byteLength : (i+1)*byteLength])
	}

	return nil
}

func (w *NativePayloadWriter) AddSparseFloatVectorToPayload(data *SparseFloatVectorFieldData) error {
	if w.finished {
		return errors.New("can't append data to finished sparse float vector payload")
	}
	builder, ok := w.builder.(*array.BinaryBuilder)
	if !ok {
		return errors.New("failed to cast SparseFloatVectorBuilder")
	}
	length := len(data.SparseFloatArray.Contents)
	builder.Reserve(length)
	for i := 0; i < length; i++ {
		builder.Append(data.SparseFloatArray.Contents[i])
	}

	return nil
}

func (w *NativePayloadWriter) AddInt8VectorToPayload(data []int8, dim int) error {
	if w.finished {
		return errors.New("can't append data to finished int8 vector payload")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into int8 vector payload")
	}

	builder, ok := w.builder.(*array.FixedSizeBinaryBuilder)
	if !ok {
		return errors.New("failed to cast Int8VectorBuilder")
	}

	byteLength := dim
	length := len(data) / byteLength

	builder.Reserve(length)
	for i := 0; i < length; i++ {
		vec := data[i*dim : (i+1)*dim]
		vecBytes := arrow.Int8Traits.CastToBytes(vec)
		builder.Append(vecBytes)
	}

	return nil
}

func (w *NativePayloadWriter) FinishPayloadWriter() error {
	if w.finished {
		return errors.New("can't reuse a finished writer")
	}

	w.finished = true

	field := arrow.Field{
		Name:     "val",
		Type:     w.arrowType,
		Nullable: w.nullable,
	}
	schema := arrow.NewSchema([]arrow.Field{
		field,
	}, nil)

	w.flushedRows += w.builder.Len()
	data := w.builder.NewArray()
	defer data.Release()
	column := arrow.NewColumnFromArr(field, data)
	defer column.Release()

	table := array.NewTable(schema, []arrow.Column{column}, int64(column.Len()))
	defer table.Release()

	return pqarrow.WriteTable(table,
		w.output,
		1024*1024*1024,
		w.writerProps,
		pqarrow.DefaultWriterProps(),
	)
}

func (w *NativePayloadWriter) Reserve(size int) {
	w.builder.Reserve(size)
}

func (w *NativePayloadWriter) GetPayloadBufferFromWriter() ([]byte, error) {
	data := w.output.Bytes()

	// The cpp version of payload writer handles the empty buffer as error
	if len(data) == 0 {
		return nil, errors.New("empty buffer")
	}

	return data, nil
}

func (w *NativePayloadWriter) GetPayloadLengthFromWriter() (int, error) {
	return w.flushedRows + w.builder.Len(), nil
}

func (w *NativePayloadWriter) ReleasePayloadWriter() {
	w.releaseOnce.Do(func() {
		w.builder.Release()
	})
}

func (w *NativePayloadWriter) Close() {
	w.ReleasePayloadWriter()
}

func milvusDataTypeToArrowType(dataType schemapb.DataType, dim int) arrow.DataType {
	switch dataType {
	case schemapb.DataType_Bool:
		return &arrow.BooleanType{}
	case schemapb.DataType_Int8:
		return &arrow.Int8Type{}
	case schemapb.DataType_Int16:
		return &arrow.Int16Type{}
	case schemapb.DataType_Int32:
		return &arrow.Int32Type{}
	case schemapb.DataType_Int64:
		return &arrow.Int64Type{}
	case schemapb.DataType_Float:
		return &arrow.Float32Type{}
	case schemapb.DataType_Double:
		return &arrow.Float64Type{}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return &arrow.StringType{}
	case schemapb.DataType_Array:
		return &arrow.BinaryType{}
	case schemapb.DataType_JSON:
		return &arrow.BinaryType{}
	case schemapb.DataType_FloatVector:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: dim * 4,
		}
	case schemapb.DataType_BinaryVector:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: dim / 8,
		}
	case schemapb.DataType_Float16Vector:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: dim * 2,
		}
	case schemapb.DataType_BFloat16Vector:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: dim * 2,
		}
	case schemapb.DataType_SparseFloatVector:
		return &arrow.BinaryType{}
	case schemapb.DataType_Int8Vector:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: dim,
		}
	default:
		panic("unsupported data type")
	}
}

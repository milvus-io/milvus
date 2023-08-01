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
	"math"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/compress"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ PayloadWriterInterface = (*NativePayloadWriter)(nil)

type NativePayloadWriter struct {
	dataType    schemapb.DataType
	arrowType   arrow.DataType
	builder     array.Builder
	finished    bool
	flushedRows int
	output      *bytes.Buffer
	releaseOnce sync.Once
	dim         int
}

func NewPayloadWriter(colType schemapb.DataType, dim ...int) (PayloadWriterInterface, error) {
	var arrowType arrow.DataType
	var dimension int
	if typeutil.IsVectorType(colType) {
		if len(dim) != 1 {
			return nil, merr.WrapErrParameterInvalid("pass in dim", "not", "incorrect input numbers")
		}
		arrowType = milvusDataTypeToArrowType(colType, dim[0])
		dimension = dim[0]
	} else {
		if len(dim) != 0 {
			return nil, merr.WrapErrParameterInvalid("not pass in dim", "pass in", "incorrect input numbers")
		}
		arrowType = milvusDataTypeToArrowType(colType, 1)
		dimension = 1
	}

	builder := array.NewBuilder(memory.DefaultAllocator, arrowType)

	return &NativePayloadWriter{
		dataType:    colType,
		arrowType:   arrowType,
		builder:     builder,
		finished:    false,
		flushedRows: 0,
		output:      new(bytes.Buffer),
		dim:         dimension,
	}, nil
}

func (w *NativePayloadWriter) AddDataToPayload(data interface{}, nulls ...bool) error {
	switch w.dataType {
	case schemapb.DataType_Bool:
		val, ok := data.([]bool)
		if !ok {
			return errors.New("incorrect data type")
		}
		return w.AddBoolToPayload(val, nulls)
	case schemapb.DataType_Int8:
		val, ok := data.([]int8)
		if !ok {
			return errors.New("incorrect data type")
		}
		return w.AddInt8ToPayload(val, nulls)
	case schemapb.DataType_Int16:
		val, ok := data.([]int16)
		if !ok {
			return errors.New("incorrect data type")
		}
		return w.AddInt16ToPayload(val, nulls)
	case schemapb.DataType_Int32:
		val, ok := data.([]int32)
		if !ok {
			return errors.New("incorrect data type")
		}
		return w.AddInt32ToPayload(val, nulls)
	case schemapb.DataType_Int64:
		val, ok := data.([]int64)
		if !ok {
			return errors.New("incorrect data type")
		}
		return w.AddInt64ToPayload(val, nulls)
	case schemapb.DataType_Float:
		val, ok := data.([]float32)
		if !ok {
			return errors.New("incorrect data type")
		}
		return w.AddFloatToPayload(val, nulls)
	case schemapb.DataType_Double:
		val, ok := data.([]float64)
		if !ok {
			return errors.New("incorrect data type")
		}
		return w.AddDoubleToPayload(val, nulls)
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		val, ok := data.(string)
		if !ok {
			return errors.New("incorrect data type")
		}
		if len(nulls) != 1 {
			return merr.WrapErrParameterInvalid(1, len(nulls), "length of null bitset wrong")
		}
		return w.AddOneStringToPayload(val, nulls[0])
	case schemapb.DataType_Array:
		val, ok := data.(*schemapb.ScalarField)
		if !ok {
			return errors.New("incorrect data type")
		}
		if len(nulls) != 1 {
			return merr.WrapErrParameterInvalid(1, len(nulls), "length of null bitset wrong")
		}
		return w.AddOneArrayToPayload(val, nulls[0])
	case schemapb.DataType_JSON:
		val, ok := data.([]byte)
		if !ok {
			return errors.New("incorrect data type")
		}
		if len(nulls) != 1 {
			return merr.WrapErrParameterInvalid(1, len(nulls), "length of null bitset wrong")
		}
		return w.AddOneJSONToPayload(val, nulls[0])
	case schemapb.DataType_BinaryVector:
		val, ok := data.([]byte)
		if !ok {
			return merr.WrapErrParameterInvalid("correct datatype", "BinaryVector", "incorrect datatype")
		}
		if len(nulls) != 0 {
			msg := "BinaryVector not support null data"
			return merr.WrapErrParameterInvalid("not pass in nulls", "pass in", msg)
		}
		return w.AddBinaryVectorToPayload(val, w.dim)
	case schemapb.DataType_FloatVector:
		val, ok := data.([]float32)
		if !ok {
			return merr.WrapErrParameterInvalid("correct datatype", "FloatVector", "incorrect datatype")
		}
		if len(nulls) != 0 {
			msg := "FloatVector not support null data"
			return merr.WrapErrParameterInvalid("not pass in nulls", "pass in", msg)
		}
		return w.AddFloatVectorToPayload(val, w.dim)
	default:
		return merr.WrapErrParameterInvalid("valid datatype", "not", "incorrect datatype")
	}
}

func (w *NativePayloadWriter) AddBoolToPayload(data []bool, nulls []bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	if len(data) != len(nulls) && nulls != nil {
		return merr.WrapErrParameterInvalid(len(data), len(nulls), "")
	}

	builder, ok := w.builder.(*array.BooleanBuilder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}
	builder.AppendValues(data, nulls)

	return nil
}

func (w *NativePayloadWriter) AddByteToPayload(data []byte, nulls []bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	if len(data) != len(nulls) && nulls != nil {
		return merr.WrapErrParameterInvalid(len(data), len(nulls), "")
	}

	builder, ok := w.builder.(*array.Int8Builder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}

	builder.Reserve(len(data))
	for i := range data {
		if nulls[i] {
			builder.AppendNull()
			continue
		}
		builder.Append(int8(data[i]))
	}

	return nil
}

func (w *NativePayloadWriter) AddInt8ToPayload(data []int8, nulls []bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	if len(data) != len(nulls) && nulls != nil {
		return merr.WrapErrParameterInvalid(len(data), len(nulls), "")
	}

	builder, ok := w.builder.(*array.Int8Builder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}
	builder.AppendValues(data, nulls)

	return nil
}

func (w *NativePayloadWriter) AddInt16ToPayload(data []int16, nulls []bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	if len(data) != len(nulls) && nulls != nil {
		return merr.WrapErrParameterInvalid(len(data), len(nulls), "")
	}

	builder, ok := w.builder.(*array.Int16Builder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}
	builder.AppendValues(data, nulls)

	return nil
}

func (w *NativePayloadWriter) AddInt32ToPayload(data []int32, nulls []bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	if len(data) != len(nulls) && nulls != nil {
		return merr.WrapErrParameterInvalid(len(data), len(nulls), "")
	}

	builder, ok := w.builder.(*array.Int32Builder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}
	builder.AppendValues(data, nulls)

	return nil
}

func (w *NativePayloadWriter) AddInt64ToPayload(data []int64, nulls []bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	if len(data) != len(nulls) && nulls != nil {
		return merr.WrapErrParameterInvalid(len(data), len(nulls), "")
	}

	builder, ok := w.builder.(*array.Int64Builder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}
	builder.AppendValues(data, nulls)

	return nil
}

func (w *NativePayloadWriter) AddFloatToPayload(data []float32, nulls []bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	if len(data) != len(nulls) && nulls != nil {
		return merr.WrapErrParameterInvalid(len(data), len(nulls), "")
	}

	builder, ok := w.builder.(*array.Float32Builder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}
	builder.AppendValues(data, nulls)

	return nil
}

func (w *NativePayloadWriter) AddDoubleToPayload(data []float64, nulls []bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	if len(data) != len(nulls) && nulls != nil {
		return merr.WrapErrParameterInvalid(len(data), len(nulls), "")
	}

	builder, ok := w.builder.(*array.Float64Builder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}
	builder.AppendValues(data, nulls)

	return nil
}

func (w *NativePayloadWriter) AddOneStringToPayload(data string, isNull bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	builder, ok := w.builder.(*array.StringBuilder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}
	if isNull {
		builder.Append(data)
	} else {
		builder.AppendNull()
	}
	return nil
}

func (w *NativePayloadWriter) AddOneArrayToPayload(data *schemapb.ScalarField, isNull bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	bytes, err := proto.Marshal(data)
	if err != nil {
		return errors.New("Marshal ListValue failed")
	}

	builder, ok := w.builder.(*array.BinaryBuilder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}

	if isNull {
		builder.Append(bytes)
	} else {
		builder.AppendNull()
	}

	return nil
}

func (w *NativePayloadWriter) AddOneJSONToPayload(data []byte, isNull bool) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	builder, ok := w.builder.(*array.BinaryBuilder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
	}

	if isNull {
		builder.Append(data)
	} else {
		builder.AppendNull()
	}

	return nil
}

func (w *NativePayloadWriter) AddBinaryVectorToPayload(data []byte, dim int) error {
	if w.finished {
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	builder, ok := w.builder.(*array.FixedSizeBinaryBuilder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
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
		return errors.New("can't append data to finished writer")
	}

	if len(data) == 0 {
		return errors.New("can't add empty msgs into payload")
	}

	builder, ok := w.builder.(*array.FixedSizeBinaryBuilder)
	if !ok {
		return errors.New("failed to cast ArrayBuilder")
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

func (w *NativePayloadWriter) FinishPayloadWriter() error {
	if w.finished {
		return errors.New("can't reuse a finished writer")
	}

	w.finished = true

	field := arrow.Field{
		Name: "val",
		Type: w.arrowType,
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

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
	)
	return pqarrow.WriteTable(table,
		w.output,
		1024*1024*1024,
		props,
		pqarrow.DefaultWriterProps(),
	)
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
	default:
		panic("unsupported data type")
	}
}

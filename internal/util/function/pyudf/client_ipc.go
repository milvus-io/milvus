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

package pyudf

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func encodeInputs(ctx context.Context, inputs []*arrow.Chunked) ([]byte, error) {
	if len(inputs) == 0 || inputs[0] == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: invalid input columns")
	}
	count := len(inputs[0].Chunks())
	if count == 0 {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: invalid query count")
	}
	fields := make([]arrow.Field, len(inputs))
	for column, input := range inputs {
		if input == nil || input.DataType() == nil || len(input.Chunks()) != count {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: inconsistent input chunks column=%d", column)
		}
		fields[column] = arrow.Field{Name: fmt.Sprintf("c%d", column), Type: input.DataType(), Nullable: true}
	}

	var sink bytes.Buffer
	schema := arrow.NewSchema(fields, nil)
	writer := ipc.NewWriter(&sink, ipc.WithSchema(schema))
	defer writer.Close()
	columns := make([]arrow.Array, len(inputs))
	for query := 0; query < count; query++ {
		var rows int64
		if err := ctx.Err(); err != nil {
			return nil, merr.Wrap(err, "py_udf: encode inputs")
		}
		for column, input := range inputs {
			chunk := input.Chunk(query)
			if chunk == nil || !arrow.TypeEqual(chunk.DataType(), input.DataType()) {
				return nil, merr.WrapErrServiceInternalMsg("py_udf: invalid input chunk column=%d query=%d", column, query)
			}
			if column == 0 {
				rows = int64(chunk.Len())
			}
			if int64(chunk.Len()) != rows {
				return nil, merr.WrapErrServiceInternalMsg("py_udf: inconsistent query rows")
			}
			columns[column] = chunk
		}
		record := array.NewRecord(schema, columns, rows)
		err := writer.Write(record)
		record.Release()
		if err != nil {
			return nil, ipcEncodeError(err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, ipcEncodeError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, merr.Wrap(err, "py_udf: encode inputs")
	}
	return sink.Bytes(), nil
}

func ipcEncodeError(err error) error {
	// Preserve typed errors and classify untyped Arrow encoding failures.
	if merr.IsMilvusError(err) || merr.IsCanceledOrTimeout(err) {
		return merr.Wrap(err, "py_udf: encode IPC")
	}
	return merr.WrapErrServiceInternalErr(err, "py_udf: encode IPC")
}

func decodeOutputs(ctx context.Context, payload []byte) (outputs []*arrow.Chunked, err error) {
	var chunks [][]arrow.Array
	defer func() {
		if recovered := recover(); recovered != nil {
			err = merr.WrapErrServiceInternalMsg("py_udf: invalid output IPC: %v", recovered)
		}
		for _, column := range chunks {
			for _, chunk := range column {
				chunk.Release()
			}
		}
		if err != nil {
			releaseColumns(outputs)
			outputs = nil
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, merr.Wrap(err, "py_udf: decode outputs")
	}
	// Delay schema decoding so Release is registered even if the schema is invalid.
	reader, err := ipc.NewReader(bytes.NewReader(payload), ipc.WithDelayReadSchema(true))
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "py_udf: decode IPC")
	}
	defer reader.Release()
	schema := reader.Schema()
	if reader.Err() != nil {
		return nil, merr.WrapErrServiceInternalErr(reader.Err(), "py_udf: decode schema")
	}
	if schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: invalid output schema")
	}
	chunks = make([][]arrow.Array, schema.NumFields())
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return nil, merr.Wrap(err, "py_udf: decode outputs")
		}
		record := reader.Record()
		for i, column := range record.Columns() {
			if values, ok := column.(*array.String); ok {
				if err := validateOutputStringOffsets(values); err != nil {
					return nil, merr.Wrapf(err, "py_udf: output column=%d", i)
				}
			}
			column.Retain()
			chunks[i] = append(chunks[i], column)
		}
	}
	if reader.Err() != nil {
		return nil, merr.WrapErrServiceInternalErr(reader.Err(), "py_udf: decode output batch")
	}
	if err := ctx.Err(); err != nil {
		return nil, merr.Wrap(err, "py_udf: decode outputs")
	}
	for i, column := range chunks {
		outputs = append(outputs, arrow.NewChunked(schema.Field(i).Type, column))
	}
	return outputs, nil
}

// Arrow's string constructor checks the final offset but does not validate
// intermediate offsets. Check them while decodeOutputs still owns the arrays:
// a later String.Value call would otherwise panic outside its recovery boundary.
func validateOutputStringOffsets(values *array.String) error {
	if values.Len() == 0 {
		return nil
	}
	dataSize := 0
	if data := values.Data().Buffers()[2]; data != nil {
		dataSize = data.Len()
	}
	var previous int32
	for i, offset := range values.ValueOffsets() {
		if offset < previous || int64(offset) > int64(dataSize) {
			return merr.WrapErrServiceInternalMsg("py_udf: invalid output string offset at index=%d", i)
		}
		previous = offset
	}
	return nil
}

func encodeExecuteRequest(ctx context.Context, request ExecuteRequest) (*pyudfpb.ExecuteRequest, error) {
	wire := &pyudfpb.ExecuteRequest{ResourceName: request.ResourceName, UdfPath: request.UDFPath, Stage: request.Stage, Params: encodeParams(request.Params)}
	// BaseOp resolves repeated field references to the same Chunked object.
	// Deduplicate by identity, not equal values: distinct columns stay distinct.
	unique := make([]*arrow.Chunked, 0, len(request.Inputs))
	positions := make(map[*arrow.Chunked]uint32, len(request.Inputs))
	wire.InputColumnIndices = make([]uint32, len(request.Inputs))
	for i, column := range request.Inputs {
		position, exists := positions[column]
		if !exists {
			position = uint32(len(unique))
			positions[column] = position
			unique = append(unique, column)
		}
		wire.InputColumnIndices[i] = position
	}
	payload, err := encodeInputs(ctx, unique)
	if err != nil {
		return nil, err
	}
	wire.Inputs = payload

	return wire, nil
}

// Translate API parameters at the Go boundary so Python only loads the
// runtime-owned protocol. Keep unset values for the worker's existing validation.
func encodeParams(params *schemapb.FunctionParamObject) *pyudfpb.FunctionParamObject {
	if params == nil {
		return nil
	}
	fields := make(map[string]*pyudfpb.FunctionParamValue, len(params.GetFields()))
	for name, value := range params.GetFields() {
		fields[name] = encodeParamValue(value)
	}
	return &pyudfpb.FunctionParamObject{Fields: fields}
}

func encodeParamValue(value *schemapb.FunctionParamValue) *pyudfpb.FunctionParamValue {
	result := &pyudfpb.FunctionParamValue{}
	switch v := value.GetValue().(type) {
	case *schemapb.FunctionParamValue_BoolValue:
		result.Value = &pyudfpb.FunctionParamValue_BoolValue{BoolValue: v.BoolValue}
	case *schemapb.FunctionParamValue_Int64Value:
		result.Value = &pyudfpb.FunctionParamValue_Int64Value{Int64Value: v.Int64Value}
	case *schemapb.FunctionParamValue_DoubleValue:
		result.Value = &pyudfpb.FunctionParamValue_DoubleValue{DoubleValue: v.DoubleValue}
	case *schemapb.FunctionParamValue_StringValue:
		result.Value = &pyudfpb.FunctionParamValue_StringValue{StringValue: v.StringValue}
	case *schemapb.FunctionParamValue_BytesValue:
		result.Value = &pyudfpb.FunctionParamValue_BytesValue{BytesValue: v.BytesValue}
	case *schemapb.FunctionParamValue_ArrayValue:
		values := make([]*pyudfpb.FunctionParamValue, len(v.ArrayValue.GetValues()))
		for i, item := range v.ArrayValue.GetValues() {
			values[i] = encodeParamValue(item)
		}
		result.Value = &pyudfpb.FunctionParamValue_ArrayValue{ArrayValue: &pyudfpb.FunctionParamArray{Values: values}}
	case *schemapb.FunctionParamValue_ObjectValue:
		result.Value = &pyudfpb.FunctionParamValue_ObjectValue{ObjectValue: encodeParams(v.ObjectValue)}
	}
	return result
}

func releaseColumns(columns []*arrow.Chunked) {
	for _, column := range columns {
		column.Release()
	}
}

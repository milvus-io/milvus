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
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func encodeInputs(ctx context.Context, inputs []*arrow.Chunked) ([]byte, error) {
	return encodeInputsWithSchema(ctx, inputs, nil)
}

func encodeInputsWithSchema(ctx context.Context, inputs []*arrow.Chunked, inputSchema *arrow.Schema) ([]byte, error) {
	if len(inputs) == 0 || inputs[0] == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: invalid input columns")
	}
	count := len(inputs[0].Chunks())
	if count == 0 {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: invalid query count")
	}
	fields := make([]arrow.Field, len(inputs))
	if inputSchema != nil && inputSchema.NumFields() != len(inputs) {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: input schema column count differs from arguments")
	}
	for column, input := range inputs {
		if input == nil || input.DataType() == nil || len(input.Chunks()) != count {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: inconsistent input chunks column=%d", column)
		}
		fields[column] = arrow.Field{Name: fmt.Sprintf("c%d", column), Type: input.DataType(), Nullable: true}
		if inputSchema != nil {
			fields[column] = inputSchema.Field(column)
			if !arrow.TypeEqual(fields[column].Type, input.DataType()) {
				return nil, merr.WrapErrServiceInternalMsg("py_udf: input schema physical type differs from column %d", column)
			}
		}
	}

	var sink bytes.Buffer
	schema := arrow.NewSchema(fields, nil)
	if inputSchema != nil {
		metadata := inputSchema.Metadata()
		schema = arrow.NewSchema(fields, &metadata)
	}
	if err := types.ValidateRuntimeArrowSchema(schema); err != nil {
		return nil, err
	}
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
			if err := types.ValidateArrowArrayAgainstField(chunk, fields[column]); err != nil {
				return nil, err
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

func decodeOutputs(ctx context.Context, payload []byte) ([]*arrow.Chunked, error) {
	_, outputs, err := decodeOutputsWithSchema(ctx, payload)
	return outputs, err
}

func decodeOutputsWithSchema(ctx context.Context, payload []byte) (schema *arrow.Schema, outputs []*arrow.Chunked, err error) {
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
			schema = nil
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, nil, merr.Wrap(err, "py_udf: decode outputs")
	}
	// Delay schema decoding so Release is registered even if the schema is invalid.
	reader, err := ipc.NewReader(bytes.NewReader(payload), ipc.WithDelayReadSchema(true))
	if err != nil {
		return nil, nil, merr.WrapErrServiceInternalErr(err, "py_udf: decode IPC")
	}
	defer reader.Release()
	schema = reader.Schema()
	if reader.Err() != nil {
		return nil, nil, merr.WrapErrServiceInternalErr(reader.Err(), "py_udf: decode schema")
	}
	if schema == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("py_udf: invalid output schema")
	}
	if err := types.ValidateRuntimeArrowSchema(schema); err != nil {
		return nil, nil, err
	}
	chunks = make([][]arrow.Array, schema.NumFields())
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, merr.Wrap(err, "py_udf: decode outputs")
		}
		record := reader.Record()
		for i, column := range record.Columns() {
			if err := validateOutputArray(column); err != nil {
				return nil, nil, merr.Wrapf(err, "py_udf: output column=%d", i)
			}
			if err := types.ValidateArrowArrayAgainstField(column, schema.Field(i)); err != nil {
				return nil, nil, err
			}
			column.Retain()
			chunks[i] = append(chunks[i], column)
		}
	}
	if reader.Err() != nil {
		return nil, nil, merr.WrapErrServiceInternalErr(reader.Err(), "py_udf: decode output batch")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, merr.Wrap(err, "py_udf: decode outputs")
	}
	for i, column := range chunks {
		outputs = append(outputs, arrow.NewChunked(schema.Field(i).Type, column))
	}
	return schema, outputs, nil
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
	for row := 0; row < values.Len(); row++ {
		if values.IsValid(row) && !utf8.ValidString(values.Value(row)) {
			return merr.WrapErrFunctionFailedMsg("py_udf: invalid output UTF-8 at row=%d", row)
		}
	}
	return nil
}

// Validate nested values while decodeOutputs owns them and has its recovery
// boundary installed. IPC decoding alone does not check intermediate offsets.
func validateOutputArray(column arrow.Array) error {
	switch values := column.(type) {
	case *array.String:
		return validateOutputStringOffsets(values)
	case *array.LargeString:
		if err := validateLargeOffsets(values.ValueOffsets(), outputValueBufferSize(values)); err != nil {
			return err
		}
		for row := 0; row < values.Len(); row++ {
			if values.IsValid(row) && !utf8.ValidString(values.Value(row)) {
				return merr.WrapErrFunctionFailedMsg("py_udf: invalid output UTF-8 at row=%d", row)
			}
		}
	case *array.Binary:
		var previous int32
		for _, offset := range values.ValueOffsets() {
			if offset < previous || int64(offset) > outputValueBufferSize(values) {
				return merr.WrapErrFunctionFailedMsg("py_udf: invalid output binary offset")
			}
			previous = offset
		}
	case *array.LargeBinary:
		return validateLargeOffsets(values.ValueOffsets(), outputValueBufferSize(values))
	case *array.List:
		for row := 0; row < values.Len(); row++ {
			start, end := values.ValueOffsets(row)
			if start < 0 || end < start || end > int64(values.ListValues().Len()) {
				return merr.WrapErrFunctionFailedMsg("py_udf: invalid output list offsets at row=%d", row)
			}
		}
		return validateOutputArray(values.ListValues())
	case *array.LargeList:
		for row := 0; row < values.Len(); row++ {
			start, end := values.ValueOffsets(row)
			if start < 0 || end < start || end > int64(values.ListValues().Len()) {
				return merr.WrapErrFunctionFailedMsg("py_udf: invalid output list offsets at row=%d", row)
			}
		}
		return validateOutputArray(values.ListValues())
	case *array.FixedSizeList:
		if values.Len() > 0 {
			start, end := values.ValueOffsets(values.Len() - 1)
			if start < 0 || end < start || end > int64(values.ListValues().Len()) {
				return merr.WrapErrFunctionFailedMsg("py_udf: invalid output vector shape")
			}
		}
		return validateOutputArray(values.ListValues())
	case *array.Struct:
		for index := 0; index < values.NumField(); index++ {
			if values.Field(index).Len() != values.Len() {
				return merr.WrapErrFunctionFailedMsg("py_udf: invalid output struct child length")
			}
			if err := validateOutputArray(values.Field(index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateLargeOffsets(offsets []int64, size int64) error {
	var previous int64
	for _, offset := range offsets {
		if offset < previous || offset > size {
			return merr.WrapErrFunctionFailedMsg("py_udf: invalid output offsets")
		}
		previous = offset
	}
	return nil
}

func outputValueBufferSize(values arrow.Array) int64 {
	buffer := values.Data().Buffers()[2]
	if buffer == nil {
		return 0
	}
	return int64(buffer.Len())
}

func encodeExecuteRequest(ctx context.Context, request ExecuteRequest) (*pyudfpb.ExecuteRequest, error) {
	wire := &pyudfpb.ExecuteRequest{ResourceName: request.ResourceName, UdfPath: request.UDFPath, Stage: request.Stage, Params: encodeParams(request.Params), ExecutionVersion: executionVersion}
	if request.TableResult {
		if !strings.EqualFold(request.Stage, "post_process") {
			return nil, merr.WrapErrServiceInternal("py_udf: TABLE execution requires POST_PROCESS")
		}
		wire.ExecutionVersion = tableExecutionVersion
		wire.TableResult = true
		wire.Reduction = request.Reduction
	} else if request.Reduction {
		return nil, merr.WrapErrServiceInternal("py_udf: reduction requires TABLE execution")
	}
	if request.InputSchema != nil {
		wire.ExecutionVersion = schemaExecutionVersion
	}
	// BaseOp resolves repeated field references to the same Chunked object.
	// Deduplicate by identity, not equal values: distinct columns stay distinct.
	// Schema-aware calls also require the complete field to match, since aliases
	// and logical types can differ while sharing the same physical column.
	unique := make([]*arrow.Chunked, 0, len(request.Inputs))
	uniqueFields := make([]arrow.Field, 0, len(request.Inputs))
	if request.InputSchema != nil && request.InputSchema.NumFields() != len(request.Inputs) {
		return nil, merr.WrapErrServiceInternal("py_udf: input schema does not align with arguments")
	}
	positions := make(map[*arrow.Chunked][]uint32, len(request.Inputs))
	wire.InputColumnIndices = make([]uint32, len(request.Inputs))
	for i, column := range request.Inputs {
		var position uint32
		var exists bool
		for _, candidate := range positions[column] {
			if request.InputSchema == nil || request.InputSchema.Field(i).Equal(uniqueFields[candidate]) {
				position, exists = candidate, true
				break
			}
		}
		if !exists {
			position = uint32(len(unique))
			positions[column] = append(positions[column], position)
			unique = append(unique, column)
			if request.InputSchema != nil {
				uniqueFields = append(uniqueFields, request.InputSchema.Field(i))
			}
		}
		wire.InputColumnIndices[i] = position
	}
	var uniqueSchema *arrow.Schema
	if request.InputSchema != nil {
		metadata := request.InputSchema.Metadata()
		uniqueSchema = arrow.NewSchema(uniqueFields, &metadata)
	}
	payload, err := encodeInputsWithSchema(ctx, unique, uniqueSchema)
	if err != nil {
		return nil, err
	}
	wire.Inputs = payload
	if request.QueryContext != nil {
		queryContext, expanded, err := encodeRequestQueryContext(request)
		if err != nil {
			return nil, err
		}
		wire.QueryContext = queryContext
		if expanded {
			wire.ExecutionVersion = queryExpansionExecutionVersion
		}
	}
	if request.RequestContext != nil {
		scope := *request.RequestContext
		wire.RequestContext = &pyudfpb.RequestContext{
			Database: scope.Database, Collection: scope.Collection,
			User: scope.User, ResourceName: scope.ResourceName,
		}
		wire.ExecutionVersion = requestContextExecutionVersion
	}

	return wire, nil
}

func encodeRequestQueryContext(request ExecuteRequest) (*pyudfpb.QueryContext, bool, error) {
	queries := request.QueryContext
	if len(queries.OriginalQueries) != len(queries.Queries) || len(queries.Queries) == 0 && !request.TableResult {
		return nil, false, merr.WrapErrServiceInternal("py_udf: query context ANN inputs do not align")
	}
	originalCount := 0
	if len(queries.OriginalQueries) > 0 {
		originalCount = len(queries.OriginalQueries[0].GetValues())
	}
	original, err := encodeQueryInputs(queries.OriginalQueries, originalCount)
	if err != nil {
		return nil, false, err
	}
	if queries.QueryOrigins != nil && len(queries.QueryOrigins) != len(queries.Queries) {
		return nil, false, merr.WrapErrServiceInternal("py_udf: prepared query mappings do not align with ANN inputs")
	}
	prepared := make([]*pyudfpb.QueryInput, len(queries.Queries))
	expanded := false
	for ann, input := range queries.Queries {
		values, err := encodeQueryInputs([]*commonpb.PlaceholderValue{input}, len(input.GetValues()))
		if err != nil {
			return nil, false, err
		}
		prepared[ann] = values[0]
		origins := []int64(nil)
		if queries.QueryOrigins != nil {
			origins = queries.QueryOrigins[ann]
		}
		if origins == nil {
			if len(input.GetValues()) != originalCount {
				return nil, false, merr.WrapErrServiceInternal("py_udf: prepared query count changed without a mapping")
			}
			continue
		}
		if err := validateQueryOrigins(origins, len(input.GetValues()), originalCount); err != nil {
			return nil, false, err
		}
		identity := len(origins) == originalCount
		for query, origin := range origins {
			identity = identity && origin == int64(query)
		}
		if !identity {
			prepared[ann].OriginalQueryIndices = origins
			expanded = true
		}
	}
	rowOrigins := queries.RowQueryOrigins
	if strings.EqualFold(request.Stage, types.StagePreProcess) {
		if len(request.Inputs[0].Chunks()) != 1 {
			return nil, false, merr.WrapErrServiceInternal("py_udf: PRE requires one query-row group")
		}
		rows := request.Inputs[0].Chunk(0).Len()
		if rowOrigins != nil {
			if err := validateQueryOrigins(rowOrigins, rows, originalCount); err != nil {
				return nil, false, err
			}
			expanded = true
		} else if rows != originalCount {
			return nil, false, merr.WrapErrServiceInternal("py_udf: PRE row count changed without row query origins")
		}
	} else if rowOrigins != nil {
		return nil, false, merr.WrapErrServiceInternal("py_udf: current row query origins require PRE")
	} else if !request.TableResult {
		for _, input := range prepared {
			if len(input.Values) != len(request.Inputs[0].Chunks()) {
				return nil, false, merr.WrapErrServiceInternal("py_udf: prepared query count differs from candidate groups")
			}
		}
	}
	return &pyudfpb.QueryContext{OriginalQueries: original, Queries: prepared, RowQueryOrigins: rowOrigins}, expanded, nil
}

func validateQueryOrigins(origins []int64, rows, originalCount int) error {
	if len(origins) != rows {
		return merr.WrapErrServiceInternal("py_udf: query mapping count differs from prepared rows")
	}
	previous := int64(-1)
	for _, origin := range origins {
		if origin < 0 || origin >= int64(originalCount) || origin < previous {
			return merr.WrapErrServiceInternal("py_udf: query mapping has invalid bounds or parent order")
		}
		previous = origin
	}
	return nil
}

func encodeQueryInputs(inputs []*commonpb.PlaceholderValue, nq int) ([]*pyudfpb.QueryInput, error) {
	result := make([]*pyudfpb.QueryInput, len(inputs))
	for index, input := range inputs {
		if input == nil || len(input.GetValues()) != nq {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: query input %d does not match NQ %d", index, nq)
		}
		name, ok := commonpb.PlaceholderType_name[int32(input.GetType())]
		if !ok || input.GetType() == commonpb.PlaceholderType_None {
			return nil, merr.WrapErrServiceInternal("py_udf: query context has invalid placeholder type")
		}
		if input.GetType() == commonpb.PlaceholderType_VarChar {
			for _, value := range input.GetValues() {
				if !utf8.Valid(value) {
					return nil, merr.WrapErrServiceInternal("py_udf: query context contains invalid UTF-8")
				}
			}
		}
		result[index] = &pyudfpb.QueryInput{DataType: name, Values: input.GetValues(), Tag: input.GetTag()}
	}
	return result, nil
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

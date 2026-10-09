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

package chain

import (
	"bytes"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	json "github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type resolvedInputGroup struct {
	SourceFieldID int64
	Inputs        []ResolvedChainInput
}

func materializeEmptyPlannedColumns(
	builder *DataFrameBuilder,
	plan *DataFrameInputPlan,
	numChunks int,
	alloc memory.Allocator,
) error {
	if _, err := groupResolvedInputsBySourceFieldID(plan.Inputs); err != nil {
		return err
	}
	for _, input := range plan.Inputs {
		dataType := input.DataType
		nullable := input.Nullable
		if input.DataType == schemapb.DataType_JSON {
			if !isSupportedJSONProjectionHint(input.DataTypeHint) {
				return merr.WrapErrServiceInternalMsg(
					"function chain input %q has unsupported resolved data type %s",
					input.LogicalName, input.DataTypeHint.String())
			}
			dataType = input.DataTypeHint
			nullable = true
		}
		chunks, err := buildTypedEmptyChunks(dataType, numChunks, alloc)
		if err != nil {
			return merr.Wrapf(err, "function chain input %q", input.LogicalName)
		}
		builder.SetFieldType(input.LogicalName, dataType)
		builder.SetFieldNullable(input.LogicalName, nullable)
		if input.DataType != schemapb.DataType_JSON {
			builder.SetFieldID(input.LogicalName, input.SourceFieldID)
		}
		if err := builder.AddColumnFromChunks(input.LogicalName, chunks); err != nil {
			return err
		}
	}
	return nil
}

func buildTypedEmptyChunks(
	dataType schemapb.DataType,
	numChunks int,
	alloc memory.Allocator,
) ([]arrow.Array, error) {
	arrowType, err := ToArrowType(dataType)
	if err != nil {
		return nil, err
	}
	chunks := make([]arrow.Array, numChunks)
	for chunkIdx := range numChunks {
		builder := array.NewBuilder(alloc, arrowType)
		chunks[chunkIdx] = builder.NewArray()
		builder.Release()
	}
	return chunks, nil
}

func importPlannedFieldData(
	builder *DataFrameBuilder,
	fieldsData []*schemapb.FieldData,
	plan *DataFrameInputPlan,
	offsets []int64,
	alloc memory.Allocator,
	seenFieldIDs map[int64]bool,
	seenFieldNames map[string]bool,
) error {
	groups, err := groupResolvedInputsBySourceFieldID(plan.Inputs)
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		return nil
	}

	neededFieldIDs := make(map[int64]struct{}, len(groups))
	for _, group := range groups {
		neededFieldIDs[group.SourceFieldID] = struct{}{}
	}
	fieldDataByID := make(map[int64]*schemapb.FieldData, len(groups))
	for _, fieldData := range fieldsData {
		if fieldData == nil {
			continue
		}
		fieldID := fieldData.GetFieldId()
		if _, needed := neededFieldIDs[fieldID]; !needed {
			continue
		}
		if existing := fieldDataByID[fieldID]; existing != nil {
			return merr.WrapErrServiceInternalMsg(
				"duplicate field id %d (fieldName=%q)", fieldID, fieldData.GetFieldName())
		}
		fieldDataByID[fieldID] = fieldData
	}

	for _, group := range groups {
		fieldData := fieldDataByID[group.SourceFieldID]
		if fieldData == nil {
			return merr.WrapErrServiceInternalMsg(
				"function chain input source field id %d is missing from search result", group.SourceFieldID)
		}
		first := group.Inputs[0]
		if fieldData.GetType() != first.DataType {
			return merr.WrapErrServiceInternalMsg(
				"function chain input source field %q type mismatch: expected %s, got %s",
				first.FieldName, first.DataType.String(), fieldData.GetType().String())
		}
		if fieldName := fieldData.GetFieldName(); fieldName != "" && fieldName != first.FieldName {
			return merr.WrapErrServiceInternalMsg(
				"function chain input source field id %d name mismatch: expected %q, got %q",
				group.SourceFieldID, first.FieldName, fieldName)
		}

		if first.DataType == schemapb.DataType_JSON {
			if err := projectJSONFieldData(builder, fieldData, group.Inputs, offsets, alloc); err != nil {
				return err
			}
		} else {
			if len(group.Inputs) != 1 {
				return merr.WrapErrServiceInternalMsg(
					"physical scalar field id %d resolves to %d chain inputs", group.SourceFieldID, len(group.Inputs))
			}
			if err := importFieldDataWithName(builder, fieldData, first.LogicalName, offsets, alloc); err != nil {
				return err
			}
		}

		// A JSON root in FieldsData and its already-typed group-by projection in
		// GroupByFieldValues intentionally share the physical field ID. Only a
		// directly imported scalar occupies that ID in the group-by dedup space.
		if first.DataType != schemapb.DataType_JSON {
			seenFieldIDs[group.SourceFieldID] = true
		}
		for _, input := range group.Inputs {
			seenFieldNames[input.LogicalName] = true
		}
	}
	return nil
}

func groupResolvedInputsBySourceFieldID(inputs []ResolvedChainInput) ([]resolvedInputGroup, error) {
	groups := make([]resolvedInputGroup, 0)
	groupOffsets := make(map[int64]int)
	seenLogicalNames := make(map[string]struct{}, len(inputs))

	for _, input := range inputs {
		if input.LogicalName == "" {
			return nil, merr.WrapErrServiceInternal("resolved function chain input has empty logical name")
		}
		if input.DataType == schemapb.DataType_JSON && len(input.NestedPath) == 0 {
			return nil, merr.WrapErrServiceInternalMsg(
				"resolved function chain input %q contains an unsupported complete JSON root",
				input.LogicalName)
		}
		if _, ok := seenLogicalNames[input.LogicalName]; ok {
			return nil, merr.WrapErrServiceInternalMsg(
				"resolved function chain input %q appears more than once", input.LogicalName)
		}
		seenLogicalNames[input.LogicalName] = struct{}{}

		if offset, ok := groupOffsets[input.SourceFieldID]; ok {
			first := groups[offset].Inputs[0]
			if first.FieldName != input.FieldName || first.DataType != input.DataType {
				return nil, merr.WrapErrServiceInternalMsg(
					"resolved function chain inputs for field id %d have inconsistent source metadata",
					input.SourceFieldID)
			}
			groups[offset].Inputs = append(groups[offset].Inputs, input)
			continue
		}

		groupOffsets[input.SourceFieldID] = len(groups)
		groups = append(groups, resolvedInputGroup{
			SourceFieldID: input.SourceFieldID,
			Inputs:        []ResolvedChainInput{input},
		})
	}
	return groups, nil
}

func projectJSONFieldData(
	builder *DataFrameBuilder,
	fieldData *schemapb.FieldData,
	inputs []ResolvedChainInput,
	offsets []int64,
	alloc memory.Allocator,
) error {
	totalRows := offsets[len(offsets)-1]
	jsonData, err := getScalarJSONData(fieldData, inputs[0].FieldName)
	if err != nil {
		return err
	}
	if int64(len(jsonData)) < totalRows {
		return merr.WrapErrServiceInternalMsg(
			"field %s: JSON data length (%d) is less than totalRows (%d)",
			inputs[0].FieldName, len(jsonData), totalRows)
	}

	validData := typeutil.GetFieldDataValidData(fieldData)
	nullableRoot := len(validData) > 0
	if nullableRoot && int64(len(validData)) < totalRows {
		return merr.WrapErrServiceInternalMsg(
			"field %s: validData length (%d) is less than totalRows (%d)",
			inputs[0].FieldName, len(validData), totalRows)
	}

	writers := make([]jsonProjectionWriter, len(inputs))
	for inputIdx, input := range inputs {
		writer, err := newJSONProjectionWriter(input.DataTypeHint, alloc)
		if err != nil {
			for _, initialized := range writers[:inputIdx] {
				initialized.release()
			}
			return merr.Wrapf(err, "function chain input %q", input.LogicalName)
		}
		writers[inputIdx] = writer
	}
	defer func() {
		for _, writer := range writers {
			writer.release()
		}
	}()

	for chunkIdx := 0; chunkIdx < len(offsets)-1; chunkIdx++ {
		for row := offsets[chunkIdx]; row < offsets[chunkIdx+1]; row++ {
			if nullableRoot && !validData[row] {
				for _, writer := range writers {
					writer.append(nil)
				}
				continue
			}

			document, err := decodeJSONDocument(jsonData[row])
			if err != nil {
				return merr.WrapErrDataIntegrity(
					err, "function chain source field %q contains invalid JSON at row %d", inputs[0].FieldName, row)
			}

			for inputIdx, input := range inputs {
				writers[inputIdx].append(lookupJSONPath(document, input.NestedPath))
			}
		}
		for _, writer := range writers {
			writer.finishChunk()
		}
	}

	for inputIdx, input := range inputs {
		dataType := input.DataTypeHint
		builder.SetFieldType(input.LogicalName, dataType)
		builder.SetFieldNullable(input.LogicalName, true)
		if err := builder.AddColumnFromChunks(input.LogicalName, writers[inputIdx].takeChunks()); err != nil {
			return err
		}
	}
	return nil
}

func getScalarJSONData(fieldData *schemapb.FieldData, fieldName string) ([][]byte, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, merr.WrapErrServiceInternalMsg("field %s: scalars is nil", fieldName)
	}
	jsonData := scalars.GetJsonData()
	if jsonData == nil {
		return nil, merr.WrapErrServiceInternalMsg("field %s: JSON data is nil", fieldName)
	}
	return jsonData.GetData(), nil
}

func decodeJSONDocument(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	return document, nil
}

func lookupJSONPath(document any, path []string) any {
	current := document
	for _, segment := range path {
		switch value := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = value[segment]
			if !ok {
				return nil
			}
		case []any:
			index, ok := parseJSONArrayIndex(segment)
			if !ok || index >= len(value) {
				return nil
			}
			current = value[index]
		default:
			return nil
		}
	}
	return current
}

// parseJSONArrayIndex follows the JSON Pointer array-index grammar: zero is
// written as "0", while non-zero indexes must not have a leading zero.
func parseJSONArrayIndex(segment string) (int, bool) {
	index, err := strconv.Atoi(segment)
	if err != nil || index < 0 {
		return 0, false
	}
	return index, strconv.Itoa(index) == segment
}

type jsonProjectionWriter interface {
	append(any)
	finishChunk()
	takeChunks() []arrow.Array
	release()
}

type typedJSONProjectionWriter[T any] struct {
	alloc      memory.Allocator
	newBuilder func(memory.Allocator) singleBuilder[T]
	convert    func(any) (T, bool)
	builder    singleBuilder[T]
	chunks     []arrow.Array
}

func newJSONProjectionWriter(
	dataType schemapb.DataType,
	alloc memory.Allocator,
) (jsonProjectionWriter, error) {
	switch dataType {
	case schemapb.DataType_Bool:
		return newTypedJSONProjectionWriter(alloc, array.NewBooleanBuilder, convertJSONBool), nil
	case schemapb.DataType_Int64:
		return newTypedJSONProjectionWriter(alloc, array.NewInt64Builder, convertJSONInt64), nil
	case schemapb.DataType_Double:
		return newTypedJSONProjectionWriter(alloc, array.NewFloat64Builder, convertJSONDouble), nil
	case schemapb.DataType_VarChar:
		return newTypedJSONProjectionWriter(alloc, array.NewStringBuilder, convertJSONString), nil
	default:
		return nil, merr.WrapErrServiceInternalMsg(
			"unsupported resolved JSON projection type %s", dataType.String())
	}
}

func newTypedJSONProjectionWriter[T any, B singleBuilder[T]](
	alloc memory.Allocator,
	newBuilder func(memory.Allocator) B,
	convert func(any) (T, bool),
) jsonProjectionWriter {
	return &typedJSONProjectionWriter[T]{
		alloc: alloc,
		newBuilder: func(alloc memory.Allocator) singleBuilder[T] {
			return newBuilder(alloc)
		},
		convert: convert,
	}
}

func (w *typedJSONProjectionWriter[T]) append(value any) {
	if w.builder == nil {
		w.builder = w.newBuilder(w.alloc)
	}
	converted, ok := w.convert(value)
	if !ok {
		w.builder.AppendNull()
		return
	}
	w.builder.Append(converted)
}

func (w *typedJSONProjectionWriter[T]) finishChunk() {
	if w.builder == nil {
		w.builder = w.newBuilder(w.alloc)
	}
	w.chunks = append(w.chunks, w.builder.NewArray())
	w.builder.Release()
	w.builder = nil
}

func (w *typedJSONProjectionWriter[T]) takeChunks() []arrow.Array {
	chunks := w.chunks
	w.chunks = nil
	return chunks
}

func (w *typedJSONProjectionWriter[T]) release() {
	if w.builder != nil {
		w.builder.Release()
		w.builder = nil
	}
	for _, chunk := range w.chunks {
		chunk.Release()
	}
	w.chunks = nil
}

/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"math"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ChunkSelection selects positions in one immediate input chunk. Use the
// constructors; the zero value is invalid, so missing selections cannot mean all.
// Index slices are borrowed and must remain immutable until assembly returns.
type ChunkSelection struct {
	kind       selectionKind
	start, end int64
	indices    []int
}

type selectionKind uint8

const (
	selectionAll selectionKind = iota + 1
	selectionRange
	selectionIndices
)

// AllRows retains every position in one chunk.
func AllRows() ChunkSelection { return ChunkSelection{kind: selectionAll} }

// RowRange selects a half-open input interval without copying value buffers.
func RowRange(start, end int64) ChunkSelection {
	return ChunkSelection{kind: selectionRange, start: start, end: end}
}

// RowIndices permits ordered, repeated positions. An empty slice selects no rows.
func RowIndices(indices []int) ChunkSelection {
	return ChunkSelection{kind: selectionIndices, indices: indices}
}

// RowSelection has exactly one entry per logical query chunk. It cannot move
// rows between chunks. Physical execution batches do not redefine query groups.
type RowSelection struct{ Chunks []ChunkSelection }

// NamedColumn owns one reference to Data. Field describes the computed value,
// including its nullability and Arrow metadata, not its source collection field.
type NamedColumn struct {
	Field arrow.Field
	Data  *arrow.Chunked
}

// OperatorResult owns its computed columns, already in selected output order.
// A nil Selection preserves input row count/order. Release is required even when
// execution or assembly fails. Input columns remain borrowed and immutable.
type OperatorResult struct {
	Columns   []NamedColumn
	Selection *RowSelection
}

func (r *OperatorResult) Release() {
	if r == nil {
		return
	}
	for i := range r.Columns {
		if r.Columns[i].Data != nil {
			r.Columns[i].Data.Release()
			r.Columns[i].Data = nil
		}
	}
}

// ExecuteOperator executes a single-input operator and assembles a complete
// DataFrame. The input is borrowed; the caller owns the returned DataFrame.
// Cancellation is checked before execution; successful execution and assembly
// may finish even if cancellation arrives after this check.
func ExecuteOperator(ctx *types.FuncContext, input *DataFrame, op Operator) (*DataFrame, error) {
	if err := validateFrameLayout(input); err != nil {
		return nil, err
	}
	if err := ctx.Context().Err(); err != nil {
		return nil, err
	}
	result, err := op.Execute(ctx, input)
	defer result.Release()
	if err != nil {
		return nil, err
	}
	return assembleOperatorResult(ctx, input, op.Outputs(), result)
}

func validateChunkedColumn(col *arrow.Chunked, sizes []int64) error {
	if col == nil {
		return merr.WrapErrServiceInternalMsg("column is nil")
	}
	if len(col.Chunks()) != len(sizes) {
		return merr.WrapErrServiceInternalMsg("column chunk count does not match dataframe")
	}
	for i, chunk := range col.Chunks() {
		if chunk == nil || int64(chunk.Len()) != sizes[i] || !arrow.TypeEqual(chunk.DataType(), col.DataType()) {
			return merr.WrapErrServiceInternalMsg("column chunk %d has invalid length or type", i)
		}
	}
	return nil
}

func validateFrameLayout(df *DataFrame) error {
	if df == nil {
		return merr.WrapErrServiceInternalMsg("dataframe is nil")
	}
	var total int64
	for _, size := range df.chunkSizes {
		if size < 0 || size > int64(math.MaxInt) || total > int64(math.MaxInt)-size {
			return merr.WrapErrServiceInternalMsg("invalid dataframe chunk size")
		}
		total += size
	}
	if df.schema == nil {
		if len(df.columns) == 0 && len(df.nameIndex) == 0 {
			return nil
		}
		return merr.WrapErrServiceInternalMsg("dataframe schema is missing")
	}
	if df.schema.NumFields() != len(df.columns) || len(df.nameIndex) != len(df.columns) {
		return merr.WrapErrServiceInternalMsg("dataframe schema and columns differ")
	}
	for i, field := range df.schema.Fields() {
		idx, ok := df.nameIndex[field.Name]
		if field.Name == "" || !ok || idx != i {
			return merr.WrapErrServiceInternalMsg("invalid dataframe column name %q", field.Name)
		}
		if err := validateChunkedColumn(df.columns[i], df.chunkSizes); err != nil {
			return merr.Wrapf(err, "column %q", field.Name)
		}
		if !arrow.TypeEqual(field.Type, df.columns[i].DataType()) {
			return merr.WrapErrServiceInternalMsg("column %q schema type differs", field.Name)
		}
	}
	return nil
}

func selectionSizes(input *DataFrame, selection *RowSelection) ([]int64, error) {
	sizes := input.ChunkSizes()
	if selection == nil {
		return sizes, nil
	}
	if len(selection.Chunks) != len(sizes) {
		return nil, merr.WrapErrServiceInternalMsg("selection must contain every input chunk")
	}
	var total int64
	for i, sel := range selection.Chunks {
		switch sel.kind {
		case selectionAll:
		case selectionRange:
			if sel.start < 0 || sel.end < sel.start || sel.end > sizes[i] {
				return nil, merr.WrapErrServiceInternalMsg("invalid selection range for chunk %d", i)
			}
			sizes[i] = sel.end - sel.start
		case selectionIndices:
			for _, idx := range sel.indices {
				if idx < 0 || int64(idx) >= sizes[i] {
					return nil, merr.WrapErrServiceInternalMsg("selection index %d out of range in chunk %d", idx, i)
				}
			}
			sizes[i] = int64(len(sel.indices))
		default:
			return nil, merr.WrapErrServiceInternalMsg("missing or invalid selection for chunk %d", i)
		}
		if total > int64(math.MaxInt)-sizes[i] {
			return nil, merr.WrapErrServiceInternalMsg("selection size overflow")
		}
		total += sizes[i]
	}
	return sizes, nil
}

// assembleOperatorResult borrows the complete inputDF (after any framework
// pruning), validated by ExecuteOperator and left unchanged by the operator,
// and opResult. The output owns independent references, so inputDF and opResult
// may be released immediately after return.
func assembleOperatorResult(ctx *types.FuncContext, inputDF *DataFrame, outputs []string, opResult *OperatorResult) (*DataFrame, error) {
	if opResult == nil {
		return nil, merr.WrapErrServiceInternalMsg("operator returned nil result")
	}
	sizes, err := selectionSizes(inputDF, opResult.Selection)
	if err != nil {
		return nil, err
	}
	computed, err := validateOperatorColumns(outputs, opResult.Columns, sizes)
	if err != nil {
		return nil, err
	}
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(sizes).CopyAllMetadata(inputDF)
	fields := make([]arrow.Field, 0, len(inputDF.columns)+len(computed))
	for _, col := range opResult.Columns {
		col.Data.Retain()
		if err := builder.addColumn(col.Field.Name, col.Data); err != nil {
			return nil, err
		}
		builder.SetFieldNullable(col.Field.Name, col.Field.Nullable)
		if typ, err := ToMilvusType(col.Field.Type); err == nil {
			builder.SetFieldType(col.Field.Name, typ)
		}
		fields = append(fields, col.Field)
	}
	for _, name := range inputDF.ColumnNames() {
		if _, replaced := computed[name]; replaced {
			continue
		}
		col := inputDF.Column(name)
		selected, err := selectColumn(ctx, col, opResult.Selection)
		if err != nil {
			return nil, merr.Wrapf(err, "select column %q", name)
		}
		if err := builder.addColumn(name, selected); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(inputDF, name)
		fields = append(fields, inputDF.schema.Field(inputDF.nameIndex[name]))
	}
	outputDF := builder.Build()
	var metadata arrow.Metadata
	if inputDF.schema != nil {
		metadata = inputDF.schema.Metadata()
	}
	outputDF.schema = arrow.NewSchema(fields, &metadata)
	return outputDF, nil
}

// validateOperatorColumns checks computed columns without taking ownership.
func validateOperatorColumns(outputs []string, columns []NamedColumn, sizes []int64) (map[string]NamedColumn, error) {
	declared := make(map[string]bool, len(outputs))
	for _, name := range outputs {
		if name == "" || declared[name] {
			return nil, merr.WrapErrServiceInternalMsg("invalid or duplicate declared output %q", name)
		}
		declared[name] = true
	}
	computed := make(map[string]NamedColumn, len(columns))
	for _, col := range columns {
		name := col.Field.Name
		if _, exists := computed[name]; exists || !declared[name] {
			return nil, merr.WrapErrServiceInternalMsg("undeclared or duplicate output %q", name)
		}
		if err := validateChunkedColumn(col.Data, sizes); err != nil {
			return nil, merr.Wrapf(err, "output %q", name)
		}
		if col.Field.Type == nil || !arrow.TypeEqual(col.Field.Type, col.Data.DataType()) {
			return nil, merr.WrapErrServiceInternalMsg("output %q schema type differs", name)
		}
		if !col.Field.Nullable && col.Data.NullN() != 0 {
			return nil, merr.WrapErrServiceInternalMsg("non-nullable output %q contains nulls", name)
		}
		computed[name] = col
	}
	if len(computed) != len(declared) {
		return nil, merr.WrapErrServiceInternalMsg("operator did not return every declared output")
	}

	return computed, nil
}

func releaseArrays(arrays []arrow.Array) {
	for _, arr := range arrays {
		if arr != nil {
			arr.Release()
		}
	}
}

func selectColumn(ctx *types.FuncContext, col *arrow.Chunked, selection *RowSelection) (*arrow.Chunked, error) {
	if selection == nil {
		col.Retain()
		return col, nil
	}
	chunks := make([]arrow.Array, len(selection.Chunks))
	defer releaseArrays(chunks)
	for i, sel := range selection.Chunks {
		input := col.Chunk(i)
		switch sel.kind {
		case selectionAll:
			input.Retain()
			chunks[i] = input
		case selectionRange:
			chunks[i] = array.NewSlice(input, sel.start, sel.end)
		case selectionIndices:
			if len(sel.indices) == 0 {
				chunks[i] = array.NewSlice(input, 0, 0)
				continue
			}
			// Keep the legacy scalar-type dispatch: TakeArray in our pinned Arrow
			// v17 fork has quadratic allocation/copy costs for strings and lists.
			// Reconsider TakeArray after upgrading Arrow with the performance fixes
			// in apache/arrow-go#557 and #573 (released in v18.5.0).
			selected, err := dispatchPickByIndices(ctx.Pool(), input, sel.indices)
			if err != nil {
				return nil, err
			}
			chunks[i] = selected
		}
	}
	return arrow.NewChunked(col.DataType(), chunks), nil
}

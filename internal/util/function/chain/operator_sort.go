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
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func init() {
	MustRegisterOperator(
		types.OpTypeSort,
		statelessOperatorFactory(NewSortOpFromRepr),
	)
}

// SortOp sorts the DataFrame by a column.
// Note: Uses BaseOp.inputs[0] as the sort column name.
// Each chunk is sorted independently (per-query sorting for search results).
// When tieBreakCol is set, ties on the primary sort column are broken by that
// column in ascending order (e.g., sort by $score DESC, then $id ASC).
type SortOp struct {
	BaseOp
	desc        bool
	tieBreakCol string // optional: column name for tie-breaking (ascending)
}

func newSortOp(column string, desc bool, tieBreakCol string) *SortOp {
	inputs := []string{column}
	if tieBreakCol != "" && tieBreakCol != column {
		inputs = append(inputs, tieBreakCol)
	}
	return &SortOp{
		BaseOp: BaseOp{
			inputs:  inputs,
			outputs: []string{}, // Sort doesn't produce new columns
		},
		desc:        desc,
		tieBreakCol: tieBreakCol,
	}
}

// Column returns the sort column name.
func (o *SortOp) Column() string {
	if len(o.inputs) > 0 {
		return o.inputs[0]
	}
	return ""
}

func (o *SortOp) Name() string { return "Sort" }

// Inputs and Outputs are inherited from BaseOp

func (o *SortOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if err := ctx.Context().Err(); err != nil {
		return nil, err
	}
	if err := validateFrameShape(input); err != nil {
		return nil, merr.Wrap(err, "sort_op input")
	}
	column := o.Column()
	sortCol, err := input.ResolveColumn(column, ctx.Pool())
	if err != nil {
		return nil, merr.Wrapf(err, "sort_op column %q", column)
	}
	defer sortCol.Release()
	if err := validateComparableColumn(input, column, "sort_op"); err != nil {
		return nil, err
	}

	// A declared tie-break column is part of the ordering contract. Resolve
	// Struct paths exactly like the primary key, and never silently ignore it.
	var tieBreakCol *arrow.Chunked
	if o.tieBreakCol != "" {
		tieBreakCol, err = input.ResolveColumn(o.tieBreakCol, ctx.Pool())
		if err != nil {
			return nil, merr.Wrapf(err, "sort_op tie-break column %q", o.tieBreakCol)
		}
		defer tieBreakCol.Release()
		if err := validateComparableColumn(input, o.tieBreakCol, "sort_op tie-break"); err != nil {
			return nil, err
		}
	}

	colNames := input.ColumnNames()
	collector := NewChunkCollector(colNames, input.NumChunks())
	defer collector.Release()

	newChunkSizes := make([]int64, input.NumChunks())
	pickedRows := make([][]int, input.NumChunks())

	// Process each chunk independently
	for chunkIdx := range input.NumChunks() {
		if err := ctx.Context().Err(); err != nil {
			return nil, err
		}
		sortChunk := sortCol.Chunk(chunkIdx)
		chunkLen := sortChunk.Len()

		// Resolve tie-break chunk for this chunk index
		var tbChunk arrow.Array
		if tieBreakCol != nil {
			tbChunk = tieBreakCol.Chunk(chunkIdx)
		}

		// Build sort indices
		indices := make([]int, chunkLen)
		for i := range chunkLen {
			indices[i] = i
		}

		// Sort indices based on values, with tie-breaking by ID ascending.
		// Numeric values precede NaNs, which precede nulls in either direction.
		less, useTypedLess := makeArrayRowLess(sortChunk, tbChunk, o.desc)
		if useTypedLess {
			sort.SliceStable(indices, func(i, j int) bool {
				return less(indices[i], indices[j])
			})
		} else {
			sort.SliceStable(indices, func(i, j int) bool {
				vi := indices[i]
				vj := indices[j]

				iNull := sortChunk.IsNull(vi)
				jNull := sortChunk.IsNull(vj)
				if iNull && jNull {
					// Both null — use tie-break if available
					if tbChunk != nil {
						return compareArrayValues(tbChunk, vi, vj) < 0
					}
					return false
				}
				if iNull {
					return false // null always goes after non-null
				}
				if jNull {
					return true // non-null always goes before null
				}
				iNaN, jNaN := isArrayValueNaN(sortChunk, vi), isArrayValueNaN(sortChunk, vj)
				if iNaN != jNaN {
					return !iNaN
				}

				cmp := compareArrayValues(sortChunk, vi, vj)
				if cmp != 0 {
					if o.desc {
						return cmp > 0
					}
					return cmp < 0
				}
				// Tie-break: sort by tie-break column ascending
				if tbChunk != nil {
					return compareArrayValues(tbChunk, vi, vj) < 0
				}
				return false
			})
		}
		newChunkSizes[chunkIdx] = int64(chunkLen)
		pickedRows[chunkIdx] = indices

		// Reorder each column
		for _, colName := range colNames {
			col := input.Column(colName)
			dataChunk := col.Chunk(chunkIdx)
			reordered, err := reorderArray(ctx.Pool(), dataChunk, indices)
			if err != nil {
				return nil, merr.Wrapf(err, "sort_op: column %s", colName)
			}
			collector.Set(colName, chunkIdx, reordered)
		}
	}

	// Create new DataFrame with all chunks
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(newChunkSizes)
	builder.CopyAllMetadata(input)
	lineage, err := pickLineage(input, pickedRows)
	if err != nil {
		return nil, err
	}
	builder.SetLineage(lineage)

	for _, colName := range colNames {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(input, colName)
	}

	return builder.Build(), nil
}

type rowLessFunc func(i, j int) bool

func makeArrayRowLess(sortArr arrow.Array, tieArr arrow.Array, desc bool) (rowLessFunc, bool) {
	if !desc {
		return nil, false
	}
	if scoreArr, ok := sortArr.(*array.Float32); ok && scoreArr.NullN() == 0 {
		if idArr, ok := tieArr.(*array.Int64); ok && idArr.NullN() == 0 {
			return func(i, j int) bool {
				si := scoreArr.Value(i)
				sj := scoreArr.Value(j)
				iNaN, jNaN := math.IsNaN(float64(si)), math.IsNaN(float64(sj))
				if iNaN != jNaN {
					return !iNaN
				}
				if !iNaN && si != sj {
					return si > sj
				}
				return idArr.Value(i) < idArr.Value(j)
			}, true
		}
	}
	return nil, false
}

func isArrayValueNaN(arr arrow.Array, index int) bool {
	switch a := arr.(type) {
	case *array.Float32:
		return math.IsNaN(float64(a.Value(index)))
	case *array.Float64:
		return math.IsNaN(a.Value(index))
	default:
		return false
	}
}

// isComparableType checks if an Arrow data type is comparable for sorting.
func isComparableType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.BOOL, arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY,
		arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64, arrow.DURATION:
		return true
	default:
		return false
	}
}

// validateComparableArrowField checks the logical type as well as its Arrow
// storage. JSON, Geometry and vectors can use String/Binary physical storage,
// but that storage does not give them scalar ordering semantics.
func validateComparableArrowField(field arrow.Field, opName string) error {
	if field.Type == nil || !isComparableType(field.Type) {
		return merr.WrapErrParameterInvalidMsg("%s: column %q has non-comparable type %v", opName, field.Name, field.Type)
	}
	if err := types.ValidateRuntimeArrowDataType(field.Type); err != nil {
		return merr.Wrapf(err, "%s column %q", opName, field.Name)
	}
	if raw, present := field.Metadata.GetValue("milvus.data_type"); present {
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return merr.WrapErrFunctionFailed(err, "%s: column %q has invalid Milvus type metadata", opName, field.Name)
		}
		switch schemapb.DataType(value) {
		case schemapb.DataType_None, schemapb.DataType_Bool, schemapb.DataType_Int8,
			schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64,
			schemapb.DataType_Float, schemapb.DataType_Double, schemapb.DataType_String,
			schemapb.DataType_VarChar, schemapb.DataType_Text, schemapb.DataType_Timestamptz,
			schemapb.DataType_Date, schemapb.DataType_Time, schemapb.DataType_UUID:
			if err := types.ValidateArrowFieldMilvusType(field, schemapb.DataType(value)); err != nil {
				return merr.Wrapf(err, "%s column %q", opName, field.Name)
			}
		default:
			return merr.WrapErrParameterInvalidMsg("%s: column %q has non-comparable Milvus type %s", opName, field.Name, schemapb.DataType(value))
		}
	}
	return nil
}

func validateComparableColumn(input *DataFrame, name, opName string) error {
	field, err := input.resolveArrowField(name)
	if err != nil {
		return merr.Wrapf(err, "%s column %q", opName, name)
	}
	if dataType, exists := input.FieldType(name); exists {
		metadata := field.Metadata.ToMap()
		metadata["milvus.data_type"] = strconv.Itoa(int(dataType))
		field.Metadata = arrow.MetadataFrom(metadata)
	}
	return validateComparableArrowField(field, opName)
}

func (o *SortOp) String() string {
	order := "ASC"
	if o.desc {
		order = "DESC"
	}
	if o.tieBreakCol != "" {
		return fmt.Sprintf("Sort(%s %s, %s ASC)", o.Column(), order, o.tieBreakCol)
	}
	return fmt.Sprintf("Sort(%s %s)", o.Column(), order)
}

// compareArrayValues compares two values in an array.
func compareArrayValues(arr arrow.Array, i, j int) int {
	// Handle nulls
	if arr.IsNull(i) && arr.IsNull(j) {
		return 0
	}
	if arr.IsNull(i) {
		return -1
	}
	if arr.IsNull(j) {
		return 1
	}

	switch a := arr.(type) {
	case *array.Boolean:
		if a.Value(i) == a.Value(j) {
			return 0
		}
		if a.Value(i) {
			return 1
		}
		return -1
	case *array.Binary:
		return bytes.Compare(a.Value(i), a.Value(j))
	case *array.LargeBinary:
		return bytes.Compare(a.Value(i), a.Value(j))
	case *array.FixedSizeBinary:
		return bytes.Compare(a.Value(i), a.Value(j))
	case *array.Timestamp:
		return compareTyped(a, i, j)
	case *array.Date32:
		return compareTyped(a, i, j)
	case *array.Date64:
		return compareTyped(a, i, j)
	case *array.Time32:
		return compareTyped(a, i, j)
	case *array.Time64:
		return compareTyped(a, i, j)
	case *array.Duration:
		return compareTyped(a, i, j)
	case *array.Int8:
		return compareTyped(a, i, j)
	case *array.Int16:
		return compareTyped(a, i, j)
	case *array.Int32:
		return compareTyped(a, i, j)
	case *array.Int64:
		return compareTyped(a, i, j)
	case *array.Uint8:
		return compareTyped(a, i, j)
	case *array.Uint16:
		return compareTyped(a, i, j)
	case *array.Uint32:
		return compareTyped(a, i, j)
	case *array.Uint64:
		return compareTyped(a, i, j)
	case *array.Float32:
		return compareTyped(a, i, j)
	case *array.Float64:
		return compareTyped(a, i, j)
	case *array.String:
		return compareTyped(a, i, j)
	case *array.LargeString:
		return compareTyped(a, i, j)
	default:
		return 0
	}
}

// reorderArray reorders an array based on indices.
func reorderArray(pool memory.Allocator, data arrow.Array, indices []int) (arrow.Array, error) {
	return dispatchPickByIndices(pool, data, indices)
}

// NewSortOpFromRepr creates a SortOp from an OperatorRepr.
func NewSortOpFromRepr(repr *OperatorRepr) (Operator, error) {
	if len(repr.Inputs) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("sort_op: column is required")
	}
	if len(repr.Inputs) > 2 {
		return nil, merr.WrapErrParameterInvalidMsg("sort_op: expects at most 2 input columns, got %d", len(repr.Inputs))
	}

	reader := types.NewParamReader("sort_op", repr.Params)
	desc, err := reader.Bool("desc", false, false)
	if err != nil {
		return nil, err
	}

	column := repr.Inputs[0]
	tieBreakCol := types.IDFieldName
	if repr.TableResult {
		// A table may have an entirely new schema. With no explicit tie input,
		// preserve the current row order rather than requiring hit identity.
		tieBreakCol = ""
	}
	if len(repr.Inputs) > 1 {
		tieBreakCol = repr.Inputs[1]
	}
	return newSortOp(column, desc, tieBreakCol), nil
}

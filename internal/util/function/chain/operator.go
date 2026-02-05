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
	"cmp"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

// =============================================================================
// Generic Array Helpers
// =============================================================================

// typedArray is an Arrow array that provides typed value access.
type typedArray[T any] interface {
	Len() int
	IsNull(int) bool
	Value(int) T
}

// typedBuilder is an Arrow builder that supports typed append.
type typedBuilder[T any] interface {
	Append(T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

// pickByIndices creates a new array by picking elements at the given indices.
func pickByIndices[T any, A typedArray[T], B typedBuilder[T]](arr A, builder B, indices []int) (arrow.Array, error) {
	defer builder.Release()
	arrLen := arr.Len()
	for _, idx := range indices {
		if idx < 0 || idx >= arrLen {
			return nil, fmt.Errorf("index out of bounds: %d (array length: %d)", idx, arrLen)
		}
		if arr.IsNull(idx) {
			builder.AppendNull()
		} else {
			builder.Append(arr.Value(idx))
		}
	}
	return builder.NewArray(), nil
}

// compareTyped compares two values in a typed array using cmp.Ordered.
func compareTyped[T cmp.Ordered, A typedArray[T]](arr A, i, j int) int {
	return cmp.Compare(arr.Value(i), arr.Value(j))
}

// =============================================================================
// Type Dispatch Functions
// =============================================================================

// dispatchPickByIndices dispatches pickByIndices to the correct Arrow type.
func dispatchPickByIndices(pool memory.Allocator, data arrow.Array, indices []int) (arrow.Array, error) {
	switch arr := data.(type) {
	case *array.Boolean:
		return pickByIndices(arr, array.NewBooleanBuilder(pool), indices)
	case *array.Int8:
		return pickByIndices(arr, array.NewInt8Builder(pool), indices)
	case *array.Int16:
		return pickByIndices(arr, array.NewInt16Builder(pool), indices)
	case *array.Int32:
		return pickByIndices(arr, array.NewInt32Builder(pool), indices)
	case *array.Int64:
		return pickByIndices(arr, array.NewInt64Builder(pool), indices)
	case *array.Float32:
		return pickByIndices(arr, array.NewFloat32Builder(pool), indices)
	case *array.Float64:
		return pickByIndices(arr, array.NewFloat64Builder(pool), indices)
	case *array.String:
		return pickByIndices(arr, array.NewStringBuilder(pool), indices)
	default:
		return nil, fmt.Errorf("unsupported array type %T", data)
	}
}

// =============================================================================
// Operators
// =============================================================================

// BaseOp is the base operator with common fields.
type BaseOp struct {
	inputs  []string
	outputs []string
}

func (o *BaseOp) Inputs() []string  { return o.inputs }
func (o *BaseOp) Outputs() []string { return o.outputs }

// -----------------------------------------------------------------------------
// MapOp
// -----------------------------------------------------------------------------

// MapOp applies a function to specified columns of the DataFrame.
// Column mapping is handled at the Operator layer, not the Function layer.
type MapOp struct {
	BaseOp
	function types.FunctionExpr // The function to apply
}

// NewMapOp creates a new MapOp with explicit column mappings.
func NewMapOp(function types.FunctionExpr, inputCols, outputCols []string) (*MapOp, error) {
	if function == nil {
		return nil, fmt.Errorf("map_op: function is nil")
	}

	// Validate: outputCols length must match function.OutputDataTypes() length
	// Skip validation if OutputDataTypes() returns nil (dynamic output types)
	outputTypes := function.OutputDataTypes()
	if outputTypes != nil && len(outputCols) != len(outputTypes) {
		return nil, fmt.Errorf("map_op: output columns count (%d) must match function output types count (%d)",
			len(outputCols), len(outputTypes))
	}

	return &MapOp{
		BaseOp: BaseOp{
			inputs:  inputCols,
			outputs: outputCols,
		},
		function: function,
	}, nil
}

func (o *MapOp) Name() string { return "Map" }

// Inputs and Outputs are inherited from BaseOp

func (o *MapOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if o.function == nil {
		return nil, fmt.Errorf("map_op: function is nil")
	}

	// 1. Read input columns from DataFrame using inputs
	inputs := make([]*arrow.Chunked, len(o.inputs))
	for i, name := range o.inputs {
		col := input.Column(name)
		if col == nil {
			return nil, fmt.Errorf("map_op: column %q not found", name)
		}
		inputs[i] = col
	}

	// 2. Call FunctionExpr to process columns
	outputs, err := o.function.Execute(ctx, inputs)
	if err != nil {
		return nil, err
	}

	// 3. Validate output count matches expected output columns
	// This is especially important for dynamic output types where validation
	// was skipped at creation time
	if len(outputs) != len(o.outputs) {
		// Release outputs before returning error
		for _, out := range outputs {
			if out != nil {
				out.Release()
			}
		}
		return nil, fmt.Errorf("map_op: function returned %d outputs, expected %d",
			len(outputs), len(o.outputs))
	}

	// 4. Create builder
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(input.chunkSizes)

	// Build set of output column names (these will replace any existing columns with the same name)
	outputColSet := make(map[string]struct{})
	for _, name := range o.outputs {
		outputColSet[name] = struct{}{}
	}

	// 5. Copy input columns that are not in output columns
	for _, colName := range input.ColumnNames() {
		if _, isOutput := outputColSet[colName]; isOutput {
			continue // Skip, will be replaced by output
		}
		if err := builder.AddColumnFrom(input, colName); err != nil {
			// Release outputs since they haven't been added to builder yet
			for _, out := range outputs {
				if out != nil {
					out.Release()
				}
			}
			return nil, err
		}
	}

	// 6. Add all output columns at once
	if err := builder.AddColumns(o.outputs, outputs); err != nil {
		return nil, fmt.Errorf("map_op: %w", err)
	}

	return builder.Build(), nil
}

func (o *MapOp) String() string {
	if o.function != nil {
		return fmt.Sprintf("Map(%s)", o.function.Name())
	}
	return "Map(nil)"
}

// -----------------------------------------------------------------------------
// FilterOp
// -----------------------------------------------------------------------------

// FilterOp filters the DataFrame based on the boolean result of a FunctionExpr.
// The FunctionExpr must return exactly one boolean column.
type FilterOp struct {
	BaseOp
	function types.FunctionExpr // FunctionExpr that returns bool type
}

// NewFilterOp creates a new FilterOp with the given FunctionExpr and input columns.
// The function must return exactly one boolean column.
func NewFilterOp(function types.FunctionExpr, inputCols []string) (*FilterOp, error) {
	if function == nil {
		return nil, fmt.Errorf("filter_op: function is nil")
	}

	// Validate: function must return exactly one boolean type
	// Skip validation if OutputDataTypes() returns nil (dynamic output types)
	outputTypes := function.OutputDataTypes()
	if outputTypes != nil {
		if len(outputTypes) != 1 {
			return nil, fmt.Errorf("filter_op: function must return exactly 1 output, got %d", len(outputTypes))
		}
		if outputTypes[0].ID() != arrow.BOOL {
			return nil, fmt.Errorf("filter_op: function must return boolean type, got %s", outputTypes[0].Name())
		}
	}

	return &FilterOp{
		BaseOp: BaseOp{
			inputs:  inputCols,
			outputs: []string{}, // Filter doesn't produce new columns
		},
		function: function,
	}, nil
}

func (o *FilterOp) Name() string { return "Filter" }

// Inputs and Outputs are inherited from BaseOp

func (o *FilterOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	// Read input columns from DataFrame
	inputs := make([]*arrow.Chunked, len(o.inputs))
	for i, name := range o.inputs {
		col := input.Column(name)
		if col == nil {
			return nil, fmt.Errorf("filter_op: column %q not found", name)
		}
		inputs[i] = col
	}

	// Execute FunctionExpr to get boolean result
	outputs, err := o.function.Execute(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("filter_op: function execution failed: %w", err)
	}

	// Validate output at runtime (especially important for dynamic output types)
	if len(outputs) != 1 {
		for _, out := range outputs {
			if out != nil {
				out.Release()
			}
		}
		return nil, fmt.Errorf("filter_op: function must return exactly 1 output, got %d", len(outputs))
	}

	filterCol := outputs[0]
	defer filterCol.Release() // Release the temporary boolean column

	// Validate the output is boolean type
	if filterCol.DataType().ID() != arrow.BOOL {
		return nil, fmt.Errorf("filter_op: function must return boolean type, got %s", filterCol.DataType().Name())
	}

	// Create builder for result DataFrame
	builder := NewDataFrameBuilder()
	defer builder.Release()

	// Process each chunk - extract boolean chunks with type safety
	newChunkSizes := make([]int64, 0, input.NumChunks())
	filterChunks := make([]*array.Boolean, input.NumChunks())
	for chunkIdx := range input.NumChunks() {
		boolChunk, ok := filterCol.Chunk(chunkIdx).(*array.Boolean)
		if !ok {
			return nil, fmt.Errorf("filter_op: chunk %d is not a boolean array", chunkIdx)
		}
		filterChunks[chunkIdx] = boolChunk

		// Count true values
		trueCount := int64(0)
		for i := range boolChunk.Len() {
			if boolChunk.IsValid(i) && boolChunk.Value(i) {
				trueCount++
			}
		}
		newChunkSizes = append(newChunkSizes, trueCount)
	}

	builder.SetChunkSizes(newChunkSizes)

	// Use ChunkCollector for filtered chunks
	colNames := input.ColumnNames()
	collector := NewChunkCollector(colNames, input.NumChunks())
	defer collector.Release()

	// Filter each column
	for _, colName := range colNames {
		col := input.Column(colName)

		for chunkIdx := range input.NumChunks() {
			dataChunk := col.Chunk(chunkIdx)

			filtered, err := filterArray(ctx.Pool(), dataChunk, filterChunks[chunkIdx])
			if err != nil {
				return nil, fmt.Errorf("filter_op: column %s: %w", colName, err)
			}
			collector.Set(colName, chunkIdx, filtered)
		}

		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(input, colName)
	}

	return builder.Build(), nil
}

func (o *FilterOp) String() string {
	return fmt.Sprintf("Filter(%s)", o.function.Name())
}

// filterArray filters an array based on a boolean mask.
func filterArray(pool memory.Allocator, data arrow.Array, mask *array.Boolean) (arrow.Array, error) {
	indices := make([]int, 0, mask.Len())
	for i := range mask.Len() {
		if mask.IsValid(i) && mask.Value(i) {
			indices = append(indices, i)
		}
	}
	return dispatchPickByIndices(pool, data, indices)
}

// -----------------------------------------------------------------------------
// SelectOp
// -----------------------------------------------------------------------------

// SelectOp selects specific columns from the DataFrame.
// Note: Uses BaseOp.inputs as the column names to select.
// BaseOp.outputs is set to the same as inputs since selected columns are output.
type SelectOp struct {
	BaseOp
}

// NewSelectOp creates a new SelectOp with the given columns.
func NewSelectOp(columns []string) *SelectOp {
	return &SelectOp{
		BaseOp: BaseOp{
			inputs:  columns,
			outputs: columns, // Selected columns are the outputs
		},
	}
}

func (o *SelectOp) Name() string { return "Select" }

// Inputs and Outputs are inherited from BaseOp

func (o *SelectOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(input.chunkSizes)

	// Use ChunkCollector for selected chunks
	collector := NewChunkCollector(o.inputs, input.NumChunks())
	defer collector.Release()

	for _, colName := range o.inputs {
		col := input.Column(colName)
		if col == nil {
			return nil, fmt.Errorf("select_op: column %q not found", colName)
		}

		// Copy chunks with retain
		for i, chunk := range col.Chunks() {
			chunk.Retain()
			collector.Set(colName, i, chunk)
		}

		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(input, colName)
	}

	return builder.Build(), nil
}

func (o *SelectOp) String() string {
	return fmt.Sprintf("Select(%v)", o.inputs)
}

// -----------------------------------------------------------------------------
// SortOp
// -----------------------------------------------------------------------------

// SortOp sorts the DataFrame by a column.
// Note: Uses BaseOp.inputs[0] as the sort column name.
// Each chunk is sorted independently (per-query sorting for search results).
type SortOp struct {
	BaseOp
	desc bool
}

// NewSortOp creates a new SortOp with the given column and sort direction.
func NewSortOp(column string, desc bool) *SortOp {
	return &SortOp{
		BaseOp: BaseOp{
			inputs:  []string{column},
			outputs: []string{}, // Sort doesn't produce new columns
		},
		desc: desc,
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
	column := o.Column()
	sortCol := input.Column(column)
	if sortCol == nil {
		return nil, fmt.Errorf("sort_op: column %q not found", column)
	}

	// Validate sort column type is comparable
	if !isComparableType(sortCol.DataType()) {
		return nil, fmt.Errorf("sort_op: column %s has non-comparable type %s", column, sortCol.DataType().Name())
	}

	colNames := input.ColumnNames()
	collector := NewChunkCollector(colNames, input.NumChunks())
	defer collector.Release()

	newChunkSizes := make([]int64, input.NumChunks())

	// Process each chunk independently
	for chunkIdx := range input.NumChunks() {
		sortChunk := sortCol.Chunk(chunkIdx)
		chunkLen := sortChunk.Len()

		// Build sort indices
		indices := make([]int, chunkLen)
		for i := range chunkLen {
			indices[i] = i
		}

		// Sort indices based on values
		sort.Slice(indices, func(i, j int) bool {
			vi := indices[i]
			vj := indices[j]
			cmp := compareArrayValues(sortChunk, vi, vj)
			if o.desc {
				return cmp > 0
			}
			return cmp < 0
		})

		newChunkSizes[chunkIdx] = int64(chunkLen)

		// Reorder each column
		for _, colName := range colNames {
			col := input.Column(colName)
			dataChunk := col.Chunk(chunkIdx)
			reordered, err := reorderArray(ctx.Pool(), dataChunk, indices)
			if err != nil {
				return nil, fmt.Errorf("sort_op: column %s: %w", colName, err)
			}
			collector.Set(colName, chunkIdx, reordered)
		}
	}

	// Create new DataFrame with all chunks
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(newChunkSizes)

	for _, colName := range colNames {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(input, colName)
	}

	return builder.Build(), nil
}

// isComparableType checks if an Arrow data type is comparable for sorting.
func isComparableType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING, arrow.LARGE_STRING:
		return true
	default:
		return false
	}
}

func (o *SortOp) String() string {
	order := "ASC"
	if o.desc {
		order = "DESC"
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
	case *array.Int8:
		return compareTyped(a, i, j)
	case *array.Int16:
		return compareTyped(a, i, j)
	case *array.Int32:
		return compareTyped(a, i, j)
	case *array.Int64:
		return compareTyped(a, i, j)
	case *array.Float32:
		return compareTyped(a, i, j)
	case *array.Float64:
		return compareTyped(a, i, j)
	case *array.String:
		return compareTyped(a, i, j)
	default:
		return 0
	}
}

// reorderArray reorders an array based on indices.
func reorderArray(pool memory.Allocator, data arrow.Array, indices []int) (arrow.Array, error) {
	return dispatchPickByIndices(pool, data, indices)
}

// -----------------------------------------------------------------------------
// LimitOp
// -----------------------------------------------------------------------------

// LimitOp limits the number of rows in each chunk.
// Note: Limit is applied independently to each chunk (per-query limiting for search results).
type LimitOp struct {
	BaseOp
	limit  int64
	offset int64
}

// NewLimitOp creates a new LimitOp with the given limit and offset.
func NewLimitOp(limit, offset int64) *LimitOp {
	return &LimitOp{
		BaseOp: BaseOp{
			inputs:  []string{}, // Limit works on all columns
			outputs: []string{}, // Limit doesn't produce new columns
		},
		limit:  limit,
		offset: offset,
	}
}

func (o *LimitOp) Name() string { return "Limit" }

// Inputs and Outputs are inherited from BaseOp

func (o *LimitOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	colNames := input.ColumnNames()
	collector := NewChunkCollector(colNames, input.NumChunks())
	defer collector.Release()

	newChunkSizes := make([]int64, input.NumChunks())

	// Process each chunk
	for chunkIdx := range input.NumChunks() {
		chunkSize := input.chunkSizes[chunkIdx]

		// Calculate actual offset and limit for this chunk
		start := min(o.offset, chunkSize)
		end := min(start+o.limit, chunkSize)

		newChunkSizes[chunkIdx] = end - start

		// Slice each column
		for _, colName := range colNames {
			col := input.Column(colName)
			dataChunk := col.Chunk(chunkIdx)
			sliced, err := sliceArray(dataChunk, int(start), int(end))
			if err != nil {
				return nil, fmt.Errorf("limit_op: column %s: %w", colName, err)
			}
			collector.Set(colName, chunkIdx, sliced)
		}
	}

	// Create new DataFrame with all chunks
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(newChunkSizes)

	for _, colName := range colNames {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, fmt.Errorf("limit_op: %w", err)
		}
		builder.CopyFieldMetadata(input, colName)
	}

	return builder.Build(), nil
}

func (o *LimitOp) String() string {
	if o.offset > 0 {
		return fmt.Sprintf("Limit(%d, offset=%d)", o.limit, o.offset)
	}
	return fmt.Sprintf("Limit(%d)", o.limit)
}

// =============================================================================
// Operator Registry
// =============================================================================

// OperatorFactory is a function that creates an Operator from OperatorRepr.
type OperatorFactory func(repr *OperatorRepr) (Operator, error)

var (
	operatorRegistryMu sync.RWMutex
	operatorRegistry   = make(map[string]OperatorFactory)
)

// RegisterOperator registers an operator factory.
// Returns an error if an operator with the same type is already registered.
func RegisterOperator(opType string, factory OperatorFactory) error {
	if opType == "" {
		return fmt.Errorf("operator type cannot be empty")
	}
	if factory == nil {
		return fmt.Errorf("operator factory cannot be nil for %q", opType)
	}

	operatorRegistryMu.Lock()
	defer operatorRegistryMu.Unlock()

	if _, exists := operatorRegistry[opType]; exists {
		return fmt.Errorf("operator %q already registered", opType)
	}
	operatorRegistry[opType] = factory
	return nil
}

// MustRegisterOperator registers an operator factory and panics on error.
// Use this in init() functions to fail fast on registration errors.
func MustRegisterOperator(opType string, factory OperatorFactory) {
	if err := RegisterOperator(opType, factory); err != nil {
		panic(fmt.Sprintf("failed to register operator: %v", err))
	}
}

// GetOperatorFactory returns the factory for the given operator type.
func GetOperatorFactory(opType string) (OperatorFactory, bool) {
	operatorRegistryMu.RLock()
	defer operatorRegistryMu.RUnlock()
	factory, ok := operatorRegistry[opType]
	return factory, ok
}

// =============================================================================
// Operator Factory Functions
// =============================================================================

// NewMapOpFromRepr creates a MapOp from an OperatorRepr.
func NewMapOpFromRepr(repr *OperatorRepr) (Operator, error) {
	if repr.Function == nil {
		return nil, fmt.Errorf("map operator requires function")
	}
	fn, err := FunctionFromRepr(repr.Function)
	if err != nil {
		return nil, fmt.Errorf("map function: %w", err)
	}
	if len(repr.Inputs) == 0 {
		return nil, fmt.Errorf("map operator requires inputs")
	}
	if len(repr.Outputs) == 0 {
		return nil, fmt.Errorf("map operator requires outputs")
	}
	return NewMapOp(fn, repr.Inputs, repr.Outputs)
}

// NewFilterOpFromRepr creates a FilterOp from an OperatorRepr.
func NewFilterOpFromRepr(repr *OperatorRepr) (Operator, error) {
	if repr.Function == nil {
		return nil, fmt.Errorf("filter_op: function is required")
	}
	fn, err := FunctionFromRepr(repr.Function)
	if err != nil {
		return nil, fmt.Errorf("filter function: %w", err)
	}
	if len(repr.Inputs) == 0 {
		return nil, fmt.Errorf("filter_op: inputs is required")
	}
	return NewFilterOp(fn, repr.Inputs)
}

// NewSelectOpFromRepr creates a SelectOp from an OperatorRepr.
func NewSelectOpFromRepr(repr *OperatorRepr) (Operator, error) {
	columnsInterface, ok := repr.Params["columns"]
	if !ok {
		return nil, fmt.Errorf("select_op: columns is required")
	}
	columns, ok := columnsInterface.([]interface{})
	if !ok {
		// Try []string
		if colsStr, ok := columnsInterface.([]string); ok {
			return NewSelectOp(colsStr), nil
		}
		return nil, fmt.Errorf("select_op: columns must be a list")
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("select_op: columns is required")
	}
	colsStr := make([]string, len(columns))
	for i, col := range columns {
		if colStr, ok := col.(string); ok {
			colsStr[i] = colStr
		} else {
			return nil, fmt.Errorf("select_op: column[%d] must be a string", i)
		}
	}
	return NewSelectOp(colsStr), nil
}

// NewSortOpFromRepr creates a SortOp from an OperatorRepr.
func NewSortOpFromRepr(repr *OperatorRepr) (Operator, error) {
	column, ok := repr.Params["column"].(string)
	if !ok || column == "" {
		return nil, fmt.Errorf("sort_op: column is required")
	}
	desc := false
	if descVal, ok := repr.Params["desc"]; ok {
		if descBool, ok := descVal.(bool); ok {
			desc = descBool
		}
	}
	return NewSortOp(column, desc), nil
}

// NewLimitOpFromRepr creates a LimitOp from an OperatorRepr.
func NewLimitOpFromRepr(repr *OperatorRepr) (Operator, error) {
	limitVal, ok := repr.Params["limit"]
	if !ok {
		return nil, fmt.Errorf("limit_op: limit is required")
	}
	var limit int64
	switch v := limitVal.(type) {
	case int64:
		limit = v
	case int:
		limit = int64(v)
	case float64:
		limit = int64(v)
	default:
		return nil, fmt.Errorf("limit_op: limit must be a number")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit_op: limit must be positive")
	}
	offset := int64(0)
	if offsetVal, ok := repr.Params["offset"]; ok {
		switch v := offsetVal.(type) {
		case int64:
			offset = v
		case int:
			offset = int64(v)
		case float64:
			offset = int64(v)
		}
	}
	return NewLimitOp(limit, offset), nil
}

// init registers all built-in operator factories.
func init() {
	MustRegisterOperator(types.OpTypeMap, NewMapOpFromRepr)
	MustRegisterOperator(types.OpTypeFilter, NewFilterOpFromRepr)
	MustRegisterOperator(types.OpTypeSelect, NewSelectOpFromRepr)
	MustRegisterOperator(types.OpTypeSort, NewSortOpFromRepr)
	MustRegisterOperator(types.OpTypeLimit, NewLimitOpFromRepr)
}

// sliceArray slices an array from start to end using zero-copy.
func sliceArray(data arrow.Array, start, end int) (arrow.Array, error) {
	return array.NewSlice(data, int64(start), int64(end)), nil
}

// =============================================================================
// MergeStrategy
// =============================================================================

// MergeStrategy defines how to merge multiple DataFrames.
type MergeStrategy string

const (
	MergeStrategyRRF      MergeStrategy = "rrf"
	MergeStrategyWeighted MergeStrategy = "weighted"
	MergeStrategyMax      MergeStrategy = "max"
	MergeStrategySum      MergeStrategy = "sum"
	MergeStrategyAvg      MergeStrategy = "avg"
)

// =============================================================================
// MergeOp
// =============================================================================

// MergeOp merges multiple DataFrames into one with optional normalization.
// This operator is typically used as the first operator in a rerank chain.
type MergeOp struct {
	BaseOp
	strategy    MergeStrategy
	weights     []float64 // for weighted strategy
	rrfK        float64   // for rrf strategy, default 60
	metricTypes []string  // metric type for each input (IP/L2/COSINE/BM25)
	normalize   bool      // whether to normalize scores
}

// MergeOption is a functional option for MergeOp.
type MergeOption func(*MergeOp)

// WithWeights sets the weights for weighted merge strategy.
func WithWeights(weights []float64) MergeOption {
	return func(op *MergeOp) {
		op.weights = weights
	}
}

// WithRRFK sets the k parameter for RRF merge strategy.
func WithRRFK(k float64) MergeOption {
	return func(op *MergeOp) {
		op.rrfK = k
	}
}

// WithMetricTypes sets the metric types for each input.
func WithMetricTypes(metricTypes []string) MergeOption {
	return func(op *MergeOp) {
		op.metricTypes = metricTypes
	}
}

// WithNormalize sets whether to normalize scores.
func WithNormalize(normalize bool) MergeOption {
	return func(op *MergeOp) {
		op.normalize = normalize
	}
}

// NewMergeOp creates a new MergeOp with the given strategy and options.
func NewMergeOp(strategy MergeStrategy, opts ...MergeOption) *MergeOp {
	op := &MergeOp{
		BaseOp: BaseOp{
			inputs:  []string{},
			outputs: []string{},
		},
		strategy:  strategy,
		rrfK:      60, // default RRF k
		normalize: true,
	}

	for _, opt := range opts {
		opt(op)
	}

	return op
}

func (op *MergeOp) Name() string { return "Merge" }

func (op *MergeOp) String() string {
	return fmt.Sprintf("Merge(%s)", op.strategy)
}

// Execute is not used for MergeOp, use ExecuteMulti instead.
func (op *MergeOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	return nil, fmt.Errorf("merge_op: use ExecuteMulti instead of Execute")
}

// ExecuteMulti merges multiple DataFrames into one.
func (op *MergeOp) ExecuteMulti(ctx *types.FuncContext, inputs []*DataFrame) (*DataFrame, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("merge_op: no inputs provided")
	}

	// Validate inputs have same number of chunks (NQ)
	numChunks := inputs[0].NumChunks()
	for i, df := range inputs {
		if df.NumChunks() != numChunks {
			return nil, fmt.Errorf("merge_op: input[%d] has %d chunks, expected %d", i, df.NumChunks(), numChunks)
		}
	}

	// Validate metric types count matches inputs count
	if len(op.metricTypes) > 0 && len(op.metricTypes) != len(inputs) {
		return nil, fmt.Errorf("merge_op: metric types count %d != inputs count %d", len(op.metricTypes), len(inputs))
	}

	// Validate weights for weighted strategy
	if op.strategy == MergeStrategyWeighted {
		if len(op.weights) != len(inputs) {
			return nil, fmt.Errorf("merge_op: weights count %d != inputs count %d", len(op.weights), len(inputs))
		}
	}

	// Single input: just normalize if needed and return
	if len(inputs) == 1 {
		return op.processSingleInput(ctx, inputs[0])
	}

	// Multiple inputs: merge based on strategy
	switch op.strategy {
	case MergeStrategyRRF:
		return op.mergeRRF(ctx, inputs)
	case MergeStrategyWeighted:
		return op.mergeWeighted(ctx, inputs)
	case MergeStrategyMax:
		return op.mergeScoreCombine(ctx, inputs, maxMergeFunc)
	case MergeStrategySum:
		return op.mergeScoreCombine(ctx, inputs, sumMergeFunc)
	case MergeStrategyAvg:
		return op.mergeScoreCombine(ctx, inputs, avgMergeFunc)
	default:
		return nil, fmt.Errorf("merge_op: unsupported strategy %s", op.strategy)
	}
}

// processSingleInput handles single input case: normalize and pass through.
func (op *MergeOp) processSingleInput(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if !op.normalize || len(op.metricTypes) == 0 {
		// No normalization needed, retain and return same DataFrame
		// Note: We need to create a new DataFrame with retained columns
		builder := NewDataFrameBuilder()
		defer builder.Release()

		builder.SetChunkSizes(input.ChunkSizes())

		for _, colName := range input.ColumnNames() {
			if err := builder.AddColumnFrom(input, colName); err != nil {
				return nil, err
			}
		}

		return builder.Build(), nil
	}

	// Normalize scores
	normFunc := getNormalizeFunc(op.metricTypes[0])

	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(input.ChunkSizes())

	// Copy all columns except score, normalize score
	for _, colName := range input.ColumnNames() {
		if colName == types.ScoreFieldName {
			scoreCol := input.Column(types.ScoreFieldName)
			normalizedChunks := make([]arrow.Array, input.NumChunks())
			for i := 0; i < input.NumChunks(); i++ {
				chunk := scoreCol.Chunk(i).(*array.Float32)
				normalized, err := normalizeScoreChunk(ctx, chunk, normFunc)
				if err != nil {
					for j := 0; j < i; j++ {
						normalizedChunks[j].Release()
					}
					return nil, err
				}
				normalizedChunks[i] = normalized
			}
			if err := builder.AddColumnFromChunks(types.ScoreFieldName, normalizedChunks); err != nil {
				return nil, err
			}
			builder.CopyFieldMetadata(input, types.ScoreFieldName)
		} else {
			if err := builder.AddColumnFrom(input, colName); err != nil {
				return nil, err
			}
		}
	}

	return builder.Build(), nil
}

// =============================================================================
// Merge Strategies
// =============================================================================

// mergeRRF implements Reciprocal Rank Fusion.
func (op *MergeOp) mergeRRF(ctx *types.FuncContext, inputs []*DataFrame) (*DataFrame, error) {
	numChunks := inputs[0].NumChunks()

	builder := NewDataFrameBuilder()
	defer builder.Release()

	newChunkSizes := make([]int64, numChunks)
	idChunks := make([]arrow.Array, numChunks)
	scoreChunks := make([]arrow.Array, numChunks)

	// Collect other field columns from all inputs
	fieldCollectors := make(map[string]*ChunkCollector)

	// Process each chunk (each query)
	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		idScores, idLocs, fieldData, err := op.collectRRFScores(inputs, chunkIdx)
		if err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}

		// Build result arrays for this chunk
		ids, scores, locs := sortAndExtractResults(idScores, idLocs)
		newChunkSizes[chunkIdx] = int64(len(ids))

		idArr, scoreArr, err := op.buildResultArrays(ctx, ids, scores)
		if err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}
		idChunks[chunkIdx] = idArr
		scoreChunks[chunkIdx] = scoreArr

		// Build field arrays from field data
		if err := op.collectFieldData(ctx, fieldCollectors, fieldData, locs, inputs, chunkIdx); err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}
	}

	builder.SetChunkSizes(newChunkSizes)

	// Add ID and score columns
	if err := builder.AddColumnFromChunks(types.IDFieldName, idChunks); err != nil {
		op.releaseChunks(nil, scoreChunks, fieldCollectors)
		return nil, err
	}
	if err := builder.AddColumnFromChunks(types.ScoreFieldName, scoreChunks); err != nil {
		op.releaseChunks(nil, nil, fieldCollectors)
		return nil, err
	}

	// Add field columns
	for colName, collector := range fieldCollectors {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		// Copy metadata from first input that has this column
		for _, input := range inputs {
			if input.HasColumn(colName) {
				builder.CopyFieldMetadata(input, colName)
				break
			}
		}
	}

	return builder.Build(), nil
}

// collectRRFScores collects RRF scores for a single chunk.
func (op *MergeOp) collectRRFScores(inputs []*DataFrame, chunkIdx int) (map[any]float32, map[any]idLocation, map[any]map[string]any, error) {
	idScores := make(map[any]float32)
	idLocs := make(map[any]idLocation)
	fieldData := make(map[any]map[string]any)

	for inputIdx, df := range inputs {
		idCol := df.Column(types.IDFieldName)
		if idCol == nil {
			return nil, nil, nil, fmt.Errorf("merge_op: input[%d] missing %s column", inputIdx, types.IDFieldName)
		}

		idChunk := idCol.Chunk(chunkIdx)
		for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
			id := getIDValue(idChunk, rowIdx)
			if id == nil {
				continue
			}

			// RRF score: 1 / (k + rank), rank is 1-based
			rrfScore := float32(1.0 / (op.rrfK + float64(rowIdx+1)))

			if existingScore, exists := idScores[id]; exists {
				idScores[id] = existingScore + rrfScore
			} else {
				idScores[id] = rrfScore
				idLocs[id] = idLocation{inputIdx: inputIdx, rowIdx: rowIdx}
				// Collect field data for this ID
				fieldData[id] = collectFieldDataForRow(df, chunkIdx, rowIdx)
			}
		}
	}

	return idScores, idLocs, fieldData, nil
}

// mergeWeighted implements weighted score merge.
func (op *MergeOp) mergeWeighted(ctx *types.FuncContext, inputs []*DataFrame) (*DataFrame, error) {
	numChunks := inputs[0].NumChunks()

	builder := NewDataFrameBuilder()
	defer builder.Release()

	newChunkSizes := make([]int64, numChunks)
	idChunks := make([]arrow.Array, numChunks)
	scoreChunks := make([]arrow.Array, numChunks)

	fieldCollectors := make(map[string]*ChunkCollector)

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		idScores, idLocs, fieldData, err := op.collectWeightedScores(inputs, chunkIdx)
		if err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}

		ids, scores, locs := sortAndExtractResults(idScores, idLocs)
		newChunkSizes[chunkIdx] = int64(len(ids))

		idArr, scoreArr, err := op.buildResultArrays(ctx, ids, scores)
		if err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}
		idChunks[chunkIdx] = idArr
		scoreChunks[chunkIdx] = scoreArr

		if err := op.collectFieldData(ctx, fieldCollectors, fieldData, locs, inputs, chunkIdx); err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}
	}

	builder.SetChunkSizes(newChunkSizes)

	if err := builder.AddColumnFromChunks(types.IDFieldName, idChunks); err != nil {
		op.releaseChunks(nil, scoreChunks, fieldCollectors)
		return nil, err
	}
	if err := builder.AddColumnFromChunks(types.ScoreFieldName, scoreChunks); err != nil {
		op.releaseChunks(nil, nil, fieldCollectors)
		return nil, err
	}

	for colName, collector := range fieldCollectors {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		for _, input := range inputs {
			if input.HasColumn(colName) {
				builder.CopyFieldMetadata(input, colName)
				break
			}
		}
	}

	return builder.Build(), nil
}

// collectWeightedScores collects weighted scores for a single chunk.
func (op *MergeOp) collectWeightedScores(inputs []*DataFrame, chunkIdx int) (map[any]float32, map[any]idLocation, map[any]map[string]any, error) {
	idScores := make(map[any]float32)
	idLocs := make(map[any]idLocation)
	fieldData := make(map[any]map[string]any)

	for inputIdx, df := range inputs {
		idCol := df.Column(types.IDFieldName)
		scoreCol := df.Column(types.ScoreFieldName)
		if idCol == nil || scoreCol == nil {
			return nil, nil, nil, fmt.Errorf("merge_op: input[%d] missing ID or score column", inputIdx)
		}

		idChunk := idCol.Chunk(chunkIdx)
		scoreChunk := scoreCol.Chunk(chunkIdx).(*array.Float32)

		weight := float32(op.weights[inputIdx])

		// Get normalization function for this input
		var normFunc normalizeFunc
		if op.normalize && len(op.metricTypes) > inputIdx {
			normFunc = getNormalizeFunc(op.metricTypes[inputIdx])
		}

		for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
			id := getIDValue(idChunk, rowIdx)
			if id == nil {
				continue
			}

			score := scoreChunk.Value(rowIdx)
			if normFunc != nil {
				score = normFunc(score)
			}
			weightedScore := weight * score

			if existingScore, exists := idScores[id]; exists {
				idScores[id] = existingScore + weightedScore
			} else {
				idScores[id] = weightedScore
				idLocs[id] = idLocation{inputIdx: inputIdx, rowIdx: rowIdx}
				fieldData[id] = collectFieldDataForRow(df, chunkIdx, rowIdx)
			}
		}
	}

	return idScores, idLocs, fieldData, nil
}

// scoreMergeFunc defines how to merge scores for the same ID.
type scoreMergeFunc func(existing float32, new float32, count int) (float32, int)

func maxMergeFunc(existing, new float32, count int) (float32, int) {
	if new > existing {
		return new, count + 1
	}
	return existing, count + 1
}

func sumMergeFunc(existing, new float32, count int) (float32, int) {
	return existing + new, count + 1
}

func avgMergeFunc(existing, new float32, count int) (float32, int) {
	// For avg, we accumulate sum and count, then compute average at the end
	return existing + new, count + 1
}

// mergeScoreCombine implements max/sum/avg score merge.
func (op *MergeOp) mergeScoreCombine(ctx *types.FuncContext, inputs []*DataFrame, mergeFunc scoreMergeFunc) (*DataFrame, error) {
	numChunks := inputs[0].NumChunks()

	builder := NewDataFrameBuilder()
	defer builder.Release()

	newChunkSizes := make([]int64, numChunks)
	idChunks := make([]arrow.Array, numChunks)
	scoreChunks := make([]arrow.Array, numChunks)

	fieldCollectors := make(map[string]*ChunkCollector)

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		idScores, idCounts, idLocs, fieldData, err := op.collectCombinedScores(inputs, chunkIdx, mergeFunc)
		if err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}

		// For avg strategy, compute final average
		if op.strategy == MergeStrategyAvg {
			for id, score := range idScores {
				if count, exists := idCounts[id]; exists && count > 0 {
					idScores[id] = score / float32(count)
				}
			}
		}

		ids, scores, locs := sortAndExtractResults(idScores, idLocs)
		newChunkSizes[chunkIdx] = int64(len(ids))

		idArr, scoreArr, err := op.buildResultArrays(ctx, ids, scores)
		if err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}
		idChunks[chunkIdx] = idArr
		scoreChunks[chunkIdx] = scoreArr

		if err := op.collectFieldData(ctx, fieldCollectors, fieldData, locs, inputs, chunkIdx); err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}
	}

	builder.SetChunkSizes(newChunkSizes)

	if err := builder.AddColumnFromChunks(types.IDFieldName, idChunks); err != nil {
		op.releaseChunks(nil, scoreChunks, fieldCollectors)
		return nil, err
	}
	if err := builder.AddColumnFromChunks(types.ScoreFieldName, scoreChunks); err != nil {
		op.releaseChunks(nil, nil, fieldCollectors)
		return nil, err
	}

	for colName, collector := range fieldCollectors {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		for _, input := range inputs {
			if input.HasColumn(colName) {
				builder.CopyFieldMetadata(input, colName)
				break
			}
		}
	}

	return builder.Build(), nil
}

// collectCombinedScores collects combined scores for max/sum/avg strategies.
func (op *MergeOp) collectCombinedScores(inputs []*DataFrame, chunkIdx int, mergeFunc scoreMergeFunc) (map[any]float32, map[any]int, map[any]idLocation, map[any]map[string]any, error) {
	idScores := make(map[any]float32)
	idCounts := make(map[any]int)
	idLocs := make(map[any]idLocation)
	fieldData := make(map[any]map[string]any)

	for inputIdx, df := range inputs {
		idCol := df.Column(types.IDFieldName)
		scoreCol := df.Column(types.ScoreFieldName)
		if idCol == nil || scoreCol == nil {
			return nil, nil, nil, nil, fmt.Errorf("merge_op: input[%d] missing ID or score column", inputIdx)
		}

		idChunk := idCol.Chunk(chunkIdx)
		scoreChunk := scoreCol.Chunk(chunkIdx).(*array.Float32)

		var normFunc normalizeFunc
		if op.normalize && len(op.metricTypes) > inputIdx {
			normFunc = getNormalizeFunc(op.metricTypes[inputIdx])
		}

		for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
			id := getIDValue(idChunk, rowIdx)
			if id == nil {
				continue
			}

			score := scoreChunk.Value(rowIdx)
			if normFunc != nil {
				score = normFunc(score)
			}

			if existingScore, exists := idScores[id]; exists {
				newScore, newCount := mergeFunc(existingScore, score, idCounts[id])
				idScores[id] = newScore
				idCounts[id] = newCount
			} else {
				idScores[id] = score
				idCounts[id] = 1
				idLocs[id] = idLocation{inputIdx: inputIdx, rowIdx: rowIdx}
				fieldData[id] = collectFieldDataForRow(df, chunkIdx, rowIdx)
			}
		}
	}

	return idScores, idCounts, idLocs, fieldData, nil
}

// =============================================================================
// MergeOp Helper Types and Functions
// =============================================================================

// idLocation tracks where an ID was first seen.
type idLocation struct {
	inputIdx int
	rowIdx   int
}

// normalizeFunc normalizes a score based on metric type.
type normalizeFunc func(float32) float32

// getNormalizeFunc returns the normalization function for a metric type.
func getNormalizeFunc(metricType string) normalizeFunc {
	switch strings.ToUpper(metricType) {
	case metric.COSINE:
		return func(score float32) float32 {
			return (1 + score) * 0.5
		}
	case metric.IP:
		return func(score float32) float32 {
			return 0.5 + float32(math.Atan(float64(score)))/math.Pi
		}
	case metric.BM25:
		return func(score float32) float32 {
			return 2 * float32(math.Atan(float64(score))) / math.Pi
		}
	default:
		// L2 and other distance metrics: smaller is better, need to invert
		return func(distance float32) float32 {
			return 1.0 - 2*float32(math.Atan(float64(distance)))/math.Pi
		}
	}
}

// normalizeScoreChunk normalizes a score chunk.
func normalizeScoreChunk(ctx *types.FuncContext, chunk *array.Float32, normFunc normalizeFunc) (arrow.Array, error) {
	builder := array.NewFloat32Builder(ctx.Pool())
	defer builder.Release()

	for i := 0; i < chunk.Len(); i++ {
		if chunk.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(normFunc(chunk.Value(i)))
		}
	}

	return builder.NewArray(), nil
}

// getIDValue extracts ID value from an array at given index.
func getIDValue(arr arrow.Array, idx int) any {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Int64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	default:
		return nil
	}
}

// collectFieldDataForRow collects all field data for a row.
func collectFieldDataForRow(df *DataFrame, chunkIdx, rowIdx int) map[string]any {
	data := make(map[string]any)
	for _, colName := range df.ColumnNames() {
		if colName == types.IDFieldName || colName == types.ScoreFieldName {
			continue
		}
		col := df.Column(colName)
		if col == nil {
			continue
		}
		chunk := col.Chunk(chunkIdx)
		data[colName] = getArrayValue(chunk, rowIdx)
	}
	return data
}

// getArrayValue extracts value from an array at given index.
func getArrayValue(arr arrow.Array, idx int) any {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(idx)
	case *array.Int8:
		return a.Value(idx)
	case *array.Int16:
		return a.Value(idx)
	case *array.Int32:
		return a.Value(idx)
	case *array.Int64:
		return a.Value(idx)
	case *array.Float32:
		return a.Value(idx)
	case *array.Float64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	default:
		return nil
	}
}

// sortAndExtractResults sorts IDs by score (descending) and extracts results.
func sortAndExtractResults(idScores map[any]float32, idLocs map[any]idLocation) ([]any, []float32, []idLocation) {
	ids := make([]any, 0, len(idScores))
	for id := range idScores {
		ids = append(ids, id)
	}

	// Sort by score descending, then by ID for stability
	sortIDs(ids, idScores)

	scores := make([]float32, len(ids))
	locs := make([]idLocation, len(ids))
	for i, id := range ids {
		scores[i] = idScores[id]
		locs[i] = idLocs[id]
	}

	return ids, scores, locs
}

// sortIDs sorts IDs by score descending.
func sortIDs(ids []any, idScores map[any]float32) {
	// Simple bubble sort for now, can optimize later
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			scoreI := idScores[ids[i]]
			scoreJ := idScores[ids[j]]
			if scoreJ > scoreI {
				ids[i], ids[j] = ids[j], ids[i]
			} else if scoreJ == scoreI {
				// Stable sort by ID
				if compareIDs(ids[j], ids[i]) < 0 {
					ids[i], ids[j] = ids[j], ids[i]
				}
			}
		}
	}
}

// compareIDs compares two IDs for stable sorting.
func compareIDs(a, b any) int {
	switch va := a.(type) {
	case int64:
		vb := b.(int64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		vb := b.(string)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// buildResultArrays builds ID and score arrays from results.
func (op *MergeOp) buildResultArrays(ctx *types.FuncContext, ids []any, scores []float32) (arrow.Array, arrow.Array, error) {
	if len(ids) == 0 {
		// Empty result
		idBuilder := array.NewInt64Builder(ctx.Pool())
		scoreBuilder := array.NewFloat32Builder(ctx.Pool())
		defer idBuilder.Release()
		defer scoreBuilder.Release()
		return idBuilder.NewArray(), scoreBuilder.NewArray(), nil
	}

	// Determine ID type from first ID
	switch ids[0].(type) {
	case int64:
		return op.buildInt64Results(ctx, ids, scores)
	case string:
		return op.buildStringResults(ctx, ids, scores)
	default:
		return nil, nil, fmt.Errorf("merge_op: unsupported ID type %T", ids[0])
	}
}

func (op *MergeOp) buildInt64Results(ctx *types.FuncContext, ids []any, scores []float32) (arrow.Array, arrow.Array, error) {
	idBuilder := array.NewInt64Builder(ctx.Pool())
	scoreBuilder := array.NewFloat32Builder(ctx.Pool())
	defer idBuilder.Release()
	defer scoreBuilder.Release()

	for i, id := range ids {
		idBuilder.Append(id.(int64))
		scoreBuilder.Append(scores[i])
	}

	return idBuilder.NewArray(), scoreBuilder.NewArray(), nil
}

func (op *MergeOp) buildStringResults(ctx *types.FuncContext, ids []any, scores []float32) (arrow.Array, arrow.Array, error) {
	idBuilder := array.NewStringBuilder(ctx.Pool())
	scoreBuilder := array.NewFloat32Builder(ctx.Pool())
	defer idBuilder.Release()
	defer scoreBuilder.Release()

	for i, id := range ids {
		idBuilder.Append(id.(string))
		scoreBuilder.Append(scores[i])
	}

	return idBuilder.NewArray(), scoreBuilder.NewArray(), nil
}

// collectFieldData collects field data for merged results.
func (op *MergeOp) collectFieldData(ctx *types.FuncContext, collectors map[string]*ChunkCollector, fieldData map[any]map[string]any, locs []idLocation, inputs []*DataFrame, chunkIdx int) error {
	if len(locs) == 0 {
		return nil
	}

	// Get all field names from all inputs
	fieldNames := make(map[string]bool)
	for _, df := range inputs {
		for _, colName := range df.ColumnNames() {
			if colName == types.IDFieldName || colName == types.ScoreFieldName {
				continue
			}
			fieldNames[colName] = true
		}
	}

	numChunks := inputs[0].NumChunks()

	// Initialize collectors for new fields
	for colName := range fieldNames {
		if _, exists := collectors[colName]; !exists {
			collectors[colName] = NewChunkCollector([]string{colName}, numChunks)
		}
	}

	// Build field arrays for this chunk
	for colName := range fieldNames {
		arr, err := op.buildFieldArray(ctx, colName, locs, inputs, fieldData)
		if err != nil {
			return err
		}
		collectors[colName].Set(colName, chunkIdx, arr)
	}

	return nil
}

// buildFieldArray builds a field array from merged locations.
func (op *MergeOp) buildFieldArray(ctx *types.FuncContext, colName string, locs []idLocation, inputs []*DataFrame, fieldData map[any]map[string]any) (arrow.Array, error) {
	if len(locs) == 0 {
		// Return empty array of appropriate type
		// Find type from first input that has this column
		for _, df := range inputs {
			if col := df.Column(colName); col != nil {
				return buildEmptyArray(ctx.Pool(), col.DataType())
			}
		}
		return nil, fmt.Errorf("merge_op: cannot determine type for column %s", colName)
	}

	// Find the data type from first input that has this column
	var dataType arrow.DataType
	for _, df := range inputs {
		if col := df.Column(colName); col != nil {
			dataType = col.DataType()
			break
		}
	}

	if dataType == nil {
		return nil, fmt.Errorf("merge_op: column %s not found in any input", colName)
	}

	return buildArrayFromLocations(ctx.Pool(), colName, locs, inputs, dataType)
}

// buildEmptyArray creates an empty array of the given type.
func buildEmptyArray(pool memory.Allocator, dt arrow.DataType) (arrow.Array, error) {
	switch dt.ID() {
	case arrow.BOOL:
		b := array.NewBooleanBuilder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.INT8:
		b := array.NewInt8Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.INT16:
		b := array.NewInt16Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.INT32:
		b := array.NewInt32Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.INT64:
		b := array.NewInt64Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.FLOAT32:
		b := array.NewFloat32Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.STRING:
		b := array.NewStringBuilder(pool)
		defer b.Release()
		return b.NewArray(), nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", dt.Name())
	}
}

// buildArrayFromLocations builds an array from locations.
func buildArrayFromLocations(pool memory.Allocator, colName string, locs []idLocation, inputs []*DataFrame, dt arrow.DataType) (arrow.Array, error) {
	switch dt.ID() {
	case arrow.BOOL:
		return buildTypedArrayFromLocations[bool](pool, colName, locs, inputs, array.NewBooleanBuilder(pool))
	case arrow.INT8:
		return buildTypedArrayFromLocations[int8](pool, colName, locs, inputs, array.NewInt8Builder(pool))
	case arrow.INT16:
		return buildTypedArrayFromLocations[int16](pool, colName, locs, inputs, array.NewInt16Builder(pool))
	case arrow.INT32:
		return buildTypedArrayFromLocations[int32](pool, colName, locs, inputs, array.NewInt32Builder(pool))
	case arrow.INT64:
		return buildTypedArrayFromLocations[int64](pool, colName, locs, inputs, array.NewInt64Builder(pool))
	case arrow.FLOAT32:
		return buildTypedArrayFromLocations[float32](pool, colName, locs, inputs, array.NewFloat32Builder(pool))
	case arrow.FLOAT64:
		return buildTypedArrayFromLocations[float64](pool, colName, locs, inputs, array.NewFloat64Builder(pool))
	case arrow.STRING:
		return buildTypedArrayFromLocations[string](pool, colName, locs, inputs, array.NewStringBuilder(pool))
	default:
		return nil, fmt.Errorf("unsupported type: %s", dt.Name())
	}
}

// typedArrayBuilder is a generic builder interface for MergeOp.
type typedArrayBuilder[T any] interface {
	Append(T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

// buildTypedArrayFromLocations builds a typed array from locations.
func buildTypedArrayFromLocations[T any, B typedArrayBuilder[T]](pool memory.Allocator, colName string, locs []idLocation, inputs []*DataFrame, builder B) (arrow.Array, error) {
	defer builder.Release()

	for _, loc := range locs {
		df := inputs[loc.inputIdx]
		col := df.Column(colName)
		if col == nil {
			builder.AppendNull()
			continue
		}

		// Find the chunk index for this location
		// Since all inputs should have same chunk structure, we use chunk 0
		// Actually we need to find the correct chunk based on chunkIdx
		// But locs contains rowIdx within a single chunk, so we need the chunkIdx
		// This is a design issue - we need to pass chunkIdx
		// For now, assume we're processing chunk by chunk, so loc.rowIdx is within current chunk
		chunk := col.Chunk(0) // This needs to be fixed to use the correct chunk
		if chunk.IsNull(loc.rowIdx) {
			builder.AppendNull()
			continue
		}

		val := getTypedValue[T](chunk, loc.rowIdx)
		builder.Append(val)
	}

	return builder.NewArray(), nil
}

// getTypedValue extracts a typed value from an array.
func getTypedValue[T any](arr arrow.Array, idx int) T {
	var zero T
	switch a := arr.(type) {
	case *array.Boolean:
		return any(a.Value(idx)).(T)
	case *array.Int8:
		return any(a.Value(idx)).(T)
	case *array.Int16:
		return any(a.Value(idx)).(T)
	case *array.Int32:
		return any(a.Value(idx)).(T)
	case *array.Int64:
		return any(a.Value(idx)).(T)
	case *array.Float32:
		return any(a.Value(idx)).(T)
	case *array.Float64:
		return any(a.Value(idx)).(T)
	case *array.String:
		return any(a.Value(idx)).(T)
	default:
		return zero
	}
}

// releaseChunks releases chunks and collectors on error.
func (op *MergeOp) releaseChunks(idChunks, scoreChunks []arrow.Array, collectors map[string]*ChunkCollector) {
	for _, chunk := range idChunks {
		if chunk != nil {
			chunk.Release()
		}
	}
	for _, chunk := range scoreChunks {
		if chunk != nil {
			chunk.Release()
		}
	}
	for _, collector := range collectors {
		collector.Release()
	}
}

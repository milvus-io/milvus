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
	"sort"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
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

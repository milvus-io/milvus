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
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// =============================================================================
// DataFrame - Immutable Data Container
// =============================================================================

// DataFrame is an immutable data container that stores Milvus data using Arrow Chunked Arrays.
// Each chunk corresponds to a query result (NQ), enabling per-query access.
//
// DataFrame is similar to Arrow Table - it is read-only after creation.
// To create or modify a DataFrame, use DataFrameBuilder.
//
// Memory management: Call Release() exactly once when done with the DataFrame.
type DataFrame struct {
	schema         *arrow.Schema                // Arrow schema with field metadata
	columns        []*arrow.Chunked             // Chunked arrays, one per column
	chunkSizes     []int64                      // Row count per chunk (corresponds to Topks)
	nameIndex      map[string]int               // Column name to index mapping
	fieldTypes     map[string]schemapb.DataType // Preserve Milvus type info for export
	fieldIDs       map[string]int64             // Field IDs for export
	fieldNullables map[string]bool              // Field nullable info for schema creation
}

// =============================================================================
// Metadata Methods (Read-only)
// =============================================================================

// NumRows returns the total number of rows across all chunks.
func (df *DataFrame) NumRows() int64 {
	var total int64
	for _, size := range df.chunkSizes {
		total += size
	}
	return total
}

// NumChunks returns the number of chunks (NQ for search results).
func (df *DataFrame) NumChunks() int {
	return len(df.chunkSizes)
}

// NumColumns returns the number of columns.
func (df *DataFrame) NumColumns() int {
	return len(df.columns)
}

// ChunkSizes returns the row count per chunk (same as Topks for search results).
func (df *DataFrame) ChunkSizes() []int64 {
	result := make([]int64, len(df.chunkSizes))
	copy(result, df.chunkSizes)
	return result
}

// Schema returns the Arrow schema.
func (df *DataFrame) Schema() *arrow.Schema {
	return df.schema
}

// =============================================================================
// Column Access Methods (Read-only)
// =============================================================================

// Column returns a column by name.
// Returns nil if the column does not exist.
func (df *DataFrame) Column(name string) *arrow.Chunked {
	idx, exists := df.nameIndex[name]
	if !exists {
		return nil
	}
	return df.columns[idx]
}

// ColumnNames returns all column names in schema order.
func (df *DataFrame) ColumnNames() []string {
	if df.schema == nil {
		return nil
	}
	fields := df.schema.Fields()
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return names
}

// HasColumn checks if a column exists.
func (df *DataFrame) HasColumn(name string) bool {
	_, exists := df.nameIndex[name]
	return exists
}

// =============================================================================
// Field Metadata Methods (Read-only)
// =============================================================================

// FieldType returns the Milvus DataType for a column.
func (df *DataFrame) FieldType(name string) (schemapb.DataType, bool) {
	dt, exists := df.fieldTypes[name]
	return dt, exists
}

// FieldID returns the field ID for a column.
func (df *DataFrame) FieldID(name string) (int64, bool) {
	id, exists := df.fieldIDs[name]
	return id, exists
}

// =============================================================================
// Lifecycle Methods
// =============================================================================

// Release releases all Arrow resources held by the DataFrame.
// Call exactly once when done. After Release(), the DataFrame should not be used.
func (df *DataFrame) Release() {
	for _, col := range df.columns {
		if col != nil {
			col.Release()
		}
	}
	df.columns = nil
	df.schema = nil
}

// =============================================================================
// DataFrameBuilder
// =============================================================================

// DataFrameBuilder helps build a DataFrame with proper resource cleanup.
// Use defer builder.Release() right after creation, then call Build() to get the result.
// Build() transfers ownership, making Release() a no-op.
//
// Example:
//
//	builder := NewDataFrameBuilder()
//	defer builder.Release()
//	builder.SetChunkSizes(sizes)
//	builder.AddColumnFromChunks("col", chunks)
//	return builder.Build(), nil
type DataFrameBuilder struct {
	result *DataFrame
	fields []arrow.Field // accumulated fields, schema created in Build()
}

// NewDataFrameBuilder creates a new empty DataFrameBuilder.
func NewDataFrameBuilder() *DataFrameBuilder {
	return &DataFrameBuilder{
		result: &DataFrame{
			columns:        make([]*arrow.Chunked, 0),
			chunkSizes:     make([]int64, 0),
			nameIndex:      make(map[string]int),
			fieldTypes:     make(map[string]schemapb.DataType),
			fieldIDs:       make(map[string]int64),
			fieldNullables: make(map[string]bool),
		},
	}
}

// SetChunkSizes sets the chunk sizes on the result DataFrame.
func (b *DataFrameBuilder) SetChunkSizes(sizes []int64) *DataFrameBuilder {
	if b.result == nil {
		return b
	}
	b.result.chunkSizes = make([]int64, len(sizes))
	copy(b.result.chunkSizes, sizes)
	return b
}

// SetFieldType sets the Milvus data type for a column.
func (b *DataFrameBuilder) SetFieldType(name string, dataType schemapb.DataType) *DataFrameBuilder {
	if b.result == nil {
		return b
	}
	b.result.fieldTypes[name] = dataType
	return b
}

// SetFieldID sets the field ID for a column.
func (b *DataFrameBuilder) SetFieldID(name string, fieldID int64) *DataFrameBuilder {
	if b.result == nil {
		return b
	}
	b.result.fieldIDs[name] = fieldID
	return b
}

// SetFieldNullable sets whether a column is nullable.
func (b *DataFrameBuilder) SetFieldNullable(name string, nullable bool) *DataFrameBuilder {
	if b.result == nil {
		return b
	}
	b.result.fieldNullables[name] = nullable
	return b
}

// addColumn adds a chunked column to the DataFrame, taking ownership.
// On error, the column is released.
func (b *DataFrameBuilder) addColumn(name string, col *arrow.Chunked) error {
	if b.result == nil {
		if col != nil {
			col.Release()
		}
		return fmt.Errorf("builder already built")
	}

	if _, exists := b.result.nameIndex[name]; exists {
		if col != nil {
			col.Release()
		}
		return fmt.Errorf("column %s already exists", name)
	}

	if col == nil {
		return fmt.Errorf("column %s is nil", name)
	}

	b.addColumnUnchecked(name, col)
	return nil
}

// addColumnUnchecked adds a column without validation (internal use).
func (b *DataFrameBuilder) addColumnUnchecked(name string, col *arrow.Chunked) {
	// Accumulate field for deferred schema creation in Build()
	b.fields = append(b.fields, arrow.Field{Name: name, Type: col.DataType(), Nullable: true})

	// Add column
	b.result.columns = append(b.result.columns, col)
	b.result.nameIndex[name] = len(b.result.columns) - 1
}

// AddColumns adds multiple columns at once, taking ownership of all.
// Either all columns are added successfully, or none are added and all are released.
// This is the preferred method when adding function outputs to avoid partial failures.
func (b *DataFrameBuilder) AddColumns(names []string, cols []*arrow.Chunked) error {
	// Helper to release all columns
	releaseAll := func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}

	if b.result == nil {
		releaseAll()
		return fmt.Errorf("builder already built")
	}

	if len(names) != len(cols) {
		releaseAll()
		return fmt.Errorf("names count (%d) != cols count (%d)", len(names), len(cols))
	}

	// Validate all before adding any
	for i, name := range names {
		if _, exists := b.result.nameIndex[name]; exists {
			releaseAll()
			return fmt.Errorf("column %s already exists", name)
		}
		if cols[i] == nil {
			releaseAll()
			return fmt.Errorf("column %s is nil", name)
		}
	}

	// All validation passed, add all columns
	for i, name := range names {
		b.addColumnUnchecked(name, cols[i])
	}
	return nil
}

// AddColumnFrom copies a column from source DataFrame, including metadata.
// This is a convenience method that combines Retain + addColumn + CopyFieldMetadata.
func (b *DataFrameBuilder) AddColumnFrom(source *DataFrame, colName string) error {
	if b.result == nil {
		return fmt.Errorf("builder already built")
	}

	col := source.Column(colName)
	if col == nil {
		return fmt.Errorf("column %s not found in source", colName)
	}

	col.Retain()
	if err := b.addColumn(colName, col); err != nil {
		return err
	}

	b.CopyFieldMetadata(source, colName)
	return nil
}

// AddColumnFromChunks creates a chunked column from arrays and adds it.
// Takes ownership of chunks - they are released after creating the chunked array.
func (b *DataFrameBuilder) AddColumnFromChunks(name string, chunks []arrow.Array) error {
	if b.result == nil {
		for _, chunk := range chunks {
			if chunk != nil {
				chunk.Release()
			}
		}
		return fmt.Errorf("builder already built")
	}

	if len(chunks) == 0 {
		return nil
	}

	arrowType := chunks[0].DataType()
	chunked := arrow.NewChunked(arrowType, chunks)

	// Release individual arrays after creating chunked
	for _, chunk := range chunks {
		chunk.Release()
	}

	// Infer Milvus type if not set
	if _, exists := b.result.fieldTypes[name]; !exists {
		if milvusType, err := ToMilvusType(arrowType); err == nil {
			b.result.fieldTypes[name] = milvusType
		}
	}

	return b.addColumn(name, chunked)
}

// CopyFieldMetadata copies field type, ID, and nullable from source DataFrame.
func (b *DataFrameBuilder) CopyFieldMetadata(source *DataFrame, colName string) *DataFrameBuilder {
	if b.result == nil {
		return b
	}
	if ft, ok := source.FieldType(colName); ok {
		b.result.fieldTypes[colName] = ft
	}
	if fid, ok := source.FieldID(colName); ok {
		b.result.fieldIDs[colName] = fid
	}
	if nullable, ok := source.fieldNullables[colName]; ok {
		b.result.fieldNullables[colName] = nullable
	}
	return b
}

// Build returns the constructed DataFrame and invalidates the builder.
// After Build(), Release() becomes a no-op.
func (b *DataFrameBuilder) Build() *DataFrame {
	// Create schema from accumulated fields with correct nullable settings
	if len(b.fields) > 0 {
		finalFields := make([]arrow.Field, len(b.fields))
		for i, f := range b.fields {
			// Look up nullable setting, default to false (Milvus default)
			finalFields[i] = arrow.Field{
				Name:     f.Name,
				Type:     f.Type,
				Nullable: b.result.fieldNullables[f.Name],
			}
		}
		b.result.schema = arrow.NewSchema(finalFields, nil)
	}

	result := b.result
	b.result = nil
	b.fields = nil
	return result
}

// Release releases all resources held by the builder.
// Safe to call multiple times. After Build(), this is a no-op.
func (b *DataFrameBuilder) Release() {
	if b.result != nil {
		// Directly release columns without going through refCount
		// since the DataFrame hasn't been officially "built" yet
		for _, col := range b.result.columns {
			if col != nil {
				col.Release()
			}
		}
		b.result.columns = nil
		b.result.schema = nil
		b.result = nil
	}
	b.fields = nil
}

// =============================================================================
// ChunkCollector
// =============================================================================

// ChunkCollector collects Arrow arrays for multiple columns before assembly.
// Similar to DataFrameBuilder, use defer cc.Release() right after creation.
// Use Consume() to get chunks and transfer ownership to a DataFrameBuilder.
//
// Example:
//
//	collector := NewChunkCollector(colNames, numChunks)
//	defer collector.Release()
//	collector.Set("col", 0, arr)
//	builder.AddColumnFromChunks("col", collector.Consume("col"))
//	return builder.Build(), nil
type ChunkCollector struct {
	chunks   map[string][]arrow.Array
	consumed map[string]bool
}

// NewChunkCollector creates a new ChunkCollector.
func NewChunkCollector(colNames []string, numChunks int) *ChunkCollector {
	cc := &ChunkCollector{
		chunks:   make(map[string][]arrow.Array),
		consumed: make(map[string]bool),
	}
	for _, name := range colNames {
		cc.chunks[name] = make([]arrow.Array, numChunks)
	}
	return cc
}

// Set sets the chunk at the given index for a column.
func (cc *ChunkCollector) Set(colName string, chunkIdx int, chunk arrow.Array) {
	cc.chunks[colName][chunkIdx] = chunk
}

// Consume returns the chunks for a column and marks it as consumed.
// Consumed columns will not be released by Release().
// The caller takes ownership of the returned chunks.
func (cc *ChunkCollector) Consume(colName string) []arrow.Array {
	cc.consumed[colName] = true
	return cc.chunks[colName]
}

// Release releases all non-consumed chunks.
// Safe to call multiple times. Consumed columns are not affected.
func (cc *ChunkCollector) Release() {
	for colName, chunks := range cc.chunks {
		if cc.consumed[colName] {
			continue
		}
		for _, chunk := range chunks {
			if chunk != nil {
				chunk.Release()
			}
		}
	}
	// Clear to prevent double-release
	cc.chunks = nil
}

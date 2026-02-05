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
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// =============================================================================
// Test Suite
// =============================================================================

type ChainTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *ChainTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *ChainTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestChainTestSuite(t *testing.T) {
	suite.Run(t, new(ChainTestSuite))
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *ChainTestSuite) createTestDataFrame() *DataFrame {
	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       5,
		Topks:      []int64{5, 4},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "age",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{25, 30, 35, 40, 45, 50, 55, 60, 65},
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"alice", "bob", "charlie", "david", "eve", "frank", "grace", "henry", "ivy"},
							},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	return df
}

// =============================================================================
// FuncChain Basic Tests
// =============================================================================

func (s *ChainTestSuite) TestNewFuncChain_NilAllocator() {
	fc := NewFuncChainWithAllocator(nil)
	s.NotNil(fc)
	s.Empty(fc.operators)
	s.NotNil(fc.alloc)
}

func (s *ChainTestSuite) TestNewFuncChainWithAllocator() {
	fc := NewFuncChainWithAllocator(s.pool)
	s.NotNil(fc)
	s.Equal(s.pool, fc.alloc)
}

func (s *ChainTestSuite) TestFuncChainSetName() {
	fc := NewFuncChainWithAllocator(nil).SetName("test-chain")
	s.Equal("test-chain", fc.name)
}

func (s *ChainTestSuite) TestFuncChainString() {
	fc := NewFuncChainWithAllocator(nil).SetName("test-chain")
	str := fc.String()
	s.Contains(str, "FuncChain: test-chain")
}

// =============================================================================
// SelectOp Tests
// =============================================================================

func (s *ChainTestSuite) TestSelectOp() {
	df := s.createTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Select(types.IDFieldName, "age").
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Verify only selected columns exist
	s.True(result.HasColumn(types.IDFieldName))
	s.True(result.HasColumn("age"))
	s.False(result.HasColumn("name"))
	s.False(result.HasColumn(types.ScoreFieldName))

	// Verify data integrity
	s.Equal(df.NumRows(), result.NumRows())
	s.Equal(df.NumChunks(), result.NumChunks())
}

func (s *ChainTestSuite) TestSelectOp_NonExistentColumn() {
	df := s.createTestDataFrame()
	defer df.Release()

	_, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Select("nonexistent").
		Execute(df)
	s.Error(err)
}

// =============================================================================
// FilterOp Tests
// =============================================================================

// MockFilterFunction creates a boolean column for filtering
type MockFilterFunction struct {
	threshold float32
}

func (f *MockFilterFunction) Name() string { return "MockFilter" }

func (f *MockFilterFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.FixedWidthTypes.Boolean}
}

func (f *MockFilterFunction) IsRunnable(stage string) bool { return true }

func (f *MockFilterFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	col := inputs[0]

	chunks := make([]arrow.Array, len(col.Chunks()))
	for i, chunk := range col.Chunks() {
		floatChunk := chunk.(*array.Float32)
		builder := array.NewBooleanBuilder(ctx.Pool())

		for j := range floatChunk.Len() {
			if floatChunk.IsNull(j) {
				builder.AppendNull()
			} else {
				builder.Append(floatChunk.Value(j) >= f.threshold)
			}
		}

		chunks[i] = builder.NewArray()
		builder.Release()
	}

	result := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, chunks)
	// Release individual arrays after creating chunked
	for _, chunk := range chunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}

func (s *ChainTestSuite) TestFilterOp() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Filter with FunctionExpr that returns boolean
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Filter(&MockFilterFunction{threshold: 0.5}, []string{types.ScoreFieldName}).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Verify filtered results - scores >= 0.5 should remain
	// Chunk 0: 0.9, 0.8, 0.7, 0.6, 0.5 (all 5 pass)
	// Chunk 1: 0.4, 0.3, 0.2, 0.1 (none pass)
	s.Equal(int64(5), result.NumRows())
	s.Equal([]int64{5, 0}, result.ChunkSizes())
}

func (s *ChainTestSuite) TestFilterOp_NonExistentColumn() {
	df := s.createTestDataFrame()
	defer df.Release()

	_, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Filter(&MockFilterFunction{threshold: 0.5}, []string{"nonexistent"}).
		Execute(df)
	s.Error(err)
}

// =============================================================================
// SortOp Tests
// =============================================================================

func (s *ChainTestSuite) TestSortOp_Ascending() {
	df := s.createTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Sort("age", false). // ascending
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Verify sorted - each chunk should be sorted independently
	ageCol := result.Column("age")

	// Check chunk 0 is sorted ascending
	chunk0 := ageCol.Chunk(0).(*array.Int64)
	for i := 1; i < chunk0.Len(); i++ {
		s.LessOrEqual(chunk0.Value(i-1), chunk0.Value(i))
	}

	// Check chunk 1 is sorted ascending
	if len(ageCol.Chunks()) > 1 {
		chunk1 := ageCol.Chunk(1).(*array.Int64)
		for i := 1; i < chunk1.Len(); i++ {
			s.LessOrEqual(chunk1.Value(i-1), chunk1.Value(i))
		}
	}
}

func (s *ChainTestSuite) TestSortOp_Descending() {
	df := s.createTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Sort("age", true). // descending
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Verify sorted descending
	ageCol := result.Column("age")

	// Check chunk 0 is sorted descending
	chunk0 := ageCol.Chunk(0).(*array.Int64)
	for i := 1; i < chunk0.Len(); i++ {
		s.GreaterOrEqual(chunk0.Value(i-1), chunk0.Value(i))
	}
}

func (s *ChainTestSuite) TestSortOp_NonExistentColumn() {
	df := s.createTestDataFrame()
	defer df.Release()

	_, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Sort("nonexistent", false).
		Execute(df)
	s.Error(err)
}

// =============================================================================
// LimitOp Tests
// =============================================================================

func (s *ChainTestSuite) TestLimitOp() {
	df := s.createTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Limit(3).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Each chunk should be limited to 3 rows
	// Chunk 0: 5 -> 3
	// Chunk 1: 4 -> 3
	s.Equal([]int64{3, 3}, result.ChunkSizes())
	s.Equal(int64(6), result.NumRows())
}

func (s *ChainTestSuite) TestLimitOp_WithOffset() {
	df := s.createTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		LimitWithOffset(2, 1).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Offset 1, Limit 2 for each chunk
	// Chunk 0: 5 rows, skip 1, take 2 -> 2 rows
	// Chunk 1: 4 rows, skip 1, take 2 -> 2 rows
	s.Equal([]int64{2, 2}, result.ChunkSizes())
	s.Equal(int64(4), result.NumRows())
}

func (s *ChainTestSuite) TestLimitOp_LargerThanChunk() {
	df := s.createTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Limit(100).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Limit larger than chunk size should return all rows
	s.Equal(df.ChunkSizes(), result.ChunkSizes())
	s.Equal(df.NumRows(), result.NumRows())
}

func (s *ChainTestSuite) TestLimitOp_OffsetBeyondChunk() {
	df := s.createTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		LimitWithOffset(10, 100).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Offset beyond chunk size should return empty chunks
	s.Equal([]int64{0, 0}, result.ChunkSizes())
	s.Equal(int64(0), result.NumRows())
}

// =============================================================================
// MapOp Tests
// =============================================================================

// MockAddColumnFunction adds a constant column
// Note: This function needs to know the chunk sizes, so it uses a special approach
// by taking $id column as input to determine the chunk structure
type MockAddColumnFunction struct {
	value int64
}

func (f *MockAddColumnFunction) Name() string { return "MockAddColumn" }

func (f *MockAddColumnFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Int64}
}

func (f *MockAddColumnFunction) IsRunnable(stage string) bool { return true }

func (f *MockAddColumnFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	// Use input column to determine chunk sizes
	idCol := inputs[0]

	chunks := make([]arrow.Array, len(idCol.Chunks()))
	for i, chunk := range idCol.Chunks() {
		builder := array.NewInt64Builder(ctx.Pool())
		for range chunk.Len() {
			builder.Append(f.value)
		}
		chunks[i] = builder.NewArray()
		builder.Release()
	}

	result := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
	// Release individual arrays after creating chunked
	for _, chunk := range chunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}

func (s *ChainTestSuite) TestMapOp() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Column mapping is now at operator level
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Map(&MockAddColumnFunction{value: 42}, []string{types.IDFieldName}, []string{"constant"}).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Verify new column exists
	s.True(result.HasColumn("constant"))

	// Verify all values are 42
	col := result.Column("constant")
	for i := range len(col.Chunks()) {
		chunk := col.Chunk(i).(*array.Int64)
		for j := range chunk.Len() {
			s.Equal(int64(42), chunk.Value(j))
		}
	}
}

func (s *ChainTestSuite) TestMapOp_NilFunction() {
	df := s.createTestDataFrame()
	defer df.Release()

	// With new Map signature, nil function should error in NewMapOp
	_, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Map(nil, []string{}, []string{}).
		Execute(df)
	s.Error(err)
}

// =============================================================================
// Chained Operations Tests
// =============================================================================

func (s *ChainTestSuite) TestChainedOperations() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Filter -> Select -> Sort -> Limit
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Filter(&MockFilterFunction{threshold: 0.3}, []string{types.ScoreFieldName}).
		Select(types.IDFieldName, types.ScoreFieldName, "age").
		Sort(types.ScoreFieldName, true). // descending
		Limit(3).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Verify columns
	s.True(result.HasColumn(types.IDFieldName))
	s.True(result.HasColumn(types.ScoreFieldName))
	s.True(result.HasColumn("age"))
	s.False(result.HasColumn("name"))
}

// =============================================================================
// Memory Leak Tests
// =============================================================================

func (s *ChainTestSuite) TestMemoryLeak_ChainedOperations() {
	for range 10 {
		df := s.createTestDataFrame()

		result, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Map(&MockAddColumnFunction{value: 1}, []string{types.IDFieldName}, []string{"temp"}).
			Select(types.IDFieldName, "age", "temp").
			Limit(3).
			Execute(df)
		s.Require().NoError(err)

		result.Release()
		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_FilterOperation() {
	for range 10 {
		df := s.createTestDataFrame()

		result, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Filter(&MockFilterFunction{threshold: 0.5}, []string{types.ScoreFieldName}).
			Execute(df)
		s.Require().NoError(err)

		result.Release()
		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_SortOperation() {
	for range 10 {
		df := s.createTestDataFrame()

		result, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Sort("age", true).
			Execute(df)
		s.Require().NoError(err)

		result.Release()
		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

// =============================================================================
// Error Path Memory Leak Tests
// =============================================================================

// MockErrorFunction is a function that returns an error during execution
type MockErrorFunction struct {
	errorMsg string
}

func (f *MockErrorFunction) Name() string { return "MockError" }

func (f *MockErrorFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Int64}
}

func (f *MockErrorFunction) IsRunnable(stage string) bool { return true }

func (f *MockErrorFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return nil, fmt.Errorf("%s", f.errorMsg)
}

// MockPartialSuccessFunction creates output but the second Map in chain will fail
// This tests cleanup when function succeeds but subsequent operations fail
type MockPartialSuccessFunction struct {
	value int64
}

func (f *MockPartialSuccessFunction) Name() string { return "MockPartialSuccess" }

func (f *MockPartialSuccessFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Int64}
}

func (f *MockPartialSuccessFunction) IsRunnable(stage string) bool { return true }

func (f *MockPartialSuccessFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	idCol := inputs[0]

	chunks := make([]arrow.Array, len(idCol.Chunks()))
	for i, chunk := range idCol.Chunks() {
		builder := array.NewInt64Builder(ctx.Pool())
		for range chunk.Len() {
			builder.Append(f.value)
		}
		chunks[i] = builder.NewArray()
		builder.Release()
	}

	result := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}

func (s *ChainTestSuite) TestMemoryLeak_MapOpError_FunctionFails() {
	// Test: function execution fails, should not leak memory
	for range 10 {
		df := s.createTestDataFrame()

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Map(&MockErrorFunction{errorMsg: "intentional error"}, []string{types.IDFieldName}, []string{"output"}).
			Execute(df)
		s.Require().Error(err)
		s.Contains(err.Error(), "intentional error")

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_MapOpError_NonExistentInputColumn() {
	// Test: MapOp fails because input column doesn't exist
	for range 10 {
		df := s.createTestDataFrame()

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Map(&MockAddColumnFunction{value: 1}, []string{"non_existent_column"}, []string{"output"}).
			Execute(df)
		s.Require().Error(err)

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_MapOpError_ChainedMapFirstSucceedsSecondFails() {
	// Test: First Map succeeds, second Map fails - should cleanup first Map's result
	for range 10 {
		df := s.createTestDataFrame()

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Map(&MockPartialSuccessFunction{value: 1}, []string{types.IDFieldName}, []string{"temp1"}).
			Map(&MockErrorFunction{errorMsg: "second map fails"}, []string{"temp1"}, []string{"temp2"}).
			Execute(df)
		s.Require().Error(err)

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_FilterOpError_FunctionFails() {
	// Test: Filter function fails, should not leak memory
	for range 10 {
		df := s.createTestDataFrame()

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Filter(&MockErrorFilterFunction{errorMsg: "filter error"}, []string{types.ScoreFieldName}).
			Execute(df)
		s.Require().Error(err)

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_FilterOpError_NonExistentColumn() {
	// Test: Filter fails because input column doesn't exist
	for range 10 {
		df := s.createTestDataFrame()

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Filter(&MockFilterFunction{threshold: 0.5}, []string{"non_existent_column"}).
			Execute(df)
		s.Require().Error(err)

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_SelectOpError_NonExistentColumn() {
	// Test: Select fails because column doesn't exist
	for range 10 {
		df := s.createTestDataFrame()

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Select(types.IDFieldName, "non_existent_column").
			Execute(df)
		s.Require().Error(err)

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_SortOpError_NonExistentColumn() {
	// Test: Sort fails because column doesn't exist
	for range 10 {
		df := s.createTestDataFrame()

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Sort("non_existent_column", true).
			Execute(df)
		s.Require().Error(err)

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_ChainedError_MiddleOperatorFails() {
	// Test: Chain with multiple operators, middle one fails
	for range 10 {
		df := s.createTestDataFrame()

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Map(&MockAddColumnFunction{value: 1}, []string{types.IDFieldName}, []string{"temp"}).
			Select("non_existent"). // This will fail
			Limit(5).
			Execute(df)
		s.Require().Error(err)

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

func (s *ChainTestSuite) TestMemoryLeak_ContextCancellation() {
	// Test: Context cancellation during chain execution
	for range 10 {
		df := s.createTestDataFrame()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Map(&MockAddColumnFunction{value: 1}, []string{types.IDFieldName}, []string{"temp"}).
			ExecuteWithContext(ctx, df)
		s.Require().Error(err)

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

// MockErrorFilterFunction is a filter function that returns an error
type MockErrorFilterFunction struct {
	errorMsg string
}

func (f *MockErrorFilterFunction) Name() string { return "MockErrorFilter" }

func (f *MockErrorFilterFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.FixedWidthTypes.Boolean}
}

func (f *MockErrorFilterFunction) IsRunnable(stage string) bool { return true }

func (f *MockErrorFilterFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return nil, fmt.Errorf("%s", f.errorMsg)
}

// MockNilOutputFunction returns one valid output and one nil output
// This triggers the error path in MapOp after some memory has been allocated
type MockNilOutputFunction struct{}

func (f *MockNilOutputFunction) Name() string { return "MockNilOutput" }

func (f *MockNilOutputFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64}
}

func (f *MockNilOutputFunction) IsRunnable(stage string) bool { return true }

func (f *MockNilOutputFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	idCol := inputs[0]

	// Create first output (valid)
	chunks := make([]arrow.Array, len(idCol.Chunks()))
	for i, chunk := range idCol.Chunks() {
		builder := array.NewInt64Builder(ctx.Pool())
		for range chunk.Len() {
			builder.Append(1)
		}
		chunks[i] = builder.NewArray()
		builder.Release()
	}

	result := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}

	// Return first output as valid, second as nil
	// This will cause addChunkedColumnDirect to fail on the second output
	return []*arrow.Chunked{result, nil}, nil
}

func (s *ChainTestSuite) TestMemoryLeak_MapOpError_NilOutputColumn() {
	// Test: Function returns a nil output column, should cleanup properly
	for range 10 {
		df := s.createTestDataFrame()

		_, err := NewFuncChainWithAllocator(s.pool).
			SetStage(types.StageL2Rerank).
			Map(&MockNilOutputFunction{}, []string{types.IDFieldName}, []string{"out1", "out2"}).
			Execute(df)
		s.Require().Error(err)
		s.Contains(err.Error(), "nil")

		df.Release()
	}
	// Memory leak check happens in TearDownTest
}

// =============================================================================
// Operator String Tests
// =============================================================================

func (s *ChainTestSuite) TestOperatorStrings() {
	// MapOp
	mapOp := &MapOp{function: &MockAddColumnFunction{value: 1}}
	s.Contains(mapOp.String(), "Map(MockAddColumn)")

	mapOpNil := &MapOp{function: nil}
	s.Equal("Map(nil)", mapOpNil.String())

	// FilterOp
	filterOp, _ := NewFilterOp(&MockFilterFunction{threshold: 0.5}, []string{"score"})
	s.Equal("Filter(MockFilter)", filterOp.String())

	// SelectOp
	selectOp := NewSelectOp([]string{"a", "b"})
	s.Contains(selectOp.String(), "Select")

	// SortOp
	sortOpAsc := NewSortOp("col", false)
	s.Equal("Sort(col ASC)", sortOpAsc.String())

	sortOpDesc := NewSortOp("col", true)
	s.Equal("Sort(col DESC)", sortOpDesc.String())

	// LimitOp
	limitOp := NewLimitOp(10, 0)
	s.Equal("Limit(10)", limitOp.String())

	limitOpOffset := NewLimitOp(10, 5)
	s.Equal("Limit(10, offset=5)", limitOpOffset.String())
}

// =============================================================================
// FuncContext Tests
// =============================================================================

func (s *ChainTestSuite) TestNewFuncContext() {
	ctx := types.NewFuncContext(s.pool)
	s.Equal(s.pool, ctx.Pool())
}

func (s *ChainTestSuite) TestNewFuncContext_NilPool() {
	// nil pool should use DefaultAllocator
	ctx := types.NewFuncContext(nil)
	s.NotNil(ctx)
	s.Equal(memory.DefaultAllocator, ctx.Pool())
}

// =============================================================================
// Edge Cases
// =============================================================================

func (s *ChainTestSuite) TestEmptyChain() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Empty chain should return input as-is
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Execute(df)
	s.Require().NoError(err)

	// Result should be the same as input
	s.Equal(df, result)
}

func (s *ChainTestSuite) TestSelectOp_AllColumns() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Select all columns
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Select(types.IDFieldName, types.ScoreFieldName, "age", "name").
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(df.NumColumns(), result.NumColumns())
	s.Equal(df.NumRows(), result.NumRows())
}

func (s *ChainTestSuite) TestLimitOp_ZeroLimit() {
	df := s.createTestDataFrame()
	defer df.Release()

	_, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Limit(0).
		Execute(df)
	s.Require().Error(err)
	s.Contains(err.Error(), "limit must be positive")
}

// =============================================================================
// Validate Tests
// =============================================================================

func (s *ChainTestSuite) TestValidate_ValidChain() {
	fc := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Select(types.IDFieldName, "age").
		Sort("age", false).
		Limit(10)

	err := fc.Validate()
	s.NoError(err)
}

func (s *ChainTestSuite) TestValidate_NilMapFunction() {
	fc := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Map(nil, []string{}, []string{})

	err := fc.Validate()
	s.Error(err)
	s.Contains(err.Error(), "chain build error")
}

func (s *ChainTestSuite) TestValidate_BuildError() {
	fc := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank)
	// Force a build error by adding a map with nil function
	fc.Map(nil, []string{"a"}, []string{"b"})

	err := fc.Validate()
	s.Error(err)
}

func (s *ChainTestSuite) TestValidate_MissingStage() {
	// Chain without stage should fail validation
	fc := NewFuncChainWithAllocator(s.pool).
		Select(types.IDFieldName, "age").
		Limit(10)

	err := fc.Validate()
	s.Error(err)
	s.Contains(err.Error(), "chain stage is required")
}

// =============================================================================
// MapWithError Tests
// =============================================================================

func (s *ChainTestSuite) TestMapWithError_Success() {
	df := s.createTestDataFrame()
	defer df.Release()

	fc := NewFuncChainWithAllocator(s.pool).SetStage(types.StageL2Rerank)
	_, err := fc.MapWithError(&MockAddColumnFunction{value: 42}, []string{types.IDFieldName}, []string{"constant"})
	s.NoError(err)

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("constant"))
}

func (s *ChainTestSuite) TestMapWithError_NilFunction() {
	fc := NewFuncChainWithAllocator(s.pool).SetStage(types.StageL2Rerank)
	_, err := fc.MapWithError(nil, []string{types.IDFieldName}, []string{"out"})
	s.Error(err)
}

// =============================================================================
// ExecuteWithStage Tests
// =============================================================================

// MockStagedFunction is a function that only runs in certain stages
type MockStagedFunction struct {
	value  int64
	stages []string
}

func (f *MockStagedFunction) Name() string { return "MockStaged" }

func (f *MockStagedFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Int64}
}

func (f *MockStagedFunction) IsRunnable(stage string) bool {
	for _, s := range f.stages {
		if s == stage {
			return true
		}
	}
	return false
}

func (f *MockStagedFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	idCol := inputs[0]
	chunks := make([]arrow.Array, len(idCol.Chunks()))
	for i, chunk := range idCol.Chunks() {
		builder := array.NewInt64Builder(ctx.Pool())
		for range chunk.Len() {
			builder.Append(f.value)
		}
		chunks[i] = builder.NewArray()
		builder.Release()
	}
	result := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	return []*arrow.Chunked{result}, nil
}

func (s *ChainTestSuite) TestExecuteWithStage_UnsupportedStage() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Create a function that only runs in "L2_rerank" stage
	stagedFn := &MockStagedFunction{value: 999, stages: []string{types.StageL2Rerank}}

	// Execute with a different stage should return an error
	_, err := NewFuncChainWithAllocator(s.pool).
		Map(stagedFn, []string{types.IDFieldName}, []string{"staged_col"}).
		SetStage(types.StageL1Rerank). // Different stage, should error
		Execute(df)

	s.Require().Error(err)
	s.Contains(err.Error(), "does not support stage")
}

func (s *ChainTestSuite) TestExecuteWithStage_RunOperator() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Create a function that only runs in "L2_rerank" stage
	stagedFn := &MockStagedFunction{value: 999, stages: []string{types.StageL2Rerank}}

	result, err := NewFuncChainWithAllocator(s.pool).
		Map(stagedFn, []string{types.IDFieldName}, []string{"staged_col"}).
		SetStage(types.StageL2Rerank). // Same stage, should run
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// The staged column should exist
	s.True(result.HasColumn("staged_col"))
}

func (s *ChainTestSuite) TestExecuteWithStage_EmptyStage() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Create a function that only runs in specific stages
	stagedFn := &MockStagedFunction{value: 999, stages: []string{types.StageL2Rerank}}

	// Empty stage should cause an error
	_, err := NewFuncChainWithAllocator(s.pool).
		Map(stagedFn, []string{types.IDFieldName}, []string{"staged_col"}).
		SetStage(""). // Empty stage should cause error
		Execute(df)

	s.Require().Error(err)
	s.Contains(err.Error(), "stage is required")
}

// =============================================================================
// FilterOp Type Validation Tests
// =============================================================================

func (s *ChainTestSuite) TestFilterOp_NonBooleanFunction() {
	// Try to create FilterOp with a function that returns non-boolean type
	_, err := NewFilterOp(&MockAddColumnFunction{value: 1}, []string{"score"})
	s.Error(err)
	s.Contains(err.Error(), "must return boolean type")
}

// =============================================================================
// FuncContext Stage Tests
// =============================================================================

func (s *ChainTestSuite) TestNewFuncContextWithStage() {
	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	s.Equal(s.pool, ctx.Pool())
	s.Equal(types.StageL2Rerank, ctx.Stage())
}

func (s *ChainTestSuite) TestFuncContextStage_Empty() {
	ctx := types.NewFuncContext(s.pool)
	s.Equal("", ctx.Stage())
}

// =============================================================================
// FuncContext Context Tests
// =============================================================================

func (s *ChainTestSuite) TestNewFuncContextWithContext() {
	goCtx := context.Background()
	ctx := types.NewFuncContextWithContext(goCtx, s.pool)
	s.Equal(s.pool, ctx.Pool())
	s.Equal(goCtx, ctx.Context())
}

func (s *ChainTestSuite) TestNewFuncContextWithContext_NilContext() {
	ctx := types.NewFuncContextWithContext(context.TODO(), s.pool)
	s.NotNil(ctx.Context())
}

func (s *ChainTestSuite) TestNewFuncContextFull() {
	goCtx := context.Background()
	ctx := types.NewFuncContextFull(goCtx, s.pool, types.StageL2Rerank)
	s.Equal(s.pool, ctx.Pool())
	s.Equal(goCtx, ctx.Context())
	s.Equal(types.StageL2Rerank, ctx.Stage())
}

func (s *ChainTestSuite) TestFuncContext_ContextMethod() {
	ctx := types.NewFuncContext(s.pool)
	s.NotNil(ctx.Context())
}

// =============================================================================
// FuncChain Stage Tests
// =============================================================================

func (s *ChainTestSuite) TestFuncChain_SetStage() {
	fc := NewFuncChainWithAllocator(s.pool).SetStage(types.StageL2Rerank)
	s.Equal(types.StageL2Rerank, fc.Stage())
}

func (s *ChainTestSuite) TestFuncChain_Validate_WithStage() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Create a function that only supports L2_rerank stage
	stagedFn := &MockStagedFunction{value: 999, stages: []string{types.StageL2Rerank}}

	// Set stage in chain - validation should fail for unsupported stage
	fc := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL1Rerank).
		Map(stagedFn, []string{types.IDFieldName}, []string{"staged_col"})

	// Execute should fail because Validate checks stage compatibility
	_, err := fc.Execute(df)
	s.Error(err)
	s.Contains(err.Error(), "does not support stage")
}

func (s *ChainTestSuite) TestExecuteWithContext_Cancellation() {
	df := s.createTestDataFrame()
	defer df.Release()

	// Create a canceled context
	goCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Execute with canceled context
	_, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Select(types.IDFieldName, "age").
		Sort("age", false).
		Limit(10).
		ExecuteWithContext(goCtx, df)

	s.Error(err)
	s.Equal(context.Canceled, err)
}

func (s *ChainTestSuite) TestExecuteWithContext_Success() {
	df := s.createTestDataFrame()
	defer df.Release()

	goCtx := context.Background()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Select(types.IDFieldName, "age").
		Sort("age", false).
		Limit(2).
		ExecuteWithContext(goCtx, df)

	s.NoError(err)
	defer result.Release()
	// Limit applies per chunk: 2 chunks with limit 2 each = 4 rows total
	s.Equal(int64(4), result.NumRows())
}

// =============================================================================
// Registry Tests
// =============================================================================

func (s *ChainTestSuite) TestFunctionRegistry() {
	registry := types.NewFunctionRegistry()
	s.NotNil(registry)

	// Test Register and Has
	factory := func(params map[string]interface{}) (types.FunctionExpr, error) {
		return &MockAddColumnFunction{value: 1}, nil
	}
	err := registry.Register("test_func", factory)
	s.NoError(err)
	s.True(registry.Has("test_func"))
	s.False(registry.Has("nonexistent"))

	// Test duplicate registration returns error
	err = registry.Register("test_func", factory)
	s.Error(err)
	s.Contains(err.Error(), "already registered")

	// Test empty name returns error
	err = registry.Register("", factory)
	s.Error(err)
	s.Contains(err.Error(), "cannot be empty")

	// Test nil factory returns error
	err = registry.Register("nil_factory", nil)
	s.Error(err)
	s.Contains(err.Error(), "cannot be nil")

	// Test Get
	f, ok := registry.Get("test_func")
	s.True(ok)
	s.NotNil(f)

	_, ok = registry.Get("nonexistent")
	s.False(ok)

	// Test Create
	expr, err := registry.Create("test_func", nil)
	s.NoError(err)
	s.NotNil(expr)

	_, err = registry.Create("nonexistent", nil)
	s.Error(err)

	// Test Names
	names := registry.Names()
	s.Contains(names, "test_func")

	// Test MustRegister panics on duplicate
	s.Panics(func() {
		registry.MustRegister("test_func", factory)
	})
}

func (s *ChainTestSuite) TestGlobalFunctionRegistry() {
	// Use a unique name to avoid conflicts with other tests
	funcName := "test_global_func_" + s.T().Name()

	// Register a test function first
	err := types.RegisterFunction(funcName, func(params map[string]interface{}) (types.FunctionExpr, error) {
		return &MockAddColumnFunction{value: 42}, nil
	})
	s.NoError(err)

	// Test global registry functions
	s.True(types.HasFunction(funcName))
	s.False(types.HasFunction("nonexistent_function"))

	// Test FunctionNames
	names := types.FunctionNames()
	s.Contains(names, funcName)

	// Test GetFunctionFactory
	factory, ok := types.GetFunctionFactory(funcName)
	s.True(ok)
	s.NotNil(factory)

	_, ok = types.GetFunctionFactory("nonexistent")
	s.False(ok)

	// Test CreateFunction
	expr, err := types.CreateFunction(funcName, nil)
	s.NoError(err)
	s.NotNil(expr)

	_, err = types.CreateFunction("nonexistent", nil)
	s.Error(err)

	// Test duplicate registration returns error
	err = types.RegisterFunction(funcName, func(params map[string]interface{}) (types.FunctionExpr, error) {
		return nil, nil
	})
	s.Error(err)
	s.Contains(err.Error(), "already registered")

	// Test MustRegisterFunction panics on duplicate
	s.Panics(func() {
		types.MustRegisterFunction(funcName, func(params map[string]interface{}) (types.FunctionExpr, error) {
			return nil, nil
		})
	})
}

// =============================================================================
// Operator Inputs/Outputs Tests
// =============================================================================

func (s *ChainTestSuite) TestOperatorInputsOutputs() {
	// BaseOp
	baseOp := &BaseOp{}
	s.Nil(baseOp.Inputs())
	s.Nil(baseOp.Outputs())

	// MapOp
	mapOp, _ := NewMapOp(&MockAddColumnFunction{value: 1}, []string{"a", "b"}, []string{"c"})
	s.Equal([]string{"a", "b"}, mapOp.Inputs())
	s.Equal([]string{"c"}, mapOp.Outputs())

	// FilterOp
	filterOp, _ := NewFilterOp(&MockFilterFunction{threshold: 0.5}, []string{"filter_col"})
	s.Equal([]string{"filter_col"}, filterOp.Inputs())
	s.Empty(filterOp.Outputs())

	// SelectOp
	selectOp := NewSelectOp([]string{"a", "b", "c"})
	s.Equal([]string{"a", "b", "c"}, selectOp.Inputs())
	s.Equal([]string{"a", "b", "c"}, selectOp.Outputs())

	// SortOp
	sortOp := NewSortOp("sort_col", true)
	s.Equal([]string{"sort_col"}, sortOp.Inputs())
	s.Empty(sortOp.Outputs())

	// LimitOp
	limitOp := NewLimitOp(10, 5)
	s.Empty(limitOp.Inputs())
	s.Empty(limitOp.Outputs())
}

// =============================================================================
// compareArrayValues Tests
// =============================================================================

func (s *ChainTestSuite) TestCompareArrayValues_AllTypes() {
	// Test Int64
	int64Builder := array.NewInt64Builder(s.pool)
	int64Builder.AppendValues([]int64{10, 20, 10}, nil)
	int64Arr := int64Builder.NewArray()
	int64Builder.Release()
	defer int64Arr.Release()

	s.Equal(-1, compareArrayValues(int64Arr, 0, 1)) // 10 < 20
	s.Equal(1, compareArrayValues(int64Arr, 1, 0))  // 20 > 10
	s.Equal(0, compareArrayValues(int64Arr, 0, 2))  // 10 == 10

	// Test Float32
	float32Builder := array.NewFloat32Builder(s.pool)
	float32Builder.AppendValues([]float32{1.5, 2.5, 1.5}, nil)
	float32Arr := float32Builder.NewArray()
	float32Builder.Release()
	defer float32Arr.Release()

	s.Equal(-1, compareArrayValues(float32Arr, 0, 1))
	s.Equal(1, compareArrayValues(float32Arr, 1, 0))
	s.Equal(0, compareArrayValues(float32Arr, 0, 2))

	// Test Float64
	float64Builder := array.NewFloat64Builder(s.pool)
	float64Builder.AppendValues([]float64{1.5, 2.5}, nil)
	float64Arr := float64Builder.NewArray()
	float64Builder.Release()
	defer float64Arr.Release()

	s.Equal(-1, compareArrayValues(float64Arr, 0, 1))
	s.Equal(1, compareArrayValues(float64Arr, 1, 0))

	// Test String
	stringBuilder := array.NewStringBuilder(s.pool)
	stringBuilder.AppendValues([]string{"apple", "banana", "apple"}, nil)
	stringArr := stringBuilder.NewArray()
	stringBuilder.Release()
	defer stringArr.Release()

	s.Equal(-1, compareArrayValues(stringArr, 0, 1)) // "apple" < "banana"
	s.Equal(1, compareArrayValues(stringArr, 1, 0))
	s.Equal(0, compareArrayValues(stringArr, 0, 2))

	// Test Int8
	int8Builder := array.NewInt8Builder(s.pool)
	int8Builder.AppendValues([]int8{1, 2}, nil)
	int8Arr := int8Builder.NewArray()
	int8Builder.Release()
	defer int8Arr.Release()

	s.Equal(-1, compareArrayValues(int8Arr, 0, 1))

	// Test Int16
	int16Builder := array.NewInt16Builder(s.pool)
	int16Builder.AppendValues([]int16{100, 200}, nil)
	int16Arr := int16Builder.NewArray()
	int16Builder.Release()
	defer int16Arr.Release()

	s.Equal(-1, compareArrayValues(int16Arr, 0, 1))

	// Test Int32
	int32Builder := array.NewInt32Builder(s.pool)
	int32Builder.AppendValues([]int32{1000, 2000}, nil)
	int32Arr := int32Builder.NewArray()
	int32Builder.Release()
	defer int32Arr.Release()

	s.Equal(-1, compareArrayValues(int32Arr, 0, 1))

	// Test with nulls
	int64WithNullBuilder := array.NewInt64Builder(s.pool)
	int64WithNullBuilder.AppendNull()
	int64WithNullBuilder.Append(10)
	int64WithNullBuilder.AppendNull()
	int64WithNullArr := int64WithNullBuilder.NewArray()
	int64WithNullBuilder.Release()
	defer int64WithNullArr.Release()

	s.Equal(0, compareArrayValues(int64WithNullArr, 0, 2))  // null == null
	s.Equal(-1, compareArrayValues(int64WithNullArr, 0, 1)) // null < 10
	s.Equal(1, compareArrayValues(int64WithNullArr, 1, 0))  // 10 > null
}

// =============================================================================
// dispatchPickByIndices Tests
// =============================================================================

func (s *ChainTestSuite) TestDispatchPickByIndices_AllTypes() {
	indices := []int{2, 0, 1}

	// Test Int8
	int8Builder := array.NewInt8Builder(s.pool)
	int8Builder.AppendValues([]int8{10, 20, 30}, nil)
	int8Arr := int8Builder.NewArray()
	int8Builder.Release()
	defer int8Arr.Release()

	result, err := dispatchPickByIndices(s.pool, int8Arr, indices)
	s.Require().NoError(err)
	defer result.Release()
	s.Equal(int8(30), result.(*array.Int8).Value(0))

	// Test Int16
	int16Builder := array.NewInt16Builder(s.pool)
	int16Builder.AppendValues([]int16{100, 200, 300}, nil)
	int16Arr := int16Builder.NewArray()
	int16Builder.Release()
	defer int16Arr.Release()

	result, err = dispatchPickByIndices(s.pool, int16Arr, indices)
	s.Require().NoError(err)
	defer result.Release()
	s.Equal(int16(300), result.(*array.Int16).Value(0))

	// Test Int32
	int32Builder := array.NewInt32Builder(s.pool)
	int32Builder.AppendValues([]int32{1000, 2000, 3000}, nil)
	int32Arr := int32Builder.NewArray()
	int32Builder.Release()
	defer int32Arr.Release()

	result, err = dispatchPickByIndices(s.pool, int32Arr, indices)
	s.Require().NoError(err)
	defer result.Release()
	s.Equal(int32(3000), result.(*array.Int32).Value(0))

	// Test Float64
	float64Builder := array.NewFloat64Builder(s.pool)
	float64Builder.AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	float64Arr := float64Builder.NewArray()
	float64Builder.Release()
	defer float64Arr.Release()

	result, err = dispatchPickByIndices(s.pool, float64Arr, indices)
	s.Require().NoError(err)
	defer result.Release()
	s.InDelta(3.3, result.(*array.Float64).Value(0), 0.001)

	// Test Boolean
	boolBuilder := array.NewBooleanBuilder(s.pool)
	boolBuilder.AppendValues([]bool{true, false, true}, nil)
	boolArr := boolBuilder.NewArray()
	boolBuilder.Release()
	defer boolArr.Release()

	result, err = dispatchPickByIndices(s.pool, boolArr, indices)
	s.Require().NoError(err)
	defer result.Release()
	s.True(result.(*array.Boolean).Value(0))
}

// =============================================================================
// SortOp with different types Tests
// =============================================================================

func (s *ChainTestSuite) TestSortOp_FloatColumn() {
	// Create DataFrame with float scores
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       3,
		Topks:      []int64{3},
		Scores:     []float32{0.5, 0.9, 0.1},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	// Sort by score descending
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Sort(types.ScoreFieldName, true).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Verify order: should be 0.9, 0.5, 0.1
	scoreCol := result.Column(types.ScoreFieldName)
	chunk := scoreCol.Chunk(0).(*array.Float32)
	s.Equal(float32(0.9), chunk.Value(0))
	s.Equal(float32(0.5), chunk.Value(1))
	s.Equal(float32(0.1), chunk.Value(2))
}

func (s *ChainTestSuite) TestSortOp_StringColumn() {
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       3,
		Topks:      []int64{3},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"charlie", "alice", "bob"}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	// Sort by name ascending
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Sort("name", false).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Verify order: should be alice, bob, charlie
	nameCol := result.Column("name")
	chunk := nameCol.Chunk(0).(*array.String)
	s.Equal("alice", chunk.Value(0))
	s.Equal("bob", chunk.Value(1))
	s.Equal("charlie", chunk.Value(2))
}

func (s *ChainTestSuite) TestSortOp_AllColumnsReordered() {
	// Create DataFrame where age is NOT already sorted
	// This tests that all columns are reordered together, not just the sort column
	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       4,
		Topks:      []int64{4},
		Scores:     []float32{0.1, 0.2, 0.3, 0.4}, // will become [0.3, 0.1, 0.4, 0.2] after sort
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{101, 102, 103, 104}}, // will become [103, 101, 104, 102]
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "age",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{30, 10, 40, 20}}, // unsorted!
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "name",
				FieldId:   101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"bob", "alice", "david", "charlie"}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	// Sort by age ascending
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Sort("age", false).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Original data:
	//   age:    [30,    10,      40,     20]
	//   name:   [bob,   alice,   david,  charlie]
	//   $id:    [101,   102,     103,    104]
	//   $score: [0.1,   0.2,     0.3,    0.4]
	//
	// After sorting by age ascending, expected:
	//   age:    [10,    20,      30,     40]
	//   name:   [alice, charlie, bob,    david]
	//   $id:    [102,   104,     101,    103]
	//   $score: [0.2,   0.4,     0.1,    0.3]

	// Verify age column is sorted
	ageCol := result.Column("age")
	ageChunk := ageCol.Chunk(0).(*array.Int64)
	s.Equal(int64(10), ageChunk.Value(0))
	s.Equal(int64(20), ageChunk.Value(1))
	s.Equal(int64(30), ageChunk.Value(2))
	s.Equal(int64(40), ageChunk.Value(3))

	// Verify name column is reordered accordingly
	nameCol := result.Column("name")
	nameChunk := nameCol.Chunk(0).(*array.String)
	s.Equal("alice", nameChunk.Value(0))   // age=10
	s.Equal("charlie", nameChunk.Value(1)) // age=20
	s.Equal("bob", nameChunk.Value(2))     // age=30
	s.Equal("david", nameChunk.Value(3))   // age=40

	// Verify $id column is reordered accordingly
	idCol := result.Column(types.IDFieldName)
	idChunk := idCol.Chunk(0).(*array.Int64)
	s.Equal(int64(102), idChunk.Value(0)) // age=10
	s.Equal(int64(104), idChunk.Value(1)) // age=20
	s.Equal(int64(101), idChunk.Value(2)) // age=30
	s.Equal(int64(103), idChunk.Value(3)) // age=40

	// Verify $score column is reordered accordingly
	scoreCol := result.Column(types.ScoreFieldName)
	scoreChunk := scoreCol.Chunk(0).(*array.Float32)
	s.Equal(float32(0.2), scoreChunk.Value(0)) // age=10
	s.Equal(float32(0.4), scoreChunk.Value(1)) // age=20
	s.Equal(float32(0.1), scoreChunk.Value(2)) // age=30
	s.Equal(float32(0.3), scoreChunk.Value(3)) // age=40
}

func (s *ChainTestSuite) TestSortOp_MultipleChunksAllColumnsReordered() {
	// Test with multiple chunks to ensure each chunk is sorted independently
	// and all columns within each chunk are reordered together
	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       3,
		Topks:      []int64{3, 3},
		Scores:     []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6}},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							// Chunk 0: [30, 10, 20], Chunk 1: [60, 40, 50]
							LongData: &schemapb.LongArray{Data: []int64{30, 10, 20, 60, 40, 50}},
						},
					},
				},
			},
		},
	}

	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	defer df.Release()

	// Sort by value ascending
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Sort("value", false).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Chunk 0: value [30,10,20] -> [10,20,30], $id [1,2,3] -> [2,3,1]
	// Chunk 1: value [60,40,50] -> [40,50,60], $id [4,5,6] -> [5,6,4]

	valueCol := result.Column("value")
	idCol := result.Column(types.IDFieldName)

	// Verify chunk 0
	valueChunk0 := valueCol.Chunk(0).(*array.Int64)
	idChunk0 := idCol.Chunk(0).(*array.Int64)
	s.Equal(int64(10), valueChunk0.Value(0))
	s.Equal(int64(20), valueChunk0.Value(1))
	s.Equal(int64(30), valueChunk0.Value(2))
	s.Equal(int64(2), idChunk0.Value(0)) // corresponds to value=10
	s.Equal(int64(3), idChunk0.Value(1)) // corresponds to value=20
	s.Equal(int64(1), idChunk0.Value(2)) // corresponds to value=30

	// Verify chunk 1
	valueChunk1 := valueCol.Chunk(1).(*array.Int64)
	idChunk1 := idCol.Chunk(1).(*array.Int64)
	s.Equal(int64(40), valueChunk1.Value(0))
	s.Equal(int64(50), valueChunk1.Value(1))
	s.Equal(int64(60), valueChunk1.Value(2))
	s.Equal(int64(5), idChunk1.Value(0)) // corresponds to value=40
	s.Equal(int64(6), idChunk1.Value(1)) // corresponds to value=50
	s.Equal(int64(4), idChunk1.Value(2)) // corresponds to value=60
}

// =============================================================================
// MapOp Name Tests
// =============================================================================

func (s *ChainTestSuite) TestMapOp_Name() {
	mapOp, _ := NewMapOp(&MockAddColumnFunction{value: 1}, []string{"a"}, []string{"b"})
	s.Equal("Map", mapOp.Name())
}

// =============================================================================
// FuncChain String with operators
// =============================================================================

func (s *ChainTestSuite) TestFuncChain_StringWithOperators() {
	fc := NewFuncChainWithAllocator(nil).
		SetName("test-chain").
		Select("a", "b").
		Filter(&MockFilterFunction{threshold: 0.5}, []string{"score"}).
		Sort("s", true).
		Limit(10)

	str := fc.String()
	s.Contains(str, "test-chain")
	s.Contains(str, "Select")
	s.Contains(str, "Filter")
	s.Contains(str, "Sort")
	s.Contains(str, "Limit")
}

// =============================================================================
// MergeOp Test Suite
// =============================================================================

type MergeOpTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *MergeOpTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *MergeOpTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestMergeOpTestSuite(t *testing.T) {
	suite.Run(t, new(MergeOpTestSuite))
}

// =============================================================================
// MergeOp Helper Functions
// =============================================================================

func (s *MergeOpTestSuite) createSearchResultData(ids []int64, scores []float32, topks []int64) *schemapb.SearchResultData {
	return &schemapb.SearchResultData{
		NumQueries: int64(len(topks)),
		TopK:       topks[0],
		Topks:      topks,
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		},
		FieldsData: []*schemapb.FieldData{},
	}
}

func (s *MergeOpTestSuite) createDataFrame(ids []int64, scores []float32, topks []int64) *DataFrame {
	resultData := s.createSearchResultData(ids, scores, topks)
	df, err := FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	return df
}

// =============================================================================
// MergeOp Tests
// =============================================================================

func (s *MergeOpTestSuite) TestNewMergeOp() {
	// Test RRF strategy
	op := NewMergeOp(MergeStrategyRRF, WithRRFK(60))
	s.Equal(MergeStrategyRRF, op.strategy)
	s.Equal(60.0, op.rrfK)
	s.True(op.normalize)

	// Test Weighted strategy
	weights := []float64{0.3, 0.7}
	op = NewMergeOp(MergeStrategyWeighted, WithWeights(weights), WithNormalize(false))
	s.Equal(MergeStrategyWeighted, op.strategy)
	s.Equal(weights, op.weights)
	s.False(op.normalize)

	// Test Max strategy
	op = NewMergeOp(MergeStrategyMax)
	s.Equal(MergeStrategyMax, op.strategy)
}

func (s *MergeOpTestSuite) TestMergeOpSingleInput() {
	// Create single input DataFrame
	df := s.createDataFrame(
		[]int64{1, 2, 3},
		[]float32{0.9, 0.8, 0.7},
		[]int64{3},
	)
	defer df.Release()

	// Create MergeOp
	op := NewMergeOp(MergeStrategyMax,
		WithMetricTypes([]string{"COSINE"}),
		WithNormalize(true))

	// Execute
	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	result, err := op.ExecuteMulti(ctx, []*DataFrame{df})
	s.Require().NoError(err)
	defer result.Release()

	// Verify result
	s.Equal(1, result.NumChunks())
	s.True(result.HasColumn(types.IDFieldName))
	s.True(result.HasColumn(types.ScoreFieldName))
}

func (s *MergeOpTestSuite) TestMergeOpRRF() {
	// Create two input DataFrames with overlapping IDs
	df1 := s.createDataFrame(
		[]int64{1, 2, 3},
		[]float32{0.9, 0.8, 0.7},
		[]int64{3},
	)
	defer df1.Release()

	df2 := s.createDataFrame(
		[]int64{2, 3, 4},
		[]float32{0.95, 0.85, 0.75},
		[]int64{3},
	)
	defer df2.Release()

	// Create MergeOp with RRF strategy
	op := NewMergeOp(MergeStrategyRRF,
		WithRRFK(60),
		WithMetricTypes([]string{"COSINE", "COSINE"}),
		WithNormalize(true))

	// Execute
	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	// Verify result
	s.Equal(1, result.NumChunks())

	// IDs 2 and 3 should have higher scores because they appear in both lists
	idCol := result.Column(types.IDFieldName)
	s.NotNil(idCol)
	s.GreaterOrEqual(idCol.Chunk(0).Len(), 3) // At least 3 unique IDs
}

func (s *MergeOpTestSuite) TestMergeOpWeighted() {
	// Create two input DataFrames
	df1 := s.createDataFrame(
		[]int64{1, 2},
		[]float32{1.0, 0.5},
		[]int64{2},
	)
	defer df1.Release()

	df2 := s.createDataFrame(
		[]int64{2, 3},
		[]float32{1.0, 0.5},
		[]int64{2},
	)
	defer df2.Release()

	// Create MergeOp with Weighted strategy
	op := NewMergeOp(MergeStrategyWeighted,
		WithWeights([]float64{0.3, 0.7}),
		WithMetricTypes([]string{"COSINE", "COSINE"}),
		WithNormalize(true))

	// Execute
	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	// Verify result
	s.Equal(1, result.NumChunks())

	// ID 2 appears in both lists, should have score = 0.3 * norm(0.5) + 0.7 * norm(1.0)
	idCol := result.Column(types.IDFieldName)
	s.NotNil(idCol)
	s.Equal(3, idCol.Chunk(0).Len()) // 3 unique IDs: 1, 2, 3
}

func (s *MergeOpTestSuite) TestMergeOpMax() {
	// Create two input DataFrames with overlapping IDs
	df1 := s.createDataFrame(
		[]int64{1, 2},
		[]float32{0.5, 0.3},
		[]int64{2},
	)
	defer df1.Release()

	df2 := s.createDataFrame(
		[]int64{1, 2},
		[]float32{0.4, 0.6},
		[]int64{2},
	)
	defer df2.Release()

	// Create MergeOp with Max strategy
	op := NewMergeOp(MergeStrategyMax,
		WithMetricTypes([]string{"COSINE", "COSINE"}),
		WithNormalize(true))

	// Execute
	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	// Verify: ID 1 should have max score, ID 2 should have max score
	s.Equal(1, result.NumChunks())
	s.Equal(int64(2), result.NumRows())
}

func (s *MergeOpTestSuite) TestMergeOpWeightsCountMismatch() {
	df1 := s.createDataFrame([]int64{1}, []float32{0.9}, []int64{1})
	defer df1.Release()

	df2 := s.createDataFrame([]int64{2}, []float32{0.8}, []int64{1})
	defer df2.Release()

	// Weights count doesn't match inputs count
	op := NewMergeOp(MergeStrategyWeighted,
		WithWeights([]float64{0.5}), // Only 1 weight for 2 inputs
		WithMetricTypes([]string{"COSINE", "COSINE"}))

	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	_, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "weights count")
}

func (s *MergeOpTestSuite) TestMergeOpEmptyInput() {
	op := NewMergeOp(MergeStrategyRRF)

	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	_, err := op.ExecuteMulti(ctx, []*DataFrame{})
	s.Error(err)
	s.Contains(err.Error(), "no inputs")
}

func (s *MergeOpTestSuite) TestMergeOpMultipleChunks() {
	// Create DataFrames with 2 chunks (2 queries)
	df1 := s.createDataFrame(
		[]int64{1, 2, 3, 4}, // Query 1: [1,2], Query 2: [3,4]
		[]float32{0.9, 0.8, 0.7, 0.6},
		[]int64{2, 2},
	)
	defer df1.Release()

	df2 := s.createDataFrame(
		[]int64{2, 5, 4, 6},
		[]float32{0.95, 0.85, 0.75, 0.65},
		[]int64{2, 2},
	)
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF,
		WithRRFK(60),
		WithMetricTypes([]string{"COSINE", "COSINE"}))

	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	// Should have 2 chunks
	s.Equal(2, result.NumChunks())
}

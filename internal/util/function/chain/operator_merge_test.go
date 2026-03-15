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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// MergeHelperTestSuite tests internal helper functions of the merge operator
// that are not covered by the main MergeOpTestSuite in chain_test.go.
type MergeHelperTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *MergeHelperTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *MergeHelperTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestMergeHelperTestSuite(t *testing.T) {
	suite.Run(t, new(MergeHelperTestSuite))
}

// helper to create a simple DF with $id (int64), $score (float32), and optionally a field column
func (s *MergeHelperTestSuite) createDF(ids []int64, scores []float32, chunkSizes []int64) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes(chunkSizes)

	offset := 0
	idChunks := make([]arrow.Array, len(chunkSizes))
	scoreChunks := make([]arrow.Array, len(chunkSizes))
	for i, size := range chunkSizes {
		idBuilder := array.NewInt64Builder(s.pool)
		scoreBuilder := array.NewFloat32Builder(s.pool)
		for j := 0; j < int(size); j++ {
			idBuilder.Append(ids[offset+j])
			scoreBuilder.Append(scores[offset+j])
		}
		idChunks[i] = idBuilder.NewArray()
		idBuilder.Release()
		scoreChunks[i] = scoreBuilder.NewArray()
		scoreBuilder.Release()
		offset += int(size)
	}

	err := builder.AddColumnFromChunks(types.IDFieldName, idChunks)
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, scoreChunks)
	s.Require().NoError(err)

	return builder.Build()
}

func (s *MergeHelperTestSuite) createDFWithField(ids []int64, scores []float32, fieldName string, fieldValues []string, chunkSizes []int64) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes(chunkSizes)

	offset := 0
	idChunks := make([]arrow.Array, len(chunkSizes))
	scoreChunks := make([]arrow.Array, len(chunkSizes))
	fieldChunks := make([]arrow.Array, len(chunkSizes))
	for i, size := range chunkSizes {
		idB := array.NewInt64Builder(s.pool)
		scoreB := array.NewFloat32Builder(s.pool)
		fieldB := array.NewStringBuilder(s.pool)
		for j := 0; j < int(size); j++ {
			idB.Append(ids[offset+j])
			scoreB.Append(scores[offset+j])
			fieldB.Append(fieldValues[offset+j])
		}
		idChunks[i] = idB.NewArray()
		idB.Release()
		scoreChunks[i] = scoreB.NewArray()
		scoreB.Release()
		fieldChunks[i] = fieldB.NewArray()
		fieldB.Release()
		offset += int(size)
	}

	err := builder.AddColumnFromChunks(types.IDFieldName, idChunks)
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, scoreChunks)
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(fieldName, fieldChunks)
	s.Require().NoError(err)

	return builder.Build()
}

// =============================================================================
// getIDValue Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestGetIDValueInt64() {
	b := array.NewInt64Builder(s.pool)
	b.Append(42)
	b.AppendNull()
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	s.Equal(int64(42), getIDValue(arr, 0))
	s.Nil(getIDValue(arr, 1)) // null
}

func (s *MergeHelperTestSuite) TestGetIDValueString() {
	b := array.NewStringBuilder(s.pool)
	b.Append("hello")
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	s.Equal("hello", getIDValue(arr, 0))
}

func (s *MergeHelperTestSuite) TestGetIDValueUnsupported() {
	b := array.NewFloat32Builder(s.pool)
	b.Append(1.0)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	s.Nil(getIDValue(arr, 0))
}

// =============================================================================
// compareIDs Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestCompareIDs() {
	// Int64 comparisons
	s.Equal(-1, compareIDs(int64(1), int64(2)))
	s.Equal(0, compareIDs(int64(1), int64(1)))
	s.Equal(1, compareIDs(int64(2), int64(1)))

	// String comparisons
	s.Equal(-1, compareIDs("a", "b"))
	s.Equal(0, compareIDs("a", "a"))
	s.Equal(1, compareIDs("b", "a"))

	// Mixed types return 0
	s.Equal(0, compareIDs(int64(1), "1"))
	s.Equal(0, compareIDs("1", int64(1)))

	// Unsupported type
	s.Equal(0, compareIDs(1.0, 2.0))
}

// =============================================================================
// collectOrderedFieldNames Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestCollectOrderedFieldNames() {
	df1 := s.createDFWithField(
		[]int64{1}, []float32{0.9},
		"field_a", []string{"val"},
		[]int64{1},
	)
	df2 := s.createDFWithField(
		[]int64{2}, []float32{0.8},
		"field_b", []string{"val"},
		[]int64{1},
	)
	defer df1.Release()
	defer df2.Release()

	names := collectOrderedFieldNames([]*DataFrame{df1, df2})
	s.Equal([]string{"field_a", "field_b"}, names)
}

func (s *MergeHelperTestSuite) TestCollectOrderedFieldNamesNoFields() {
	df1 := s.createDF([]int64{1}, []float32{0.9}, []int64{1})
	defer df1.Release()

	names := collectOrderedFieldNames([]*DataFrame{df1})
	s.Empty(names)
}

func (s *MergeHelperTestSuite) TestCollectOrderedFieldNamesDedup() {
	df1 := s.createDFWithField([]int64{1}, []float32{0.9}, "name", []string{"a"}, []int64{1})
	df2 := s.createDFWithField([]int64{2}, []float32{0.8}, "name", []string{"b"}, []int64{1})
	defer df1.Release()
	defer df2.Release()

	names := collectOrderedFieldNames([]*DataFrame{df1, df2})
	s.Equal([]string{"name"}, names) // deduplicated
}

// =============================================================================
// sortAndExtractResults Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestSortAndExtractResultsDescending() {
	idScores := map[any]float32{
		int64(1): 0.5,
		int64(2): 0.9,
		int64(3): 0.7,
	}
	idLocs := map[any]idLocation{
		int64(1): {inputIdx: 0, rowIdx: 0},
		int64(2): {inputIdx: 0, rowIdx: 1},
		int64(3): {inputIdx: 1, rowIdx: 0},
	}

	ids, scores, locs := sortAndExtractResults(idScores, idLocs, true)
	s.Equal(3, len(ids))
	// Descending: 0.9, 0.7, 0.5
	s.Equal(int64(2), ids[0])
	s.InDelta(0.9, float64(scores[0]), 1e-6)
	s.Equal(int64(3), ids[1])
	s.InDelta(0.7, float64(scores[1]), 1e-6)
	s.Equal(int64(1), ids[2])
	s.InDelta(0.5, float64(scores[2]), 1e-6)
	s.Equal(0, locs[0].inputIdx)
}

func (s *MergeHelperTestSuite) TestSortAndExtractResultsAscending() {
	idScores := map[any]float32{
		int64(1): 0.5,
		int64(2): 0.9,
	}
	idLocs := map[any]idLocation{
		int64(1): {inputIdx: 0, rowIdx: 0},
		int64(2): {inputIdx: 0, rowIdx: 1},
	}

	ids, scores, _ := sortAndExtractResults(idScores, idLocs, false)
	s.Equal(int64(1), ids[0])
	s.InDelta(0.5, float64(scores[0]), 1e-6)
	s.Equal(int64(2), ids[1])
	s.InDelta(0.9, float64(scores[1]), 1e-6)
}

func (s *MergeHelperTestSuite) TestSortAndExtractResultsTieBreaking() {
	// Same score, should be sorted by ID ascending
	idScores := map[any]float32{
		int64(3): 0.5,
		int64(1): 0.5,
		int64(2): 0.5,
	}
	idLocs := map[any]idLocation{
		int64(3): {inputIdx: 0, rowIdx: 2},
		int64(1): {inputIdx: 0, rowIdx: 0},
		int64(2): {inputIdx: 0, rowIdx: 1},
	}

	ids, _, _ := sortAndExtractResults(idScores, idLocs, true)
	s.Equal(int64(1), ids[0])
	s.Equal(int64(2), ids[1])
	s.Equal(int64(3), ids[2])
}

// =============================================================================
// buildEmptyArray Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestBuildEmptyArrayAllTypes() {
	dtypes := []arrow.DataType{
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
		arrow.BinaryTypes.String,
	}

	for _, dt := range dtypes {
		arr, err := buildEmptyArray(s.pool, dt)
		s.Require().NoError(err, "type: %s", dt.Name())
		s.Equal(0, arr.Len(), "type: %s", dt.Name())
		arr.Release()
	}
}

func (s *MergeHelperTestSuite) TestBuildEmptyArrayUnsupportedType() {
	_, err := buildEmptyArray(s.pool, arrow.BinaryTypes.Binary)
	s.Error(err)
	s.Contains(err.Error(), "unsupported type")
}

// =============================================================================
// getArrayValue Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestGetArrayValueTypes() {
	testCases := []struct {
		name  string
		build func() arrow.Array
		check func(any)
	}{
		{
			name: "bool",
			build: func() arrow.Array {
				b := array.NewBooleanBuilder(s.pool)
				b.Append(true)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal(true, v) },
		},
		{
			name: "int8",
			build: func() arrow.Array {
				b := array.NewInt8Builder(s.pool)
				b.Append(7)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal(int8(7), v) },
		},
		{
			name: "int16",
			build: func() arrow.Array {
				b := array.NewInt16Builder(s.pool)
				b.Append(100)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal(int16(100), v) },
		},
		{
			name: "int32",
			build: func() arrow.Array {
				b := array.NewInt32Builder(s.pool)
				b.Append(1000)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal(int32(1000), v) },
		},
		{
			name: "int64",
			build: func() arrow.Array {
				b := array.NewInt64Builder(s.pool)
				b.Append(42)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal(int64(42), v) },
		},
		{
			name: "uint8",
			build: func() arrow.Array {
				b := array.NewUint8Builder(s.pool)
				b.Append(5)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal(uint8(5), v) },
		},
		{
			name: "uint16",
			build: func() arrow.Array {
				b := array.NewUint16Builder(s.pool)
				b.Append(500)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal(uint16(500), v) },
		},
		{
			name: "uint32",
			build: func() arrow.Array {
				b := array.NewUint32Builder(s.pool)
				b.Append(5000)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal(uint32(5000), v) },
		},
		{
			name: "uint64",
			build: func() arrow.Array {
				b := array.NewUint64Builder(s.pool)
				b.Append(50000)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal(uint64(50000), v) },
		},
		{
			name: "float32",
			build: func() arrow.Array {
				b := array.NewFloat32Builder(s.pool)
				b.Append(3.14)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.InDelta(3.14, float64(v.(float32)), 1e-5) },
		},
		{
			name: "float64",
			build: func() arrow.Array {
				b := array.NewFloat64Builder(s.pool)
				b.Append(2.718)
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.InDelta(2.718, v.(float64), 1e-9) },
		},
		{
			name: "string",
			build: func() arrow.Array {
				b := array.NewStringBuilder(s.pool)
				b.Append("hello")
				arr := b.NewArray()
				b.Release()
				return arr
			},
			check: func(v any) { s.Equal("hello", v) },
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			arr := tc.build()
			defer arr.Release()
			val := getArrayValue(arr, 0)
			tc.check(val)
		})
	}
}

func (s *MergeHelperTestSuite) TestGetArrayValueNull() {
	b := array.NewInt64Builder(s.pool)
	b.AppendNull()
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	s.Nil(getArrayValue(arr, 0))
}

// =============================================================================
// normalizeScoreChunk Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestNormalizeScoreChunk() {
	b := array.NewFloat32Builder(s.pool)
	b.Append(0.5)
	b.AppendNull()
	b.Append(-0.5)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	chunk := arr.(*array.Float32)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	normFunc := getNormalizeFunc("COSINE")
	result, err := normalizeScoreChunk(ctx, chunk, normFunc)
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Float32)
	s.Equal(3, r.Len())
	s.InDelta(0.75, float64(r.Value(0)), 1e-6) // (1+0.5)*0.5
	s.True(r.IsNull(1))
	s.InDelta(0.25, float64(r.Value(2)), 1e-6) // (1-0.5)*0.5
}

func (s *MergeHelperTestSuite) TestNormalizeScoreChunkEmpty() {
	b := array.NewFloat32Builder(s.pool)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	chunk := arr.(*array.Float32)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	normFunc := getNormalizeFunc("COSINE")
	result, err := normalizeScoreChunk(ctx, chunk, normFunc)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(0, result.Len())
}

// =============================================================================
// getNormalizeFunc Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestGetNormalizeFuncCosine() {
	fn := getNormalizeFunc("COSINE")
	s.NotNil(fn)
	s.InDelta(0.75, float64(fn(0.5)), 1e-6) // (1+0.5)*0.5
	s.InDelta(1.0, float64(fn(1.0)), 1e-6)  // (1+1.0)*0.5
	s.InDelta(0.0, float64(fn(-1.0)), 1e-6) // (1-1.0)*0.5
	s.InDelta(0.5, float64(fn(0.0)), 1e-6)  // (1+0)*0.5
}

func (s *MergeHelperTestSuite) TestGetNormalizeFuncIP() {
	fn := getNormalizeFunc("IP")
	s.NotNil(fn)
	s.InDelta(0.5, float64(fn(0.0)), 1e-6) // 0.5 + atan(0)/pi = 0.5
}

func (s *MergeHelperTestSuite) TestGetNormalizeFuncBM25() {
	fn := getNormalizeFunc("BM25")
	s.NotNil(fn)
	s.InDelta(0.0, float64(fn(0.0)), 1e-6) // 2*atan(0)/pi = 0
}

func (s *MergeHelperTestSuite) TestGetNormalizeFuncL2() {
	fn := getNormalizeFunc("L2")
	s.NotNil(fn)
	s.InDelta(1.0, float64(fn(0.0)), 1e-6) // 1 - 2*atan(0)/pi = 1
	s.True(fn(1.0) < 1.0)                  // distance > 0 -> normalized < 1
}

// =============================================================================
// getDirectionConvertFunc Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestGetDirectionConvertFunc() {
	s.Nil(getDirectionConvertFunc("COSINE"))
	s.Nil(getDirectionConvertFunc("IP"))
	s.Nil(getDirectionConvertFunc("BM25"))

	fn := getDirectionConvertFunc("L2")
	s.NotNil(fn)
	s.InDelta(1.0, float64(fn(0.0)), 1e-6)
}

// =============================================================================
// MergeOp Option Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestWithWeightsOption() {
	op := NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{0.3, 0.7}))
	s.Equal([]float64{0.3, 0.7}, op.weights)
}

func (s *MergeHelperTestSuite) TestWithRRFKOption() {
	op := NewMergeOp(MergeStrategyRRF, WithRRFK(30))
	s.InDelta(30.0, op.rrfK, 1e-9)
}

func (s *MergeHelperTestSuite) TestWithMetricTypesOption() {
	op := NewMergeOp(MergeStrategyRRF, WithMetricTypes([]string{"COSINE", "L2"}))
	s.Equal([]string{"COSINE", "L2"}, op.metricTypes)
}

func (s *MergeHelperTestSuite) TestWithNormalizeOption() {
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false))
	s.False(op.normalize)
}

// =============================================================================
// SortDescending Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestSortDescendingWithNormalize() {
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(true))
	s.True(op.SortDescending())
}

func (s *MergeHelperTestSuite) TestSortDescendingNoNormalizeNoMetrics() {
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false))
	s.True(op.SortDescending())
}

func (s *MergeHelperTestSuite) TestSortDescendingNoNormalizeCosine() {
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false), WithMetricTypes([]string{"COSINE"}))
	s.True(op.SortDescending())
}

func (s *MergeHelperTestSuite) TestSortDescendingNoNormalizeL2() {
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false), WithMetricTypes([]string{"L2"}))
	s.False(op.SortDescending())
}

func (s *MergeHelperTestSuite) TestSortDescendingMixedMetrics() {
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false), WithMetricTypes([]string{"COSINE", "L2"}))
	s.True(op.SortDescending()) // mixed -> descending
}

// =============================================================================
// createDFWithTypedField helpers - build DFs with typed field columns
// =============================================================================

func (s *MergeHelperTestSuite) createDFWithBoolField(ids []int64, scores []float32, fieldName string, fieldValues []bool) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{int64(len(ids))})

	idB := array.NewInt64Builder(s.pool)
	scoreB := array.NewFloat32Builder(s.pool)
	fieldB := array.NewBooleanBuilder(s.pool)
	for i := range ids {
		idB.Append(ids[i])
		scoreB.Append(scores[i])
		fieldB.Append(fieldValues[i])
	}
	idArr := idB.NewArray()
	idB.Release()
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	fieldArr := fieldB.NewArray()
	fieldB.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(fieldName, []arrow.Array{fieldArr})
	s.Require().NoError(err)

	return builder.Build()
}

func (s *MergeHelperTestSuite) createDFWithInt32Field(ids []int64, scores []float32, fieldName string, fieldValues []int32) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{int64(len(ids))})

	idB := array.NewInt64Builder(s.pool)
	scoreB := array.NewFloat32Builder(s.pool)
	fieldB := array.NewInt32Builder(s.pool)
	for i := range ids {
		idB.Append(ids[i])
		scoreB.Append(scores[i])
		fieldB.Append(fieldValues[i])
	}
	idArr := idB.NewArray()
	idB.Release()
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	fieldArr := fieldB.NewArray()
	fieldB.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(fieldName, []arrow.Array{fieldArr})
	s.Require().NoError(err)

	return builder.Build()
}

func (s *MergeHelperTestSuite) createDFWithFloat64Field(ids []int64, scores []float32, fieldName string, fieldValues []float64) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{int64(len(ids))})

	idB := array.NewInt64Builder(s.pool)
	scoreB := array.NewFloat32Builder(s.pool)
	fieldB := array.NewFloat64Builder(s.pool)
	for i := range ids {
		idB.Append(ids[i])
		scoreB.Append(scores[i])
		fieldB.Append(fieldValues[i])
	}
	idArr := idB.NewArray()
	idB.Release()
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	fieldArr := fieldB.NewArray()
	fieldB.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(fieldName, []arrow.Array{fieldArr})
	s.Require().NoError(err)

	return builder.Build()
}

func (s *MergeHelperTestSuite) createDFWithFloat32Field(ids []int64, scores []float32, fieldName string, fieldValues []float32) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{int64(len(ids))})

	idB := array.NewInt64Builder(s.pool)
	scoreB := array.NewFloat32Builder(s.pool)
	fieldB := array.NewFloat32Builder(s.pool)
	for i := range ids {
		idB.Append(ids[i])
		scoreB.Append(scores[i])
		fieldB.Append(fieldValues[i])
	}
	idArr := idB.NewArray()
	idB.Release()
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	fieldArr := fieldB.NewArray()
	fieldB.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(fieldName, []arrow.Array{fieldArr})
	s.Require().NoError(err)

	return builder.Build()
}

func (s *MergeHelperTestSuite) createDFWithInt8Field(ids []int64, scores []float32, fieldName string, fieldValues []int8) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{int64(len(ids))})

	idB := array.NewInt64Builder(s.pool)
	scoreB := array.NewFloat32Builder(s.pool)
	fieldB := array.NewInt8Builder(s.pool)
	for i := range ids {
		idB.Append(ids[i])
		scoreB.Append(scores[i])
		fieldB.Append(fieldValues[i])
	}
	idArr := idB.NewArray()
	idB.Release()
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	fieldArr := fieldB.NewArray()
	fieldB.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(fieldName, []arrow.Array{fieldArr})
	s.Require().NoError(err)

	return builder.Build()
}

func (s *MergeHelperTestSuite) createDFWithInt16Field(ids []int64, scores []float32, fieldName string, fieldValues []int16) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{int64(len(ids))})

	idB := array.NewInt64Builder(s.pool)
	scoreB := array.NewFloat32Builder(s.pool)
	fieldB := array.NewInt16Builder(s.pool)
	for i := range ids {
		idB.Append(ids[i])
		scoreB.Append(scores[i])
		fieldB.Append(fieldValues[i])
	}
	idArr := idB.NewArray()
	idB.Release()
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	fieldArr := fieldB.NewArray()
	fieldB.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(fieldName, []arrow.Array{fieldArr})
	s.Require().NoError(err)

	return builder.Build()
}

// =============================================================================
// scoreMergeFunc Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestMaxMergeFunc() {
	result, count := maxMergeFunc(0.5, 0.8, 1)
	s.InDelta(0.8, float64(result), 1e-6)
	s.Equal(2, count)

	result, count = maxMergeFunc(0.8, 0.3, 2)
	s.InDelta(0.8, float64(result), 1e-6)
	s.Equal(3, count)
}

func (s *MergeHelperTestSuite) TestSumMergeFunc() {
	result, count := sumMergeFunc(0.5, 0.3, 1)
	s.InDelta(0.8, float64(result), 1e-6)
	s.Equal(2, count)
}

func (s *MergeHelperTestSuite) TestAvgMergeFunc() {
	// avgMergeFunc accumulates sum; average is computed later
	result, count := avgMergeFunc(0.5, 0.3, 1)
	s.InDelta(0.8, float64(result), 1e-6) // accumulated sum
	s.Equal(2, count)
}

// =============================================================================
// releaseChunks Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestReleaseChunksWithArraysAndCollectors() {
	op := NewMergeOp(MergeStrategyRRF)

	// Build some arrays
	idB := array.NewInt64Builder(s.pool)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()

	scoreB := array.NewFloat32Builder(s.pool)
	scoreB.Append(0.5)
	scoreArr := scoreB.NewArray()
	scoreB.Release()

	// Build a collector with a chunk
	collector := NewChunkCollector([]string{"field_a"}, 1)
	fieldB := array.NewStringBuilder(s.pool)
	fieldB.Append("hello")
	fieldArr := fieldB.NewArray()
	fieldB.Release()
	collector.Set("field_a", 0, fieldArr)

	collectors := map[string]*ChunkCollector{"field_a": collector}

	// releaseChunks should release everything without panic
	op.releaseChunks([]arrow.Array{idArr}, []arrow.Array{scoreArr}, collectors)
}

func (s *MergeHelperTestSuite) TestReleaseChunksWithNils() {
	op := NewMergeOp(MergeStrategyRRF)

	// Should handle nil entries gracefully
	op.releaseChunks([]arrow.Array{nil, nil}, []arrow.Array{nil}, nil)
}

func (s *MergeHelperTestSuite) TestReleaseChunksPartialArrays() {
	op := NewMergeOp(MergeStrategyRRF)

	idB := array.NewInt64Builder(s.pool)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()

	// Mixed nil and non-nil
	op.releaseChunks([]arrow.Array{idArr, nil}, nil, map[string]*ChunkCollector{})
}

// =============================================================================
// Merge with typed field columns (exercises buildArrayFromLocations / getTypedValue)
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeRRFWithBoolField() {
	df1 := s.createDFWithBoolField([]int64{1, 2}, []float32{0.9, 0.8}, "flag", []bool{true, false})
	df2 := s.createDFWithBoolField([]int64{3, 1}, []float32{0.7, 0.6}, "flag", []bool{true, true})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("flag"))
	s.Equal(int64(3), result.NumRows())
}

func (s *MergeHelperTestSuite) TestMergeRRFWithInt8Field() {
	df1 := s.createDFWithInt8Field([]int64{1, 2}, []float32{0.9, 0.8}, "age", []int8{10, 20})
	df2 := s.createDFWithInt8Field([]int64{3, 1}, []float32{0.7, 0.6}, "age", []int8{30, 10})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("age"))
	s.Equal(int64(3), result.NumRows())
}

func (s *MergeHelperTestSuite) TestMergeRRFWithInt16Field() {
	df1 := s.createDFWithInt16Field([]int64{1, 2}, []float32{0.9, 0.8}, "val", []int16{100, 200})
	df2 := s.createDFWithInt16Field([]int64{3}, []float32{0.7}, "val", []int16{300})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("val"))
	s.Equal(int64(3), result.NumRows())
}

func (s *MergeHelperTestSuite) TestMergeRRFWithInt32Field() {
	df1 := s.createDFWithInt32Field([]int64{1, 2}, []float32{0.9, 0.8}, "count", []int32{1000, 2000})
	df2 := s.createDFWithInt32Field([]int64{3, 1}, []float32{0.7, 0.6}, "count", []int32{3000, 1000})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("count"))
	s.Equal(int64(3), result.NumRows())
}

func (s *MergeHelperTestSuite) TestMergeRRFWithFloat32Field() {
	df1 := s.createDFWithFloat32Field([]int64{1, 2}, []float32{0.9, 0.8}, "weight", []float32{1.1, 2.2})
	df2 := s.createDFWithFloat32Field([]int64{3, 1}, []float32{0.7, 0.6}, "weight", []float32{3.3, 1.1})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("weight"))
	s.Equal(int64(3), result.NumRows())
}

func (s *MergeHelperTestSuite) TestMergeRRFWithFloat64Field() {
	df1 := s.createDFWithFloat64Field([]int64{1, 2}, []float32{0.9, 0.8}, "distance", []float64{1.11, 2.22})
	df2 := s.createDFWithFloat64Field([]int64{3, 1}, []float32{0.7, 0.6}, "distance", []float64{3.33, 1.11})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("distance"))
	s.Equal(int64(3), result.NumRows())
}

// =============================================================================
// buildTypedArrayFromLocations: missing column path (AppendNull)
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeWithMissingFieldColumn() {
	// df1 has "extra" column, df2 does NOT -> buildTypedArrayFromLocations appends null
	df1 := s.createDFWithField([]int64{1, 2}, []float32{0.9, 0.8}, "extra", []string{"a", "b"}, []int64{2})
	df2 := s.createDF([]int64{3}, []float32{0.7}, []int64{1})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("extra"))
	s.Equal(int64(3), result.NumRows())

	// ID=3 came from df2 which has no "extra" column, so it should be null
	extraCol := result.Column("extra")
	s.NotNil(extraCol)
	chunk := extraCol.Chunk(0)
	// Find the row index for ID=3 (should be null)
	idCol := result.Column(types.IDFieldName)
	idChunk := idCol.Chunk(0).(*array.Int64)
	for i := 0; i < idChunk.Len(); i++ {
		if idChunk.Value(i) == 3 {
			s.True(chunk.IsNull(i), "ID=3 should have null 'extra' field")
		}
	}
}

// =============================================================================
// buildFieldArray: empty locs path
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeWithEmptyChunk() {
	// Create a DataFrame with 2 chunks where one query returns empty results
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{2, 0})

	idB := array.NewInt64Builder(s.pool)
	idB.AppendValues([]int64{1, 2}, nil)
	idArr := idB.NewArray()
	idB.Release()

	scoreB := array.NewFloat32Builder(s.pool)
	scoreB.AppendValues([]float32{0.9, 0.8}, nil)
	scoreArr := scoreB.NewArray()
	scoreB.Release()

	// Empty chunk for second query
	idB2 := array.NewInt64Builder(s.pool)
	idArr2 := idB2.NewArray()
	idB2.Release()

	scoreB2 := array.NewFloat32Builder(s.pool)
	scoreArr2 := scoreB2.NewArray()
	scoreB2.Release()

	fieldB := array.NewStringBuilder(s.pool)
	fieldB.AppendValues([]string{"a", "b"}, nil)
	fieldArr := fieldB.NewArray()
	fieldB.Release()

	fieldB2 := array.NewStringBuilder(s.pool)
	fieldArr2 := fieldB2.NewArray()
	fieldB2.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr, idArr2})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr, scoreArr2})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks("name", []arrow.Array{fieldArr, fieldArr2})
	s.Require().NoError(err)

	df := builder.Build()
	defer df.Release()

	// Merge single input, no normalization -> pass through
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(2, result.NumChunks())
}

// =============================================================================
// processSingleInput Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestProcessSingleInputNoNormalization() {
	df := s.createDF([]int64{1, 2, 3}, []float32{0.5, 0.8, 0.3}, []int64{3})
	defer df.Release()

	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	// Should pass through unchanged
	s.Equal(int64(3), result.NumRows())
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	s.InDelta(0.5, float64(scores.Value(0)), 1e-6)
	s.InDelta(0.8, float64(scores.Value(1)), 1e-6)
	s.InDelta(0.3, float64(scores.Value(2)), 1e-6)
}

func (s *MergeHelperTestSuite) TestProcessSingleInputWithNormalization() {
	df := s.createDF([]int64{1, 2}, []float32{0.5, -0.5}, []int64{2})
	defer df.Release()

	op := NewMergeOp(MergeStrategyRRF, WithNormalize(true), WithMetricTypes([]string{"COSINE"}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(2), result.NumRows())
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	s.InDelta(0.75, float64(scores.Value(0)), 1e-6) // (1+0.5)*0.5
	s.InDelta(0.25, float64(scores.Value(1)), 1e-6) // (1-0.5)*0.5
}

func (s *MergeHelperTestSuite) TestProcessSingleInputNormalizeNoMetricTypes() {
	df := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df.Release()

	// normalize=true but no metric types -> skip normalization
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(true))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	s.InDelta(0.5, float64(scores.Value(0)), 1e-6) // unchanged
}

func (s *MergeHelperTestSuite) TestProcessSingleInputScoreNotFloat32() {
	// Create DF with int64 score column (not Float32)
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{1})

	idB := array.NewInt64Builder(s.pool)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()

	scoreB := array.NewInt64Builder(s.pool)
	scoreB.Append(100)
	scoreArr := scoreB.NewArray()
	scoreB.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)

	df := builder.Build()
	defer df.Release()

	op := NewMergeOp(MergeStrategyRRF, WithNormalize(true), WithMetricTypes([]string{"COSINE"}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err = op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "not Float32")
}

// =============================================================================
// mergeScoreCombine Tests (max/sum/avg with actual DataFrames)
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeMaxStrategy() {
	df1 := s.createDF([]int64{1, 2, 3}, []float32{0.5, 0.8, 0.3}, []int64{3})
	df2 := s.createDF([]int64{1, 2, 4}, []float32{0.7, 0.6, 0.9}, []int64{3})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyMax, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(4), result.NumRows())

	// Verify max scores: ID=1 max(0.5,0.7)=0.7, ID=2 max(0.8,0.6)=0.8
	idChunk := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scoreChunk := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	idScoreMap := make(map[int64]float32)
	for i := 0; i < idChunk.Len(); i++ {
		idScoreMap[idChunk.Value(i)] = scoreChunk.Value(i)
	}
	s.InDelta(0.7, float64(idScoreMap[1]), 1e-6)
	s.InDelta(0.8, float64(idScoreMap[2]), 1e-6)
	s.InDelta(0.3, float64(idScoreMap[3]), 1e-6)
	s.InDelta(0.9, float64(idScoreMap[4]), 1e-6)
}

func (s *MergeHelperTestSuite) TestMergeSumStrategy() {
	df1 := s.createDF([]int64{1, 2}, []float32{0.5, 0.8}, []int64{2})
	df2 := s.createDF([]int64{1, 3}, []float32{0.3, 0.9}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategySum, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())

	idChunk := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scoreChunk := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	idScoreMap := make(map[int64]float32)
	for i := 0; i < idChunk.Len(); i++ {
		idScoreMap[idChunk.Value(i)] = scoreChunk.Value(i)
	}
	s.InDelta(0.8, float64(idScoreMap[1]), 1e-6) // 0.5+0.3
	s.InDelta(0.8, float64(idScoreMap[2]), 1e-6)
	s.InDelta(0.9, float64(idScoreMap[3]), 1e-6)
}

func (s *MergeHelperTestSuite) TestMergeAvgStrategy() {
	df1 := s.createDF([]int64{1, 2}, []float32{0.4, 0.8}, []int64{2})
	df2 := s.createDF([]int64{1, 3}, []float32{0.6, 0.9}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyAvg, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())

	idChunk := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scoreChunk := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	idScoreMap := make(map[int64]float32)
	for i := 0; i < idChunk.Len(); i++ {
		idScoreMap[idChunk.Value(i)] = scoreChunk.Value(i)
	}
	s.InDelta(0.5, float64(idScoreMap[1]), 1e-6) // (0.4+0.6)/2
	s.InDelta(0.8, float64(idScoreMap[2]), 1e-6) // only appears once
	s.InDelta(0.9, float64(idScoreMap[3]), 1e-6)
}

func (s *MergeHelperTestSuite) TestMergeScoreCombineMissingIDColumn() {
	// Create DF without ID column
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{1})
	scoreB := array.NewFloat32Builder(s.pool)
	scoreB.Append(0.5)
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	err := builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	df1 := builder.Build()
	defer df1.Release()

	df2 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df2.Release()

	op := NewMergeOp(MergeStrategyMax, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err = op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "missing ID or score column")
}

func (s *MergeHelperTestSuite) TestMergeScoreCombineMissingScoreColumn() {
	// Create DF without score column
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{1})
	idB := array.NewInt64Builder(s.pool)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	df1 := builder.Build()
	defer df1.Release()

	df2 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df2.Release()

	op := NewMergeOp(MergeStrategySum, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err = op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "missing ID or score column")
}

func (s *MergeHelperTestSuite) TestMergeScoreCombineScoreNotFloat32() {
	// Create DF with int64 score column
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{1})
	idB := array.NewInt64Builder(s.pool)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	scoreB := array.NewInt64Builder(s.pool)
	scoreB.Append(100)
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	df1 := builder.Build()
	defer df1.Release()

	df2 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df2.Release()

	op := NewMergeOp(MergeStrategyMax, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err = op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "not Float32")
}

func (s *MergeHelperTestSuite) TestMergeScoreCombineWithNormalization() {
	df1 := s.createDF([]int64{1, 2}, []float32{0.5, 0.8}, []int64{2})
	df2 := s.createDF([]int64{1, 3}, []float32{0.3, 0.9}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyMax, WithNormalize(true), WithMetricTypes([]string{"COSINE", "L2"}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())
}

func (s *MergeHelperTestSuite) TestMergeScoreCombineWithMixedMetricsNoNormalize() {
	df1 := s.createDF([]int64{1, 2}, []float32{0.5, 0.8}, []int64{2})
	df2 := s.createDF([]int64{1, 3}, []float32{0.3, 0.9}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	// Mixed metrics (COSINE + L2) without normalize -> applies direction conversion
	op := NewMergeOp(MergeStrategySum, WithNormalize(false), WithMetricTypes([]string{"COSINE", "L2"}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())
}

// =============================================================================
// ExecuteMulti validation Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeNoInputs() {
	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err := op.ExecuteMulti(ctx, []*DataFrame{})
	s.Error(err)
	s.Contains(err.Error(), "no inputs provided")
}

func (s *MergeHelperTestSuite) TestMergeMismatchedChunks() {
	df1 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	// df2 has 2 chunks
	df2 := s.createDF([]int64{1, 2}, []float32{0.5, 0.8}, []int64{1, 1})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "chunks")
}

func (s *MergeHelperTestSuite) TestMergeMetricTypesCountMismatch() {
	df1 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	df2 := s.createDF([]int64{2}, []float32{0.8}, []int64{1})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF, WithMetricTypes([]string{"COSINE"})) // 1 metric, 2 inputs
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "metric types count")
}

func (s *MergeHelperTestSuite) TestMergeWeightedCountMismatch() {
	df1 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	df2 := s.createDF([]int64{2}, []float32{0.8}, []int64{1})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{0.5})) // 1 weight, 2 inputs
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "weights count")
}

func (s *MergeHelperTestSuite) TestMergeUnsupportedStrategy() {
	df1 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	df2 := s.createDF([]int64{2}, []float32{0.8}, []int64{1})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategy("unknown"))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "unsupported strategy")
}

// =============================================================================
// MergeOp String/Name Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeOpString() {
	op := NewMergeOp(MergeStrategyRRF)
	s.Equal("Merge(rrf)", op.String())

	op2 := NewMergeOp(MergeStrategyWeighted)
	s.Equal("Merge(weighted)", op2.String())
}

func (s *MergeHelperTestSuite) TestMergeOpName() {
	op := NewMergeOp(MergeStrategyRRF)
	s.Equal("Merge", op.Name())
}

// =============================================================================
// MergeOp with field columns across strategies
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeWeightedWithFieldColumns() {
	df1 := s.createDFWithField([]int64{1, 2}, []float32{0.9, 0.8}, "name", []string{"alice", "bob"}, []int64{2})
	df2 := s.createDFWithField([]int64{2, 3}, []float32{0.7, 0.6}, "name", []string{"bob", "carol"}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{0.5, 0.5}), WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("name"))
	s.Equal(int64(3), result.NumRows())
}

func (s *MergeHelperTestSuite) TestMergeSumWithFieldColumns() {
	df1 := s.createDFWithField([]int64{1}, []float32{0.5}, "tag", []string{"x"}, []int64{1})
	df2 := s.createDFWithField([]int64{1, 2}, []float32{0.3, 0.8}, "tag", []string{"x", "y"}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategySum, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("tag"))
	s.Equal(int64(2), result.NumRows())
}

// =============================================================================
// buildTypedArrayFromLocations: null value path
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeWithNullFieldValues() {
	// Create DF with null values in the field column
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{2})

	idB := array.NewInt64Builder(s.pool)
	idB.AppendValues([]int64{1, 2}, nil)
	idArr := idB.NewArray()
	idB.Release()

	scoreB := array.NewFloat32Builder(s.pool)
	scoreB.AppendValues([]float32{0.9, 0.8}, nil)
	scoreArr := scoreB.NewArray()
	scoreB.Release()

	fieldB := array.NewStringBuilder(s.pool)
	fieldB.Append("hello")
	fieldB.AppendNull() // null value for ID=2
	fieldArr := fieldB.NewArray()
	fieldB.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks("text", []arrow.Array{fieldArr})
	s.Require().NoError(err)

	df1 := builder.Build()
	defer df1.Release()

	df2 := s.createDFWithField([]int64{3}, []float32{0.7}, "text", []string{"world"}, []int64{1})
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn("text"))
	s.Equal(int64(3), result.NumRows())

	// Verify that the null field value is preserved
	textCol := result.Column("text")
	idCol := result.Column(types.IDFieldName)
	idChunk := idCol.Chunk(0).(*array.Int64)
	textChunk := textCol.Chunk(0)
	for i := 0; i < idChunk.Len(); i++ {
		if idChunk.Value(i) == 2 {
			s.True(textChunk.IsNull(i), "ID=2 should have null 'text' field")
		}
	}
}

// =============================================================================
// classifyMetricsOrder Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestClassifyMetricsOrderAllLargerBetter() {
	mixed, desc := classifyMetricsOrder([]string{"COSINE", "IP", "BM25"})
	s.False(mixed)
	s.True(desc)
}

func (s *MergeHelperTestSuite) TestClassifyMetricsOrderAllSmallerBetter() {
	mixed, desc := classifyMetricsOrder([]string{"L2", "HAMMING"})
	s.False(mixed)
	s.False(desc)
}

func (s *MergeHelperTestSuite) TestClassifyMetricsOrderMixed() {
	mixed, desc := classifyMetricsOrder([]string{"COSINE", "L2"})
	s.True(mixed)
	s.True(desc) // mixed -> descending
}

// =============================================================================
// Multi-chunk merge (exercises chunkIdx > 0 paths)
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeRRFMultiChunk() {
	// 2 chunks per DF (2 queries)
	df1 := s.createDF([]int64{1, 2, 3, 4}, []float32{0.9, 0.8, 0.7, 0.6}, []int64{2, 2})
	df2 := s.createDF([]int64{2, 5, 4, 6}, []float32{0.8, 0.7, 0.6, 0.5}, []int64{2, 2})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(2, result.NumChunks())
}

func (s *MergeHelperTestSuite) TestMergeMaxMultiChunkWithFields() {
	df1 := s.createDFWithField(
		[]int64{1, 2, 3, 4},
		[]float32{0.9, 0.8, 0.7, 0.6},
		"name", []string{"a", "b", "c", "d"},
		[]int64{2, 2},
	)
	df2 := s.createDFWithField(
		[]int64{2, 5, 4, 6},
		[]float32{0.85, 0.7, 0.65, 0.5},
		"name", []string{"b", "e", "d", "f"},
		[]int64{2, 2},
	)
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyMax, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(2, result.NumChunks())
	s.True(result.HasColumn("name"))
}

// =============================================================================
// MergeOp.Execute (single input wrapper) Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeOpExecuteSingleInput() {
	df := s.createDF([]int64{1, 2}, []float32{0.5, 0.8}, []int64{2})
	defer df.Release()

	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(2), result.NumRows())
}

// =============================================================================
// Weighted merge with normalization
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeWeightedWithNormalization() {
	df1 := s.createDF([]int64{1, 2}, []float32{0.5, 0.8}, []int64{2})
	df2 := s.createDF([]int64{1, 3}, []float32{0.3, 0.9}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyWeighted,
		WithWeights([]float64{0.6, 0.4}),
		WithNormalize(true),
		WithMetricTypes([]string{"COSINE", "L2"}),
	)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())
}

func (s *MergeHelperTestSuite) TestMergeWeightedMissingIDOrScore() {
	// Create DF without ID column
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{1})
	scoreB := array.NewFloat32Builder(s.pool)
	scoreB.Append(0.5)
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	err := builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	df1 := builder.Build()
	defer df1.Release()

	df2 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df2.Release()

	op := NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{0.5, 0.5}), WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err = op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "missing ID or score column")
}

func (s *MergeHelperTestSuite) TestMergeWeightedScoreNotFloat32() {
	// Create DF with int64 score column
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{1})
	idB := array.NewInt64Builder(s.pool)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	scoreB := array.NewInt64Builder(s.pool)
	scoreB.Append(100)
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	df1 := builder.Build()
	defer df1.Release()

	df2 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df2.Release()

	op := NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{0.5, 0.5}), WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err = op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "not Float32")
}

// =============================================================================
// RRF merge error paths
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeRRFMissingIDColumn() {
	// Create DF without ID column
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{1})
	scoreB := array.NewFloat32Builder(s.pool)
	scoreB.Append(0.5)
	scoreArr := scoreB.NewArray()
	scoreB.Release()
	err := builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr})
	s.Require().NoError(err)
	df1 := builder.Build()
	defer df1.Release()

	df2 := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err = op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "missing")
}

// =============================================================================
// Weighted merge with mixed metrics and direction conversion (no normalize)
// =============================================================================

func (s *MergeHelperTestSuite) TestMergeWeightedMixedMetricsNoNormalize() {
	df1 := s.createDF([]int64{1, 2}, []float32{0.5, 0.8}, []int64{2})
	df2 := s.createDF([]int64{1, 3}, []float32{0.3, 0.9}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyWeighted,
		WithWeights([]float64{0.6, 0.4}),
		WithNormalize(false),
		WithMetricTypes([]string{"COSINE", "L2"}), // mixed
	)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())
}

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
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func (s *MergeHelperTestSuite) createElementDF(ids []int64, elements []int32, scores []float32, fieldValues []string) *DataFrame {
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{int64(len(ids))})
	idBuilder := array.NewInt64Builder(s.pool)
	elementBuilder := array.NewInt32Builder(s.pool)
	scoreBuilder := array.NewFloat32Builder(s.pool)
	fieldBuilder := array.NewStringBuilder(s.pool)
	defer idBuilder.Release()
	defer elementBuilder.Release()
	defer scoreBuilder.Release()
	defer fieldBuilder.Release()

	idBuilder.AppendValues(ids, nil)
	elementBuilder.AppendValues(elements, nil)
	scoreBuilder.AppendValues(scores, nil)
	fieldBuilder.AppendValues(fieldValues, nil)
	s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
	s.Require().NoError(builder.AddColumnFromChunks(types.ElementIndicesFieldName, []arrow.Array{elementBuilder.NewArray()}))
	s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
	s.Require().NoError(builder.AddColumnFromChunks("text", []arrow.Array{fieldBuilder.NewArray()}))
	return builder.Build()
}

func (s *MergeHelperTestSuite) createStringElementDF(ids []string, elements []int32, scores []float32, fieldValues []string) *DataFrame {
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{int64(len(ids))})
	idBuilder := array.NewStringBuilder(s.pool)
	elementBuilder := array.NewInt32Builder(s.pool)
	scoreBuilder := array.NewFloat32Builder(s.pool)
	fieldBuilder := array.NewStringBuilder(s.pool)
	defer idBuilder.Release()
	defer elementBuilder.Release()
	defer scoreBuilder.Release()
	defer fieldBuilder.Release()

	idBuilder.AppendValues(ids, nil)
	elementBuilder.AppendValues(elements, nil)
	scoreBuilder.AppendValues(scores, nil)
	fieldBuilder.AppendValues(fieldValues, nil)
	s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
	s.Require().NoError(builder.AddColumnFromChunks(types.ElementIndicesFieldName, []arrow.Array{elementBuilder.NewArray()}))
	s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
	s.Require().NoError(builder.AddColumnFromChunks("text", []arrow.Array{fieldBuilder.NewArray()}))
	return builder.Build()
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
	idScores := map[candidateKey]float32{
		{intID: 1}: 0.5,
		{intID: 2}: 0.9,
		{intID: 3}: 0.7,
	}
	idLocs := map[candidateKey]idLocation{
		{intID: 1}: {inputIdx: 0, rowIdx: 0},
		{intID: 2}: {inputIdx: 0, rowIdx: 1},
		{intID: 3}: {inputIdx: 1, rowIdx: 0},
	}

	scores, locs := sortAndExtractResults(idScores, idLocs, true)
	s.Equal(3, len(scores))
	// Descending: 0.9, 0.7, 0.5
	s.InDelta(0.9, float64(scores[0]), 1e-6)
	s.InDelta(0.7, float64(scores[1]), 1e-6)
	s.InDelta(0.5, float64(scores[2]), 1e-6)
	s.Equal([]idLocation{{inputIdx: 0, rowIdx: 1}, {inputIdx: 1, rowIdx: 0}, {inputIdx: 0, rowIdx: 0}}, locs)
}

func (s *MergeHelperTestSuite) TestSortAndExtractResultsAscending() {
	idScores := map[candidateKey]float32{
		{intID: 1}: 0.5,
		{intID: 2}: 0.9,
	}
	idLocs := map[candidateKey]idLocation{
		{intID: 1}: {inputIdx: 0, rowIdx: 0},
		{intID: 2}: {inputIdx: 0, rowIdx: 1},
	}

	scores, locs := sortAndExtractResults(idScores, idLocs, false)
	s.InDelta(0.5, float64(scores[0]), 1e-6)
	s.InDelta(0.9, float64(scores[1]), 1e-6)
	s.Equal([]idLocation{{inputIdx: 0, rowIdx: 0}, {inputIdx: 0, rowIdx: 1}}, locs)
}

func (s *MergeHelperTestSuite) TestSortAndExtractResultsTieBreaking() {
	// Same score, should be sorted by ID ascending
	idScores := map[candidateKey]float32{
		{intID: 3}: 0.5,
		{intID: 1}: 0.5,
		{intID: 2}: 0.5,
	}
	idLocs := map[candidateKey]idLocation{
		{intID: 3}: {inputIdx: 0, rowIdx: 2},
		{intID: 1}: {inputIdx: 0, rowIdx: 0},
		{intID: 2}: {inputIdx: 0, rowIdx: 1},
	}

	_, locs := sortAndExtractResults(idScores, idLocs, true)
	s.Equal([]idLocation{{inputIdx: 0, rowIdx: 0}, {inputIdx: 0, rowIdx: 1}, {inputIdx: 0, rowIdx: 2}}, locs)
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

func (s *MergeHelperTestSuite) TestWithRRFWeightsOption() {
	op := NewMergeOp(MergeStrategyRRF, WithWeights([]float64{0.8, 0.2}))
	s.Equal([]float64{0.8, 0.2}, op.weights)
}

func (s *MergeHelperTestSuite) TestWithMetricTypesOption() {
	op := NewMergeOp(MergeStrategyRRF, WithMetricTypes([]string{"COSINE", "L2"}))
	// Mixed metrics → sortDescending=true, scoreNormFuncs populated for direction conversion
	s.True(op.SortDescending())
	s.Len(op.scoreNormFuncs, 2)
	s.Nil(op.scoreNormFuncs[0])    // COSINE: already larger-is-better, no conversion
	s.NotNil(op.scoreNormFuncs[1]) // L2: needs direction conversion
}

func (s *MergeHelperTestSuite) TestWithNormalizeOption() {
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false))
	// No metric types + normalize=false → sortDescending=true (default), no normFuncs
	s.True(op.SortDescending())
	s.Nil(op.scoreNormFuncs)
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

func (s *MergeHelperTestSuite) TestMergeStrategyRRFWithBoolField() {
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

func (s *MergeHelperTestSuite) TestMergeStrategyRRFWithInt8Field() {
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

func (s *MergeHelperTestSuite) TestMergeStrategyRRFWithInt16Field() {
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

func (s *MergeHelperTestSuite) TestMergeStrategyRRFWithInt32Field() {
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

func (s *MergeHelperTestSuite) TestMergeStrategyRRFWithFloat32Field() {
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

func (s *MergeHelperTestSuite) TestMergeStrategyRRFWithFloat64Field() {
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

// buildVarCharIDDataFrame builds a VarChar-PK DataFrame whose first query has
// hits and whose second query returns nothing (topks = [len(ids), 0]).
func (s *MergeHelperTestSuite) buildVarCharIDDataFrame(ids []string, scores []float32) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{int64(len(ids)), 0})

	idB := array.NewStringBuilder(s.pool)
	idB.AppendValues(ids, nil)
	idArr := idB.NewArray()
	idB.Release()

	idB2 := array.NewStringBuilder(s.pool)
	idArr2 := idB2.NewArray()
	idB2.Release()

	scoreB := array.NewFloat32Builder(s.pool)
	scoreB.AppendValues(scores, nil)
	scoreArr := scoreB.NewArray()
	scoreB.Release()

	scoreB2 := array.NewFloat32Builder(s.pool)
	scoreArr2 := scoreB2.NewArray()
	scoreB2.Release()

	s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArr, idArr2}))
	s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreArr, scoreArr2}))
	return builder.Build()
}

// A zero-hit chunk must adopt the ID type of the other chunks. Building it as
// Int64 while the chunks with hits are utf8 makes arrow.NewChunked reject the
// column ("mismatch data type int64 vs utf8"), which panics the proxy for any
// VarChar-PK collection where one query of the batch returns nothing (#51372).
func (s *MergeHelperTestSuite) TestMergeWithEmptyChunkVarCharIDs() {
	df := s.buildVarCharIDDataFrame([]string{"a", "b"}, []float32{0.9, 0.8})
	defer df.Release()

	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(2, result.NumChunks())
	s.Equal(int64(2), result.NumRows())

	idType, ok := result.FieldType(types.IDFieldName)
	s.Require().True(ok)
	s.Equal(schemapb.DataType_VarChar, idType)

	idCol := result.Column(types.IDFieldName)
	s.Require().NotNil(idCol)
	s.Equal(arrow.STRING, idCol.DataType().ID())
	s.Equal(2, idCol.Chunk(0).Len())
	s.Equal(0, idCol.Chunk(1).Len())
}

// Same for multi-input merge (hybrid search): one leg is entirely empty, the
// other carries VarChar IDs with a zero-hit query.
func (s *MergeHelperTestSuite) TestMergeMultiInputEmptyLegVarCharIDs() {
	dfEmpty := s.buildVarCharIDDataFrame(nil, nil)
	defer dfEmpty.Release()
	dfHits := s.buildVarCharIDDataFrame([]string{"a", "b"}, []float32{0.9, 0.8})
	defer dfHits.Release()

	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{dfEmpty, dfHits})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(2, result.NumChunks())
	s.Equal(int64(2), result.NumRows())
	s.Equal(arrow.STRING, result.Column(types.IDFieldName).DataType().ID())
}

// =============================================================================
// Single Input Tests
// =============================================================================

func (s *MergeHelperTestSuite) TestSingleInputRRFProducesRankBasedScores() {
	df := s.createDF([]int64{1, 2, 3}, []float32{0.5, 0.8, 0.3}, []int64{3})
	defer df.Release()

	op := NewMergeOp(MergeStrategyRRF, WithNormalize(false))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	// Single input RRF should compute rank-based scores: 1/(k+rank)
	s.Equal(int64(3), result.NumRows())
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	s.InDelta(1.0/(60+1), float64(scores.Value(0)), 1e-6)
	s.InDelta(1.0/(60+2), float64(scores.Value(1)), 1e-6)
	s.InDelta(1.0/(60+3), float64(scores.Value(2)), 1e-6)
}

func (s *MergeHelperTestSuite) TestSingleInputWeightedAppliesWeight() {
	df := s.createDF([]int64{1, 2}, []float32{0.5, -0.5}, []int64{2})
	defer df.Release()

	op := NewMergeOp(MergeStrategyWeighted,
		WithWeights([]float64{0.7}),
		WithNormalize(true),
		WithMetricTypes([]string{"COSINE"}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	// COSINE normalize: (1+score)*0.5, then multiply by weight 0.7
	// ID 1: (1+0.5)*0.5 * 0.7 = 0.525
	// ID 2: (1-0.5)*0.5 * 0.7 = 0.175
	s.Equal(int64(2), result.NumRows())
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	s.InDelta(0.525, float64(scores.Value(0)), 1e-6)
	s.InDelta(0.175, float64(scores.Value(1)), 1e-6)
}

func (s *MergeHelperTestSuite) TestSingleInputRRFIgnoresOriginalScores() {
	df := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df.Release()

	// RRF computes rank-based scores regardless of normalize or metricTypes
	op := NewMergeOp(MergeStrategyRRF, WithNormalize(true))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	s.InDelta(1.0/(60+1), float64(scores.Value(0)), 1e-6) // RRF score, not original
}

func (s *MergeHelperTestSuite) TestWeightedRRFProducesExpectedScoresAndOrder() {
	df1 := s.createDF([]int64{1, 2, 3}, []float32{0.1, 0.2, 0.3}, []int64{3})
	df2 := s.createDF([]int64{3, 2, 4}, []float32{0.9, 0.8, 0.7}, []int64{3})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF, WithRRFK(1), WithWeights([]float64{0.7, 0.2}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	expectedIDs := []int64{1, 2, 3, 4}
	expectedScores := []float64{0.35, 0.3, 0.275, 0.05}
	for i := range expectedIDs {
		s.Equal(expectedIDs[i], ids.Value(i))
		s.InDelta(expectedScores[i], float64(scores.Value(i)), 1e-6)
	}
}

func (s *MergeHelperTestSuite) TestAllOneRRFWeightsPreserveClassicScores() {
	df1 := s.createDF([]int64{1, 2, 3}, []float32{0.1, 0.2, 0.3}, []int64{3})
	df2 := s.createDF([]int64{3, 2, 4}, []float32{0.9, 0.8, 0.7}, []int64{3})
	defer df1.Release()
	defer df2.Release()

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	classic, err := NewMergeOp(MergeStrategyRRF).ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer classic.Release()
	weighted, err := NewMergeOp(MergeStrategyRRF, WithWeights([]float64{1, 1})).ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer weighted.Release()

	classicIDs := classic.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	weightedIDs := weighted.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	classicScores := classic.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	weightedScores := weighted.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	s.Equal(classicIDs.Len(), weightedIDs.Len())
	for i := 0; i < classicIDs.Len(); i++ {
		s.Equal(classicIDs.Value(i), weightedIDs.Value(i))
		s.Equal(classicScores.Value(i), weightedScores.Value(i))
	}
}

func (s *MergeHelperTestSuite) TestWeightedRRFInputCountMismatch() {
	df1 := s.createDF([]int64{1}, []float32{0.1}, []int64{1})
	df2 := s.createDF([]int64{2}, []float32{0.2}, []int64{1})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF, WithWeights([]float64{1}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Error(err)
	s.Contains(err.Error(), "weights count 1 != inputs count 2")
}

func (s *MergeHelperTestSuite) TestWeightedStrategyRequiresWeights() {
	df := s.createDF([]int64{1}, []float32{0.1}, []int64{1})
	defer df.Release()

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	for _, op := range []*MergeOp{
		NewMergeOp(MergeStrategyWeighted),
		NewMergeOp(MergeStrategyWeighted, WithWeights(nil)),
		NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{})),
	} {
		_, err := op.ExecuteMulti(ctx, []*DataFrame{df})
		s.Error(err)
		s.Contains(err.Error(), "weights count 0 != inputs count 1")
	}
}

func (s *MergeHelperTestSuite) TestRRFExplicitEmptyWeightsRejected() {
	df := s.createDF([]int64{1}, []float32{0.1}, []int64{1})
	defer df.Release()

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	for _, op := range []*MergeOp{
		NewMergeOp(MergeStrategyRRF, WithWeights(nil)),
		NewMergeOp(MergeStrategyRRF, WithWeights([]float64{})),
	} {
		_, err := op.ExecuteMulti(ctx, []*DataFrame{df})
		s.Error(err)
		s.Contains(err.Error(), "weights count 0 != inputs count 1")
	}
}

func (s *MergeHelperTestSuite) TestAllZeroRRFWeightsKeepCandidateUnion() {
	df1 := s.createDF([]int64{3, 1}, []float32{0.2, 0.1}, []int64{2})
	df2 := s.createDF([]int64{2, 1}, []float32{0.4, 0.3}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := NewMergeOp(MergeStrategyRRF, WithWeights([]float64{0, 0})).ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	s.Equal([]int64{1, 2, 3}, []int64{ids.Value(0), ids.Value(1), ids.Value(2)})
	for index := 0; index < scores.Len(); index++ {
		s.Zero(scores.Value(index))
	}
}

func (s *MergeHelperTestSuite) TestMergeRejectsInvalidConfiguredWeights() {
	df := s.createDF([]int64{1}, []float32{0.1}, []int64{1})
	defer df.Release()

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	for _, strategy := range []MergeStrategy{MergeStrategyRRF, MergeStrategyWeighted} {
		for _, weight := range []float64{math.NaN(), math.Inf(1), -0.1, 1.1} {
			_, err := NewMergeOp(strategy, WithWeights([]float64{weight})).ExecuteMulti(ctx, []*DataFrame{df})
			s.Require().Error(err)
			s.Contains(err.Error(), "must be finite and in range [0, 1]")
		}
	}
}

func (s *MergeHelperTestSuite) TestSingleInputWeightedScoreNotFloat32() {
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

	// Weighted strategy reads scores, so non-Float32 should fail
	op := NewMergeOp(MergeStrategyWeighted,
		WithWeights([]float64{1.0}),
		WithNormalize(true),
		WithMetricTypes([]string{"COSINE"}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err = op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "not Float32")
}

// =============================================================================
// mergeNumCombine Tests (max/sum/avg with actual DataFrames)
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

func (s *MergeHelperTestSuite) TestMergeNumCombineMissingIDColumn() {
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
	s.Contains(err.Error(), "missing $id column")
}

func (s *MergeHelperTestSuite) TestMergeNumCombineMissingScoreColumn() {
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
	s.Contains(err.Error(), "missing $score column")
}

func (s *MergeHelperTestSuite) TestMergeNumCombineScoreNotFloat32() {
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

func (s *MergeHelperTestSuite) TestMergeNumCombineWithNormalization() {
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

func (s *MergeHelperTestSuite) TestMergeNumCombineWithMixedMetricsNoNormalize() {
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
	s.Contains(err.Error(), "scoreNormFuncs count")
}

func (s *MergeHelperTestSuite) TestMergeRuntimeInputCountMismatch() {
	df := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df.Release()
	op := NewMergeOp(MergeStrategyRRF, withExpectedInputs(2))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "input count 1 != expected count 2")
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

func (s *MergeHelperTestSuite) TestMergeRejectsNilInput() {
	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err := op.ExecuteMulti(ctx, []*DataFrame{nil})
	s.Error(err)
	s.Contains(err.Error(), "input[0] is nil")
	s.True(errors.Is(err, merr.ErrFunctionFailed))
}

func (s *MergeHelperTestSuite) TestMergeRejectsNoQueryChunks() {
	df := NewDataFrameBuilder().Build()
	defer df.Release()
	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "no query chunks")
}

func (s *MergeHelperTestSuite) TestMergeRejectsMismatchedPrimaryKeyTypes() {
	intDF := s.createDF([]int64{1}, []float32{0.5}, []int64{1, 0})
	stringDF := s.buildVarCharIDDataFrame([]string{"1"}, []float32{0.5})
	defer intDF.Release()
	defer stringDF.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err := op.ExecuteMulti(ctx, []*DataFrame{intDF, stringDF})
	s.Error(err)
	s.Contains(err.Error(), "does not match")
}

func (s *MergeHelperTestSuite) TestMergeRejectsNullID() {
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
	idBuilder := array.NewInt64Builder(s.pool)
	idBuilder.AppendNull()
	idArray := idBuilder.NewArray()
	idBuilder.Release()
	s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idArray}))
	df := builder.Build()
	defer df.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "$id has null")
	s.True(errors.Is(err, merr.ErrFunctionFailed))
}

func (s *MergeHelperTestSuite) TestMergeRejectsMisalignedSystemColumn() {
	df := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	defer df.Release()
	df.chunkSizes[0] = 2

	op := NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{1}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "$id has 1 rows, expected 2")
}

func (s *MergeHelperTestSuite) TestMergeElementIdentityAndSourceGathering() {
	df1 := s.createElementDF(
		[]int64{1, 1}, []int32{0, 1}, []float32{0.9, 0.5}, []string{"a0", "a1"})
	df2 := s.createElementDF(
		[]int64{1, 1}, []int32{0, 2}, []float32{0.8, 0.7}, []string{"b0", "b2"})
	defer df1.Release()
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())
	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	elements := result.Column(types.ElementIndicesFieldName).Chunk(0).(*array.Int32)
	texts := result.Column("text").Chunk(0).(*array.String)
	s.Equal([]int64{1, 1, 1}, ids.Int64Values())
	s.Equal([]int32{0, 1, 2}, elements.Int32Values())
	s.Equal("a0", texts.Value(0))
	s.Equal("a1", texts.Value(1))
	s.Equal("b2", texts.Value(2))
}

func (s *MergeHelperTestSuite) TestMergeElementIdentityAllStrategiesWithStringPK() {
	tests := []struct {
		name             string
		strategy         MergeStrategy
		opts             []MergeOption
		expectedIDs      []string
		expectedElements []int32
		expectedTexts    []string
	}{
		{
			name:             "rrf",
			strategy:         MergeStrategyRRF,
			expectedIDs:      []string{"a", "a", "b"},
			expectedElements: []int32{0, 1, 0},
			expectedTexts:    []string{"a0-first", "a1", "b0"},
		},
		{
			name:             "weighted",
			strategy:         MergeStrategyWeighted,
			opts:             []MergeOption{WithWeights([]float64{0.5, 0.5})},
			expectedIDs:      []string{"a", "b", "a"},
			expectedElements: []int32{0, 0, 1},
			expectedTexts:    []string{"a0-first", "b0", "a1"},
		},
		{
			name:             "max",
			strategy:         MergeStrategyMax,
			expectedIDs:      []string{"a", "b", "a"},
			expectedElements: []int32{0, 0, 1},
			expectedTexts:    []string{"a0-first", "b0", "a1"},
		},
		{
			name:             "sum",
			strategy:         MergeStrategySum,
			expectedIDs:      []string{"a", "b", "a"},
			expectedElements: []int32{0, 0, 1},
			expectedTexts:    []string{"a0-first", "b0", "a1"},
		},
		{
			name:             "avg",
			strategy:         MergeStrategyAvg,
			expectedIDs:      []string{"a", "b", "a"},
			expectedElements: []int32{0, 0, 1},
			expectedTexts:    []string{"a0-first", "b0", "a1"},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			df1 := s.createStringElementDF(
				[]string{"a", "a"}, []int32{0, 1}, []float32{0.9, 0.5}, []string{"a0-first", "a1"})
			df2 := s.createStringElementDF(
				[]string{"a", "b"}, []int32{0, 0}, []float32{0.8, 0.7}, []string{"a0-second", "b0"})
			defer df1.Release()
			defer df2.Release()

			op := NewMergeOp(test.strategy, test.opts...)
			ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
			result, err := op.ExecuteMulti(ctx, []*DataFrame{df1, df2})
			s.Require().NoError(err)
			defer result.Release()

			ids := result.Column(types.IDFieldName).Chunk(0).(*array.String)
			elements := result.Column(types.ElementIndicesFieldName).Chunk(0).(*array.Int32)
			texts := result.Column("text").Chunk(0).(*array.String)
			actualIDs := []string{ids.Value(0), ids.Value(1), ids.Value(2)}
			actualTexts := []string{texts.Value(0), texts.Value(1), texts.Value(2)}
			s.Equal(test.expectedIDs, actualIDs)
			s.Equal(test.expectedElements, elements.Int32Values())
			s.Equal(test.expectedTexts, actualTexts)
		})
	}
}

func (s *MergeHelperTestSuite) TestDeclarativeMergeMatchesProgrammaticMerge() {
	df1 := s.createDF([]int64{1, 2}, []float32{0.8, 0.3}, []int64{2})
	df2 := s.createDF([]int64{1, 3}, []float32{0.2, 0.6}, []int64{2})
	defer df1.Release()
	defer df2.Release()

	declarativeOp, err := NewMergeOpFromReprWithContext(mergeRepr("weighted", map[string]*schemapb.FunctionParamValue{
		MergeParamWeights:   arrayParam(doubleParam(0.6), doubleParam(0.4)),
		MergeParamNormScore: boolParam(true),
	}), types.FunctionBuildContext{Search: &types.SearchRuntimeInfo{MetricTypes: []string{"COSINE", "L2"}}})
	s.Require().NoError(err)
	programmaticOp := NewMergeOp(MergeStrategyWeighted,
		WithWeights([]float64{0.6, 0.4}),
		WithNormalize(true),
		WithMetricTypes([]string{"COSINE", "L2"}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	declarativeResult, err := declarativeOp.(*MergeOp).ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer declarativeResult.Release()
	programmaticResult, err := programmaticOp.ExecuteMulti(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)
	defer programmaticResult.Release()

	declarativeIDs := declarativeResult.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	programmaticIDs := programmaticResult.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	s.Equal(programmaticIDs.Int64Values(), declarativeIDs.Int64Values())
	declarativeScores := declarativeResult.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	programmaticScores := programmaticResult.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	s.Equal(programmaticScores.Float32Values(), declarativeScores.Float32Values())
}

func (s *MergeHelperTestSuite) TestMergeRejectsMixedElementIdentity() {
	rowDF := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
	elementDF := s.createElementDF([]int64{1}, []int32{0}, []float32{0.5}, []string{"a"})
	defer rowDF.Release()
	defer elementDF.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err := op.ExecuteMulti(ctx, []*DataFrame{rowDF, elementDF})
	s.Error(err)
	s.Contains(err.Error(), "inconsistent $element_indices presence")
	s.True(errors.Is(err, merr.ErrFunctionFailed))
}

func (s *MergeHelperTestSuite) TestMergeRejectsInvalidElementColumn() {
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	op := NewMergeOp(MergeStrategyRRF)

	s.Run("wrong type", func() {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		idBuilder := array.NewInt64Builder(s.pool)
		idBuilder.Append(1)
		elementBuilder := array.NewStringBuilder(s.pool)
		elementBuilder.Append("0")
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks(types.ElementIndicesFieldName, []arrow.Array{elementBuilder.NewArray()}))
		idBuilder.Release()
		elementBuilder.Release()
		df := builder.Build()
		defer df.Release()

		_, err := op.Execute(ctx, df)
		s.Error(err)
		s.Contains(err.Error(), "$element_indices is not Int32")
	})

	s.Run("null value", func() {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		idBuilder := array.NewInt64Builder(s.pool)
		idBuilder.Append(1)
		elementBuilder := array.NewInt32Builder(s.pool)
		elementBuilder.AppendNull()
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks(types.ElementIndicesFieldName, []arrow.Array{elementBuilder.NewArray()}))
		idBuilder.Release()
		elementBuilder.Release()
		df := builder.Build()
		defer df.Release()

		_, err := op.Execute(ctx, df)
		s.Error(err)
		s.Contains(err.Error(), "$element_indices has null")
	})
}

func (s *MergeHelperTestSuite) TestMergeRejectsNullScore() {
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
	idBuilder := array.NewInt64Builder(s.pool)
	idBuilder.Append(1)
	scoreBuilder := array.NewFloat32Builder(s.pool)
	scoreBuilder.AppendNull()
	s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
	s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
	idBuilder.Release()
	scoreBuilder.Release()
	df := builder.Build()
	defer df.Release()

	op := NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{1}))
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "$score has null")
}

func (s *MergeHelperTestSuite) TestMergeRejectsMalformedInputLayouts() {
	s.Run("nil context", func() {
		df := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
		defer df.Release()

		_, err := NewMergeOp(MergeStrategyRRF).Execute(nil, df)
		s.ErrorContains(err, "function context is nil")
		s.True(errors.Is(err, merr.ErrServiceInternal))
	})

	s.Run("nil non-first input", func() {
		df := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
		defer df.Release()
		ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

		_, err := NewMergeOp(MergeStrategyRRF).ExecuteMulti(ctx, []*DataFrame{df, nil})
		s.ErrorContains(err, "input[1] is nil")
		s.True(errors.Is(err, merr.ErrFunctionFailed))
	})

	s.Run("id chunk count", func() {
		df := s.createDF([]int64{1}, []float32{0.5}, []int64{1})
		defer df.Release()
		df.chunkSizes = []int64{1, 0}
		ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

		_, err := NewMergeOp(MergeStrategyRRF).Execute(ctx, df)
		s.ErrorContains(err, "column $id has 1 chunks, expected 2")
	})

	s.Run("score chunk count", func() {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1, 0})
		idBuilder1 := array.NewInt64Builder(s.pool)
		idBuilder2 := array.NewInt64Builder(s.pool)
		scoreBuilder := array.NewFloat32Builder(s.pool)
		idBuilder1.Append(1)
		scoreBuilder.Append(0.5)
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder1.NewArray(), idBuilder2.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
		idBuilder1.Release()
		idBuilder2.Release()
		scoreBuilder.Release()
		df := builder.Build()
		defer df.Release()
		ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

		_, err := NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{1})).Execute(ctx, df)
		s.ErrorContains(err, "column $score has 1 chunks, expected 2")
	})

	s.Run("element chunk count", func() {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1, 0})
		idBuilder1 := array.NewInt64Builder(s.pool)
		idBuilder2 := array.NewInt64Builder(s.pool)
		elementBuilder := array.NewInt32Builder(s.pool)
		idBuilder1.Append(1)
		elementBuilder.Append(0)
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder1.NewArray(), idBuilder2.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks(types.ElementIndicesFieldName, []arrow.Array{elementBuilder.NewArray()}))
		idBuilder1.Release()
		idBuilder2.Release()
		elementBuilder.Release()
		df := builder.Build()
		defer df.Release()
		ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

		_, err := NewMergeOp(MergeStrategyRRF).Execute(ctx, df)
		s.ErrorContains(err, "column $element_indices has 1 chunks, expected 2")
	})

	s.Run("unsupported id type", func() {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		idBuilder := array.NewFloat32Builder(s.pool)
		idBuilder.Append(1)
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
		idBuilder.Release()
		df := builder.Build()
		defer df.Release()
		ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

		_, err := NewMergeOp(MergeStrategyRRF).Execute(ctx, df)
		s.ErrorContains(err, "column $id has unsupported type")
	})

	s.Run("score row count", func() {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		idBuilder := array.NewInt64Builder(s.pool)
		scoreBuilder := array.NewFloat32Builder(s.pool)
		idBuilder.Append(1)
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
		idBuilder.Release()
		scoreBuilder.Release()
		df := builder.Build()
		defer df.Release()
		ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

		_, err := NewMergeOp(MergeStrategyWeighted, WithWeights([]float64{1})).Execute(ctx, df)
		s.ErrorContains(err, "column $score has 0 rows, expected 1")
	})

	s.Run("element row count", func() {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		idBuilder := array.NewInt64Builder(s.pool)
		elementBuilder := array.NewInt32Builder(s.pool)
		idBuilder.Append(1)
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks(types.ElementIndicesFieldName, []arrow.Array{elementBuilder.NewArray()}))
		idBuilder.Release()
		elementBuilder.Release()
		df := builder.Build()
		defer df.Release()
		ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

		_, err := NewMergeOp(MergeStrategyRRF).Execute(ctx, df)
		s.ErrorContains(err, "column $element_indices has 0 rows, expected 1")
	})
}

func (s *MergeHelperTestSuite) TestMergeRejectsMalformedPassThroughFields() {
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")

	s.Run("field type mismatch", func() {
		df1 := s.createDFWithField(
			[]int64{1}, []float32{0.9}, "payload", []string{"first"}, []int64{1})
		defer df1.Release()

		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		idBuilder := array.NewInt64Builder(s.pool)
		scoreBuilder := array.NewFloat32Builder(s.pool)
		fieldBuilder := array.NewInt64Builder(s.pool)
		idBuilder.Append(2)
		scoreBuilder.Append(0.8)
		fieldBuilder.Append(200)
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks("payload", []arrow.Array{fieldBuilder.NewArray()}))
		idBuilder.Release()
		scoreBuilder.Release()
		fieldBuilder.Release()
		df2 := builder.Build()
		defer df2.Release()

		_, err := NewMergeOp(MergeStrategyRRF).ExecuteMulti(ctx, []*DataFrame{df1, df2})
		s.ErrorContains(err, "column payload type int64 does not match output type")
		s.True(errors.Is(err, merr.ErrFunctionFailed))
	})

	s.Run("field missing chunk", func() {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1, 1})
		idBuilder1 := array.NewInt64Builder(s.pool)
		idBuilder2 := array.NewInt64Builder(s.pool)
		scoreBuilder1 := array.NewFloat32Builder(s.pool)
		scoreBuilder2 := array.NewFloat32Builder(s.pool)
		fieldBuilder := array.NewStringBuilder(s.pool)
		idBuilder1.Append(1)
		idBuilder2.Append(2)
		scoreBuilder1.Append(0.9)
		scoreBuilder2.Append(0.8)
		fieldBuilder.Append("first")
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder1.NewArray(), idBuilder2.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreBuilder1.NewArray(), scoreBuilder2.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks("payload", []arrow.Array{fieldBuilder.NewArray()}))
		idBuilder1.Release()
		idBuilder2.Release()
		scoreBuilder1.Release()
		scoreBuilder2.Release()
		fieldBuilder.Release()
		df := builder.Build()
		defer df.Release()

		_, err := NewMergeOp(MergeStrategyRRF).Execute(ctx, df)
		s.ErrorContains(err, "column payload missing chunk 1")
		s.True(errors.Is(err, merr.ErrFunctionFailed))
	})

	s.Run("field row out of bounds", func() {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		idBuilder := array.NewInt64Builder(s.pool)
		scoreBuilder := array.NewFloat32Builder(s.pool)
		fieldBuilder := array.NewStringBuilder(s.pool)
		idBuilder.Append(1)
		scoreBuilder.Append(0.9)
		s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
		s.Require().NoError(builder.AddColumnFromChunks("payload", []arrow.Array{fieldBuilder.NewArray()}))
		idBuilder.Release()
		scoreBuilder.Release()
		fieldBuilder.Release()
		df := builder.Build()
		defer df.Release()

		_, err := NewMergeOp(MergeStrategyRRF).Execute(ctx, df)
		s.ErrorContains(err, "column payload has no row 0")
		s.True(errors.Is(err, merr.ErrFunctionFailed))
	})
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

func (s *MergeHelperTestSuite) TestMergeStrategyRRFMultiChunk() {
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
	s.Contains(err.Error(), "missing $id column")
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

func (s *MergeHelperTestSuite) TestMergeStrategyRRFMissingIDColumn() {
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

// TestMemoryLeak_MergePartialChunkError verifies that when mergeWithScoreCollector
// fails mid-way (after some chunks have been allocated), the deferred cleanup releases
// all intermediate Arrow arrays. The CheckedAllocator in TearDownTest asserts zero
// outstanding bytes, so any leak will fail the test.
func (s *MergeHelperTestSuite) TestMemoryLeak_MergePartialChunkError() {
	// Create 2 valid 2-chunk inputs so chunk 0 succeeds and allocates id/score arrays.
	df1 := s.createDF(
		[]int64{1, 2, 3, 4},
		[]float32{0.9, 0.8, 0.7, 0.6},
		[]int64{2, 2},
	)
	defer df1.Release()

	df2 := s.createDF(
		[]int64{2, 3, 5, 6},
		[]float32{0.85, 0.75, 0.65, 0.55},
		[]int64{2, 2},
	)
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	layout, err := op.validateInputs(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)

	// Inject a collectFn that succeeds on chunk 0 (allocating id/score arrays)
	// but fails on chunk 1, exercising the deferred cleanup of partial allocations.
	callCount := 0
	failOnSecondChunk := func(inputs []*DataFrame, chunkIdx int, layout *mergeInputLayout) (map[candidateKey]float32, map[candidateKey]idLocation, error) {
		callCount++
		if callCount > 1 {
			return nil, nil, fmt.Errorf("injected error on chunk %d", chunkIdx)
		}
		return op.collectRRFScores(inputs, chunkIdx, layout)
	}

	// Run 10 iterations to amplify any leak into a visible CheckedAllocator failure.
	for i := 0; i < 10; i++ {
		callCount = 0
		_, err := op.mergeWithScoreCollector(ctx, []*DataFrame{df1, df2}, layout, failOnSecondChunk)
		s.Error(err)
		s.Contains(err.Error(), "injected error")
	}
	// TearDownTest asserts s.pool.AssertSize(s.T(), 0) — any leaked arrays will fail.
}

// TestMemoryLeak_MergeFieldCollectorError verifies cleanup when an error occurs
// during the field-collector assembly phase (after all chunks have been built
// but during AddColumnFromChunks for field columns).
func (s *MergeHelperTestSuite) TestMemoryLeak_MergeFieldCollectorPartialSuccess() {
	// Create 2 inputs with field columns — chunk 0 succeeds fully, building
	// id/score arrays and field collector data for both chunks.
	df1 := s.createDFWithField(
		[]int64{1, 2, 3, 4},
		[]float32{0.9, 0.8, 0.7, 0.6},
		"text",
		[]string{"a", "b", "c", "d"},
		[]int64{2, 2},
	)
	defer df1.Release()

	df2 := s.createDFWithField(
		[]int64{2, 3, 5, 6},
		[]float32{0.85, 0.75, 0.65, 0.55},
		"text",
		[]string{"e", "f", "g", "h"},
		[]int64{2, 2},
	)
	defer df2.Release()

	op := NewMergeOp(MergeStrategyRRF)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	layout, err := op.validateInputs(ctx, []*DataFrame{df1, df2})
	s.Require().NoError(err)

	// First verify the success path works (sanity check).
	result, err := op.mergeWithScoreCollector(ctx, []*DataFrame{df1, df2}, layout, op.collectRRFScores)
	s.Require().NoError(err)
	result.Release()

	// Now inject an error after building all chunks but during field assembly.
	// We do this by making collectFn succeed for all chunks, then replacing
	// the collected field data with a broken collector that causes an error
	// in buildFieldArray. Since we can't easily break buildFieldArray,
	// we instead verify the success path doesn't leak via CheckedAllocator.
	for i := 0; i < 10; i++ {
		res, err := op.mergeWithScoreCollector(ctx, []*DataFrame{df1, df2}, layout, op.collectRRFScores)
		s.Require().NoError(err)
		res.Release()
	}
	// TearDownTest asserts zero outstanding allocations.
}

func mergeRepr(strategy string, params map[string]*schemapb.FunctionParamValue) *OperatorRepr {
	if params == nil {
		params = make(map[string]*schemapb.FunctionParamValue)
	}
	params[MergeParamStrategy] = stringParam(strategy)
	return &OperatorRepr{Type: types.OpTypeMerge, Params: params}
}

func TestNewMergeOpFromReprWithContext(t *testing.T) {
	tests := []struct {
		name           string
		repr           *OperatorRepr
		metrics        []string
		strategy       MergeStrategy
		sortDescending bool
		verify         func(*testing.T, *MergeOp)
	}{
		{
			name:           "rrf defaults",
			repr:           mergeRepr("rrf", nil),
			metrics:        []string{"L2", "L2"},
			strategy:       MergeStrategyRRF,
			sortDescending: true,
			verify: func(t *testing.T, op *MergeOp) {
				assert.Equal(t, float64(60), op.rrfK)
				assert.False(t, op.weightsSet)
				assert.Empty(t, op.weights)
				assert.Empty(t, op.scoreNormFuncs)
			},
		},
		{
			name: "rrf weighted typed params",
			repr: mergeRepr("rrf", map[string]*schemapb.FunctionParamValue{
				MergeParamK:       intParam(30),
				MergeParamWeights: arrayParam(doubleParam(0.8), doubleParam(0.2)),
			}),
			metrics:        []string{"COSINE", "IP"},
			strategy:       MergeStrategyRRF,
			sortDescending: true,
			verify: func(t *testing.T, op *MergeOp) {
				assert.Equal(t, float64(30), op.rrfK)
				assert.True(t, op.weightsSet)
				assert.Equal(t, []float64{0.8, 0.2}, op.weights)
			},
		},
		{
			name: "weighted typed params",
			repr: mergeRepr("weighted", map[string]*schemapb.FunctionParamValue{
				MergeParamWeights:   arrayParam(doubleParam(0.7), intParam(0)),
				MergeParamNormScore: boolParam(true),
			}),
			metrics:        []string{"COSINE", "L2"},
			strategy:       MergeStrategyWeighted,
			sortDescending: true,
			verify: func(t *testing.T, op *MergeOp) {
				assert.Equal(t, []float64{0.7, 0}, op.weights)
				assert.Len(t, op.scoreNormFuncs, 2)
			},
		},
		{
			name:           "max preserves L2 order",
			repr:           mergeRepr("max", nil),
			metrics:        []string{"L2", "L2"},
			strategy:       MergeStrategyMax,
			sortDescending: false,
		},
		{
			name:           "sum",
			repr:           mergeRepr("sum", nil),
			metrics:        []string{"COSINE"},
			strategy:       MergeStrategySum,
			sortDescending: true,
		},
		{
			name: "avg normalized",
			repr: mergeRepr("avg", map[string]*schemapb.FunctionParamValue{
				MergeParamNormScore: boolParam(true),
			}),
			metrics:        []string{"L2"},
			strategy:       MergeStrategyAvg,
			sortDescending: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics := append([]string(nil), test.metrics...)
			op, err := NewMergeOpFromReprWithContext(test.repr, types.FunctionBuildContext{
				Search: &types.SearchRuntimeInfo{MetricTypes: metrics},
			})
			require.NoError(t, err)
			merge, ok := op.(*MergeOp)
			require.True(t, ok)
			assert.Equal(t, test.strategy, merge.strategy)
			assert.Equal(t, len(test.metrics), merge.expectedInputs)
			assert.Equal(t, test.sortDescending, merge.SortDescending())
			if test.verify != nil {
				test.verify(t, merge)
			}

			// Construction consumes runtime metrics; later caller mutation cannot
			// alter the compiled behavior.
			metrics[0] = "L2"
			assert.Equal(t, test.sortDescending, merge.SortDescending())
		})
	}
}

func TestMergeReprRejectsInvalidPublicConfiguration(t *testing.T) {
	tests := []struct {
		name string
		repr *OperatorRepr
	}{
		{"nil repr", nil},
		{"wrong type", &OperatorRepr{Type: types.OpTypeSort}},
		{"expression", &OperatorRepr{Type: types.OpTypeMerge, Function: &FunctionRepr{Name: "x"}}},
		{"inputs", &OperatorRepr{Type: types.OpTypeMerge, Inputs: []string{"$id"}}},
		{"outputs", &OperatorRepr{Type: types.OpTypeMerge, Outputs: []string{"$score"}}},
		{"missing strategy", &OperatorRepr{Type: types.OpTypeMerge}},
		{"unsupported strategy", mergeRepr("median", nil)},
		{"rrf empty weights", mergeRepr("rrf", map[string]*schemapb.FunctionParamValue{MergeParamWeights: arrayParam()})},
		{"rrf bad weight", mergeRepr("rrf", map[string]*schemapb.FunctionParamValue{MergeParamWeights: arrayParam(doubleParam(math.NaN()))})},
		{"rrf wrong weights type", mergeRepr("rrf", map[string]*schemapb.FunctionParamValue{MergeParamWeights: stringParam("[1]")})},
		{"rrf weights count mismatch", mergeRepr("rrf", map[string]*schemapb.FunctionParamValue{MergeParamWeights: arrayParam(doubleParam(0.8), doubleParam(0.2))})},
		{"rrf wrong k type", mergeRepr("rrf", map[string]*schemapb.FunctionParamValue{MergeParamK: stringParam("60")})},
		{"rrf non finite k", mergeRepr("rrf", map[string]*schemapb.FunctionParamValue{MergeParamK: doubleParam(math.Inf(1))})},
		{"rrf zero k", mergeRepr("rrf", map[string]*schemapb.FunctionParamValue{MergeParamK: intParam(0)})},
		{"weighted missing weights", mergeRepr("weighted", nil)},
		{"weighted empty weights", mergeRepr("weighted", map[string]*schemapb.FunctionParamValue{MergeParamWeights: arrayParam()})},
		{"weighted bad weight", mergeRepr("weighted", map[string]*schemapb.FunctionParamValue{MergeParamWeights: arrayParam(doubleParam(math.NaN()))})},
		{"weighted incompatible k", mergeRepr("weighted", map[string]*schemapb.FunctionParamValue{MergeParamWeights: arrayParam(doubleParam(1)), MergeParamK: intParam(10)})},
		{"weighted wrong norm type", mergeRepr("weighted", map[string]*schemapb.FunctionParamValue{MergeParamWeights: arrayParam(doubleParam(1)), MergeParamNormScore: stringParam("true")})},
		{"max wrong norm type", mergeRepr("max", map[string]*schemapb.FunctionParamValue{MergeParamNormScore: stringParam("true")})},
		{"sum unknown param", mergeRepr("sum", map[string]*schemapb.FunctionParamValue{"typo": intParam(1)})},
	}

	ctx := types.FunctionBuildContext{Search: &types.SearchRuntimeInfo{MetricTypes: []string{"COSINE"}}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewMergeOpFromReprWithContext(test.repr, ctx)
			require.Error(t, err)
			assert.True(t, errors.Is(err, merr.ErrParameterInvalid), "error=%v", err)
		})
	}
}

func TestMergeReprRuntimeContextErrors(t *testing.T) {
	repr := mergeRepr("weighted", map[string]*schemapb.FunctionParamValue{
		MergeParamWeights: arrayParam(doubleParam(1)),
	})

	_, err := NewMergeOpFromReprWithContext(repr, types.FunctionBuildContext{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	_, err = NewMergeOpFromReprWithContext(repr, types.FunctionBuildContext{Search: &types.SearchRuntimeInfo{}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	_, err = NewMergeOpFromReprWithContext(repr, types.FunctionBuildContext{
		Search: &types.SearchRuntimeInfo{MetricTypes: []string{"COSINE", "L2"}},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
}

func TestMergeFactoryErrorCodeSurvivesOperatorIndexContext(t *testing.T) {
	repr := &ChainRepr{
		Stage: types.StageL2Rerank,
		Operators: []OperatorRepr{*mergeRepr("rrf", map[string]*schemapb.FunctionParamValue{
			MergeParamK: intParam(0),
		})},
	}

	_, err := FuncChainFromReprWithContext(repr, memory.NewGoAllocator(), types.FunctionBuildContext{
		Search: &types.SearchRuntimeInfo{MetricTypes: []string{"COSINE"}},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "operator[0]")
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
}

func TestMergeRefreshInfoDoesNotDeclareSystemColumns(t *testing.T) {
	repr := &ChainRepr{Operators: []OperatorRepr{
		*mergeRepr("rrf", nil),
		{Type: types.OpTypeMap, Inputs: []string{types.ScoreFieldName, "quality"}, Outputs: []string{"rerank_score"}},
		{Type: types.OpTypeSort, Inputs: []string{"rerank_score", types.IDFieldName}},
	}}

	require.NoError(t, repr.RefreshInfo())
	assert.Equal(t, []string{types.ScoreFieldName, "quality", types.IDFieldName}, repr.Info.RequiredInputs)
	assert.Equal(t, []string{"rerank_score"}, repr.Info.WrittenNames)
	require.Len(t, repr.Info.Ops, 3)
	assert.Empty(t, repr.Info.Ops[0].ReadNames)
	assert.Empty(t, repr.Info.Ops[0].WriteNames)
	assert.Empty(t, repr.Operators[0].Outputs)
}

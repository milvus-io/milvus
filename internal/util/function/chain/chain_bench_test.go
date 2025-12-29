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
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// =============================================================================
// Helper Functions for Benchmark Data Generation
// =============================================================================

// generateSearchResultData creates test SearchResultData with configurable size.
// nq: number of queries
// topK: number of results per query
// numFields: number of additional fields (besides id and score)
func generateSearchResultData(nq int, topK int, numFields int) *schemapb.SearchResultData {
	totalRows := nq * topK

	// Generate topks
	topks := make([]int64, nq)
	for i := range topks {
		topks[i] = int64(topK)
	}

	// Generate IDs
	ids := make([]int64, totalRows)
	for i := range ids {
		ids[i] = int64(i)
	}

	// Generate scores (descending order per query)
	scores := make([]float32, totalRows)
	for q := 0; q < nq; q++ {
		for k := 0; k < topK; k++ {
			idx := q*topK + k
			scores[idx] = 1.0 - float32(k)/float32(topK)
		}
	}

	// Generate fields data
	fieldsData := make([]*schemapb.FieldData, 0, numFields)

	// Add Int64 field
	if numFields >= 1 {
		int64Data := make([]int64, totalRows)
		for i := range int64Data {
			int64Data[i] = rand.Int63n(1000000)
		}
		fieldsData = append(fieldsData, &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: "int64_field",
			FieldId:   100,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: int64Data},
					},
				},
			},
		})
	}

	// Add Float64 field
	if numFields >= 2 {
		float64Data := make([]float64, totalRows)
		for i := range float64Data {
			float64Data[i] = rand.Float64() * 1000
		}
		fieldsData = append(fieldsData, &schemapb.FieldData{
			Type:      schemapb.DataType_Double,
			FieldName: "float64_field",
			FieldId:   101,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{Data: float64Data},
					},
				},
			},
		})
	}

	// Add VarChar field
	if numFields >= 3 {
		stringData := make([]string, totalRows)
		for i := range stringData {
			stringData[i] = fmt.Sprintf("value_%d", i)
		}
		fieldsData = append(fieldsData, &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: "varchar_field",
			FieldId:   102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: stringData},
					},
				},
			},
		})
	}

	// Add Float32 field
	if numFields >= 4 {
		float32Data := make([]float32, totalRows)
		for i := range float32Data {
			float32Data[i] = rand.Float32() * 100
		}
		fieldsData = append(fieldsData, &schemapb.FieldData{
			Type:      schemapb.DataType_Float,
			FieldName: "float32_field",
			FieldId:   103,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{Data: float32Data},
					},
				},
			},
		})
	}

	// Add Int32 field
	if numFields >= 5 {
		int32Data := make([]int32, totalRows)
		for i := range int32Data {
			int32Data[i] = rand.Int31n(100000)
		}
		fieldsData = append(fieldsData, &schemapb.FieldData{
			Type:      schemapb.DataType_Int32,
			FieldName: "int32_field",
			FieldId:   104,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: int32Data},
					},
				},
			},
		})
	}

	return &schemapb.SearchResultData{
		NumQueries: int64(nq),
		TopK:       int64(topK),
		Topks:      topks,
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: ids},
			},
		},
		FieldsData: fieldsData,
	}
}

// =============================================================================
// Benchmark Filter Function
// =============================================================================

// BenchFilterFunction creates a boolean column for filtering based on score threshold.
type BenchFilterFunction struct {
	threshold float32
}

func (f *BenchFilterFunction) Name() string { return "BenchFilter" }

func (f *BenchFilterFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.FixedWidthTypes.Boolean}
}

func (f *BenchFilterFunction) IsRunnable(stage string) bool { return true }

func (f *BenchFilterFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
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
	for _, chunk := range chunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}

// =============================================================================
// Benchmark Map Function (Score Transformation)
// =============================================================================

// BenchScoreTransformFunction transforms scores by applying a mathematical operation.
type BenchScoreTransformFunction struct {
	multiplier float32
}

func (f *BenchScoreTransformFunction) Name() string { return "BenchScoreTransform" }

func (f *BenchScoreTransformFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

func (f *BenchScoreTransformFunction) IsRunnable(stage string) bool { return true }

func (f *BenchScoreTransformFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	col := inputs[0]

	chunks := make([]arrow.Array, len(col.Chunks()))
	for i, chunk := range col.Chunks() {
		floatChunk := chunk.(*array.Float32)
		builder := array.NewFloat32Builder(ctx.Pool())

		for j := range floatChunk.Len() {
			if floatChunk.IsNull(j) {
				builder.AppendNull()
			} else {
				builder.Append(floatChunk.Value(j) * f.multiplier)
			}
		}

		chunks[i] = builder.NewArray()
		builder.Release()
	}

	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}

// =============================================================================
// DataFrame Construction Benchmarks
// =============================================================================

func BenchmarkDataFrame_FromSearchResultData(b *testing.B) {
	benchCases := []struct {
		name      string
		nq        int
		topK      int
		numFields int
	}{
		{"Small_10x100", 10, 100, 3},
		{"Medium_100x1000", 100, 1000, 3},
		{"Large_1000x1000", 1000, 1000, 3},
		{"XLarge_1000x10000", 1000, 10000, 3},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			// Generate test data
			resultData := generateSearchResultData(bc.nq, bc.topK, bc.numFields)
			pool := memory.NewGoAllocator()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				df, err := FromSearchResultData(resultData, pool)
				if err != nil {
					b.Fatal(err)
				}
				df.Release()
			}
		})
	}
}

func BenchmarkDataFrame_ToSearchResultData(b *testing.B) {
	benchCases := []struct {
		name      string
		nq        int
		topK      int
		numFields int
	}{
		{"Small_10x100", 10, 100, 3},
		{"Medium_100x1000", 100, 1000, 3},
		{"Large_1000x1000", 1000, 1000, 3},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			// Generate and convert to DataFrame
			resultData := generateSearchResultData(bc.nq, bc.topK, bc.numFields)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := ToSearchResultData(df)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// =============================================================================
// Individual Operator Benchmarks
// =============================================================================

func BenchmarkSelectOp(b *testing.B) {
	benchCases := []struct {
		name       string
		nq         int
		topK       int
		numFields  int
		selectCols int
	}{
		{"Small_Select2", 100, 1000, 5, 2},
		{"Medium_Select3", 100, 1000, 5, 3},
		{"Large_Select5", 1000, 1000, 5, 5},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			resultData := generateSearchResultData(bc.nq, bc.topK, bc.numFields)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			// Build column list to select
			selectCols := []string{types.IDFieldName, types.ScoreFieldName}
			if bc.selectCols >= 3 {
				selectCols = append(selectCols, "int64_field")
			}
			if bc.selectCols >= 4 {
				selectCols = append(selectCols, "float64_field")
			}
			if bc.selectCols >= 5 {
				selectCols = append(selectCols, "varchar_field")
			}

			chain := NewFuncChainWithAllocator(nil).Select(selectCols...)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				if result != df {
					result.Release()
				}
			}
		})
	}
}

func BenchmarkFilterOp(b *testing.B) {
	benchCases := []struct {
		name      string
		nq        int
		topK      int
		threshold float32 // Percentage of rows to keep
	}{
		{"Small_Keep50%", 100, 1000, 0.5},
		{"Medium_Keep25%", 100, 1000, 0.75},
		{"Large_Keep10%", 1000, 1000, 0.9},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			resultData := generateSearchResultData(bc.nq, bc.topK, 3)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			chain := NewFuncChainWithAllocator(nil).
				SetStage(types.StageL2Rerank).
				Filter(&BenchFilterFunction{threshold: bc.threshold}, []string{types.ScoreFieldName})

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func BenchmarkSortOp(b *testing.B) {
	benchCases := []struct {
		name string
		nq   int
		topK int
		desc bool
	}{
		{"Small_Asc", 10, 100, false},
		{"Small_Desc", 10, 100, true},
		{"Medium_Asc", 100, 1000, false},
		{"Medium_Desc", 100, 1000, true},
		{"Large_Asc", 100, 10000, false},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			resultData := generateSearchResultData(bc.nq, bc.topK, 3)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			chain := NewFuncChainWithAllocator(nil).Sort("int64_field", bc.desc)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func BenchmarkLimitOp(b *testing.B) {
	benchCases := []struct {
		name   string
		nq     int
		topK   int
		limit  int64
		offset int64
	}{
		{"Small_Limit10", 100, 1000, 10, 0},
		{"Small_Limit100", 100, 1000, 100, 0},
		{"Medium_Limit10_Offset5", 100, 1000, 10, 5},
		{"Large_Limit100", 1000, 1000, 100, 0},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			resultData := generateSearchResultData(bc.nq, bc.topK, 3)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			var chain *FuncChain
			if bc.offset > 0 {
				chain = NewFuncChainWithAllocator(nil).LimitWithOffset(bc.limit, bc.offset)
			} else {
				chain = NewFuncChainWithAllocator(nil).Limit(bc.limit)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func BenchmarkMapOp(b *testing.B) {
	benchCases := []struct {
		name string
		nq   int
		topK int
	}{
		{"Small", 10, 100},
		{"Medium", 100, 1000},
		{"Large", 1000, 1000},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			resultData := generateSearchResultData(bc.nq, bc.topK, 3)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			chain := NewFuncChainWithAllocator(nil).Map(&BenchScoreTransformFunction{multiplier: 2.0}, []string{types.ScoreFieldName}, []string{"transformed_score"})

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

// =============================================================================
// Chained Operations Benchmarks
// =============================================================================

func BenchmarkChain_FilterSortLimit(b *testing.B) {
	benchCases := []struct {
		name      string
		nq        int
		topK      int
		threshold float32
		limit     int64
	}{
		{"Small", 10, 100, 0.5, 10},
		{"Medium", 100, 1000, 0.5, 100},
		{"Large", 1000, 1000, 0.5, 100},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			resultData := generateSearchResultData(bc.nq, bc.topK, 3)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			chain := NewFuncChainWithAllocator(nil).
				SetStage(types.StageL2Rerank).
				Filter(&BenchFilterFunction{threshold: bc.threshold}, []string{types.ScoreFieldName}).
				Sort(types.ScoreFieldName, true).
				Limit(bc.limit)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func BenchmarkChain_MapFilterSelect(b *testing.B) {
	benchCases := []struct {
		name string
		nq   int
		topK int
	}{
		{"Small", 10, 100},
		{"Medium", 100, 1000},
		{"Large", 1000, 1000},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			resultData := generateSearchResultData(bc.nq, bc.topK, 5)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			chain := NewFuncChainWithAllocator(nil).
				SetStage(types.StageL2Rerank).
				Map(&BenchScoreTransformFunction{multiplier: 1.5}, []string{types.ScoreFieldName}, []string{"transformed_score"}).
				Filter(&BenchFilterFunction{threshold: 0.5}, []string{types.ScoreFieldName}).
				Select(types.IDFieldName, types.ScoreFieldName, "transformed_score", "int64_field")

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func BenchmarkChain_FullPipeline(b *testing.B) {
	benchCases := []struct {
		name string
		nq   int
		topK int
	}{
		{"Small", 10, 100},
		{"Medium", 100, 1000},
		{"Large", 500, 1000},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			resultData := generateSearchResultData(bc.nq, bc.topK, 5)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			// Full pipeline: Map -> Filter -> Sort -> Limit -> Select
			chain := NewFuncChainWithAllocator(nil).
				SetStage(types.StageL2Rerank).
				Map(&BenchScoreTransformFunction{multiplier: 2.0}, []string{types.ScoreFieldName}, []string{"transformed_score"}).
				Filter(&BenchFilterFunction{threshold: 0.3}, []string{types.ScoreFieldName}).
				Sort("transformed_score", true).
				Limit(50).
				Select(types.IDFieldName, "transformed_score", "int64_field", "float64_field")

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

// =============================================================================
// Scale Testing Benchmarks
// =============================================================================

func BenchmarkScale_VaryingNQ(b *testing.B) {
	nqValues := []int{1, 10, 100, 1000}
	topK := 100
	pool := memory.NewGoAllocator()

	for _, nq := range nqValues {
		b.Run(fmt.Sprintf("NQ_%d", nq), func(b *testing.B) {
			resultData := generateSearchResultData(nq, topK, 3)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			chain := NewFuncChainWithAllocator(nil).
				Sort(types.ScoreFieldName, true).
				Limit(10)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func BenchmarkScale_VaryingTopK(b *testing.B) {
	nq := 100
	topKValues := []int{10, 100, 1000, 10000}
	pool := memory.NewGoAllocator()

	for _, topK := range topKValues {
		b.Run(fmt.Sprintf("TopK_%d", topK), func(b *testing.B) {
			resultData := generateSearchResultData(nq, topK, 3)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			chain := NewFuncChainWithAllocator(nil).
				Sort(types.ScoreFieldName, true).
				Limit(10)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func BenchmarkScale_VaryingColumns(b *testing.B) {
	nq := 100
	topK := 1000
	columnCounts := []int{1, 3, 5}
	pool := memory.NewGoAllocator()

	for _, numCols := range columnCounts {
		b.Run(fmt.Sprintf("Cols_%d", numCols), func(b *testing.B) {
			resultData := generateSearchResultData(nq, topK, numCols)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			chain := NewFuncChainWithAllocator(nil).
				SetStage(types.StageL2Rerank).
				Filter(&BenchFilterFunction{threshold: 0.5}, []string{types.ScoreFieldName}).
				Sort(types.ScoreFieldName, true).
				Limit(100)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

// =============================================================================
// Memory Allocator Benchmarks
// =============================================================================

func BenchmarkWithCheckedAllocator(b *testing.B) {
	benchCases := []struct {
		name string
		nq   int
		topK int
	}{
		{"Small", 10, 100},
		{"Medium", 100, 1000},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())

			resultData := generateSearchResultData(bc.nq, bc.topK, 3)
			df, err := FromSearchResultData(resultData, pool)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Release()

			chain := NewFuncChainWithAllocator(pool).
				SetStage(types.StageL2Rerank).
				Filter(&BenchFilterFunction{threshold: 0.5}, []string{types.ScoreFieldName}).
				Sort(types.ScoreFieldName, true).
				Limit(10)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}

			b.StopTimer()
			// Verify no memory leaks
			pool.AssertSize(b, 0)
		})
	}
}

// =============================================================================
// End-to-End Pipeline Benchmarks (Import -> Process -> Export)
// =============================================================================

func BenchmarkEndToEnd_Pipeline(b *testing.B) {
	benchCases := []struct {
		name string
		nq   int
		topK int
	}{
		{"Small", 10, 100},
		{"Medium", 100, 1000},
		{"Large", 500, 1000},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			resultData := generateSearchResultData(bc.nq, bc.topK, 3)

			chain := NewFuncChainWithAllocator(nil).
				SetStage(types.StageL2Rerank).
				Filter(&BenchFilterFunction{threshold: 0.5}, []string{types.ScoreFieldName}).
				Sort(types.ScoreFieldName, true).
				Limit(50).
				Select(types.IDFieldName, types.ScoreFieldName, "int64_field")

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Import
				df, err := FromSearchResultData(resultData, pool)
				if err != nil {
					b.Fatal(err)
				}

				// Process
				result, err := chain.Execute(df)
				if err != nil {
					b.Fatal(err)
				}

				// Export
				_, err = ToSearchResultData(result)
				if err != nil {
					b.Fatal(err)
				}

				// Cleanup
				result.Release()
				df.Release()
			}
		})
	}
}

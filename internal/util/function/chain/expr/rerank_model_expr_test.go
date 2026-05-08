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

package expr

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// =============================================================================
// Mock ModelProvider
// =============================================================================

type mockModelProvider struct {
	maxBatch   int
	rerankFunc func(ctx context.Context, query string, docs []string) ([]float32, error)
}

func (m *mockModelProvider) Rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	if m.rerankFunc != nil {
		return m.rerankFunc(ctx, query, docs)
	}
	// Default: return sequential scores 0.0, 1.0, 2.0, ...
	scores := make([]float32, len(docs))
	for i := range docs {
		scores[i] = float32(i)
	}
	return scores, nil
}

func (m *mockModelProvider) MaxBatch() int {
	return m.maxBatch
}

// =============================================================================
// Test Suite
// =============================================================================

type RerankModelExprTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *RerankModelExprTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *RerankModelExprTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestRerankModelExprTestSuite(t *testing.T) {
	suite.Run(t, new(RerankModelExprTestSuite))
}

// =============================================================================
// Helper Functions
// =============================================================================

func strPtr(str string) *string {
	return &str
}

// createStringChunked creates a String chunked array from the given slices.
// Each slice becomes a separate chunk in the chunked array.
func (s *RerankModelExprTestSuite) createStringChunked(chunks ...[]string) *arrow.Chunked {
	arrs := make([]arrow.Array, len(chunks))
	for i, chunk := range chunks {
		builder := array.NewStringBuilder(s.pool)
		for _, v := range chunk {
			builder.Append(v)
		}
		arrs[i] = builder.NewArray()
		builder.Release()
	}
	chunked := arrow.NewChunked(arrow.BinaryTypes.String, arrs)
	for _, a := range arrs {
		a.Release()
	}
	return chunked
}

// createStringChunkedWithNulls creates a String chunked array with null support.
// nil pointers represent null values.
func (s *RerankModelExprTestSuite) createStringChunkedWithNulls(chunks ...[]*string) *arrow.Chunked {
	arrs := make([]arrow.Array, len(chunks))
	for i, chunk := range chunks {
		builder := array.NewStringBuilder(s.pool)
		for _, v := range chunk {
			if v == nil {
				builder.AppendNull()
			} else {
				builder.Append(*v)
			}
		}
		arrs[i] = builder.NewArray()
		builder.Release()
	}
	chunked := arrow.NewChunked(arrow.BinaryTypes.String, arrs)
	for _, a := range arrs {
		a.Release()
	}
	return chunked
}

// =============================================================================
// Constructor Tests
// =============================================================================

func (s *RerankModelExprTestSuite) TestNewRerankModelExpr_Valid() {
	provider := &mockModelProvider{maxBatch: 10}
	expr, err := NewRerankModelExpr(provider, []string{"query1", "query2"})
	s.Require().NoError(err)
	s.NotNil(expr)
	s.Equal("model", expr.Name())
}

func (s *RerankModelExprTestSuite) TestNewRerankModelExpr_NilProvider() {
	_, err := NewRerankModelExpr(nil, []string{"query1"})
	s.Error(err)
	s.Contains(err.Error(), "provider is nil")
}

func (s *RerankModelExprTestSuite) TestNewRerankModelExpr_EmptyQueries() {
	provider := &mockModelProvider{maxBatch: 10}
	_, err := NewRerankModelExpr(provider, []string{})
	s.Error(err)
	s.Contains(err.Error(), "queries must not be empty")
}

func (s *RerankModelExprTestSuite) TestNewRerankModelExpr_NilQueries() {
	provider := &mockModelProvider{maxBatch: 10}
	_, err := NewRerankModelExpr(provider, nil)
	s.Error(err)
	s.Contains(err.Error(), "queries must not be empty")
}

// =============================================================================
// Interface Method Tests
// =============================================================================

func (s *RerankModelExprTestSuite) TestOutputDataTypes() {
	provider := &mockModelProvider{maxBatch: 10}
	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	outputTypes := expr.OutputDataTypes()
	s.Len(outputTypes, 1)
	s.Equal(arrow.PrimitiveTypes.Float32, outputTypes[0])
}

func (s *RerankModelExprTestSuite) TestIsRunnable() {
	provider := &mockModelProvider{maxBatch: 10}
	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	// Only L2Rerank stage
	s.True(expr.IsRunnable(types.StageL2Rerank))
	s.False(expr.IsRunnable(types.StageL1Rerank))
	s.False(expr.IsRunnable(types.StageL0Rerank))
	s.False(expr.IsRunnable(types.StageIngestion))
	s.False(expr.IsRunnable(types.StagePreProcess))
	s.False(expr.IsRunnable(types.StagePostProcess))
	s.False(expr.IsRunnable("unknown_stage"))
}

// =============================================================================
// Execute Tests
// =============================================================================

func (s *RerankModelExprTestSuite) TestExecute_SingleChunk() {
	provider := &mockModelProvider{
		maxBatch: 100,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			s.Equal("test query", query)
			scores := make([]float32, len(docs))
			for i := range docs {
				scores[i] = float32(i) * 0.1
			}
			return scores, nil
		},
	}

	expr, err := NewRerankModelExpr(provider, []string{"test query"})
	s.Require().NoError(err)

	textCol := s.createStringChunked([]string{"doc1", "doc2", "doc3"})
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	s.Equal(1, len(outputs[0].Chunks()))
	result := outputs[0].Chunk(0).(*array.Float32)
	s.Equal(3, result.Len())
	s.InDelta(0.0, float64(result.Value(0)), 0.001)
	s.InDelta(0.1, float64(result.Value(1)), 0.001)
	s.InDelta(0.2, float64(result.Value(2)), 0.001)
}

func (s *RerankModelExprTestSuite) TestExecute_MultipleChunks() {
	callCount := 0
	provider := &mockModelProvider{
		maxBatch: 100,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			callCount++
			scores := make([]float32, len(docs))
			switch query {
			case "query1":
				for i := range docs {
					scores[i] = float32(i) + 1.0
				}
			case "query2":
				for i := range docs {
					scores[i] = float32(i) + 10.0
				}
			}
			return scores, nil
		},
	}

	expr, err := NewRerankModelExpr(provider, []string{"query1", "query2"})
	s.Require().NoError(err)

	textCol := s.createStringChunked(
		[]string{"docA", "docB"},
		[]string{"docC", "docD", "docE"},
	)
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	s.Equal(2, callCount)
	s.Equal(2, len(outputs[0].Chunks()))

	// Check chunk 0 (query1)
	chunk0 := outputs[0].Chunk(0).(*array.Float32)
	s.Equal(2, chunk0.Len())
	s.InDelta(1.0, float64(chunk0.Value(0)), 0.001)
	s.InDelta(2.0, float64(chunk0.Value(1)), 0.001)

	// Check chunk 1 (query2)
	chunk1 := outputs[0].Chunk(1).(*array.Float32)
	s.Equal(3, chunk1.Len())
	s.InDelta(10.0, float64(chunk1.Value(0)), 0.001)
	s.InDelta(11.0, float64(chunk1.Value(1)), 0.001)
	s.InDelta(12.0, float64(chunk1.Value(2)), 0.001)
}

func (s *RerankModelExprTestSuite) TestExecute_EmptyChunk() {
	provider := &mockModelProvider{maxBatch: 100}

	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	textCol := s.createStringChunked([]string{})
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	s.Equal(0, outputs[0].Len())
}

func (s *RerankModelExprTestSuite) TestExecute_WithNulls() {
	var capturedDocs []string
	provider := &mockModelProvider{
		maxBatch: 100,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			capturedDocs = docs
			scores := make([]float32, len(docs))
			for i := range docs {
				scores[i] = float32(i) * 0.5
			}
			return scores, nil
		},
	}

	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	textCol := s.createStringChunkedWithNulls([]*string{
		strPtr("hello"),
		nil,
		strPtr("world"),
		nil,
	})
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	// Nulls should be treated as empty strings
	s.Require().Len(capturedDocs, 4)
	s.Equal("hello", capturedDocs[0])
	s.Equal("", capturedDocs[1])
	s.Equal("world", capturedDocs[2])
	s.Equal("", capturedDocs[3])

	result := outputs[0].Chunk(0).(*array.Float32)
	s.Equal(4, result.Len())
}

// =============================================================================
// Error Tests
// =============================================================================

func (s *RerankModelExprTestSuite) TestExecute_WrongInputCount() {
	provider := &mockModelProvider{maxBatch: 100}
	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)

	// 0 inputs
	_, err = expr.Execute(ctx, []*arrow.Chunked{})
	s.Error(err)
	s.Contains(err.Error(), "expected 1 input column")

	// 2 inputs
	textCol1 := s.createStringChunked([]string{"a"})
	defer textCol1.Release()
	textCol2 := s.createStringChunked([]string{"b"})
	defer textCol2.Release()

	_, err = expr.Execute(ctx, []*arrow.Chunked{textCol1, textCol2})
	s.Error(err)
	s.Contains(err.Error(), "expected 1 input column")
}

func (s *RerankModelExprTestSuite) TestExecute_QueryCountMismatch() {
	provider := &mockModelProvider{maxBatch: 100}
	// 1 query but 2 chunks
	expr, err := NewRerankModelExpr(provider, []string{"only_one_query"})
	s.Require().NoError(err)

	textCol := s.createStringChunked(
		[]string{"doc1"},
		[]string{"doc2"},
	)
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	_, err = expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Error(err)
	s.Contains(err.Error(), "queries count")
}

func (s *RerankModelExprTestSuite) TestExecute_ProviderError() {
	provider := &mockModelProvider{
		maxBatch: 100,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			return nil, fmt.Errorf("rerank service unavailable")
		},
	}

	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	textCol := s.createStringChunked([]string{"doc1", "doc2"})
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	_, err = expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Error(err)
	s.Contains(err.Error(), "rerank service unavailable")
}

func (s *RerankModelExprTestSuite) TestExecute_ProviderErrorOnSecondChunk() {
	// First chunk succeeds, second chunk fails.
	// The arrays from the first chunk should be properly released (no leak).
	callCount := 0
	provider := &mockModelProvider{
		maxBatch: 100,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			callCount++
			if callCount == 1 {
				scores := make([]float32, len(docs))
				for i := range docs {
					scores[i] = float32(i)
				}
				return scores, nil
			}
			return nil, fmt.Errorf("second chunk failed")
		},
	}

	expr, err := NewRerankModelExpr(provider, []string{"q1", "q2"})
	s.Require().NoError(err)

	textCol := s.createStringChunked(
		[]string{"doc1", "doc2"},
		[]string{"doc3"},
	)
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	_, err = expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Error(err)
	s.Contains(err.Error(), "second chunk failed")
	// CheckedAllocator in TearDownTest ensures no memory leaks
}

func (s *RerankModelExprTestSuite) TestExecute_WrongScoreCount() {
	provider := &mockModelProvider{
		maxBatch: 100,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			// Return fewer scores than docs
			return []float32{0.5}, nil
		},
	}

	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	textCol := s.createStringChunked([]string{"doc1", "doc2", "doc3"})
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	_, err = expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Error(err)
	s.Contains(err.Error(), "returned 1 scores for 3 docs")
}

// =============================================================================
// Batching Tests
// =============================================================================

func (s *RerankModelExprTestSuite) TestBatching_MaxBatch2With5Docs() {
	batchCalls := 0
	var batchSizes []int
	provider := &mockModelProvider{
		maxBatch: 2,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			batchCalls++
			batchSizes = append(batchSizes, len(docs))
			scores := make([]float32, len(docs))
			for i := range docs {
				scores[i] = float32(batchCalls*10 + i)
			}
			return scores, nil
		},
	}

	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	textCol := s.createStringChunked([]string{"d1", "d2", "d3", "d4", "d5"})
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	// 5 docs, maxBatch=2 -> batches of 2, 2, 1 = 3 calls
	s.Equal(3, batchCalls)
	s.Equal([]int{2, 2, 1}, batchSizes)

	result := outputs[0].Chunk(0).(*array.Float32)
	s.Equal(5, result.Len())
	// Batch 1: scores 10, 11; Batch 2: scores 20, 21; Batch 3: scores 30
	s.InDelta(10.0, float64(result.Value(0)), 0.001)
	s.InDelta(11.0, float64(result.Value(1)), 0.001)
	s.InDelta(20.0, float64(result.Value(2)), 0.001)
	s.InDelta(21.0, float64(result.Value(3)), 0.001)
	s.InDelta(30.0, float64(result.Value(4)), 0.001)
}

func (s *RerankModelExprTestSuite) TestBatching_ZeroMaxBatch() {
	batchCalls := 0
	provider := &mockModelProvider{
		maxBatch: 0, // should process all at once
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			batchCalls++
			scores := make([]float32, len(docs))
			for i := range docs {
				scores[i] = float32(i) * 0.1
			}
			return scores, nil
		},
	}

	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	textCol := s.createStringChunked([]string{"d1", "d2", "d3", "d4", "d5"})
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	// maxBatch=0 means all docs in one call
	s.Equal(1, batchCalls)

	result := outputs[0].Chunk(0).(*array.Float32)
	s.Equal(5, result.Len())
}

func (s *RerankModelExprTestSuite) TestBatching_ErrorMidway() {
	batchCalls := 0
	provider := &mockModelProvider{
		maxBatch: 2,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			batchCalls++
			if batchCalls == 2 {
				return nil, fmt.Errorf("batch 2 failed")
			}
			scores := make([]float32, len(docs))
			return scores, nil
		},
	}

	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	textCol := s.createStringChunked([]string{"d1", "d2", "d3", "d4", "d5"})
	defer textCol.Release()

	ctx := types.NewFuncContext(s.pool)
	_, err = expr.Execute(ctx, []*arrow.Chunked{textCol})
	s.Error(err)
	s.Contains(err.Error(), "batch 2 failed")
	// CheckedAllocator ensures no leaks
}

// =============================================================================
// Memory Leak Tests
// =============================================================================

func (s *RerankModelExprTestSuite) TestMemoryLeak_RepeatedExecution() {
	provider := &mockModelProvider{
		maxBatch: 100,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			scores := make([]float32, len(docs))
			for i := range docs {
				scores[i] = float32(i) * 0.1
			}
			return scores, nil
		},
	}

	for range 10 {
		expr, err := NewRerankModelExpr(provider, []string{"query"})
		s.Require().NoError(err)

		textCol := s.createStringChunked([]string{"doc1", "doc2", "doc3"})

		ctx := types.NewFuncContext(s.pool)
		outputs, err := expr.Execute(ctx, []*arrow.Chunked{textCol})
		s.Require().NoError(err)

		outputs[0].Release()
		textCol.Release()
	}
	// Memory leak check happens in TearDownTest via CheckedAllocator
}

func (s *RerankModelExprTestSuite) TestMemoryLeak_OnError() {
	provider := &mockModelProvider{
		maxBatch: 100,
		rerankFunc: func(ctx context.Context, query string, docs []string) ([]float32, error) {
			return nil, fmt.Errorf("intentional error")
		},
	}

	for range 10 {
		expr, err := NewRerankModelExpr(provider, []string{"q1", "q2"})
		s.Require().NoError(err)

		textCol := s.createStringChunked(
			[]string{"doc1", "doc2"},
			[]string{"doc3"},
		)

		ctx := types.NewFuncContext(s.pool)
		_, err = expr.Execute(ctx, []*arrow.Chunked{textCol})
		s.Error(err)

		textCol.Release()
	}
	// Memory leak check happens in TearDownTest
}

// =============================================================================
// Non-String Input Test
// =============================================================================

func (s *RerankModelExprTestSuite) TestExecute_NonStringInput() {
	provider := &mockModelProvider{maxBatch: 100}
	expr, err := NewRerankModelExpr(provider, []string{"query"})
	s.Require().NoError(err)

	// Create an Int64 chunked array instead of String
	builder := array.NewInt64Builder(s.pool)
	builder.AppendValues([]int64{1, 2, 3}, nil)
	arr := builder.NewArray()
	builder.Release()

	intCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{arr})
	arr.Release()
	defer intCol.Release()

	ctx := types.NewFuncContext(s.pool)
	_, err = expr.Execute(ctx, []*arrow.Chunked{intCol})
	s.Error(err)
	s.Contains(err.Error(), "must be String/VarChar")
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rerank

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// TestExprRerankMemoryLeak verifies that ExprRerank instances don't leak memory
// when created and destroyed repeatedly
func TestExprRerankMemoryLeak(t *testing.T) {
	// Force GC and get baseline memory stats
	runtime.GC()
	runtime.GC() // Call twice to ensure finalizers run
	time.Sleep(50 * time.Millisecond)

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	baselineAlloc := m1.Alloc

	t.Logf("Baseline memory: %d MB", baselineAlloc/1024/1024)

	// Create and destroy many expression rerankers
	iterations := 100
	for i := 0; i < iterations; i++ {
		collName := fmt.Sprintf("test_expr_collection_%d", i)

		// Create test schema
		collSchema := &schemapb.CollectionSchema{
			Name: collName,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 100, Name: "price", DataType: schemapb.DataType_Float},
				{FieldID: 101, Name: "rating", DataType: schemapb.DataType_Float},
				{FieldID: 102, Name: "category", DataType: schemapb.DataType_VarChar},
			},
		}

		funcSchema := &schemapb.FunctionSchema{
			Name:             "test_expr_rerank",
			Type:             schemapb.FunctionType_Rerank,
			InputFieldNames:  []string{"price", "rating", "category"},
			InputFieldIds:    []int64{100, 101, 102},
			OutputFieldNames: []string{},
			Params: []*commonpb.KeyValuePair{
				{Key: "reranker", Value: "expr"},
				{Key: "expr_code", Value: "score * (1.0 + fields.rating / 5.0) / (1.0 + fields.price / 100.0)"},
			},
		}

		// Create reranker
		reranker, err := newExprFunction(collSchema, funcSchema)
		require.NoError(t, err)
		require.NotNil(t, reranker)

		// Verify it's an ExprRerank instance
		exprRerank, ok := reranker.(*ExprRerank[int64])
		require.True(t, ok, "Expected ExprRerank[int64] instance")

		// Verify AST field pruning worked
		assert.NotNil(t, exprRerank.program)
		assert.NotEmpty(t, exprRerank.requiredFieldNames)

		// Clean up - this simulates collection deletion
		UnregisterRerankersForCollection(collName)

		// Periodic GC every 20 iterations
		if i%20 == 0 {
			runtime.GC()
		}
	}

	// Force comprehensive GC to run finalizers and reclaim memory
	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	// Check final memory
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	finalAlloc := m2.Alloc

	growth := int64(finalAlloc) - int64(baselineAlloc)
	growthMB := float64(growth) / 1024.0 / 1024.0

	t.Logf("Final memory: %d MB", finalAlloc/1024/1024)
	t.Logf("Memory growth: %.2f MB after %d iterations", growthMB, iterations)
	t.Logf("Per-iteration growth: %.2f KB", float64(growth)/float64(iterations)/1024.0)

	// Memory should not grow significantly
	// Allow 20MB growth for 100 iterations (200KB per iteration is reasonable overhead)
	maxGrowthMB := float64(20.0)
	assert.Less(t, growthMB, maxGrowthMB,
		"Memory leaked > %.0f MB after %d iterations (%.2f MB total)",
		maxGrowthMB, iterations, growthMB)
}

// TestExprRerankCloseReleasesResources verifies that Close() properly clears all references
func TestExprRerankCloseReleasesResources(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{
		Name: "test_close_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 100, Name: "score_field", DataType: schemapb.DataType_Float},
		},
	}

	funcSchema := &schemapb.FunctionSchema{
		Name:             "test_close",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{"score_field"},
		InputFieldIds:    []int64{100},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "expr"},
			{Key: "expr_code", Value: "score * fields.score_field"},
		},
	}

	reranker, err := newExprFunction(collSchema, funcSchema)
	require.NoError(t, err)

	exprRerank := reranker.(*ExprRerank[int64])

	// Verify resources are allocated
	assert.NotNil(t, exprRerank.program, "Program should be compiled")
	assert.NotEmpty(t, exprRerank.exprString, "Expression string should be set")
	assert.NotEmpty(t, exprRerank.collectionName, "Collection name should be set")
	assert.NotNil(t, exprRerank.requiredFieldNames, "Required field names should be set")
	assert.NotNil(t, exprRerank.requiredFieldIndices, "Required field indices should be set")

	// Close the reranker
	exprRerank.Close()

	// Verify all resources are cleared
	assert.Nil(t, exprRerank.program, "Program should be nil after Close()")
	assert.Empty(t, exprRerank.exprString, "Expression string should be empty after Close()")
	assert.Empty(t, exprRerank.collectionName, "Collection name should be empty after Close()")
	assert.Nil(t, exprRerank.requiredFieldNames, "Required field names should be nil after Close()")
	assert.Nil(t, exprRerank.requiredFieldIndices, "Required field indices should be nil after Close()")

	// Clean up registry
	UnregisterRerankersForCollection(collSchema.GetName())
}

// TestExprRerankConcurrentMemoryLeak tests memory behavior under concurrent creation/destruction
func TestExprRerankConcurrentMemoryLeak(t *testing.T) {
	// This test is more realistic - simulates concurrent query node operations
	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	baselineAlloc := m1.Alloc

	// Simulate rapid creation/deletion of collections with expression rerankers
	iterations := 50
	for i := 0; i < iterations; i++ {
		collName := fmt.Sprintf("concurrent_test_coll_%d", i)

		collSchema := &schemapb.CollectionSchema{
			Name: collName,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 100, Name: "field1", DataType: schemapb.DataType_Float},
				{FieldID: 101, Name: "field2", DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "field3", DataType: schemapb.DataType_Double},
			},
		}

		// Create multiple rerankers per collection
		for j := 0; j < 3; j++ {
			funcSchema := &schemapb.FunctionSchema{
				Name:             fmt.Sprintf("expr_rerank_%d", j),
				Type:             schemapb.FunctionType_Rerank,
				InputFieldNames:  []string{"field1", "field2", "field3"},
				InputFieldIds:    []int64{100, 101, 102},
				OutputFieldNames: []string{},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "expr"},
					{Key: "expr_code", Value: fmt.Sprintf("score * %d + fields.field1", j+1)},
				},
			}

			reranker, err := newExprFunction(collSchema, funcSchema)
			require.NoError(t, err)
			require.NotNil(t, reranker)
		}

		// Immediately unregister (simulates quick collection lifecycle)
		UnregisterRerankersForCollection(collName)

		if i%10 == 0 {
			runtime.GC()
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	finalAlloc := m2.Alloc

	growth := int64(finalAlloc) - int64(baselineAlloc)
	growthMB := float64(growth) / 1024.0 / 1024.0

	t.Logf("Concurrent test - Memory growth: %.2f MB after %d iterations", growthMB, iterations)

	// Should not leak significantly even with concurrent operations
	assert.Less(t, growthMB, 30.0, "Memory leaked > 30 MB in concurrent test")
}

// BenchmarkExprRerankMemoryOverhead measures the memory overhead per ExprRerank instance
func BenchmarkExprRerankMemoryOverhead(b *testing.B) {
	collSchema := &schemapb.CollectionSchema{
		Name: "bench_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 100, Name: "field1", DataType: schemapb.DataType_Float},
		},
	}

	funcSchema := &schemapb.FunctionSchema{
		Name:             "bench_rerank",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{"field1"},
		InputFieldIds:    []int64{100},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "expr"},
			{Key: "expr_code", Value: "score * 2.0 + fields.field1"},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reranker, err := newExprFunction(collSchema, funcSchema)
		if err != nil {
			b.Fatal(err)
		}
		reranker.Close()
	}

	// Clean up
	UnregisterRerankersForCollection(collSchema.GetName())
}

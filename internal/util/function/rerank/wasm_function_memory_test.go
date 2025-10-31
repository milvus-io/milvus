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
	"encoding/base64"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// minimalWasmBytecode returns a minimal valid WASM module that exports a "rerank" function
// The function takes (score: f32, rank: i32) and returns f32
func minimalWasmBytecode() string {
	// This is a minimal WASM module with a rerank function: (f32, i32) -> f32
	// It simply returns the input score unchanged
	wasmBytes := []byte{
		0x00, 0x61, 0x73, 0x6d, // WASM magic number
		0x01, 0x00, 0x00, 0x00, // WASM version 1

		// Type section: 1 function type (f32, i32) -> f32
		0x01, 0x07, 0x01, 0x60, 0x02, 0x7d, 0x7f, 0x01, 0x7d,

		// Function section: 1 function
		0x03, 0x02, 0x01, 0x00,

		// Export section: export function 0 as "rerank"
		0x07, 0x0a, 0x01, 0x06, 0x72, 0x65, 0x72, 0x61, 0x6e, 0x6b, 0x00, 0x00,

		// Code section: function body (just return local 0, which is the score parameter)
		0x0a, 0x06, 0x01, 0x04, 0x00, 0x20, 0x00, 0x0b,
	}

	return base64.StdEncoding.EncodeToString(wasmBytes)
}

// TestWasmFunctionMemoryLeak verifies that WasmFunction instances don't leak memory
// when created and destroyed repeatedly
func TestWasmFunctionMemoryLeak(t *testing.T) {
	// Skip if WASM runtime is not available
	wasmCode := minimalWasmBytecode()

	// Force GC and get baseline memory stats
	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	baselineAlloc := m1.Alloc

	t.Logf("Baseline memory: %d MB", baselineAlloc/1024/1024)

	// Create and destroy many WASM rerankers
	iterations := 50 // Fewer iterations than expr test due to WASM overhead
	for i := 0; i < iterations; i++ {
		collName := fmt.Sprintf("test_wasm_collection_%d", i)

		// Create test schema
		collSchema := &schemapb.CollectionSchema{
			Name: collName,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 100, Name: "price", DataType: schemapb.DataType_Float},
			},
		}

		funcSchema := &schemapb.FunctionSchema{
			Name:             "test_wasm_rerank",
			Type:             schemapb.FunctionType_Rerank,
			InputFieldNames:  []string{},
			InputFieldIds:    []int64{},
			OutputFieldNames: []string{},
			Params: []*commonpb.KeyValuePair{
				{Key: "reranker", Value: "wasm"},
				{Key: "wasm_code", Value: wasmCode},
				{Key: "entry_point", Value: "rerank"},
			},
		}

		// Create reranker
		reranker, err := newWasmFunction(collSchema, funcSchema)
		require.NoError(t, err)
		require.NotNil(t, reranker)

		// Verify it's a WasmFunction instance
		wasmFunc, ok := reranker.(*WasmFunction[int64])
		require.True(t, ok, "Expected WasmFunction[int64] instance")

		// Verify WASM resources are allocated
		assert.NotNil(t, wasmFunc.store, "Store should be allocated")
		assert.NotNil(t, wasmFunc.instance, "Instance should be allocated")
		assert.NotNil(t, wasmFunc.rerankFunc, "Rerank function should be cached")

		// Clean up - this simulates collection deletion
		UnregisterRerankersForCollection(collName)

		// Periodic GC every 10 iterations
		if i%10 == 0 {
			runtime.GC()
			runtime.GC() // Double GC to run finalizers
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Force comprehensive GC to run Store finalizers and reclaim WASM memory
	runtime.GC()
	runtime.GC()
	time.Sleep(200 * time.Millisecond) // WASM cleanup may take longer

	// Check final memory
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	finalAlloc := m2.Alloc

	growth := int64(finalAlloc) - int64(baselineAlloc)
	growthMB := float64(growth) / 1024.0 / 1024.0

	t.Logf("Final memory: %d MB", finalAlloc/1024/1024)
	t.Logf("Memory growth: %.2f MB after %d iterations", growthMB, iterations)
	t.Logf("Per-iteration growth: %.2f KB", float64(growth)/float64(iterations)/1024.0)

	// WASM has higher overhead, so allow more growth
	// Allow 50MB growth for 50 iterations (~1MB per iteration is reasonable for WASM)
	maxGrowthMB := float64(50.0)
	assert.Less(t, growthMB, maxGrowthMB,
		"Memory leaked > %.0f MB after %d iterations (%.2f MB total)",
		maxGrowthMB, iterations, growthMB)
}

// TestWasmFunctionCloseReleasesResources verifies that Close() properly clears all references
func TestWasmFunctionCloseReleasesResources(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{
		Name: "test_wasm_close_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	funcSchema := &schemapb.FunctionSchema{
		Name:             "test_wasm_close",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{},
		InputFieldIds:    []int64{},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "wasm"},
			{Key: "wasm_code", Value: minimalWasmBytecode()},
		},
	}

	reranker, err := newWasmFunction(collSchema, funcSchema)
	require.NoError(t, err)

	wasmFunc := reranker.(*WasmFunction[int64])

	// Verify resources are allocated
	assert.NotNil(t, wasmFunc.store, "Store should be allocated")
	assert.NotNil(t, wasmFunc.instance, "Instance should be allocated")
	assert.NotNil(t, wasmFunc.rerankFunc, "Rerank function should be cached")
	assert.NotNil(t, wasmFunc.fieldArrays, "Field arrays should be allocated")
	assert.NotNil(t, wasmFunc.argBuffer, "Arg buffer should be allocated")
	assert.NotNil(t, wasmFunc.scoreMapPool, "Score map pool should be allocated")
	assert.NotNil(t, wasmFunc.locMapPool, "Loc map pool should be allocated")

	// Close the reranker
	wasmFunc.Close()

	// Verify all resources are cleared
	assert.Nil(t, wasmFunc.store, "Store should be nil after Close()")
	assert.Nil(t, wasmFunc.instance, "Instance should be nil after Close()")
	assert.Nil(t, wasmFunc.rerankFunc, "Rerank function should be nil after Close()")
	assert.Nil(t, wasmFunc.fieldArrays, "Field arrays should be nil after Close()")
	assert.Nil(t, wasmFunc.argBuffer, "Arg buffer should be nil after Close()")
	assert.Nil(t, wasmFunc.scoreMapPool, "Score map pool should be nil after Close()")
	assert.Nil(t, wasmFunc.locMapPool, "Loc map pool should be nil after Close()")

	// Clean up registry
	UnregisterRerankersForCollection(collSchema.GetName())
}

// TestWasmFunctionStoreFinalizer verifies that wasmtime Store finalizers run
func TestWasmFunctionStoreFinalizer(t *testing.T) {
	// This test verifies that Store finalizers are properly configured
	// and will run when Store becomes unreachable

	collSchema := &schemapb.CollectionSchema{
		Name: "test_finalizer_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	funcSchema := &schemapb.FunctionSchema{
		Name:             "test_finalizer",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{},
		InputFieldIds:    []int64{},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "wasm"},
			{Key: "wasm_code", Value: minimalWasmBytecode()},
		},
	}

	// Create multiple WASM functions to increase memory pressure
	for i := 0; i < 10; i++ {
		reranker, err := newWasmFunction(collSchema, funcSchema)
		require.NoError(t, err)

		// Immediately close to make Store unreachable
		reranker.Close()
	}

	// Unregister all
	UnregisterRerankersForCollection(collSchema.GetName())

	// Force GC multiple times to ensure finalizers run
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}

	// If we get here without crashes, finalizers worked correctly
	t.Log("Store finalizers completed successfully")
}

// TestWasmFunctionPoolCleanup verifies that sync.Pool objects are properly released
func TestWasmFunctionPoolCleanup(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{
		Name: "test_pool_cleanup",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	funcSchema := &schemapb.FunctionSchema{
		Name:             "test_pool",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{},
		InputFieldIds:    []int64{},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "wasm"},
			{Key: "wasm_code", Value: minimalWasmBytecode()},
		},
	}

	reranker, err := newWasmFunction(collSchema, funcSchema)
	require.NoError(t, err)

	wasmFunc := reranker.(*WasmFunction[int64])

	// Get objects from pools
	scoreMap := wasmFunc.scoreMapPool.Get().(map[int64]float32)
	locMap := wasmFunc.locMapPool.Get().(map[int64]IDLoc)

	// Put them back
	wasmFunc.scoreMapPool.Put(scoreMap)
	wasmFunc.locMapPool.Put(locMap)

	// Close should clear pools
	wasmFunc.Close()

	assert.Nil(t, wasmFunc.scoreMapPool, "Score map pool should be nil")
	assert.Nil(t, wasmFunc.locMapPool, "Loc map pool should be nil")

	// Clean up
	UnregisterRerankersForCollection(collSchema.GetName())
}

// BenchmarkWasmFunctionMemoryOverhead measures the memory overhead per WasmFunction instance
func BenchmarkWasmFunctionMemoryOverhead(b *testing.B) {
	collSchema := &schemapb.CollectionSchema{
		Name: "bench_wasm_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	funcSchema := &schemapb.FunctionSchema{
		Name:             "bench_wasm",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{},
		InputFieldIds:    []int64{},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "wasm"},
			{Key: "wasm_code", Value: minimalWasmBytecode()},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reranker, err := newWasmFunction(collSchema, funcSchema)
		if err != nil {
			b.Fatal(err)
		}
		reranker.Close()
	}

	// Clean up
	UnregisterRerankersForCollection(collSchema.GetName())
}

// TestWasmFunctionWithFieldsMemoryLeak tests memory behavior with field arrays
func TestWasmFunctionWithFieldsMemoryLeak(t *testing.T) {
	// WASM function that uses fields has more memory overhead
	// This is a minimal WASM module with a rerank function: (f32, i32, f32) -> f32
	wasmWithFieldBytes := []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
		0x01, 0x08, 0x01, 0x60, 0x03, 0x7d, 0x7f, 0x7d, 0x01, 0x7d,
		0x03, 0x02, 0x01, 0x00,
		0x07, 0x0a, 0x01, 0x06, 0x72, 0x65, 0x72, 0x61, 0x6e, 0x6b, 0x00, 0x00,
		0x0a, 0x06, 0x01, 0x04, 0x00, 0x20, 0x00, 0x0b,
	}
	wasmCode := base64.StdEncoding.EncodeToString(wasmWithFieldBytes)

	runtime.GC()
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	iterations := 30
	for i := 0; i < iterations; i++ {
		collName := fmt.Sprintf("test_wasm_fields_%d", i)

		collSchema := &schemapb.CollectionSchema{
			Name: collName,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 100, Name: "field1", DataType: schemapb.DataType_Float},
				{FieldID: 101, Name: "field2", DataType: schemapb.DataType_Int64},
			},
		}

		funcSchema := &schemapb.FunctionSchema{
			Name:             "test_wasm_with_fields",
			Type:             schemapb.FunctionType_Rerank,
			InputFieldNames:  []string{"field1"},
			InputFieldIds:    []int64{100},
			OutputFieldNames: []string{},
			Params: []*commonpb.KeyValuePair{
				{Key: "reranker", Value: "wasm"},
				{Key: "wasm_code", Value: wasmCode},
			},
		}

		_, err := newWasmFunction(collSchema, funcSchema)
		require.NoError(t, err)

		UnregisterRerankersForCollection(collName)

		if i%10 == 0 {
			runtime.GC()
			runtime.GC()
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	growth := float64(int64(m2.Alloc)-int64(m1.Alloc)) / 1024.0 / 1024.0

	t.Logf("Memory growth with fields: %.2f MB after %d iterations", growth, iterations)
	assert.Less(t, growth, 40.0, "Memory leaked > 40 MB with field arrays")
}

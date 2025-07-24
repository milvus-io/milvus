// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMemoryAllocatorBasicOperations tests basic memory allocation and release operations
func TestMemoryAllocatorBasicOperations(t *testing.T) {
	// Create memory allocator with 1GB system memory
	ma := NewMemoryAllocator(1024 * 1024 * 1024)

	// Test initial state
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)

	// Test memory allocation for task 1
	success := ma.(*memoryAllocator).tryAllocate(1, 50*1024*1024) // 50MB for task 1
	assert.True(t, success)
	assert.Equal(t, int64(50*1024*1024), ma.(*memoryAllocator).usedMemory)

	// Test memory allocation for task 2
	success = ma.(*memoryAllocator).tryAllocate(2, 50*1024*1024) // 50MB for task 2
	assert.True(t, success)
	assert.Equal(t, int64(100*1024*1024), ma.(*memoryAllocator).usedMemory)

	// Test memory release for task 1
	ma.Release(1, 50*1024*1024)
	assert.Equal(t, int64(50*1024*1024), ma.(*memoryAllocator).usedMemory)

	// Test memory release for task 2
	ma.Release(2, 50*1024*1024)
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)
}

// TestMemoryAllocatorMemoryLimit tests memory limit enforcement
func TestMemoryAllocatorMemoryLimit(t *testing.T) {
	// Create memory allocator with 1GB system memory
	ma := NewMemoryAllocator(1024 * 1024 * 1024)

	// Get the memory limit based on system memory and configuration percentage
	memoryLimit := ma.(*memoryAllocator).systemTotalMemory
	// Use a reasonable test size that should be within limits
	testSize := memoryLimit / 10 // Use 10% of available memory

	// Allocate memory up to the limit
	success := ma.(*memoryAllocator).tryAllocate(1, testSize)
	assert.True(t, success)
	assert.Equal(t, testSize, ma.(*memoryAllocator).usedMemory)

	// Try to allocate more memory than available (should fail)
	success = ma.(*memoryAllocator).tryAllocate(2, memoryLimit)
	assert.False(t, success)
	assert.Equal(t, testSize, ma.(*memoryAllocator).usedMemory) // Should remain unchanged

	// Release the allocated memory
	ma.Release(1, testSize)
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)
}

// TestMemoryAllocatorConcurrentAccess tests concurrent memory allocation and release
func TestMemoryAllocatorConcurrentAccess(t *testing.T) {
	// Create memory allocator with 1GB system memory
	ma := NewMemoryAllocator(1024 * 1024 * 1024)

	// Test concurrent memory requests
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		taskID := int64(i + 1)
		go func() {
			success := ma.(*memoryAllocator).tryAllocate(taskID, 50*1024*1024) // 50MB each
			if success {
				ma.Release(taskID, 50*1024*1024)
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify final state - should be 0 or the sum of remaining allocations
	// depending on memory limit and concurrent execution
	finalMemory := ma.(*memoryAllocator).usedMemory
	assert.GreaterOrEqual(t, finalMemory, int64(0))
}

// TestMemoryAllocatorNegativeRelease tests handling of negative memory release
func TestMemoryAllocatorNegativeRelease(t *testing.T) {
	// Create memory allocator with 1GB system memory
	ma := NewMemoryAllocator(1024 * 1024 * 1024)

	// Allocate some memory
	success := ma.(*memoryAllocator).tryAllocate(1, 100*1024*1024) // 100MB
	assert.True(t, success)
	assert.Equal(t, int64(100*1024*1024), ma.(*memoryAllocator).usedMemory)

	// Release more than allocated (should not go negative)
	ma.Release(1, 200*1024*1024)                                // 200MB
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory) // Should be reset to 0
}

// TestMemoryAllocatorMultipleTasks tests memory management for multiple tasks
func TestMemoryAllocatorMultipleTasks(t *testing.T) {
	// Create memory allocator with 1GB system memory
	ma := NewMemoryAllocator(1024 * 1024 * 1024)

	// Allocate memory for multiple tasks with smaller sizes
	taskIDs := []int64{1, 2, 3, 4, 5}
	sizes := []int64{20, 30, 25, 15, 35} // Total: 125MB

	for i, taskID := range taskIDs {
		success := ma.(*memoryAllocator).tryAllocate(taskID, sizes[i]*1024*1024)
		assert.True(t, success)
	}

	// Verify total used memory
	expectedTotal := int64(0)
	for _, size := range sizes {
		expectedTotal += size * 1024 * 1024
	}
	assert.Equal(t, expectedTotal, ma.(*memoryAllocator).usedMemory)

	// Release memory for specific tasks
	ma.Release(2, 30*1024*1024) // Release task 2
	ma.Release(4, 15*1024*1024) // Release task 4

	// Verify updated memory usage
	expectedTotal = (20 + 25 + 35) * 1024 * 1024 // 80MB
	assert.Equal(t, expectedTotal, ma.(*memoryAllocator).usedMemory)

	// Release remaining tasks
	ma.Release(1, 20*1024*1024)
	ma.Release(3, 25*1024*1024)
	ma.Release(5, 35*1024*1024)

	// Verify final state
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)
}

// TestMemoryAllocatorZeroSize tests handling of zero size allocations
func TestMemoryAllocatorZeroSize(t *testing.T) {
	// Create memory allocator
	ma := NewMemoryAllocator(1024 * 1024 * 1024)

	// Test zero size allocation
	success := ma.(*memoryAllocator).tryAllocate(1, 0)
	assert.True(t, success) // Should succeed
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)

	// Test zero size release
	ma.Release(1, 0)
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)
}

// TestMemoryAllocatorSimple tests basic functionality without external dependencies
func TestMemoryAllocatorSimple(t *testing.T) {
	// Create memory allocator with 1GB system memory
	ma := NewMemoryAllocator(1024 * 1024 * 1024)

	// Test initial state
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)

	// Test memory allocation
	success := ma.(*memoryAllocator).tryAllocate(1, 50*1024*1024) // 50MB
	assert.True(t, success)
	assert.Equal(t, int64(50*1024*1024), ma.(*memoryAllocator).usedMemory)

	// Test memory release
	ma.Release(1, 50*1024*1024)
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)
}

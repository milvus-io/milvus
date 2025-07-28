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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestMemoryAllocatorBasicOperations tests basic memory allocation and release operations
func TestMemoryAllocatorBasicOperations(t *testing.T) {
	// Create memory allocator with 1GB system memory
	ma := NewMemoryAllocator(1024 * 1024 * 1024)

	// Test initial state
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)

	// Test memory allocation for task 1 using BlockingAllocate
	ma.BlockingAllocate(1, 50*1024*1024) // 50MB for task 1
	assert.Equal(t, int64(50*1024*1024), ma.(*memoryAllocator).usedMemory)

	// Test memory allocation for task 2
	ma.BlockingAllocate(2, 50*1024*1024) // 50MB for task 2
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
	ma.BlockingAllocate(1, testSize)
	assert.Equal(t, testSize, ma.(*memoryAllocator).usedMemory)

	// Try to allocate more memory than available (this will block, so we test in a goroutine)
	done := make(chan bool)
	go func() {
		ma.BlockingAllocate(2, testSize)
		done <- true
	}()

	// Release the allocated memory to unblock the waiting allocation
	ma.Release(1, testSize)
	<-done

	// Verify that the second allocation succeeded after release
	assert.Equal(t, testSize, ma.(*memoryAllocator).usedMemory)

	// Release the second allocation
	ma.Release(2, testSize)
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
			ma.BlockingAllocate(taskID, 50*1024*1024) // 50MB each
			ma.Release(taskID, 50*1024*1024)
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify final state - should be 0 since all allocations were released
	finalMemory := ma.(*memoryAllocator).usedMemory
	assert.Equal(t, int64(0), finalMemory)
}

// TestMemoryAllocatorNegativeRelease tests handling of negative memory release
func TestMemoryAllocatorNegativeRelease(t *testing.T) {
	// Create memory allocator with 1GB system memory
	ma := NewMemoryAllocator(1024 * 1024 * 1024)

	// Allocate some memory
	ma.BlockingAllocate(1, 100*1024*1024) // 100MB
	assert.Equal(t, int64(100*1024*1024), ma.(*memoryAllocator).usedMemory)

	// Release more than allocated (should not go negative)
	ma.Release(1, 200*1024*1024)                                // 200MB
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory) // Should be reset to 0
}

// TestMemoryAllocatorMultipleTasks tests memory management for multiple tasks
func TestMemoryAllocatorMultipleTasks(t *testing.T) {
	// Create memory allocator with 1GB system memory
	ma := NewMemoryAllocator(1024 * 1024 * 1024 * 2)

	// Allocate memory for multiple tasks with smaller sizes
	taskIDs := []int64{1, 2, 3, 4, 5}
	sizes := []int64{20, 30, 25, 15, 35} // Total: 125MB

	for i, taskID := range taskIDs {
		ma.BlockingAllocate(taskID, sizes[i]*1024*1024)
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
	ma.BlockingAllocate(1, 0)
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
	ma.BlockingAllocate(1, 50*1024*1024) // 50MB
	assert.Equal(t, int64(50*1024*1024), ma.(*memoryAllocator).usedMemory)

	// Test memory release
	ma.Release(1, 50*1024*1024)
	assert.Equal(t, int64(0), ma.(*memoryAllocator).usedMemory)
}

// TestMemoryAllocatorMassiveConcurrency tests massive concurrent memory allocation and release
func TestMemoryAllocatorMassiveConcurrency(t *testing.T) {
	// Create memory allocator with 1.6GB system memory
	totalMemory := int64(16 * 1024 * 1024 * 1024) // 16GB * 10%
	ma := NewMemoryAllocator(totalMemory)

	const numTasks = 200

	var wg sync.WaitGroup
	wg.Add(numTasks)
	// Start concurrent allocation and release
	for i := 0; i < numTasks; i++ {
		taskID := int64(i + 1)
		var memorySize int64
		// 10% chance to allocate 1.6GB, 90% chance to allocate 128MB-1536MB
		if rand.Float64() < 0.1 {
			memorySize = int64(1600 * 1024 * 1024)
		} else {
			multiple := rand.Intn(12) + 1
			memorySize = int64(multiple * 128 * 1024 * 1024) // 128MB to 1536MB
		}

		go func(id int64, size int64) {
			defer wg.Done()
			ma.BlockingAllocate(id, size)
			time.Sleep(1 * time.Millisecond)
			ma.Release(id, size)
		}(taskID, memorySize)
	}
	wg.Wait()

	// Assert that all memory is released
	finalMemory := ma.(*memoryAllocator).usedMemory
	assert.Equal(t, int64(0), finalMemory, "All memory should be released")
}

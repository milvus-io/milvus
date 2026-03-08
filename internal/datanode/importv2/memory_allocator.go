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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	globalMemoryAllocator     MemoryAllocator
	globalMemoryAllocatorOnce sync.Once
)

// GetMemoryAllocator returns the global memory allocator instance
func GetMemoryAllocator() MemoryAllocator {
	globalMemoryAllocatorOnce.Do(func() {
		globalMemoryAllocator = NewMemoryAllocator(int64(hardware.GetMemoryCount()))
	})
	return globalMemoryAllocator
}

// MemoryAllocator handles memory allocation and deallocation for import tasks
type MemoryAllocator interface {
	// BlockingAllocate blocks until memory is available and then allocates
	// This method will block until memory becomes available
	BlockingAllocate(taskID int64, size int64)

	// Release releases memory of the specified size
	Release(taskID int64, size int64)
}

type memoryAllocator struct {
	systemTotalMemory int64
	usedMemory        int64
	mutex             sync.RWMutex
	cond              *sync.Cond
}

// NewMemoryAllocator creates a new MemoryAllocator instance
func NewMemoryAllocator(systemTotalMemory int64) MemoryAllocator {
	log.Info("new import memory allocator", zap.Int64("systemTotalMemory", systemTotalMemory))
	ma := &memoryAllocator{
		systemTotalMemory: systemTotalMemory,
		usedMemory:        0,
	}
	ma.cond = sync.NewCond(&ma.mutex)
	return ma
}

// BlockingAllocate blocks until memory is available and then allocates
func (ma *memoryAllocator) BlockingAllocate(taskID int64, size int64) {
	ma.mutex.Lock()
	defer ma.mutex.Unlock()

	percentage := paramtable.Get().DataNodeCfg.ImportMemoryLimitPercentage.GetAsFloat()
	memoryLimit := int64(float64(ma.systemTotalMemory) * percentage / 100.0)

	// Wait until enough memory is available
	for ma.usedMemory+size > memoryLimit {
		log.Warn("task waiting for memory allocation...",
			zap.Int64("taskID", taskID),
			zap.Int64("requestedSize", size),
			zap.Int64("usedMemory", ma.usedMemory),
			zap.Int64("availableMemory", memoryLimit-ma.usedMemory))

		ma.cond.Wait()
	}

	// Allocate memory
	ma.usedMemory += size
	log.Info("memory allocated successfully",
		zap.Int64("taskID", taskID),
		zap.Int64("allocatedSize", size),
		zap.Int64("usedMemory", ma.usedMemory),
		zap.Int64("availableMemory", memoryLimit-ma.usedMemory))
}

// Release releases memory of the specified size
func (ma *memoryAllocator) Release(taskID int64, size int64) {
	ma.mutex.Lock()
	defer ma.mutex.Unlock()

	ma.usedMemory -= size
	if ma.usedMemory < 0 {
		ma.usedMemory = 0 // Prevent negative memory usage
		log.Warn("memory release resulted in negative usage, reset to 0",
			zap.Int64("taskID", taskID),
			zap.Int64("releaseSize", size))
	}

	log.Info("memory released successfully",
		zap.Int64("taskID", taskID),
		zap.Int64("releasedSize", size),
		zap.Int64("usedMemory", ma.usedMemory))

	// Wake up waiting tasks after memory is released
	ma.cond.Broadcast()
}

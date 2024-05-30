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

package vralloc

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/util/hardware"
)

type Resource struct {
	Memory uint64 // Memory occupation in bytes
	Cpu    uint64 // CPU in cycles per second
	Disk   uint64 // Disk occpuation in bytes
}

// Add adds r2 to r
func (r *Resource) Add(r2 *Resource) *Resource {
	r.Memory += r2.Memory
	r.Cpu += r2.Cpu
	r.Disk += r2.Disk
	return r
}

// Sub subtracts r2 from r
func (r *Resource) Sub(r2 *Resource) *Resource {
	r.Memory -= r2.Memory
	r.Cpu -= r2.Cpu
	r.Disk -= r2.Disk
	return r
}

// Le tests if the resource is less than or equal to the limit
func (r Resource) Le(limit *Resource) bool {
	return r.Memory <= limit.Memory && r.Cpu <= limit.Cpu && r.Disk <= limit.Disk
}

type Allocator interface {
	Allocate(id string, r *Resource) bool
	Release(id string)
	Used() Resource
	Inspect() map[string]*Resource
}

type FixedSizeAllocator struct {
	limit *Resource

	lock   sync.RWMutex
	used   Resource
	allocs map[string]*Resource
}

var _ Allocator = (*FixedSizeAllocator)(nil)

func (a *FixedSizeAllocator) Allocate(id string, r *Resource) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.used.Add(r).Le(a.limit) {
		a.allocs[id] = r
		return true
	}
	a.used.Sub(r)
	return false
}

func (a *FixedSizeAllocator) Release(id string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	r, ok := a.allocs[id]
	if !ok {
		return
	}
	delete(a.allocs, id)
	a.used.Sub(r)
}

func (a *FixedSizeAllocator) Used() Resource {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.used
}

func (a *FixedSizeAllocator) Inspect() map[string]*Resource {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.allocs
}

// PhysicalAwareFixedSizeAllocator allocates resources with additional consideration of physical memory limit
type PhysicalAwareFixedSizeAllocator struct {
	FixedSizeAllocator
}

var _ Allocator = (*PhysicalAwareFixedSizeAllocator)(nil)

func (a *PhysicalAwareFixedSizeAllocator) Allocate(id string, r *Resource) bool {
	memoryUsage := hardware.GetUsedMemoryCount()
	totalMemory := hardware.GetMemoryCount()

	// Check if memory usage + future request estimation will exceed the memory limit
	// Note that different allocators will not coordinate with each other, so the memory limit
	// may be exceeded in concurrent allocations.
	if a.Used().Memory+r.Memory+memoryUsage > totalMemory {
		return false
	}
	return a.FixedSizeAllocator.Allocate(id, r)
}

func NewPhysicalAwareFixedSizeAllocator(limit *Resource) *PhysicalAwareFixedSizeAllocator {
	return &PhysicalAwareFixedSizeAllocator{
		FixedSizeAllocator: FixedSizeAllocator{
			limit:  limit,
			allocs: make(map[string]*Resource),
		},
	}
}

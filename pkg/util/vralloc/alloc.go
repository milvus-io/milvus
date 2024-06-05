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
	"maps"
	"sync"

	"github.com/shirou/gopsutil/v3/disk"

	"github.com/milvus-io/milvus/pkg/util/hardware"
)

type Resource struct {
	Memory int64 // Memory occupation in bytes
	CPU    int64 // CPU in cycles per second
	Disk   int64 // Disk occpuation in bytes
}

// Add adds r2 to r
func (r *Resource) Add(r2 *Resource) *Resource {
	r.Memory += r2.Memory
	r.CPU += r2.CPU
	r.Disk += r2.Disk
	return r
}

// Sub subtracts r2 from r
func (r *Resource) Sub(r2 *Resource) *Resource {
	r.Memory -= r2.Memory
	r.CPU -= r2.CPU
	r.Disk -= r2.Disk
	return r
}

func (r *Resource) Diff(r2 *Resource) *Resource {
	return &Resource{
		Memory: r.Memory - r2.Memory,
		CPU:    r.CPU - r2.CPU,
		Disk:   r.Disk - r2.Disk,
	}
}

// Le tests if the resource is less than or equal to the limit
func (r Resource) Le(limit *Resource) bool {
	return r.Memory <= limit.Memory && r.CPU <= limit.CPU && r.Disk <= limit.Disk
}

type Allocator[T comparable] interface {
	// Allocate allocates the resource, returns true if the resource is allocated. If allocation failed, returns the short resource.
	// The short resource is a positive value, e.g., if there is additional 8 bytes in disk needed, returns (0, 0, 8).
	Allocate(id T, r *Resource) (allocated bool, short *Resource)
	// Release releases the resource
	Release(id T)
	// Used returns the used resource
	Used() Resource
	// Inspect returns the allocated resources
	Inspect() map[T]*Resource
}

type FixedSizeAllocator[T comparable] struct {
	limit *Resource

	lock   sync.RWMutex
	used   Resource
	allocs map[T]*Resource
}

func (a *FixedSizeAllocator[T]) Allocate(id T, r *Resource) (allocated bool, short *Resource) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.used.Add(r).Le(a.limit) {
		_, ok := a.allocs[id]
		if ok {
			// Re-allocate on identical id is not allowed
			return false, nil
		}
		a.allocs[id] = r
		return true, nil
	}
	short = a.used.Diff(a.limit)
	a.used.Sub(r)
	return false, short
}

func (a *FixedSizeAllocator[T]) Release(id T) {
	a.lock.Lock()
	defer a.lock.Unlock()
	r, ok := a.allocs[id]
	if !ok {
		return
	}
	delete(a.allocs, id)
	a.used.Sub(r)
}

func (a *FixedSizeAllocator[T]) Used() Resource {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.used
}

func (a *FixedSizeAllocator[T]) Inspect() map[T]*Resource {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return maps.Clone(a.allocs)
}

func NewFixedSizeAllocator[T comparable](limit *Resource) *FixedSizeAllocator[T] {
	return &FixedSizeAllocator[T]{
		limit:  limit,
		allocs: make(map[T]*Resource),
	}
}

// PhysicalAwareFixedSizeAllocator allocates resources with additional consideration of physical resource usage.
type PhysicalAwareFixedSizeAllocator[T comparable] struct {
	FixedSizeAllocator[T]

	hwLimit *Resource
	dir     string // watching directory for disk usage, probably got by paramtable.Get().LocalStorageCfg.Path.GetValue()
}

func (a *PhysicalAwareFixedSizeAllocator[T]) Allocate(id T, r *Resource) (allocated bool, short *Resource) {
	memoryUsage := int64(hardware.GetUsedMemoryCount())
	diskUsage := int64(0)
	if usageStats, err := disk.Usage(a.dir); err != nil {
		diskUsage = int64(usageStats.Used)
	}

	// Check if memory usage + future request estimation will exceed the memory limit
	// Note that different allocators will not coordinate with each other, so the memory limit
	// may be exceeded in concurrent allocations.
	expected := &Resource{
		Memory: a.Used().Memory + r.Memory + memoryUsage,
		Disk:   a.Used().Disk + r.Disk + diskUsage,
	}
	if expected.Le(a.hwLimit) {
		return a.FixedSizeAllocator.Allocate(id, r)
	}
	return false, expected.Diff(a.hwLimit)
}

func NewPhysicalAwareFixedSizeAllocator[T comparable](limit *Resource, hwMemoryLimit, hwDiskLimit int64, dir string) *PhysicalAwareFixedSizeAllocator[T] {
	return &PhysicalAwareFixedSizeAllocator[T]{
		FixedSizeAllocator: FixedSizeAllocator[T]{
			limit:  limit,
			allocs: make(map[T]*Resource),
		},
		hwLimit: &Resource{Memory: hwMemoryLimit, Disk: hwDiskLimit},
		dir:     dir,
	}
}

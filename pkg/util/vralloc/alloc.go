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

	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

var zero = &Resource{0, 0, 0}

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
	// Allocate on identical id is not allowed, in which case it returns (false, nil). Use #Reallocate instead.
	Allocate(id T, r *Resource) (allocated bool, short *Resource)
	// Reallocate re-allocates the resource on given id with delta resource. Delta can be negative, in which case the resource is released.
	// If delta is negative and the allocated resource is less than the delta, returns (false, nil).
	Reallocate(id T, delta *Resource) (allocated bool, short *Resource)
	// Release releases the resource
	Release(id T) *Resource
	// Used returns the used resource
	Used() Resource
	// Wait waits for new release. Releases could be initiated by #Release or #Reallocate.
	Wait()
	// Inspect returns the allocated resources
	Inspect() map[T]*Resource

	// notify notifies the waiters.
	notify()
}

type FixedSizeAllocator[T comparable] struct {
	limit *Resource

	lock   sync.RWMutex
	used   Resource
	allocs map[T]*Resource
	cond   sync.Cond
}

func (a *FixedSizeAllocator[T]) Allocate(id T, r *Resource) (allocated bool, short *Resource) {
	if r.Le(zero) {
		return false, nil
	}
	a.lock.Lock()
	defer a.lock.Unlock()

	_, ok := a.allocs[id]
	if ok {
		// Re-allocate on identical id is not allowed
		return false, nil
	}

	if a.used.Add(r).Le(a.limit) {
		a.allocs[id] = r
		return true, nil
	}
	short = a.used.Diff(a.limit)
	a.used.Sub(r)
	return false, short
}

func (a *FixedSizeAllocator[T]) Reallocate(id T, delta *Resource) (allocated bool, short *Resource) {
	a.lock.Lock()
	r, ok := a.allocs[id]
	a.lock.Unlock()

	if !ok {
		return a.Allocate(id, delta)
	}

	a.lock.Lock()
	defer a.lock.Unlock()
	r.Add(delta)
	if !zero.Le(r) {
		r.Sub(delta)
		return false, nil
	}

	if a.used.Add(delta).Le(a.limit) {
		if !zero.Le(delta) {
			// If delta is negative, notify waiters
			a.notify()
		}
		return true, nil
	}
	short = a.used.Diff(a.limit)
	r.Sub(delta)
	a.used.Sub(delta)
	return false, short
}

func (a *FixedSizeAllocator[T]) Release(id T) *Resource {
	a.lock.Lock()
	defer a.lock.Unlock()
	r, ok := a.allocs[id]
	if !ok {
		return zero
	}
	delete(a.allocs, id)
	a.used.Sub(r)
	a.notify()
	return r
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

func (a *FixedSizeAllocator[T]) Wait() {
	a.cond.L.Lock()
	a.cond.Wait()
	a.cond.L.Unlock()
}

func (a *FixedSizeAllocator[T]) notify() {
	a.cond.Broadcast()
}

func NewFixedSizeAllocator[T comparable](limit *Resource) *FixedSizeAllocator[T] {
	return &FixedSizeAllocator[T]{
		limit:  limit,
		allocs: make(map[T]*Resource),
		cond:   sync.Cond{L: &sync.Mutex{}},
	}
}

// PhysicalAwareFixedSizeAllocator allocates resources with additional consideration of physical resource usage.
// Note: wait on PhysicalAwareFixedSizeAllocator may only be notified if there is virtual resource released.
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

func (a *PhysicalAwareFixedSizeAllocator[T]) Reallocate(id T, delta *Resource) (allocated bool, short *Resource) {
	memoryUsage := int64(hardware.GetUsedMemoryCount())
	diskUsage := int64(0)
	if usageStats, err := disk.Usage(a.dir); err != nil {
		diskUsage = int64(usageStats.Used)
	}

	expected := &Resource{
		Memory: a.Used().Memory + delta.Memory + memoryUsage,
		Disk:   a.Used().Disk + delta.Disk + diskUsage,
	}
	if expected.Le(a.hwLimit) {
		return a.FixedSizeAllocator.Reallocate(id, delta)
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

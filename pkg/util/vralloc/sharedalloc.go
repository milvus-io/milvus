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

type SharedAllocator struct {
	Allocator[string]
	parent *GroupedAllocator
	name   string
}

// GroupedAllocator is a shared allocator that can be grouped with other shared allocators. The sum of used resources of all
// children should not exceed the limit.
type GroupedAllocator struct {
	SharedAllocator
	name     string
	children map[string]Allocator[string]
}

// Allocate allocates the resource, returns true if the resource is allocated. If allocation failed, returns the short resource.
// The short resource is a positive value, e.g., if there is additional 8 bytes in disk needed, returns (0, 0, 8).
func (sa *SharedAllocator) Allocate(id string, r *Resource) (allocated bool, short *Resource) {
	allocated, short = sa.Allocator.Allocate(id, r)
	if !allocated {
		return
	}
	if sa.parent != nil {
		allocated, short = sa.parent.Reallocate(sa.name, r) // Ask for allocation on self name.
		if !allocated {
			sa.Allocator.Release(id)
		}
	}

	return
}

// Reallocate re-allocates the resource on given id with delta resource. Delta can be negative, in which case the resource is released.
// If delta is negative and the allocated resource is less than the delta, returns (false, nil).
func (sa *SharedAllocator) Reallocate(id string, delta *Resource) (allocated bool, short *Resource) {
	allocated, short = sa.Allocator.Reallocate(id, delta)
	if !allocated {
		return
	}
	if sa.parent != nil {
		allocated, short = sa.parent.Reallocate(sa.name, delta)
		if !allocated {
			sa.Allocator.Reallocate(id, zero.Diff(delta))
		}
	}
	return
}

// Release releases the resource
func (sa *SharedAllocator) Release(id string) *Resource {
	r := sa.Allocator.Release(id)
	if sa.parent != nil {
		sa.parent.Reallocate(sa.name, zero.Diff(r))
	}
	return r
}

// Allocate allocates the resource, returns true if the resource is allocated. If allocation failed, returns the short resource.
// The short resource is a positive value, e.g., if there is additional 8 bytes in disk needed, returns (0, 0, 8).
// Allocate on identical id is not allowed, in which case it returns (false, nil). Use #Reallocate instead.
func (ga *GroupedAllocator) Allocate(id string, r *Resource) (allocated bool, short *Resource) {
	return false, nil
}

// Release releases the resource
func (ga *GroupedAllocator) Release(id string) *Resource {
	return nil
}

func (ga *GroupedAllocator) Reallocate(id string, delta *Resource) (allocated bool, short *Resource) {
	allocated, short = ga.SharedAllocator.Reallocate(id, delta)
	if allocated {
		// Propagate to parent.
		if ga.parent != nil {
			allocated, short = ga.parent.Reallocate(ga.name, delta)
			if !allocated {
				ga.SharedAllocator.Reallocate(id, zero.Diff(delta))
				return
			}
		}
		// Notify siblings of id.
		for name := range ga.children {
			if name != id {
				ga.children[name].notify()
			}
		}
	}

	return
}

func (ga *GroupedAllocator) GetAllocator(name string) Allocator[string] {
	return ga.children[name]
}

type GroupedAllocatorBuilder struct {
	ga GroupedAllocator
}

func NewGroupedAllocatorBuilder(name string, limit *Resource) *GroupedAllocatorBuilder {
	return &GroupedAllocatorBuilder{
		ga: GroupedAllocator{
			SharedAllocator: SharedAllocator{
				Allocator: NewFixedSizeAllocator[string](limit),
				name:      name,
			},
			name:     name,
			children: make(map[string]Allocator[string]),
		},
	}
}

func (b *GroupedAllocatorBuilder) AddChild(name string, limit *Resource) *GroupedAllocatorBuilder {
	b.ga.children[name] = &SharedAllocator{
		Allocator: NewFixedSizeAllocator[string](limit),
		parent:    &b.ga,
		name:      name,
	}
	return b
}

func (b *GroupedAllocatorBuilder) AddChildGroup(allocator *GroupedAllocator) *GroupedAllocatorBuilder {
	allocator.parent = &b.ga
	b.ga.children[allocator.name] = allocator
	return b
}

func (b *GroupedAllocatorBuilder) Build() *GroupedAllocator {
	return &b.ga
}

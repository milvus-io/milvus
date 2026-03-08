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

package typeutil

import (
	"sync"
)

// UniqueSet is set type, which contains only UniqueIDs,
// the underlying type is map[UniqueID]struct{}.
// Create a UniqueSet instance with make(UniqueSet) like creating a map instance.
type UniqueSet = Set[UniqueID]

func NewUniqueSet(ids ...UniqueID) UniqueSet {
	set := make(UniqueSet)
	set.Insert(ids...)
	return set
}

type Set[T comparable] map[T]struct{}

func NewSet[T comparable](elements ...T) Set[T] {
	set := make(Set[T])
	set.Insert(elements...)
	return set
}

// Insert elements into the set,
// do nothing if the id existed
func (set Set[T]) Insert(elements ...T) {
	for i := range elements {
		set[elements[i]] = struct{}{}
	}
}

// Intersection returns the intersection with the given set
func (set Set[T]) Intersection(other Set[T]) Set[T] {
	ret := NewSet[T]()
	for elem := range set {
		if other.Contain(elem) {
			ret.Insert(elem)
		}
	}
	return ret
}

// Union returns the union with the given set
func (set Set[T]) Union(other Set[T]) Set[T] {
	ret := NewSet(set.Collect()...)
	ret.Insert(other.Collect()...)
	return ret
}

// Complement returns the complement with the given set
func (set Set[T]) Complement(other Set[T]) Set[T] {
	if other == nil {
		return set
	}
	ret := NewSet(set.Collect()...)
	ret.Remove(other.Collect()...)
	return ret
}

// Check whether the elements exist
func (set Set[T]) Contain(elements ...T) bool {
	for i := range elements {
		_, ok := set[elements[i]]
		if !ok {
			return false
		}
	}
	return true
}

// Remove elements from the set,
// do nothing if set is nil or id not exists
func (set Set[T]) Remove(elements ...T) {
	for i := range elements {
		delete(set, elements[i])
	}
}

func (set Set[T]) Clear() {
	set.Remove(set.Collect()...)
}

// Get all elements in the set
func (set Set[T]) Collect() []T {
	elements := make([]T, 0, len(set))
	for elem := range set {
		elements = append(elements, elem)
	}
	return elements
}

// Len returns the number of elements in the set
func (set Set[T]) Len() int {
	return len(set)
}

// Range iterates over elements in the set
func (set Set[T]) Range(f func(element T) bool) {
	for elem := range set {
		if !f(elem) {
			break
		}
	}
}

// Clone returns a new set with the same elements
func (set Set[T]) Clone() Set[T] {
	ret := make(Set[T], set.Len())
	for elem := range set {
		ret.Insert(elem)
	}
	return ret
}

type ConcurrentSet[T comparable] struct {
	inner sync.Map
}

func NewConcurrentSet[T comparable]() *ConcurrentSet[T] {
	return &ConcurrentSet[T]{}
}

// Insert elements into the set,
// do nothing if the id existed
func (set *ConcurrentSet[T]) Upsert(elements ...T) {
	for i := range elements {
		set.inner.Store(elements[i], struct{}{})
	}
}

func (set *ConcurrentSet[T]) Insert(element T) bool {
	_, exist := set.inner.LoadOrStore(element, struct{}{})
	return !exist
}

// Check whether the elements exist
func (set *ConcurrentSet[T]) Contain(elements ...T) bool {
	for i := range elements {
		_, ok := set.inner.Load(elements[i])
		if !ok {
			return false
		}
	}
	return true
}

// Remove elements from the set,
// do nothing if set is nil or id not exists
func (set *ConcurrentSet[T]) Remove(elements ...T) {
	for i := range elements {
		set.inner.Delete(elements[i])
	}
}

// Try remove element from set,
// return false if not exist
func (set *ConcurrentSet[T]) TryRemove(element T) bool {
	_, exist := set.inner.LoadAndDelete(element)
	return exist
}

// Get all elements in the set
func (set *ConcurrentSet[T]) Collect() []T {
	elements := make([]T, 0)
	set.inner.Range(func(key, value any) bool {
		elements = append(elements, key.(T))
		return true
	})
	return elements
}

func (set *ConcurrentSet[T]) Range(f func(element T) bool) {
	set.inner.Range(func(key, value any) bool {
		trueKey := key.(T)
		return f(trueKey)
	})
}

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

package common

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
)

var (
	// ErrRoutingTableNoHasher indicates that a hash routing table has no hasher configured.
	ErrRoutingTableNoHasher = errors.New("routing table has no hasher")
	// ErrRoutingTableNoValues indicates that a routing table has no values to route to.
	ErrRoutingTableNoValues = errors.New("routing table has no values")
	// ErrRoutingTableOpCommitted indicates that a value operation has already been committed.
	ErrRoutingTableOpCommitted = errors.New("routing table operation has already been committed")
)

// Hasher hashes a key for routing table lookup.
type Hasher[K comparable] interface {
	Hash(key K) (uint64, error)
}

// RoutingMember is a string-backed routing table value.
type RoutingMember string

func (m RoutingMember) String() string {
	return string(m)
}

// HashRoutingTable routes keys to values by taking hash(key) modulo the current value count.
type HashRoutingTable[K comparable, V fmt.Stringer] struct {
	mu     sync.RWMutex
	hasher Hasher[K]
	values map[string]V
	keys   []string
}

// NewHashRoutingTable creates a hash routing table with a copy of values.
func NewHashRoutingTable[K comparable, V fmt.Stringer](values []V, hasher Hasher[K]) *HashRoutingTable[K, V] {
	valueMap := make(map[string]V, len(values))
	keys := make([]string, 0, len(values))
	for _, value := range values {
		key := value.String()
		if _, ok := valueMap[key]; ok {
			continue
		}
		valueMap[key] = value
		keys = append(keys, key)
	}
	return &HashRoutingTable[K, V]{
		hasher: hasher,
		values: valueMap,
		keys:   keys,
	}
}

func (t *HashRoutingTable[K, V]) LocateKey(key K) (V, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var v V
	if len(t.keys) == 0 {
		return v, ErrRoutingTableNoValues
	}
	if t.hasher == nil {
		return v, ErrRoutingTableNoHasher
	}

	h, err := t.hasher.Hash(key)
	if err != nil {
		return v, err
	}
	return t.values[t.keys[h%uint64(len(t.keys))]], nil
}

func (t *HashRoutingTable[K, V]) NewValueOp() ValueOp[K, V] {
	return &hashValueOp[K, V]{
		table:   t,
		adds:    make([]V, 0),
		removes: make([]V, 0),
	}
}

type hashValueOp[K comparable, V fmt.Stringer] struct {
	table     *HashRoutingTable[K, V]
	adds      []V
	removes   []V
	committed bool
}

func (o *hashValueOp[K, V]) Add(v V) ValueOp[K, V] {
	o.adds = append(o.adds, v)
	return o
}

func (o *hashValueOp[K, V]) Remove(v V) ValueOp[K, V] {
	o.removes = append(o.removes, v)
	return o
}

func (o *hashValueOp[K, V]) Commit() error {
	o.table.mu.Lock()
	defer o.table.mu.Unlock()

	if o.committed {
		return ErrRoutingTableOpCommitted
	}
	o.committed = true

	for _, add := range o.adds {
		key := add.String()
		if _, ok := o.table.values[key]; ok {
			continue
		}
		o.table.values[key] = add
		o.table.keys = append(o.table.keys, key)
	}
	for _, remove := range o.removes {
		key := remove.String()
		if _, ok := o.table.values[key]; !ok {
			continue
		}
		delete(o.table.values, key)
		for i, valueKey := range o.table.keys {
			if valueKey == key {
				o.table.keys = append(o.table.keys[:i], o.table.keys[i+1:]...)
				break
			}
		}
	}
	o.adds = nil
	o.removes = nil
	return nil
}

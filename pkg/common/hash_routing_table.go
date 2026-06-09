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
	values []V
}

// NewHashRoutingTable creates a hash routing table with a copy of values.
func NewHashRoutingTable[K comparable, V fmt.Stringer](values []V, hasher Hasher[K]) *HashRoutingTable[K, V] {
	return &HashRoutingTable[K, V]{
		hasher: hasher,
		values: append([]V(nil), values...),
	}
}

func (t *HashRoutingTable[K, V]) LocateKey(key K) (V, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var v V
	if len(t.values) == 0 {
		return v, ErrRoutingTableNoValues
	}
	if t.hasher == nil {
		return v, ErrRoutingTableNoHasher
	}

	h, err := t.hasher.Hash(key)
	if err != nil {
		return v, err
	}
	return t.values[h%uint64(len(t.values))], nil
}

func (t *HashRoutingTable[K, V]) NewValueOp() ValueOp[K, V] {
	return &hashValueOp[K, V]{
		table:   t,
		adds:    make([]V, 0),
		removes: make([]V, 0),
	}
}

type hashValueOp[K comparable, V fmt.Stringer] struct {
	table   *HashRoutingTable[K, V]
	adds    []V
	removes []V
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

	for _, add := range o.adds {
		o.table.values = append(o.table.values, add)
	}
	for _, remove := range o.removes {
		for i, value := range o.table.values {
			if value.String() == remove.String() {
				o.table.values = append(o.table.values[:i], o.table.values[i+1:]...)
				break
			}
		}
	}
	return nil
}

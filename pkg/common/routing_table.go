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

import "fmt"

// RoutingTable is a table that maps keys to values.
// Values are identified by their String value.
type RoutingTable[K comparable, V fmt.Stringer] interface {
	// LocateKey locates the value for the given key.
	LocateKey(key K) (V, error)
	// NewValueOp creates an operation for updating the table values.
	NewValueOp() ValueOp[K, V]
}

// ValueOp is a set of operations on a value.
type ValueOp[K comparable, V fmt.Stringer] interface {
	// Add adds a value to the operation.
	Add(v V) ValueOp[K, V]
	// Remove removes a value from the operation.
	Remove(v V) ValueOp[K, V]
	// Commit the operation. Operations are assumed to be atomic.
	Commit() error
}

// KeyOrganizedRoutingTable is a routing table that organizes keys by value.
// The key inserted into the table must be unique.
type KeyOrganizedRoutingTable[K comparable, V fmt.Stringer] interface {
	RoutingTable[K, V]
	// AddKey adds a new key to the table. Return the value that is mapped to the key.
	// If the key already exists, return an error.
	AddKey(key K) (V, error)
	// RemoveKey removes a key from the table. Return the value that is mapped to the key.
	// If the key is not found, return an error.
	RemoveKey(key K) (V, error)
	// GetKeys returns the keys that are mapped to the given value.
	GetKeys(v V) []K
}

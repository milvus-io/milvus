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

package cache

import (
	"math"
	"reflect"
)

// Hash is an interface implemented by cache keys to
// override default hash function.
type Hash interface {
	Sum64() uint64
}

// sum calculates hash value of the given key.
func sum(k interface{}) uint64 {
	switch h := k.(type) {
	case Hash:
		return h.Sum64()
	case int:
		return hashU64(uint64(h))
	case int8:
		return hashU32(uint32(h))
	case int16:
		return hashU32(uint32(h))
	case int32:
		return hashU32(uint32(h))
	case int64:
		return hashU64(uint64(h))
	case uint:
		return hashU64(uint64(h))
	case uint8:
		return hashU32(uint32(h))
	case uint16:
		return hashU32(uint32(h))
	case uint32:
		return hashU32(h)
	case uint64:
		return hashU64(h)
	case uintptr:
		return hashU64(uint64(h))
	case float32:
		return hashU32(math.Float32bits(h))
	case float64:
		return hashU64(math.Float64bits(h))
	case bool:
		if h {
			return 1
		}
		return 0
	case string:
		return hashString(h)
	}
	// TODO: complex64 and complex128
	if h, ok := hashPointer(k); ok {
		return h
	}
	// TODO: use gob to encode k to bytes then hash.
	return 0
}

const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

func hashU64(v uint64) uint64 {
	// Inline code from hash/fnv to reduce memory allocations
	h := fnvOffset
	// for i := uint(0); i < 64; i += 8 {
	// 	h ^= (v >> i) & 0xFF
	// 	h *= fnvPrime
	// }
	h ^= (v >> 0) & 0xFF
	h *= fnvPrime
	h ^= (v >> 8) & 0xFF
	h *= fnvPrime
	h ^= (v >> 16) & 0xFF
	h *= fnvPrime
	h ^= (v >> 24) & 0xFF
	h *= fnvPrime
	h ^= (v >> 32) & 0xFF
	h *= fnvPrime
	h ^= (v >> 40) & 0xFF
	h *= fnvPrime
	h ^= (v >> 48) & 0xFF
	h *= fnvPrime
	h ^= (v >> 56) & 0xFF
	h *= fnvPrime
	return h
}

func hashU32(v uint32) uint64 {
	h := fnvOffset
	h ^= uint64(v>>0) & 0xFF
	h *= fnvPrime
	h ^= uint64(v>>8) & 0xFF
	h *= fnvPrime
	h ^= uint64(v>>16) & 0xFF
	h *= fnvPrime
	h ^= uint64(v>>24) & 0xFF
	h *= fnvPrime
	return h
}

// hashString calculates hash value using FNV-1a algorithm.
func hashString(data string) uint64 {
	// Inline code from hash/fnv to reduce memory allocations
	h := fnvOffset
	for _, b := range data {
		h ^= uint64(b)
		h *= fnvPrime
	}
	return h
}

func hashPointer(k interface{}) (uint64, bool) {
	v := reflect.ValueOf(k)
	switch v.Kind() {
	case reflect.Ptr, reflect.UnsafePointer, reflect.Func, reflect.Slice, reflect.Map, reflect.Chan:
		return hashU64(uint64(v.Pointer())), true
	default:
		return 0, false
	}
}

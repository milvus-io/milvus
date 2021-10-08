// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package typeutil

import (
	"encoding/binary"
	"unsafe"

	"github.com/spaolacci/murmur3"
)

// Hash32Bytes hashing a byte array to uint32
func Hash32Bytes(b []byte) (uint32, error) {
	h := murmur3.New32()
	if _, err := h.Write(b); err != nil {
		return 0, err
	}
	return h.Sum32() & 0x7fffffff, nil
}

// Hash32Uint64 hashing an uint64 nubmer to uint32
func Hash32Uint64(v uint64) (uint32, error) {
	// need unsafe package to get element byte size
	/* #nosec G103 */
	b := make([]byte, unsafe.Sizeof(v))
	binary.LittleEndian.PutUint64(b, v)
	return Hash32Bytes(b)
}

// Hash32Int64 hashing an int64 number to uint32
func Hash32Int64(v int64) (uint32, error) {
	return Hash32Uint64(uint64(v))
}

// Hash32String hashing a string to int64
func Hash32String(s string) (int64, error) {
	b := []byte(s)
	v, err := Hash32Bytes(b)
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

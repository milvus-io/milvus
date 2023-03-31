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
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func sumFNV(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

func sumFNVu64(v uint64) uint64 {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return sumFNV(b)
}

func sumFNVu32(v uint32) uint64 {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return sumFNV(b)
}

func TestSum(t *testing.T) {
	var tests = []struct {
		k interface{}
		h uint64
	}{
		{int(-1), sumFNVu64(^uint64(1) + 1)},
		{int8(-8), sumFNVu32(^uint32(8) + 1)},
		{int16(-16), sumFNVu32(^uint32(16) + 1)},
		{int32(-32), sumFNVu32(^uint32(32) + 1)},
		{int64(-64), sumFNVu64(^uint64(64) + 1)},
		{uint(1), sumFNVu64(1)},
		{uint8(8), sumFNVu32(8)},
		{uint16(16), sumFNVu32(16)},
		{uint32(32), sumFNVu32(32)},
		{uint64(64), sumFNVu64(64)},
		{byte(255), sumFNVu32(255)},
		{rune(1024), sumFNVu32(1024)},
		{true, 1},
		{false, 0},
		{float32(2.5), sumFNVu32(0x40200000)},
		{float64(2.5), sumFNVu64(0x4004000000000000)},
		/* #nosec G103 */
		{uintptr(unsafe.Pointer(t)), sumFNVu64(uint64(uintptr(unsafe.Pointer(t))))},
		{"", sumFNV(nil)},
		{"string", sumFNV([]byte("string"))},
		/* #nosec G103 */
		{t, sumFNVu64(uint64(uintptr(unsafe.Pointer(t))))},
		{(*testing.T)(nil), sumFNVu64(0)},
	}

	for _, tt := range tests {
		h := sum(tt.k)
		assert.Equal(t, h, tt.h, fmt.Sprintf("unexpected hash: %v (0x%x), key: %+v (%T), want: %v",
			h, h, tt.k, tt.k, tt.h))
	}
}

func BenchmarkSumInt(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sum(0x0105)
		}
	})
}

func BenchmarkSumString(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sum("09130105060103210913010506010321091301050601032109130105060103210913010506010321")
		}
	})
}

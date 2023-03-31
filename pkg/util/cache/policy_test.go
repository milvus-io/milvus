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
	"sync/atomic"
	"testing"
)

func cacheSize(c *cache) int {
	length := 0
	c.walk(func(*entry) {
		length++
	})
	return length
}

func BenchmarkCacheSegment(b *testing.B) {
	c := cache{}
	const count = 1 << 10
	entries := make([]*entry, count)
	for i := range entries {
		entries[i] = newEntry(i, i, uint64(i))
	}
	var n int32
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt32(&n, 1)
			c.getOrSet(entries[i&(count-1)])
			if i > 0 && i&0xf == 0 {
				c.delete(entries[(i-1)&(count-1)])
			}
		}
	})
}

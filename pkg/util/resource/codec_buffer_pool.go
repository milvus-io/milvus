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

package resource

import (
	"sync"

	"google.golang.org/grpc/mem"
)

// codecBufferPool is private to the release codec. Reused buffers are dirty:
// MarshalAppend and MaterializeToBuffer must overwrite every visible byte before
// publishing or decoding. Never use this pool for partially initialized output.
// Size classes and page-rounded oversized allocation match gRPC's default pool;
// reused buffers are not cleared. sync.Pool retention is opportunistic, not bounded.
type codecBufferPool struct {
	tiers    [5]sync.Pool
	fallback sync.Pool
}

var codecBufferSizes = [...]int{256, 4 << 10, 16 << 10, 32 << 10, 1 << 20}

func (p *codecBufferPool) Get(length int) *[]byte {
	if length > 0 {
		for i, size := range codecBufferSizes {
			if length > size {
				continue
			}
			if value := p.tiers[i].Get(); value != nil {
				b := value.(*[]byte)
				*b = (*b)[:length]
				return b
			}
			b := make([]byte, length, size)
			return &b
		}
	}
	if value := p.fallback.Get(); value != nil {
		b := value.(*[]byte)
		if cap(*b) >= length {
			*b = (*b)[:length]
			return b
		}
		p.fallback.Put(b)
	}
	capacity := (length + 4095) &^ 4095
	b := make([]byte, length, capacity)
	return &b
}

func (p *codecBufferPool) Put(b *[]byte) {
	capacity := cap(*b)
	if capacity == 0 {
		return
	}
	if capacity > codecBufferSizes[len(codecBufferSizes)-1] {
		p.fallback.Put(b)
		return
	}
	for i := len(codecBufferSizes) - 1; i >= 0; i-- {
		if capacity >= codecBufferSizes[i] {
			p.tiers[i].Put(b)
			return
		}
	}
}

var releaseCodecBufferPool mem.BufferPool = &codecBufferPool{}

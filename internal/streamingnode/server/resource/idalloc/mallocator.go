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

package idalloc

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
)

type mAllocator struct {
	allocator Allocator
}

func NewMAllocator(allocator Allocator) allocator.Interface {
	return &mAllocator{allocator: allocator}
}

func (m *mAllocator) Alloc(count uint32) (int64, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var (
		start int64 = 0
		end   int64 = 0
	)
	for i := 0; i < int(count)+1; i++ {
		id, err := m.allocator.Allocate(ctx)
		if err != nil {
			return 0, 0, err
		}
		if i == 0 {
			start = int64(id)
		}
		if i == int(count) {
			end = int64(id)
		}
	}
	return start, end, nil
}

func (m *mAllocator) AllocOne() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := m.allocator.Allocate(ctx)
	return int64(id), err
}

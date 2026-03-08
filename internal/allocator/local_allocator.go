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

package allocator

import (
	"fmt"
	"sync"
)

// localAllocator implements the Interface.
// It is constructed from a range of IDs.
// Once all IDs are allocated, an error will be returned.
type localAllocator struct {
	mu      sync.Mutex
	idStart int64
	idEnd   int64
}

func NewLocalAllocator(start, end int64) Interface {
	return &localAllocator{
		idStart: start,
		idEnd:   end,
	}
}

func (a *localAllocator) Alloc(count uint32) (int64, int64, error) {
	cnt := int64(count)
	if cnt <= 0 {
		return 0, 0, fmt.Errorf("non-positive count is not allowed, count=%d", cnt)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.idStart+cnt > a.idEnd {
		return 0, 0, fmt.Errorf("ID is exhausted, start=%d, end=%d, count=%d", a.idStart, a.idEnd, cnt)
	}
	start := a.idStart
	a.idStart += cnt
	return start, start + cnt, nil
}

func (a *localAllocator) AllocOne() (int64, error) {
	start, _, err := a.Alloc(1)
	return start, err
}

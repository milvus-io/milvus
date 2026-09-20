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

package dedup

import (
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/pkindex/authority"
)

// stripeLocks is lock striping: primary keys are hashed onto a fixed number of mutexes.
type stripeLocks struct {
	locks []sync.Mutex
}

func newStripeLocks(n int) *stripeLocks {
	if n <= 0 {
		n = 1
	}
	return &stripeLocks{locks: make([]sync.Mutex, n)}
}

// acquire locks the stripes of pks in ascending stripe order, so that two
// callers with overlapping keys can never deadlock.
func (s *stripeLocks) acquire(pks []authority.PK) *StripeGuard {
	seen := make(map[int]struct{}, len(pks))
	stripes := make([]int, 0, len(pks))
	for _, pk := range pks {
		idx := int(pk.Hash() % uint64(len(s.locks)))
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		stripes = append(stripes, idx)
	}
	sort.Ints(stripes)
	for _, idx := range stripes {
		s.locks[idx].Lock()
	}
	return &StripeGuard{locks: s, stripes: stripes}
}

// StripeGuard holds the stripes of one decision until Release.
type StripeGuard struct {
	locks    *stripeLocks
	stripes  []int
	released bool
}

// Release unlocks in descending order. It is safe to call on nil and more than once.
// A guard is owned by one Intent and is not released concurrently.
func (g *StripeGuard) Release() {
	if g == nil || g.released {
		return
	}
	g.released = true
	for i := len(g.stripes) - 1; i >= 0; i-- {
		g.locks.locks[g.stripes[i]].Unlock()
	}
}

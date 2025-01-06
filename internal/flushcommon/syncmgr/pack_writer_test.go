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

package syncmgr

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/allocator"
)

func TestNextID(t *testing.T) {
	al := allocator.NewMockGIDAllocator()
	i := int64(0)
	al.AllocF = func(count uint32) (int64, int64, error) {
		rt := i
		i += int64(count)
		return rt, int64(count), nil
	}
	al.AllocOneF = func() (allocator.UniqueID, error) {
		rt := i
		i++
		return rt, nil
	}
	bw := NewBulkPackWriter(nil, nil, al)
	bw.prefetchIDs(new(SyncPack).WithFlush())

	t.Run("normal_next", func(t *testing.T) {
		id := bw.nextID()
		assert.Equal(t, int64(0), id)
	})
	t.Run("id_exhausted", func(t *testing.T) {
		assert.Panics(t, func() {
			bw.nextID()
		})
	})
}

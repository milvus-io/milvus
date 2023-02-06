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

package delegator

import (
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type deleteBufferItem struct {
	partitionID int64
	deleteData  storage.DeleteData
}

// deleteBuffer caches L0 delete buffer for remote segments.
type deleteBuffer struct {
	// timestamp => DeleteData
	cache *typeutil.SkipList[uint64, []deleteBufferItem]
}

// Cache delete data.
func (b *deleteBuffer) Cache(timestamp uint64, data []deleteBufferItem) {
	b.cache.Upsert(timestamp, data)
}

func (b *deleteBuffer) List(since uint64) [][]deleteBufferItem {
	return b.cache.ListAfter(since, false)
}

func (b *deleteBuffer) TruncateBefore(ts uint64) {
	b.cache.TruncateBefore(ts)
}

func NewDeleteBuffer() *deleteBuffer {
	cache, _ := typeutil.NewSkipList[uint64, []deleteBufferItem]()
	return &deleteBuffer{
		cache: cache,
	}
}

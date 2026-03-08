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

package writebuffer

import (
	"github.com/milvus-io/milvus/internal/storage"
)

// stats buffer used for bm25 stats
type statsBuffer struct {
	bm25Stats map[int64]*storage.BM25Stats
}

func (b *statsBuffer) Buffer(stats map[int64]*storage.BM25Stats) {
	for fieldID, stat := range stats {
		if fieldMeta, ok := b.bm25Stats[fieldID]; ok {
			fieldMeta.Merge(stat)
		} else {
			b.bm25Stats[fieldID] = stat
		}
	}
}

func (b *statsBuffer) yieldBuffer() map[int64]*storage.BM25Stats {
	result := b.bm25Stats
	b.bm25Stats = make(map[int64]*storage.BM25Stats)
	return result
}

func newStatsBuffer() *statsBuffer {
	return &statsBuffer{
		bm25Stats: make(map[int64]*storage.BM25Stats),
	}
}

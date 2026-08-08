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

// statsBuffer accumulates a segment's BM25 stats until the next flush yields
// them.
//
// Its footprint is deliberately NOT accounted against the write buffer's memory
// budget. It used to be, and that was worse than not counting it: the bytes were
// folded into insertBuffer.size, handed to the task as payload, and released by
// Prepare — while SyncPack.ReleaseData deliberately keeps bm25Stats alive until
// Commit. So the accounting claimed the memory was gone during exactly the
// window it was still held. BM25 stats grow with distinct terms rather than with
// rows, so they are small next to the row payload; counting them consistently
// nowhere beats counting them wrong.
type statsBuffer struct {
	bm25Stats map[int64]*storage.BM25Stats
}

func (b *statsBuffer) Buffer(stats map[int64]*storage.BM25Stats) {
	for fieldID, stat := range stats {
		if stat == nil {
			continue
		}
		if fieldMeta, ok := b.bm25Stats[fieldID]; ok && fieldMeta != nil {
			fieldMeta.Merge(stat)
			continue
		}
		b.bm25Stats[fieldID] = stat
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

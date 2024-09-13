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

package metacache

import (
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
)

type SegmentBM25Stats struct {
	mut   sync.RWMutex
	stats map[int64]*storage.BM25Stats
}

func (s *SegmentBM25Stats) Merge(stats map[int64]*storage.BM25Stats) {
	s.mut.Lock()
	defer s.mut.Unlock()

	for fieldID, current := range stats {
		if history, ok := s.stats[fieldID]; !ok {
			s.stats[fieldID] = current.Clone()
		} else {
			history.Merge(current)
		}
	}
}

func (s *SegmentBM25Stats) Serialize() (map[int64][]byte, map[int64]int64, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	result := make(map[int64][]byte)
	numRow := make(map[int64]int64)
	for fieldID, stats := range s.stats {
		bytes, err := stats.Serialize()
		if err != nil {
			log.Warn("serialize history bm25 stats failed", zap.Int64("fieldID", fieldID))
			return nil, nil, err
		}
		result[fieldID] = bytes
		numRow[fieldID] = stats.NumRow()
	}
	return result, numRow, nil
}

func NewEmptySegmentBM25Stats() *SegmentBM25Stats {
	return &SegmentBM25Stats{
		stats: make(map[int64]*storage.BM25Stats),
	}
}

func NewSegmentBM25Stats(stats map[int64]*storage.BM25Stats) *SegmentBM25Stats {
	return &SegmentBM25Stats{
		stats: stats,
	}
}

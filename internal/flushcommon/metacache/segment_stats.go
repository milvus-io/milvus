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

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// SegmentStats holds the growing segment's cumulative StatisticsCollector,
// fed per sync and read at flush. Mirrors SegmentBM25Stats: shared by pointer
// across SegmentInfo.Clone, guarded by its own mutex. Never persisted.
type SegmentStats struct {
	mut       sync.RWMutex
	collector *storage.StatisticsCollector
}

// NewEmptySegmentStats returns a SegmentStats with an empty collector — used
// for a brand-new growing segment that has no prior Stats.
func NewEmptySegmentStats() *SegmentStats {
	return &SegmentStats{collector: storage.NewStatisticsCollector()}
}

// NewSegmentStatsFromStats reseeds a SegmentStats from a segment's persisted
// Statistics, restoring the cumulative collector on datanode recovery so a
// growing segment keeps accumulating from where it left off (rather than
// undercounting from empty). stats is the SegmentInfo.Stats loaded from etcd;
// numRows its persisted row count. nil stats yields an empty collector.
func NewSegmentStatsFromStats(stats *datapb.Statistics, numRows int64) *SegmentStats {
	return &SegmentStats{collector: storage.NewStatisticsCollectorFromStats(stats, numRows)}
}

// Digest folds one sync's writes into the cumulative collector.
func (s *SegmentStats) Digest(inserts map[int64]*datapb.FieldBinlog, delta *datapb.FieldBinlog, statsBlobSize, rows int64, tsFrom, tsTo uint64) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.collector.Digest(inserts, delta, statsBlobSize, rows, tsFrom, tsTo)
}

// Publish returns the cumulative Statistics digested so far.
// Returns nil when nothing has been digested.
func (s *SegmentStats) Publish() *datapb.Statistics {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.collector.Publish()
}

// Clone returns a SegmentStats with a deep-copied collector. SegmentInfo.Clone
// shares the *SegmentStats by pointer; this is for callers that need an
// independent snapshot.
func (s *SegmentStats) Clone() *SegmentStats {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return &SegmentStats{collector: s.collector.Clone()}
}

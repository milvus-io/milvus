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

package pkoracle

import (
	"sync"

	bloom "github.com/bits-and-blooms/bloom/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ Candidate = (*BloomFilterSet)(nil)

// BloomFilterSet is one implementation of Candidate with bloom filter in statslog.
type BloomFilterSet struct {
	statsMutex   sync.RWMutex
	segmentID    int64
	paritionID   int64
	segType      commonpb.SegmentState
	currentStat  *storage.PkStatistics
	historyStats []*storage.PkStatistics

	kHashFunc uint
}

// MayPkExist returns whether any bloom filters returns positive.
func (s *BloomFilterSet) MayPkExist(pk storage.PrimaryKey) bool {
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()
	if s.currentStat != nil && s.currentStat.PkExist(pk) {
		return true
	}

	// for sealed, if one of the stats shows it exist, then we have to check it
	for _, historyStat := range s.historyStats {
		if historyStat.PkExist(pk) {
			return true
		}
	}
	return false
}

func (s *BloomFilterSet) TestLocations(pk storage.PrimaryKey, locs []uint64) bool {
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()

	if s.currentStat != nil {
		k := s.currentStat.PkFilter.K()
		if k > uint(len(locs)) {
			log.Error("locations num is less than hash func num",
				zap.Int("locationNum", len(locs)),
				zap.Uint("hashFuncNum", k),
				zap.Int64("segmentID", s.segmentID))
			return true
		}

		if s.currentStat.TestLocations(pk, locs[:k]) {
			return true
		}
	}

	// for sealed, if one of the stats shows it exist, then we have to check it
	for _, historyStat := range s.historyStats {
		k := historyStat.PkFilter.K()
		if k > uint(len(locs)) {
			log.Error("locations num is less than hash func num",
				zap.Int("locationNum", len(locs)),
				zap.Uint("hashFuncNum", k),
				zap.Int64("segmentID", s.segmentID))
			// make it false positive
			return true
		}
		if historyStat.TestLocations(pk, locs[:k]) {
			return true
		}
	}
	return false
}

func (s *BloomFilterSet) GetHashFuncNum() uint {
	return s.kHashFunc
}

// ID implement candidate.
func (s *BloomFilterSet) ID() int64 {
	return s.segmentID
}

// Partition implements candidate.
func (s *BloomFilterSet) Partition() int64 {
	return s.paritionID
}

// Type implements candidate.
func (s *BloomFilterSet) Type() commonpb.SegmentState {
	return s.segType
}

// UpdateBloomFilter updates currentStats with provided pks.
func (s *BloomFilterSet) UpdateBloomFilter(pks []storage.PrimaryKey) {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()

	if s.currentStat == nil {
		m, k := bloom.EstimateParameters(paramtable.Get().CommonCfg.BloomFilterSize.GetAsUint(),
			paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat())
		if k > s.kHashFunc {
			s.kHashFunc = k
		}
		s.currentStat = &storage.PkStatistics{
			PkFilter: bloom.New(m, k),
		}
	}

	for _, pk := range pks {
		s.currentStat.UpdateMinMax(pk)
		switch pk.Type() {
		case schemapb.DataType_Int64:
			buf := make([]byte, 8)
			int64Value := pk.(*storage.Int64PrimaryKey).Value
			common.Endian.PutUint64(buf, uint64(int64Value))
			s.currentStat.PkFilter.Add(buf)
		case schemapb.DataType_VarChar:
			stringValue := pk.(*storage.VarCharPrimaryKey).Value
			s.currentStat.PkFilter.AddString(stringValue)
		default:
			log.Error("failed to update bloomfilter", zap.Any("PK type", pk.Type()))
			panic("failed to update bloomfilter")
		}
	}
}

// AddHistoricalStats add loaded historical stats.
func (s *BloomFilterSet) AddHistoricalStats(stats *storage.PkStatistics) {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()

	if stats.PkFilter.K() > s.kHashFunc {
		s.kHashFunc = stats.PkFilter.K()
	}
	s.historyStats = append(s.historyStats, stats)
}

// NewBloomFilterSet returns a new BloomFilterSet.
func NewBloomFilterSet(segmentID int64, paritionID int64, segType commonpb.SegmentState) *BloomFilterSet {
	bfs := &BloomFilterSet{
		segmentID:  segmentID,
		paritionID: paritionID,
		segType:    segType,
	}
	// does not need to init current
	return bfs
}

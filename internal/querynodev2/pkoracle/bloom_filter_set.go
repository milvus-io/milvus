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
	bloom "github.com/bits-and-blooms/bloom/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ Candidate = (*BloomFilterSet)(nil)

// BloomFilterSet is one implementation of Candidate with bloom filter in statslog.
type BloomFilterSet struct {
	segmentID    int64
	paritionID   int64
	segType      commonpb.SegmentState
	currentStat  *storage.PkStatistics
	historyStats typeutil.ConcurrentSet[*storage.PkStatistics]
}

// MayPkExist returns whether any bloom filters returns positive.
func (s *BloomFilterSet) MayPkExist(pk storage.PrimaryKey) bool {
	if s.currentStat != nil && s.currentStat.PkExist(pk) {
		return true
	}

	// for sealed, if one of the stats shows it exist, then we have to check it
	exist := false
	s.historyStats.Range(func(historyStat *storage.PkStatistics) bool {
		if historyStat.PkExist(pk) {
			exist = true
			return false
		}
		return true
	})

	return exist
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
	if s.currentStat == nil {
		s.currentStat = &storage.PkStatistics{
			PkFilter: bloom.NewWithEstimates(paramtable.Get().CommonCfg.BloomFilterSize.GetAsUint(),
				paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat()),
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
	s.historyStats.Insert(stats)
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

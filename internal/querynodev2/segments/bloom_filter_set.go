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

package segments

import (
	"sync"

	bloom "github.com/bits-and-blooms/bloom/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
)

type bloomFilterSet struct {
	statsMutex   sync.RWMutex
	currentStat  *storage.PkStatistics
	historyStats []*storage.PkStatistics
}

func newBloomFilterSet() *bloomFilterSet {
	return &bloomFilterSet{}
}

// MayPkExist returns whether any bloom filters returns positive.
func (s *bloomFilterSet) MayPkExist(pk storage.PrimaryKey) bool {
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

// UpdateBloomFilter updates currentStats with provided pks.
func (s *bloomFilterSet) UpdateBloomFilter(pks []storage.PrimaryKey) {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()

	if s.currentStat == nil {
		s.initCurrentStat()
	}

	buf := make([]byte, 8)
	for _, pk := range pks {
		s.currentStat.UpdateMinMax(pk)
		switch pk.Type() {
		case schemapb.DataType_Int64:
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
func (s *bloomFilterSet) AddHistoricalStats(stats *storage.PkStatistics) {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()

	s.historyStats = append(s.historyStats, stats)
}

// initCurrentStat initialize currentStats if nil.
// Note: invoker shall acquire statsMutex lock first.
func (s *bloomFilterSet) initCurrentStat() {
	s.currentStat = &storage.PkStatistics{
		PkFilter: bloom.NewWithEstimates(storage.BloomFilterSize, storage.MaxBloomFalsePositive),
	}
}

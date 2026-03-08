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

/*
#cgo pkg-config: milvus_core

#include "segcore/load_index_c.h"
*/
import "C"

import (
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

	// Resource tracking
	trackedSize     int64 // memory size that was charged
	resourceCharged bool  // tracks whether memory resources were charged for this bloom filter set
}

// MayPkExist returns whether any bloom filters returns positive.
func (s *BloomFilterSet) MayPkExist(lc *storage.LocationsCache) bool {
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()
	if s.currentStat != nil && s.currentStat.TestLocationCache(lc) {
		return true
	}

	// for sealed, if one of the stats shows it exist, then we have to check it
	for _, historyStat := range s.historyStats {
		if historyStat.TestLocationCache(lc) {
			return true
		}
	}
	return false
}

func (s *BloomFilterSet) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()

	hits := make([]bool, lc.Size())
	if s.currentStat != nil {
		s.currentStat.BatchPkExist(lc, hits)
	}

	for _, bf := range s.historyStats {
		bf.BatchPkExist(lc, hits)
	}
	return hits
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

// Get stats
func (s *BloomFilterSet) Stats() *storage.PkStatistics {
	return s.currentStat
}

// Have BloomFilter exist
func (s *BloomFilterSet) BloomFilterExist() bool {
	return s.currentStat != nil || s.historyStats != nil
}

// UpdateBloomFilter updates currentStats with provided pks.
func (s *BloomFilterSet) UpdateBloomFilter(pks []storage.PrimaryKey) {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()

	if s.currentStat == nil {
		s.currentStat = &storage.PkStatistics{
			PkFilter: bloomfilter.NewBloomFilterWithType(
				paramtable.Get().CommonCfg.BloomFilterSize.GetAsUint(),
				paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
				paramtable.Get().CommonCfg.BloomFilterType.GetValue(),
			),
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

	s.historyStats = append(s.historyStats, stats)
}

// MemSize returns the total memory size of all bloom filters in bytes.
// This includes both currentStat and all historyStats.
func (s *BloomFilterSet) MemSize() int64 {
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()

	var size int64
	if s.currentStat != nil && s.currentStat.PkFilter != nil {
		size += int64(s.currentStat.PkFilter.Cap() / 8) // Cap returns bits, convert to bytes
	}
	for _, stats := range s.historyStats {
		if stats != nil && stats.PkFilter != nil {
			size += int64(stats.PkFilter.Cap() / 8)
		}
	}
	return size
}

// Charge charges memory resource for this bloom filter set via caching layer.
// Safe to call multiple times - only charges once.
func (s *BloomFilterSet) Charge() {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()

	if s.resourceCharged {
		return // Already charged
	}

	size := s.memSizeLocked()
	if size > 0 {
		C.ChargeLoadedResource(C.CResourceUsage{
			memory_bytes: C.int64_t(size),
			disk_bytes:   0,
		})
		s.trackedSize = size
		s.resourceCharged = true
		log.Debug("charged bloom filter resource",
			zap.Int64("segmentID", s.segmentID),
			zap.Int64("size", size))
	}
}

// Refund refunds any charged resources. Safe to call multiple times.
func (s *BloomFilterSet) Refund() {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()

	if !s.resourceCharged || s.trackedSize <= 0 {
		return
	}

	C.RefundLoadedResource(C.CResourceUsage{
		memory_bytes: C.int64_t(s.trackedSize),
		disk_bytes:   0,
	})
	log.Debug("refunded bloom filter resource",
		zap.Int64("segmentID", s.segmentID),
		zap.Int64("size", s.trackedSize))
	s.trackedSize = 0
	s.resourceCharged = false
}

// IsResourceCharged returns whether memory resources have been charged for this bloom filter set.
func (s *BloomFilterSet) IsResourceCharged() bool {
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()
	return s.resourceCharged
}

// SetResourceCharged sets the resourceCharged flag and trackedSize for testing purposes.
// This allows tests to simulate charged state without calling the actual C code.
func (s *BloomFilterSet) SetResourceCharged(charged bool) {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()
	s.resourceCharged = charged
	if charged {
		s.trackedSize = s.memSizeLocked()
	} else {
		s.trackedSize = 0
	}
}

// memSizeLocked returns the total memory size without acquiring the lock.
// Caller must hold statsMutex.
func (s *BloomFilterSet) memSizeLocked() int64 {
	var size int64
	if s.currentStat != nil && s.currentStat.PkFilter != nil {
		size += int64(s.currentStat.PkFilter.Cap() / 8)
	}
	for _, stats := range s.historyStats {
		if stats != nil && stats.PkFilter != nil {
			size += int64(stats.PkFilter.Cap() / 8)
		}
	}
	return size
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

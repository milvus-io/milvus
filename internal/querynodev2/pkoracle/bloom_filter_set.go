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
import (
	"C"
)

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ Candidate = (*BloomFilterSet)(nil)

const (
	initialBloomFilterRetryDelay = time.Second
	maxBloomFilterRetryDelay     = 30 * time.Second
)

type lazyBloomFilter struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   atomic.Bool

	mu         sync.Mutex
	load       func(context.Context, func([]*storage.PkStatistics)) error
	retryAt    time.Time
	retryDelay time.Duration
}

// BloomFilterSet is one implementation of Candidate with bloom filter in statslog.
type BloomFilterSet struct {
	statsMutex   sync.RWMutex
	segmentID    int64
	partitionID  int64
	segType      commonpb.SegmentState
	currentStat  *storage.PkStatistics
	historyStats []*storage.PkStatistics
	lazy         *lazyBloomFilter

	// Resource tracking
	trackedSize     int64 // memory size that was charged
	resourceCharged bool  // tracks whether memory resources were charged for this bloom filter set
}

// MayPkExist returns whether any bloom filters returns positive.
func (s *BloomFilterSet) MayPkExist(lc *storage.LocationsCache) bool {
	s.loadOnDemand()
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()
	if s.lazy != nil && len(s.historyStats) == 0 {
		return true
	}
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
	s.loadOnDemand()
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()

	hits := make([]bool, lc.Size())
	if s.lazy != nil && len(s.historyStats) == 0 {
		for i := range hits {
			hits[i] = true
		}
		return hits
	}
	if s.currentStat != nil {
		s.currentStat.BatchPkExist(lc, hits)
	}

	for _, bf := range s.historyStats {
		bf.BatchPkExist(lc, hits)
	}
	return hits
}

// ID implements Candidate.
func (s *BloomFilterSet) ID() int64 {
	return s.segmentID
}

// Partition implements Candidate.
func (s *BloomFilterSet) Partition() int64 {
	return s.partitionID
}

// Type implements Candidate.
func (s *BloomFilterSet) Type() commonpb.SegmentState {
	return s.segType
}

// Stats returns the current bloom filter statistics.
func (s *BloomFilterSet) Stats() *storage.PkStatistics {
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()
	return s.currentStat
}

// PkCandidateExist reports whether bloom filter data has been loaded (current or historical).
func (s *BloomFilterSet) PkCandidateExist() bool {
	s.loadOnDemand()
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()
	return s.currentStat != nil || len(s.historyStats) > 0
}

// UpdatePkCandidate updates currentStats with provided pks.
func (s *BloomFilterSet) UpdatePkCandidate(pks []storage.PrimaryKey) {
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
			mlog.Error(context.TODO(), "failed to update bloomfilter", mlog.Any("PK type", pk.Type()))
			panic("failed to update bloomfilter")
		}
	}
}

// GetMinPk returns the global minimum PK across all statistics (current + historical).
// Returns nil if no statistics with a valid MinPK are available.
func (s *BloomFilterSet) GetMinPk() *storage.PrimaryKey {
	s.loadOnDemand()
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()

	var minPk storage.PrimaryKey
	if s.currentStat != nil && s.currentStat.MinPK != nil {
		minPk = s.currentStat.MinPK
	}
	for _, stat := range s.historyStats {
		if stat == nil || stat.MinPK == nil {
			continue
		}
		if minPk == nil || stat.MinPK.LT(minPk) {
			minPk = stat.MinPK
		}
	}
	if minPk == nil {
		return nil
	}
	return &minPk
}

// GetMaxPk returns the global maximum PK across all statistics (current + historical).
// Returns nil if no statistics with a valid MaxPK are available.
func (s *BloomFilterSet) GetMaxPk() *storage.PrimaryKey {
	s.loadOnDemand()
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()

	var maxPk storage.PrimaryKey
	if s.currentStat != nil && s.currentStat.MaxPK != nil {
		maxPk = s.currentStat.MaxPK
	}
	for _, stat := range s.historyStats {
		if stat == nil || stat.MaxPK == nil {
			continue
		}
		if maxPk == nil || stat.MaxPK.GT(maxPk) {
			maxPk = stat.MaxPK
		}
	}
	if maxPk == nil {
		return nil
	}
	return &maxPk
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

	return s.memSizeLocked()
}

// Charge charges memory resource for this bloom filter set via caching layer.
// Safe to call multiple times - only charges once.
func (s *BloomFilterSet) Charge() {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()

	s.chargeLocked(context.TODO())
}

func (s *BloomFilterSet) chargeLocked(ctx context.Context) {
	if s.resourceCharged || (s.lazy != nil && s.lazy.ctx.Err() != nil) {
		return
	}

	size := s.memSizeLocked()
	if size > 0 {
		C.ChargeLoadedResource(C.CResourceUsage{
			memory_bytes: C.int64_t(size),
			disk_bytes:   0,
		})
		s.trackedSize = size
		s.resourceCharged = true
		mlog.Debug(ctx, "charged bloom filter resource",
			mlog.FieldSegmentID(s.segmentID),
			mlog.Int64("size", size))
	}
}

// Refund refunds any charged resources. Safe to call multiple times.
func (s *BloomFilterSet) Refund() {
	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()

	if s.lazy != nil {
		s.lazy.cancel()
	}
	if !s.resourceCharged || s.trackedSize <= 0 {
		return
	}

	C.RefundLoadedResource(C.CResourceUsage{
		memory_bytes: C.int64_t(s.trackedSize),
		disk_bytes:   0,
	})
	mlog.Debug(context.TODO(), "refunded bloom filter resource",
		mlog.FieldSegmentID(s.segmentID),
		mlog.Int64("size", s.trackedSize))
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
func NewBloomFilterSet(segmentID int64, partitionID int64, segType commonpb.SegmentState) *BloomFilterSet {
	return &BloomFilterSet{
		segmentID:   segmentID,
		partitionID: partitionID,
		segType:     segType,
	}
}

// NewLazyBloomFilterSet defers sealed PK stats loading until the first PK check.
// load publishes complete stats synchronously while its loading reservation is held.
func NewLazyBloomFilterSet(ctx context.Context, segmentID, partitionID int64, load func(context.Context, func([]*storage.PkStatistics)) error) *BloomFilterSet {
	bfs := NewBloomFilterSet(segmentID, partitionID, commonpb.SegmentState_Sealed)
	// Segment preparation can finish before the candidate is first used.
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	bfs.lazy = &lazyBloomFilter{ctx: ctx, cancel: cancel, load: load}
	return bfs
}

func (s *BloomFilterSet) loadOnDemand() {
	lazy := s.lazy
	if lazy == nil || lazy.done.Load() {
		return
	}
	lazy.mu.Lock()
	defer lazy.mu.Unlock()
	if lazy.done.Load() || lazy.ctx.Err() != nil || time.Now().Before(lazy.retryAt) {
		return
	}

	err := lazy.load(lazy.ctx, func(stats []*storage.PkStatistics) {
		s.statsMutex.Lock()
		defer s.statsMutex.Unlock()
		if lazy.ctx.Err() == nil {
			s.historyStats = stats
			s.chargeLocked(lazy.ctx)
		}
	})
	if err == nil {
		lazy.load = nil
		lazy.done.Store(true)
		return
	}
	if lazy.ctx.Err() != nil {
		return
	}
	lazy.retryDelay = min(max(initialBloomFilterRetryDelay, 2*lazy.retryDelay), maxBloomFilterRetryDelay)
	lazy.retryAt = time.Now().Add(lazy.retryDelay)
	mlog.Warn(lazy.ctx, "failed to load sealed segment bloom filter",
		mlog.FieldSegmentID(s.segmentID),
		mlog.Duration("retryDelay", lazy.retryDelay),
		mlog.Err(err))
}

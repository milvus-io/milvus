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

package cache

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Stats is statistics about performance of a cache.
type Stats struct {
	HitCount         uint64
	MissCount        uint64
	LoadSuccessCount uint64
	LoadErrorCount   uint64
	TotalLoadTime    time.Duration
	EvictionCount    uint64
}

// RequestCount returns a total of HitCount and MissCount.
func (s *Stats) RequestCount() uint64 {
	return s.HitCount + s.MissCount
}

// HitRate returns the ratio of cache requests which were hits.
func (s *Stats) HitRate() float64 {
	total := s.RequestCount()
	if total == 0 {
		return 1.0
	}
	return float64(s.HitCount) / float64(total)
}

// MissRate returns the ratio of cache requests which were misses.
func (s *Stats) MissRate() float64 {
	total := s.RequestCount()
	if total == 0 {
		return 0.0
	}
	return float64(s.MissCount) / float64(total)
}

// LoadErrorRate returns the ratio of cache loading attempts which returned errors.
func (s *Stats) LoadErrorRate() float64 {
	total := s.LoadSuccessCount + s.LoadErrorCount
	if total == 0 {
		return 0.0
	}
	return float64(s.LoadErrorCount) / float64(total)
}

// AverageLoadPenalty returns the average time spent loading new values.
func (s *Stats) AverageLoadPenalty() time.Duration {
	total := s.LoadSuccessCount + s.LoadErrorCount
	if total == 0 {
		return 0.0
	}
	return s.TotalLoadTime / time.Duration(total)
}

// String returns a string representation of this statistics.
func (s *Stats) String() string {
	return fmt.Sprintf("hits: %d, misses: %d, successes: %d, errors: %d, time: %s, evictions: %d",
		s.HitCount, s.MissCount, s.LoadSuccessCount, s.LoadErrorCount, s.TotalLoadTime, s.EvictionCount)
}

// StatsCounter accumulates statistics of a cache.
type StatsCounter interface {
	// RecordHits records cache hits.
	RecordHits(count uint64)

	// RecordMisses records cache misses.
	RecordMisses(count uint64)

	// RecordLoadSuccess records successful load of a new entry.
	RecordLoadSuccess(loadTime time.Duration)

	// RecordLoadError records failed load of a new entry.
	RecordLoadError(loadTime time.Duration)

	// RecordEviction records eviction of an entry from the cache.
	RecordEviction()

	// Snapshot writes snapshot of this counter values to the given Stats pointer.
	Snapshot(*Stats)
}

// statsCounter is a simple implementation of StatsCounter.
type statsCounter struct {
	Stats
}

// RecordHits increases HitCount atomically.
func (s *statsCounter) RecordHits(count uint64) {
	atomic.AddUint64(&s.Stats.HitCount, count)
}

// RecordMisses increases MissCount atomically.
func (s *statsCounter) RecordMisses(count uint64) {
	atomic.AddUint64(&s.Stats.MissCount, count)
}

// RecordLoadSuccess increases LoadSuccessCount atomically.
func (s *statsCounter) RecordLoadSuccess(loadTime time.Duration) {
	atomic.AddUint64(&s.Stats.LoadSuccessCount, 1)
	atomic.AddInt64((*int64)(&s.Stats.TotalLoadTime), int64(loadTime))
}

// RecordLoadError increases LoadErrorCount atomically.
func (s *statsCounter) RecordLoadError(loadTime time.Duration) {
	atomic.AddUint64(&s.Stats.LoadErrorCount, 1)
	atomic.AddInt64((*int64)(&s.Stats.TotalLoadTime), int64(loadTime))
}

// RecordEviction increases EvictionCount atomically.
func (s *statsCounter) RecordEviction() {
	atomic.AddUint64(&s.Stats.EvictionCount, 1)
}

// Snapshot copies current stats to t.
func (s *statsCounter) Snapshot(t *Stats) {
	t.HitCount = atomic.LoadUint64(&s.HitCount)
	t.MissCount = atomic.LoadUint64(&s.MissCount)
	t.LoadSuccessCount = atomic.LoadUint64(&s.LoadSuccessCount)
	t.LoadErrorCount = atomic.LoadUint64(&s.LoadErrorCount)
	t.TotalLoadTime = time.Duration(atomic.LoadInt64((*int64)(&s.TotalLoadTime)))
	t.EvictionCount = atomic.LoadUint64(&s.EvictionCount)
}

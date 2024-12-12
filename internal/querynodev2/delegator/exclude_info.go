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

package delegator

import (
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

type ExcludedSegments struct {
	mu            sync.RWMutex
	segments      map[int64]uint64 // segmentID -> Excluded TS
	lastClean     atomic.Time
	cleanInterval time.Duration
}

func NewExcludedSegments(cleanInterval time.Duration) *ExcludedSegments {
	return &ExcludedSegments{
		segments:      make(map[int64]uint64),
		cleanInterval: cleanInterval,
	}
}

func (s *ExcludedSegments) Insert(excludeInfo map[int64]uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for segmentID, ts := range excludeInfo {
		log.Ctx(context.TODO()).Debug("add exclude info",
			zap.Int64("segmentID", segmentID),
			zap.Uint64("ts", ts),
		)
		s.segments[segmentID] = ts
	}
}

// return false if segment has been excluded
func (s *ExcludedSegments) Verify(segmentID int64, ts uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if excludeTs, ok := s.segments[segmentID]; ok && ts <= excludeTs {
		return false
	}
	return true
}

func (s *ExcludedSegments) CleanInvalid(ts uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	invalidExcludedInfos := []int64{}
	for segmentsID, excludeTs := range s.segments {
		if excludeTs < ts {
			invalidExcludedInfos = append(invalidExcludedInfos, segmentsID)
		}
	}

	for _, segmentID := range invalidExcludedInfos {
		delete(s.segments, segmentID)
		log.Info("remove segment from exclude info", zap.Int64("segmentID", segmentID))
	}
	s.lastClean.Store(time.Now())
}

func (s *ExcludedSegments) ShouldClean() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Since(s.lastClean.Load()) > s.cleanInterval
}

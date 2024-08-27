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

package segment

import (
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// SegmentInfo wraps datapb.SegmentInfo and patches some extra info on it
type SegmentInfo struct {
	*datapb.SegmentInfo
	currRows      int64
	allocations   []*Allocation
	lastFlushTime time.Time
	isCompacting  bool
	// a cache to avoid calculate twice
	size            atomic.Int64
	deltaRowcount   atomic.Int64
	lastWrittenTime time.Time
}

// NewSegmentInfo create `SegmentInfo` wrapper from `datapb.SegmentInfo`
// assign current rows to last checkpoint and pre-allocate `allocations` slice
// Note that the allocation information is not preserved,
// the worst case scenario is to have a segment with twice size we expects
func NewSegmentInfo(info *datapb.SegmentInfo) *SegmentInfo {
	s := &SegmentInfo{
		SegmentInfo: info,
		currRows:    info.GetNumOfRows(),
	}
	// setup growing fields
	if s.GetState() == commonpb.SegmentState_Growing {
		s.allocations = make([]*Allocation, 0, 16)
		s.lastFlushTime = time.Now().Add(-1 * paramtable.Get().DataCoordCfg.SegmentFlushInterval.GetAsDuration(time.Second))
		// A growing segment from recovery can be also considered idle.
		s.lastWrittenTime = getZeroTime()
	}
	// mark as uninitialized
	s.deltaRowcount.Store(-1)
	return s
}

// Clone deep clone the segment info and return a new instance
func (s *SegmentInfo) Clone(opts ...SegmentOperator) *SegmentInfo {
	info := typeutil.Clone(s.SegmentInfo)
	cloned := &SegmentInfo{
		SegmentInfo:   info,
		currRows:      s.currRows,
		allocations:   s.allocations,
		lastFlushTime: s.lastFlushTime,
		isCompacting:  s.isCompacting,
		// cannot copy size, since binlog may be changed
		lastWrittenTime: s.lastWrittenTime,
	}
	for _, opt := range opts {
		opt.UpdateSegment(cloned)
	}

	// invalidate cache since segment info maybe updated
	cloned.size.Store(-1)
	cloned.deltaRowcount.Store(-1)

	return cloned
}

// ShadowClone shadow clone the segment and return a new instance
func (s *SegmentInfo) ShadowClone(opts ...SegmentOperator) *SegmentInfo {
	cloned := &SegmentInfo{
		SegmentInfo:     s.SegmentInfo,
		currRows:        s.currRows,
		allocations:     s.allocations,
		lastFlushTime:   s.lastFlushTime,
		isCompacting:    s.isCompacting,
		lastWrittenTime: s.lastWrittenTime,
	}
	cloned.size.Store(s.size.Load())
	cloned.deltaRowcount.Store(s.deltaRowcount.Load())

	for _, opt := range opts {
		opt.UpdateSegment(cloned)
	}
	return cloned
}

func (s *SegmentInfo) IsDeltaLogExists(logID int64) bool {
	for _, deltaLogs := range s.GetDeltalogs() {
		for _, l := range deltaLogs.GetBinlogs() {
			if l.GetLogID() == logID {
				return true
			}
		}
	}
	return false
}

func (s *SegmentInfo) IsStatsLogExists(logID int64) bool {
	for _, statsLogs := range s.GetStatslogs() {
		for _, l := range statsLogs.GetBinlogs() {
			if l.GetLogID() == logID {
				return true
			}
		}
	}
	return false
}

func (s *SegmentInfo) GetCurrentRows() int64 {
	if s == nil {
		return 0
	}
	return s.currRows
}

func (s *SegmentInfo) IsCompacting() bool {
	if s == nil {
		return false
	}
	return s.isCompacting
}

func (s *SegmentInfo) GetAllocations() []*Allocation {
	return s.allocations
}

func (s *SegmentInfo) GetLastFlushTime() time.Time {
	return s.lastFlushTime
}

func (s *SegmentInfo) GetLastWrittenTime() time.Time {
	return s.lastWrittenTime
}

// GetSegmentSize use cached value when segment is immutable
func (s *SegmentInfo) GetSegmentSize() int64 {
	if s.size.Load() <= 0 || s.GetState() != commonpb.SegmentState_Flushed {
		var size int64
		for _, binlogs := range s.GetBinlogs() {
			for _, l := range binlogs.GetBinlogs() {
				size += l.GetMemorySize()
			}
		}

		for _, deltaLogs := range s.GetDeltalogs() {
			for _, l := range deltaLogs.GetBinlogs() {
				size += l.GetMemorySize()
			}
		}

		for _, statsLogs := range s.GetStatslogs() {
			for _, l := range statsLogs.GetBinlogs() {
				size += l.GetMemorySize()
			}
		}
		if size > 0 {
			s.size.Store(size)
		}
	}
	return s.size.Load()
}

// GetDeltaCount use cached value when segment is immutable
func (s *SegmentInfo) GetDeltaCount() int64 {
	if s.deltaRowcount.Load() < 0 || s.State != commonpb.SegmentState_Flushed {
		var rc int64
		for _, deltaLogs := range s.GetDeltalogs() {
			for _, l := range deltaLogs.GetBinlogs() {
				rc += l.GetEntriesNum()
			}
		}
		s.deltaRowcount.Store(rc)
	}
	r := s.deltaRowcount.Load()
	return r
}

func getZeroTime() time.Time {
	var t time.Time
	return t
}

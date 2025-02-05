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

package datacoord

import (
	"time"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// SegmentsInfo wraps a map, which maintains ID to SegmentInfo relation
type SegmentsInfo struct {
	segments         map[UniqueID]*SegmentInfo
	secondaryIndexes segmentInfoIndexes
	// map the compact relation, value is the segment which `CompactFrom` contains key.
	// now segment could be compacted to multiple segments
	compactionTo map[UniqueID][]UniqueID
}

type segmentInfoIndexes struct {
	coll2Segments    map[UniqueID]map[UniqueID]*SegmentInfo
	channel2Segments map[string]map[UniqueID]*SegmentInfo
}

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

	// It is only to ensure mutual exclusion between L0 compacting and stats tasks
	isStating bool
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

// NewSegmentsInfo creates a `SegmentsInfo` instance, which makes sure internal map is initialized
// note that no mutex is wrapped so external concurrent control is needed
func NewSegmentsInfo() *SegmentsInfo {
	return &SegmentsInfo{
		segments: make(map[UniqueID]*SegmentInfo),
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments:    make(map[UniqueID]map[UniqueID]*SegmentInfo),
			channel2Segments: make(map[string]map[UniqueID]*SegmentInfo),
		},
		compactionTo: make(map[UniqueID][]UniqueID),
	}
}

// GetSegment returns SegmentInfo
// the logPath in meta is empty
func (s *SegmentsInfo) GetSegment(segmentID UniqueID) *SegmentInfo {
	segment, ok := s.segments[segmentID]
	if !ok {
		return nil
	}
	return segment
}

// GetSegments iterates internal map and returns all SegmentInfo in a slice
// no deep copy applied
// the logPath in meta is empty
func (s *SegmentsInfo) GetSegments() []*SegmentInfo {
	return lo.Values(s.segments)
}

func (s *SegmentsInfo) getCandidates(criterion *segmentCriterion) map[UniqueID]*SegmentInfo {
	if criterion.collectionID > 0 {
		collSegments, ok := s.secondaryIndexes.coll2Segments[criterion.collectionID]
		if !ok {
			return nil
		}

		// both collection id and channel are filters of criterion
		if criterion.channel != "" {
			return lo.OmitBy(collSegments, func(k UniqueID, v *SegmentInfo) bool {
				return v.InsertChannel != criterion.channel
			})
		}
		return collSegments
	}

	if criterion.channel != "" {
		channelSegments, ok := s.secondaryIndexes.channel2Segments[criterion.channel]
		if !ok {
			return nil
		}
		return channelSegments
	}

	return s.segments
}

func (s *SegmentsInfo) GetSegmentsBySelector(filters ...SegmentFilter) []*SegmentInfo {
	criterion := &segmentCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	// apply criterion
	candidates := s.getCandidates(criterion)
	result := make([]*SegmentInfo, 0, len(candidates))
	for _, segment := range candidates {
		if criterion.Match(segment) {
			result = append(result, segment)
		}
	}
	return result
}

func (s *SegmentsInfo) GetRealSegmentsForChannel(channel string) []*SegmentInfo {
	channelSegments := s.secondaryIndexes.channel2Segments[channel]
	var result []*SegmentInfo
	for _, segment := range channelSegments {
		if !segment.GetIsFake() {
			result = append(result, segment)
		}
	}
	return result
}

// GetCompactionTo returns the segment that the provided segment is compacted to.
// Return (nil, false) if given segmentID can not found in the meta.
// Return (nil, true) if given segmentID can be found with no compaction to.
// Return (notnil, true) if given segmentID can be found and has compaction to.
func (s *SegmentsInfo) GetCompactionTo(fromSegmentID int64) ([]*SegmentInfo, bool) {
	if _, ok := s.segments[fromSegmentID]; !ok {
		return nil, false
	}
	if compactTos, ok := s.compactionTo[fromSegmentID]; ok {
		result := []*SegmentInfo{}
		for _, compactTo := range compactTos {
			to, ok := s.segments[compactTo]
			if !ok {
				log.Warn("compactionTo relation is broken", zap.Int64("from", fromSegmentID), zap.Int64("to", compactTo))
				return nil, true
			}
			result = append(result, to)
		}
		return result, true
	}
	return nil, true
}

// DropSegment deletes provided segmentID
// no extra method is taken when segmentID not exists
func (s *SegmentsInfo) DropSegment(segmentID UniqueID) {
	if segment, ok := s.segments[segmentID]; ok {
		s.deleteCompactTo(segment)
		s.removeSecondaryIndex(segment)
		delete(s.segments, segmentID)
	}
}

// SetSegment sets SegmentInfo with segmentID, perform overwrite if already exists
// set the logPath of segment in meta empty, to save space
// if segment has logPath, make it empty
func (s *SegmentsInfo) SetSegment(segmentID UniqueID, segment *SegmentInfo) {
	if segment, ok := s.segments[segmentID]; ok {
		// Remove old segment compact to relation first.
		s.deleteCompactTo(segment)
		s.removeSecondaryIndex(segment)
	}
	s.segments[segmentID] = segment
	s.addSecondaryIndex(segment)
	s.addCompactTo(segment)
}

// SetRowCount sets rowCount info for SegmentInfo with provided segmentID
// if SegmentInfo not found, do nothing
func (s *SegmentsInfo) SetRowCount(segmentID UniqueID, rowCount int64) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetRowCount(rowCount))
	}
}

// SetDmlPosition sets DmlPosition info (checkpoint for recovery) for SegmentInfo with provided segmentID
// if SegmentInfo not found, do nothing
func (s *SegmentsInfo) SetDmlPosition(segmentID UniqueID, pos *msgpb.MsgPosition) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetDmlPosition(pos))
	}
}

// SetStartPosition sets StartPosition info (recovery info when no checkout point found) for SegmentInfo with provided segmentID
// if SegmentInfo not found, do nothing
func (s *SegmentsInfo) SetStartPosition(segmentID UniqueID, pos *msgpb.MsgPosition) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetStartPosition(pos))
	}
}

// SetAllocations sets allocations for segment with specified id
// if the segment id is not found, do nothing
// uses `ShadowClone` since internal SegmentInfo is not changed
func (s *SegmentsInfo) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetAllocations(allocations))
	}
}

// AddAllocation adds a new allocation to specified segment
// if the segment is not found, do nothing
// uses `Clone` since internal SegmentInfo's LastExpireTime is changed
func (s *SegmentsInfo) AddAllocation(segmentID UniqueID, allocation *Allocation) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(AddAllocation(allocation))
	}
}

// SetCurrentRows sets rows count for segment
// if the segment is not found, do nothing
// uses `ShadowClone` since internal SegmentInfo is not changed
func (s *SegmentsInfo) SetCurrentRows(segmentID UniqueID, rows int64) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetCurrentRows(rows))
	}
}

// SetFlushTime sets flush time for segment
// if the segment is not found, do nothing
// uses `ShadowClone` since internal SegmentInfo is not changed
func (s *SegmentsInfo) SetFlushTime(segmentID UniqueID, t time.Time) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetFlushTime(t))
	}
}

// SetIsCompacting sets compaction status for segment
func (s *SegmentsInfo) SetIsCompacting(segmentID UniqueID, isCompacting bool) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetIsCompacting(isCompacting))
	}
}

// SetIsStating sets stating status for segment
func (s *SegmentsInfo) SetIsStating(segmentID UniqueID, isStating bool) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetIsStating(isStating))
	}
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

// SetLevel sets level for segment
func (s *SegmentsInfo) SetLevel(segmentID UniqueID, level datapb.SegmentLevel) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetLevel(level))
	}
}

// Clone deep clone the segment info and return a new instance
func (s *SegmentInfo) Clone(opts ...SegmentInfoOption) *SegmentInfo {
	info := proto.Clone(s.SegmentInfo).(*datapb.SegmentInfo)
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
		opt(cloned)
	}
	return cloned
}

// ShadowClone shadow clone the segment and return a new instance
func (s *SegmentInfo) ShadowClone(opts ...SegmentInfoOption) *SegmentInfo {
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
		opt(cloned)
	}
	return cloned
}

func (s *SegmentsInfo) addSecondaryIndex(segment *SegmentInfo) {
	collID := segment.GetCollectionID()
	channel := segment.GetInsertChannel()
	if _, ok := s.secondaryIndexes.coll2Segments[collID]; !ok {
		s.secondaryIndexes.coll2Segments[collID] = make(map[UniqueID]*SegmentInfo)
	}
	s.secondaryIndexes.coll2Segments[collID][segment.ID] = segment

	if _, ok := s.secondaryIndexes.channel2Segments[channel]; !ok {
		s.secondaryIndexes.channel2Segments[channel] = make(map[UniqueID]*SegmentInfo)
	}
	s.secondaryIndexes.channel2Segments[channel][segment.ID] = segment
}

func (s *SegmentsInfo) removeSecondaryIndex(segment *SegmentInfo) {
	collID := segment.GetCollectionID()
	channel := segment.GetInsertChannel()
	if segments, ok := s.secondaryIndexes.coll2Segments[collID]; ok {
		delete(segments, segment.ID)
		if len(segments) == 0 {
			delete(s.secondaryIndexes.coll2Segments, collID)
		}
	}

	if segments, ok := s.secondaryIndexes.channel2Segments[channel]; ok {
		delete(segments, segment.ID)
		if len(segments) == 0 {
			delete(s.secondaryIndexes.channel2Segments, channel)
		}
	}
}

// addCompactTo adds the compact relation to the segment
func (s *SegmentsInfo) addCompactTo(segment *SegmentInfo) {
	for _, from := range segment.GetCompactionFrom() {
		s.compactionTo[from] = append(s.compactionTo[from], segment.GetID())
	}
}

// deleteCompactTo deletes the compact relation to the segment
func (s *SegmentsInfo) deleteCompactTo(segment *SegmentInfo) {
	for _, from := range segment.GetCompactionFrom() {
		delete(s.compactionTo, from)
	}
}

// SegmentInfoOption is the option to set fields in segment info
type SegmentInfoOption func(segment *SegmentInfo)

// SetRowCount is the option to set row count for segment info
func SetRowCount(rowCount int64) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.NumOfRows = rowCount
	}
}

// SetExpireTime is the option to set expire time for segment info
func SetExpireTime(expireTs Timestamp) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.LastExpireTime = expireTs
	}
}

// SetState is the option to set state for segment info
func SetState(state commonpb.SegmentState) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.State = state
	}
}

// SetDmlPosition is the option to set dml position for segment info
func SetDmlPosition(pos *msgpb.MsgPosition) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.DmlPosition = pos
	}
}

// SetStartPosition is the option to set start position for segment info
func SetStartPosition(pos *msgpb.MsgPosition) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.StartPosition = pos
	}
}

// SetAllocations is the option to set allocations for segment info
func SetAllocations(allocations []*Allocation) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.allocations = allocations
	}
}

// AddAllocation is the option to add allocation info for segment info
func AddAllocation(allocation *Allocation) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.allocations = append(segment.allocations, allocation)
		segment.LastExpireTime = allocation.ExpireTime
	}
}

// SetCurrentRows is the option to set current row count for segment info
func SetCurrentRows(rows int64) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.currRows = rows
		segment.lastWrittenTime = time.Now()
	}
}

// SetFlushTime is the option to set flush time for segment info
func SetFlushTime(t time.Time) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.lastFlushTime = t
	}
}

// SetIsCompacting is the option to set compaction state for segment info
func SetIsCompacting(isCompacting bool) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.isCompacting = isCompacting
	}
}

// SetIsStating is the option to set stats state for segment info
func SetIsStating(isStating bool) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.isStating = isStating
	}
}

// SetLevel is the option to set level for segment info
func SetLevel(level datapb.SegmentLevel) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.Level = level
	}
}

// getSegmentSize use cached value when segment is immutable
func (s *SegmentInfo) getSegmentSize() int64 {
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

// Any edits on deltalogs of flushed segments will reset deltaRowcount to -1
func (s *SegmentInfo) getDeltaCount() int64 {
	if s.deltaRowcount.Load() < 0 || s.GetState() != commonpb.SegmentState_Flushed {
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

// SegmentInfoSelector is the function type to select SegmentInfo from meta
type SegmentInfoSelector func(*SegmentInfo) bool

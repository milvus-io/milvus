package datacoord

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

// SegmentsInfo wraps a map, which maintains ID to SegmentInfo relation
type SegmentsInfo struct {
	segments map[UniqueID]*SegmentInfo
}

// SegmentInfo wraps datapb.SegmentInfo and patches some extra info on it
type SegmentInfo struct {
	*datapb.SegmentInfo
	currRows      int64
	allocations   []*Allocation
	lastFlushTime time.Time
}

// NewSegmentInfo create `SegmentInfo` wrapper from `datapb.SegmentInfo`
// assign current rows to 0 and pre-allocate `allocations` slice
// Note that the allocation information is not preserved,
// the worst case scenario is to have a segment with twice size we expects
func NewSegmentInfo(info *datapb.SegmentInfo) *SegmentInfo {
	return &SegmentInfo{
		SegmentInfo: info,
		currRows:    0,
		allocations: make([]*Allocation, 0, 16),
	}
}

// NewSegmentsInfo create `SegmentsInfo` instance, which makes sure internal map is initialized
// note that no mutex is wrapper so external concurrent control is needed
func NewSegmentsInfo() *SegmentsInfo {
	return &SegmentsInfo{segments: make(map[UniqueID]*SegmentInfo)}
}

// GetSegment returns SegmentInfo
func (s *SegmentsInfo) GetSegment(segmentID UniqueID) *SegmentInfo {
	segment, ok := s.segments[segmentID]
	if !ok {
		return nil
	}
	return segment
}

// GetSegments iterates internal map and returns all SegmentInfo in a slice
// no deep copy applied
func (s *SegmentsInfo) GetSegments() []*SegmentInfo {
	segments := make([]*SegmentInfo, 0, len(s.segments))
	for _, segment := range s.segments {
		segments = append(segments, segment)
	}
	return segments
}

// DropSegment deletes provided segmentID
// no extra method is taken when segmentID not exists
func (s *SegmentsInfo) DropSegment(segmentID UniqueID) {
	delete(s.segments, segmentID)
}

// SetSegment sets SegmentInfo with segmentID, perform overwrite if already exists
func (s *SegmentsInfo) SetSegment(segmentID UniqueID, segment *SegmentInfo) {
	s.segments[segmentID] = segment
}

// SetRowCount sets rowCount info for SegmentInfo with provided segmentID
// if SegmentInfo not found, do nothing
func (s *SegmentsInfo) SetRowCount(segmentID UniqueID, rowCount int64) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetRowCount(rowCount))
	}
}

// SetState sets Segment State info for SegmentInfo with provided segmentID
// if SegmentInfo not found, do nothing
func (s *SegmentsInfo) SetState(segmentID UniqueID, state commonpb.SegmentState) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetState(state))
	}
}

// SetDmlPosition sets DmlPosition info (checkpoint for recovery) for SegmentInfo with provided segmentID
// if SegmentInfo not found, do nothing
func (s *SegmentsInfo) SetDmlPosition(segmentID UniqueID, pos *internalpb.MsgPosition) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetDmlPosition(pos))
	}
}

// SetStartPosition sets StartPosition info (recovery info when no checkout point found) for SegmentInfo with provided segmentID
// if SegmentInfo not found, do nothing
func (s *SegmentsInfo) SetStartPosition(segmentID UniqueID, pos *internalpb.MsgPosition) {
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

// SetBinlogs sets binlog paths for segment
// if the segment is not found, do nothing
// uses `Clone` since internal SegmentInfo's Binlogs is changed
func (s *SegmentsInfo) SetBinlogs(segmentID UniqueID, binlogs []*datapb.FieldBinlog) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetBinlogs(binlogs))
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

// AddSegmentBinlogs adds binlogs for segment
// if the segment is not found, do nothing
// uses `Clone` since internal SegmentInfo's Binlogs is changed
func (s *SegmentsInfo) AddSegmentBinlogs(segmentID UniqueID, field2Binlogs map[UniqueID][]string) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(addSegmentBinlogs(field2Binlogs))
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
	}
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

// ShadowClone shadow clone the segment and return a new instance
func (s *SegmentInfo) ShadowClone(opts ...SegmentInfoOption) *SegmentInfo {
	cloned := &SegmentInfo{
		SegmentInfo:   s.SegmentInfo,
		currRows:      s.currRows,
		allocations:   s.allocations,
		lastFlushTime: s.lastFlushTime,
	}

	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
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
func SetDmlPosition(pos *internalpb.MsgPosition) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.DmlPosition = pos
	}
}

// SetStartPosition is the option to set start position for segment info
func SetStartPosition(pos *internalpb.MsgPosition) SegmentInfoOption {
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
	}
}

func SetBinlogs(binlogs []*datapb.FieldBinlog) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.Binlogs = binlogs
	}
}

func SetFlushTime(t time.Time) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.lastFlushTime = t
	}
}

func addSegmentBinlogs(field2Binlogs map[UniqueID][]string) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		for fieldID, binlogPaths := range field2Binlogs {
			found := false
			for _, binlog := range segment.Binlogs {
				if binlog.FieldID != fieldID {
					continue
				}
				binlog.Binlogs = append(binlog.Binlogs, binlogPaths...)
				found = true
				break
			}
			if !found {
				// if no field matched
				segment.Binlogs = append(segment.Binlogs, &datapb.FieldBinlog{
					FieldID: fieldID,
					Binlogs: binlogPaths,
				})
			}
		}
	}
}

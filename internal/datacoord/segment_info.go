package datacoord

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type SegmentsInfo struct {
	segments map[UniqueID]*SegmentInfo
}

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

func NewSegmentsInfo() *SegmentsInfo {
	return &SegmentsInfo{segments: make(map[UniqueID]*SegmentInfo)}
}

func (s *SegmentsInfo) GetSegment(segmentID UniqueID) *SegmentInfo {
	segment, ok := s.segments[segmentID]
	if !ok {
		return nil
	}
	return segment
}

func (s *SegmentsInfo) GetSegments() []*SegmentInfo {
	segments := make([]*SegmentInfo, 0, len(s.segments))
	for _, segment := range s.segments {
		segments = append(segments, segment)
	}
	return segments
}

func (s *SegmentsInfo) DropSegment(segmentID UniqueID) {
	delete(s.segments, segmentID)
}

func (s *SegmentsInfo) SetSegment(segmentID UniqueID, segment *SegmentInfo) {
	s.segments[segmentID] = segment
}

func (s *SegmentsInfo) SetRowCount(segmentID UniqueID, rowCount int64) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetRowCount(rowCount))
	}
}

func (s *SegmentsInfo) SetState(segmentID UniqueID, state commonpb.SegmentState) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetState(state))
	}
}

func (s *SegmentsInfo) SetDmlPosition(segmentID UniqueID, pos *internalpb.MsgPosition) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetDmlPosition(pos))
	}
}

func (s *SegmentsInfo) SetStartPosition(segmentID UniqueID, pos *internalpb.MsgPosition) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetStartPosition(pos))
	}
}

func (s *SegmentsInfo) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetAllocations(allocations))
	}
}

func (s *SegmentsInfo) AddAllocation(segmentID UniqueID, allocation *Allocation) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(AddAllocation(allocation))
	}
}

func (s *SegmentsInfo) SetCurrentRows(segmentID UniqueID, rows int64) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetCurrentRows(rows))
	}
}

func (s *SegmentsInfo) SetBinlogs(segmentID UniqueID, binlogs []*datapb.FieldBinlog) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(SetBinlogs(binlogs))
	}
}

func (s *SegmentsInfo) SetFlushTime(segmentID UniqueID, t time.Time) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.ShadowClone(SetFlushTime(t))
	}
}

func (s *SegmentsInfo) AddSegmentBinlogs(segmentID UniqueID, field2Binlogs map[UniqueID][]string) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = segment.Clone(addSegmentBinlogs(field2Binlogs))
	}
}

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

type SegmentInfoOption func(segment *SegmentInfo)

func SetRowCount(rowCount int64) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.NumOfRows = rowCount
	}
}

func SetExpireTime(expireTs Timestamp) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.LastExpireTime = expireTs
	}
}

func SetState(state commonpb.SegmentState) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.State = state
	}
}

func SetDmlPosition(pos *internalpb.MsgPosition) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.DmlPosition = pos
	}
}

func SetStartPosition(pos *internalpb.MsgPosition) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.StartPosition = pos
	}
}

func SetAllocations(allocations []*Allocation) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.allocations = allocations
	}
}

func AddAllocation(allocation *Allocation) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.allocations = append(segment.allocations, allocation)
		segment.LastExpireTime = allocation.ExpireTime
	}
}

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

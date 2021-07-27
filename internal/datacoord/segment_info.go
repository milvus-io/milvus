package datacoord

import (
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
	currRows    int64
	allocations []*Allocation
}

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
		s.segments[segmentID] = s.ShadowClone(segment, SetRowCount(rowCount))
	}
}

func (s *SegmentsInfo) SetLasteExpiraTime(segmentID UniqueID, expireTs Timestamp) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = s.ShadowClone(segment, SetExpireTime(expireTs))
	}
}

func (s *SegmentsInfo) SetState(segmentID UniqueID, state commonpb.SegmentState) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = s.ShadowClone(segment, SetState(state))
	}
}

func (s *SegmentsInfo) SetDmlPositino(segmentID UniqueID, pos *internalpb.MsgPosition) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = s.Clone(segment, SetDmlPositino(pos))
	}
}

func (s *SegmentsInfo) SetStartPosition(segmentID UniqueID, pos *internalpb.MsgPosition) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = s.Clone(segment, SetStartPosition(pos))
	}
}

func (s *SegmentsInfo) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = s.ShadowClone(segment, SetAllocations(allocations))
	}
}

func (s *SegmentsInfo) AddAllocation(segmentID UniqueID, allocation *Allocation) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = s.Clone(segment, AddAllocation(allocation))
	}
}

func (s *SegmentsInfo) SetCurrentRows(segmentID UniqueID, rows int64) {
	if segment, ok := s.segments[segmentID]; ok {
		s.segments[segmentID] = s.ShadowClone(segment, SetCurrentRows(rows))
	}
}

func (s *SegmentsInfo) Clone(segment *SegmentInfo, opts ...SegmentInfoOption) *SegmentInfo {
	info := proto.Clone(segment.SegmentInfo).(*datapb.SegmentInfo)
	cloned := &SegmentInfo{
		SegmentInfo: info,
		currRows:    segment.currRows,
		allocations: segment.allocations,
	}
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

func (s *SegmentsInfo) ShadowClone(segment *SegmentInfo, opts ...SegmentInfoOption) *SegmentInfo {
	cloned := &SegmentInfo{
		SegmentInfo: segment.SegmentInfo,
		currRows:    segment.currRows,
		allocations: segment.allocations,
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

func SetDmlPositino(pos *internalpb.MsgPosition) SegmentInfoOption {
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

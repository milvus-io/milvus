package datacoord

import (
	"github.com/gogo/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type SegmentsInfo struct {
	segments map[UniqueID]*datapb.SegmentInfo
}

func NewSegmentsInfo() *SegmentsInfo {
	return &SegmentsInfo{segments: make(map[UniqueID]*datapb.SegmentInfo)}
}

func (s *SegmentsInfo) GetSegment(segmentID UniqueID) *datapb.SegmentInfo {
	segment, ok := s.segments[segmentID]
	if !ok {
		return nil
	}
	return segment
}

func (s *SegmentsInfo) GetSegments() []*datapb.SegmentInfo {
	segments := make([]*datapb.SegmentInfo, 0, len(s.segments))
	for _, segment := range s.segments {
		segments = append(segments, segment)
	}
	return segments
}

func (s *SegmentsInfo) DropSegment(segmentID UniqueID) {
	delete(s.segments, segmentID)
}

func (s *SegmentsInfo) SetSegment(segmentID UniqueID, segment *datapb.SegmentInfo) {
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

func (s *SegmentsInfo) Clone(segment *datapb.SegmentInfo, opts ...SegmentInfoOption) *datapb.SegmentInfo {
	dmlPos := proto.Clone(segment.DmlPosition).(*internalpb.MsgPosition)
	startPos := proto.Clone(segment.StartPosition).(*internalpb.MsgPosition)
	cloned := &datapb.SegmentInfo{
		ID:             segment.ID,
		CollectionID:   segment.CollectionID,
		PartitionID:    segment.PartitionID,
		InsertChannel:  segment.InsertChannel,
		NumOfRows:      segment.NumOfRows,
		State:          segment.State,
		DmlPosition:    dmlPos,
		MaxRowNum:      segment.MaxRowNum,
		LastExpireTime: segment.LastExpireTime,
		StartPosition:  startPos,
	}
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

func (s *SegmentsInfo) ShadowClone(segment *datapb.SegmentInfo, opts ...SegmentInfoOption) *datapb.SegmentInfo {
	cloned := &datapb.SegmentInfo{
		ID:             segment.ID,
		CollectionID:   segment.CollectionID,
		PartitionID:    segment.PartitionID,
		InsertChannel:  segment.InsertChannel,
		NumOfRows:      segment.NumOfRows,
		State:          segment.State,
		DmlPosition:    segment.DmlPosition,
		MaxRowNum:      segment.MaxRowNum,
		LastExpireTime: segment.LastExpireTime,
		StartPosition:  segment.StartPosition,
	}

	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

type SegmentInfoOption func(segment *datapb.SegmentInfo)

func SetRowCount(rowCount int64) SegmentInfoOption {
	return func(segment *datapb.SegmentInfo) {
		segment.NumOfRows = rowCount
	}
}

func SetExpireTime(expireTs Timestamp) SegmentInfoOption {
	return func(segment *datapb.SegmentInfo) {
		segment.LastExpireTime = expireTs
	}
}

func SetState(state commonpb.SegmentState) SegmentInfoOption {
	return func(segment *datapb.SegmentInfo) {
		segment.State = state
	}
}

func SetDmlPositino(pos *internalpb.MsgPosition) SegmentInfoOption {
	return func(segment *datapb.SegmentInfo) {
		segment.DmlPosition = pos
	}
}

func SetStartPosition(pos *internalpb.MsgPosition) SegmentInfoOption {
	return func(segment *datapb.SegmentInfo) {
		segment.StartPosition = pos
	}
}

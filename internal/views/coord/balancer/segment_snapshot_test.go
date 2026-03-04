package balancer

type mapSegmentSnapshot struct {
	infos map[int64]*SegmentInfo
}

func newMapSegmentSnapshot(infos map[int64]*SegmentInfo) *mapSegmentSnapshot {
	return &mapSegmentSnapshot{infos: infos}
}

func (s *mapSegmentSnapshot) Get(segmentID int64) (*SegmentInfo, bool) {
	if s == nil {
		return nil, false
	}
	info, ok := s.infos[segmentID]
	return info, ok
}

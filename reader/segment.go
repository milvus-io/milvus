package reader

import "C"

type Segment struct {
	Id string
	Status int
	SegmentCloseTime uint64
}

func (s *Segment) GetRowCount() int64 {
	// TODO: C type to go type
	return C.GetRowCount(s)
}

func (s *Segment) GetStatus() int {
	// TODO: C type to go type
	return C.GetStatus(s)
}

func (s *Segment) GetMaxTimestamp() uint64 {
	// TODO: C type to go type
	return C.GetMaxTimestamp(s)
}

func (s *Segment) GetMinTimestamp() uint64 {
	// TODO: C type to go type
	return C.GetMinTimestamp(s)
}

func (s *Segment) GetDeletedCount() uint64 {
	// TODO: C type to go type
	return C.GetDeletedCount(s)
}

func (s *Segment) Close() {
	// TODO: C type to go type
	C.CloseSegment(s)
}

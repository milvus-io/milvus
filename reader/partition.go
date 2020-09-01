package reader

import "C"

type Partition struct {
	PartitionPtr *C.CPartition
	PartitionName string
	Segments []*Segment
}

func (p *Partition) NewSegment(segmentId uint64) *Segment {
	segmentPtr := C.NewSegment(p.PartitionPtr, segmentId)

	var newSegment = &Segment{SegmentPtr: segmentPtr, SegmentId: segmentId}
	p.Segments = append(p.Segments, newSegment)
	return newSegment
}

func (p *Partition) DeleteSegment(segment *Segment) {
	cPtr := segment.SegmentPtr
	C.DeleteSegment(cPtr)

	// TODO: remove from p.Segments
}

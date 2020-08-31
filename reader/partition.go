package reader

import "C"
import "errors"

type Partition struct {
	PartitionPtr *C.CPartition
	PartitionName string
	Segments []*Segment
}

func (p *Partition) NewSegment(segmentId uint64) (*Segment, error) {
	segmentPtr, status := C.NewSegment(p.PartitionPtr, segmentId)

	if status != 0 {
		return nil, errors.New("create segment failed")
	}

	var newSegment = &Segment{SegmentPtr: segmentPtr, SegmentId: segmentId}
	p.Segments = append(p.Segments, newSegment)
	return newSegment, nil
}

func (p *Partition) DeleteSegment() error {
	status := C.DeleteSegment(p.PartitionPtr)

	if status != 0 {
		return errors.New("delete segment failed")
	}

	// TODO: remove from p.Segments
	return nil
}

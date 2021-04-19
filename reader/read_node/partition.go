package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../../core/include

#cgo LDFLAGS: -L${SRCDIR}/../../core/lib -lmilvus_dog_segment -Wl,-rpath=${SRCDIR}/../../core/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"

type Partition struct {
	PartitionPtr C.CPartition
	PartitionName string
	OpenedSegments []*Segment
	ClosedSegments []*Segment
}

func (p *Partition) NewSegment(segmentId int64) *Segment {
	segmentPtr := C.NewSegment(p.PartitionPtr, C.ulong(segmentId))

	var newSegment = &Segment{SegmentPtr: segmentPtr, SegmentId: segmentId}
	p.OpenedSegments = append(p.OpenedSegments, newSegment)
	return newSegment
}

func (p *Partition) DeleteSegment(segment *Segment) {
	cPtr := segment.SegmentPtr
	C.DeleteSegment(cPtr)

	// TODO: remove from p.Segments
}

package reader

/*

#cgo CFLAGS: -I../core/include

#cgo LDFLAGS: -L../core/lib -lmilvus_dog_segment -Wl,-rpath=../core/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"

type Partition struct {
	PartitionPtr C.CPartition
	PartitionName string
	Segments []*Segment
}

func (p *Partition) NewSegment(segmentId uint64) *Segment {
	segmentPtr := C.NewSegment(p.PartitionPtr, C.ulong(segmentId))

	var newSegment = &Segment{SegmentPtr: segmentPtr, SegmentId: segmentId}
	p.Segments = append(p.Segments, newSegment)
	return newSegment
}

func (p *Partition) DeleteSegment(segment *Segment) {
	cPtr := segment.SegmentPtr
	C.DeleteSegment(cPtr)

	// TODO: remove from p.Segments
}

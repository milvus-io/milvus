package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"

type Partition struct {
	PartitionPtr  C.CPartition
	PartitionName string
	Segments      []*Segment
}

func (p *Partition) NewSegment(segmentID int64) *Segment {
	/*
		CSegmentBase
		NewSegment(CPartition partition, unsigned long segment_id);
	*/
	segmentPtr := C.NewSegment(p.PartitionPtr, C.ulong(segmentID))

	var newSegment = &Segment{SegmentPtr: segmentPtr, SegmentID: segmentID}
	p.Segments = append(p.Segments, newSegment)
	return newSegment
}

func (p *Partition) DeleteSegment(node *QueryNode, segment *Segment) {
	/*
		void
		DeleteSegment(CSegmentBase segment);
	*/
	cPtr := segment.SegmentPtr
	C.DeleteSegment(cPtr)

	tmpSegments := make([]*Segment, 0)

	for _, s := range p.Segments {
		if s.SegmentID == segment.SegmentID {
			delete(node.SegmentsMap, s.SegmentID)
		} else {
			tmpSegments = append(tmpSegments, s)
		}
	}

	p.Segments = tmpSegments
}

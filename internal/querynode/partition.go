package querynodeimp

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"

type Partition struct {
	partitionTag string
	segments     []*Segment
}

func (p *Partition) Tag() string {
	return (*p).partitionTag
}

func (p *Partition) Segments() *[]*Segment {
	return &(*p).segments
}

func newPartition(partitionTag string) *Partition {
	var newPartition = &Partition{
		partitionTag: partitionTag,
	}

	return newPartition
}

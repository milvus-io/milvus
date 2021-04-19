package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"

type Partition struct {
	id       UniqueID
	segments []*Segment
	enableDM bool
}

func (p *Partition) ID() UniqueID {
	return p.id
}

func (p *Partition) Segments() *[]*Segment {
	return &(*p).segments
}

func newPartition(partitionID UniqueID) *Partition {
	var newPartition = &Partition{
		id:       partitionID,
		enableDM: false,
	}

	return newPartition
}

package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"

type Partition struct {
	partitionTag string
	id           UniqueID
	segments     []*Segment
}

func (p *Partition) ID() UniqueID {
	return p.id
}

func (p *Partition) Tag() string {
	return (*p).partitionTag
}

func (p *Partition) Segments() *[]*Segment {
	return &(*p).segments
}

func newPartition2(partitionTag string) *Partition {
	var newPartition = &Partition{
		partitionTag: partitionTag,
	}

	return newPartition
}

func newPartition(partitionID UniqueID) *Partition {
	var newPartition = &Partition{
		id: partitionID,
	}

	return newPartition
}

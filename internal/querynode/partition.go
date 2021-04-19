package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import "fmt"

type Partition struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentIDs   []UniqueID
	enable       bool
}

func (p *Partition) ID() UniqueID {
	return p.partitionID
}

func (p *Partition) addSegmentID(segmentID UniqueID) {
	p.segmentIDs = append(p.segmentIDs, segmentID)
}

func (p *Partition) removeSegmentID(segmentID UniqueID) {
	tmpIDs := make([]UniqueID, 0)
	for _, id := range p.segmentIDs {
		if id == segmentID {
			tmpIDs = append(tmpIDs, id)
		}
	}
	p.segmentIDs = tmpIDs
}

func newPartition(collectionID UniqueID, partitionID UniqueID) *Partition {
	var newPartition = &Partition{
		collectionID: collectionID,
		partitionID:  partitionID,
		enable:       false,
	}

	fmt.Println("create partition", partitionID)
	return newPartition
}

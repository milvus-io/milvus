package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_dog_segment -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"

type Collection struct {
	CollectionPtr  C.CCollection
	CollectionName string
	CollectionID   uint64
	Partitions     []*Partition
}

func (c *Collection) NewPartition(partitionName string) *Partition {
	/*
		CPartition
		NewPartition(CCollection collection, const char* partition_name);
	*/
	cName := C.CString(partitionName)
	partitionPtr := C.NewPartition(c.CollectionPtr, cName)

	var newPartition = &Partition{PartitionPtr: partitionPtr, PartitionName: partitionName}
	c.Partitions = append(c.Partitions, newPartition)
	return newPartition
}

func (c *Collection) DeletePartition(node *QueryNode, partition *Partition) {
	/*
		void
		DeletePartition(CPartition partition);
	*/
	cPtr := partition.PartitionPtr
	C.DeletePartition(cPtr)

	tmpPartitions := make([]*Partition, 0)

	for _, p := range c.Partitions {
		if p.PartitionName == partition.PartitionName {
			for _, s := range p.Segments {
				delete(node.SegmentsMap, s.SegmentId)
			}
		} else {
			tmpPartitions = append(tmpPartitions, p)
		}
	}

	c.Partitions = tmpPartitions
}

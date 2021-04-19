package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../../core/include

#cgo LDFLAGS: -L${SRCDIR}/../../core/lib -lmilvus_dog_segment -Wl,-rpath=${SRCDIR}/../../core/lib

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
	cName := C.CString(partitionName)
	partitionPtr := C.NewPartition(c.CollectionPtr, cName)

	var newPartition = &Partition{PartitionPtr: partitionPtr, PartitionName: partitionName}
	c.Partitions = append(c.Partitions, newPartition)
	return newPartition
}

func (c *Collection) DeletePartition(partition *Partition) {
	cPtr := partition.PartitionPtr
	C.DeletePartition(cPtr)

	// TODO: remove from c.Partitions
}

package reader

/*

#cgo CFLAGS: -I../core/include

#cgo LDFLAGS: -L../core/lib -lmilvus_dog_segment -Wl,-rpath=../core/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"

type Collection struct {
	CollectionPtr C.CCollection
	CollectionName string
	Partitions []*Partition
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

func (c *Collection) GetSegments() ([]*Segment, error) {
	// TODO: add get segments
	//segments, status := C.GetSegments(c.CollectionPtr)
	//
	//if status != 0 {
	//	return nil, errors.New("get segments failed")
	//}
	//
	//return segments, nil
	return nil, nil
}

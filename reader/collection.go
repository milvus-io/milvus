package reader

import "C"
import (
	"errors"
)

type Collection struct {
	CollectionPtr *C.Collection
	CollectionName string
	Partitions []*Partition
}

func (c *Collection) NewPartition(partitionName string) (*Partition, error) {
	cName := C.CString(partitionName)
	partitionPtr, status := C.NewPartition(c.CollectionPtr, cName)

	if status != 0 {
		return nil, errors.New("create partition failed")
	}

	var newPartition = &Partition{PartitionPtr: partitionPtr, PartitionName: partitionName}
	c.Partitions = append(c.Partitions, newPartition)
	return newPartition, nil
}

func (c *Collection) DeletePartition(partitionName string) error {
	cName := C.CString(partitionName)
	status := C.DeletePartition(c.CollectionPtr, cName)

	if status != 0 {
		return errors.New("create partition failed")
	}

	// TODO: remove from c.Partitions
	return nil
}

func (c *Collection) GetSegments() ([]*Segment, error) {
	segments, status := C.GetSegments(c.CollectionPtr)

	if status != 0 {
		return nil, errors.New("get segments failed")
	}

	return segments, nil
}

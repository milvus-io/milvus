package reader

import "C"
import "errors"

type Partition struct {
	PartitionPtr *C.CPartition
	PartitionName string
	Segments []*Segment
}

func (c *Collection) NewPartition(partitionName string) (*Partition, error) {
	cName := C.CString(partitionName)
	partitionPtr, status := C.NewPartition(c.CollectionPtr, cName)

	if status != 0 {
		return nil, errors.New("create partition failed")
	}

	return &Partition{PartitionPtr: partitionPtr, PartitionName: partitionName}, nil
}

func (c *Collection) DeletePartition(partitionName string) error {
	cName := C.CString(partitionName)
	status := C.DeletePartition(c.CollectionPtr, cName)

	if status != 0 {
		return errors.New("create partition failed")
	}

	return nil
}

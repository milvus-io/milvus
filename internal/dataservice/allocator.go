package dataservice

type allocator interface {
	allocTimestamp() (Timestamp, error)
	allocID() (UniqueID, error)
}

type allocatorImpl struct {
	// TODO call allocate functions in  client.go in master service
}

// TODO implements
func newAllocatorImpl() *allocatorImpl {
	return nil
}

func (allocator *allocatorImpl) allocTimestamp() (Timestamp, error) {
	return 0, nil
}

func (allocator *allocatorImpl) allocID() (UniqueID, error) {
	return 0, nil
}

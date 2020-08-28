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

// TODO: Schema
type CollectionSchema string

func NewCollection(collectionName string, schema CollectionSchema) (*Collection, error) {
	cName := C.CString(collectionName)
	cSchema := C.CString(schema)
	collection, status := C.NewCollection(cName, cSchema)

	if status != 0 {
		return nil, errors.New("create collection failed")
	}

	return &Collection{CollectionPtr: collection, CollectionName: collectionName}, nil
}

func DeleteCollection(collection *Collection) error {
	status := C.DeleteCollection(collection.CollectionPtr)

	if status != 0 {
		return errors.New("delete collection failed")
	}

	return nil
}

func (c *Collection) GetSegments() ([]*Segment, error) {
	segments, status := C.GetSegments(c.CollectionPtr)

	if status != 0 {
		return nil, errors.New("get segments failed")
	}

	return segments, nil
}

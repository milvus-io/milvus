package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/proto/segcorepb"
)

// CreateCCollectionRequest is a request to create a CCollection.
type CreateCCollectionRequest struct {
	CollectionID int64
	Schema       *schemapb.CollectionSchema
	IndexMeta    *segcorepb.CollectionIndexMeta
}

// CreateCCollection creates a CCollection from a CreateCCollectionRequest.
func CreateCCollection(req *CreateCCollectionRequest) (*CCollection, error) {
	schemaBlob, err := proto.Marshal(req.Schema)
	if err != nil {
		return nil, errors.New("marshal schema failed")
	}
	var indexMetaBlob []byte
	if req.IndexMeta != nil {
		indexMetaBlob, err = proto.Marshal(req.IndexMeta)
		if err != nil {
			return nil, errors.New("marshal index meta failed")
		}
	}
	ptr := C.NewCollection(unsafe.Pointer(&schemaBlob[0]), (C.int64_t)(len(schemaBlob)))
	if indexMetaBlob != nil {
		C.SetIndexMeta(ptr, unsafe.Pointer(&indexMetaBlob[0]), (C.int64_t)(len(indexMetaBlob)))
	}
	return &CCollection{
		collectionID: req.CollectionID,
		ptr:          ptr,
		schema:       req.Schema,
		indexMeta:    req.IndexMeta,
	}, nil
}

// CCollection is just a wrapper of the underlying C-structure CCollection.
// Contains some additional immutable properties of collection.
type CCollection struct {
	ptr          C.CCollection
	collectionID int64
	schema       *schemapb.CollectionSchema
	indexMeta    *segcorepb.CollectionIndexMeta
}

// ID returns the collection ID.
func (c *CCollection) ID() int64 {
	return c.collectionID
}

// rawPointer returns the underlying C-structure pointer.
func (c *CCollection) rawPointer() C.CCollection {
	return c.ptr
}

func (c *CCollection) Schema() *schemapb.CollectionSchema {
	return c.schema
}

func (c *CCollection) IndexMeta() *segcorepb.CollectionIndexMeta {
	return c.indexMeta
}

// Release releases the underlying collection
func (c *CCollection) Release() {
	C.DeleteCollection(c.ptr)
	c.ptr = nil
}

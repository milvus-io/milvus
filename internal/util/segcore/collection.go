package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "storage/storage_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// CreateCCollectionRequest is a request to create a CCollection.
type CreateCCollectionRequest struct {
	CollectionID  int64
	Schema        *schemapb.CollectionSchema
	SchemaRef     *SchemaRef
	IndexMeta     *segcorepb.CollectionIndexMeta
	LoadFieldList []int64
}

// CreateCCollection creates a CCollection from a CreateCCollectionRequest.
func CreateCCollection(req *CreateCCollectionRequest) (*CCollection, error) {
	defer runtime.KeepAlive(req.SchemaRef)
	var err error
	var indexMetaBlob []byte
	if req.IndexMeta != nil {
		indexMetaBlob, err = proto.Marshal(req.IndexMeta)
		if err != nil {
			return nil, merr.WrapErrSegcoreMsg("marshal index meta failed")
		}
	}
	var ptr C.CCollection
	var status C.CStatus
	if req.SchemaRef != nil {
		if req.SchemaRef.rawPointer() == nil {
			return nil, merr.WrapErrServiceInternalMsg("schema reference is released")
		}
		status = C.NewCollectionWithSchema(req.SchemaRef.rawPointer(), &ptr)
	} else {
		schemaBlob, err := proto.Marshal(req.Schema)
		if err != nil {
			return nil, merr.WrapErrSegcoreMsg("marshal schema failed")
		}
		if len(schemaBlob) == 0 {
			return nil, merr.WrapErrSegcoreMsg("marshaled schema is empty")
		}
		status = C.NewCollection(unsafe.Pointer(&schemaBlob[0]), C.int64_t(len(schemaBlob)), &ptr)
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	if indexMetaBlob != nil {
		status = C.SetIndexMeta(ptr, unsafe.Pointer(&indexMetaBlob[0]), (C.int64_t)(len(indexMetaBlob)))
		if err := ConsumeCStatusIntoError(&status); err != nil {
			C.DeleteCollection(ptr)
			return nil, err
		}
	}
	// Cached logical schemas are immutable. Effective load fields live in the
	// separate load-schema handle passed to sealed segments and reopen.
	if req.SchemaRef == nil && len(req.LoadFieldList) > 0 {
		status = C.UpdateLoadFields(ptr, (*C.int64_t)(unsafe.Pointer(&req.LoadFieldList[0])),
			C.int64_t(len(req.LoadFieldList)))
		if err := ConsumeCStatusIntoError(&status); err != nil {
			C.DeleteCollection(ptr)
			return nil, err
		}
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

func (c *CCollection) UpdateIndexMeta(meta *segcorepb.CollectionIndexMeta) error {
	if meta == nil {
		return nil
	}

	indexMetaBlob, err := proto.Marshal(meta)
	if err != nil {
		return err
	}

	status := C.SetIndexMeta(c.ptr, unsafe.Pointer(&indexMetaBlob[0]), (C.int64_t)(len(indexMetaBlob)))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	c.indexMeta = meta
	return nil
}

func (c *CCollection) UpdateSchema(sch *schemapb.CollectionSchema) error {
	if sch == nil {
		return merr.WrapErrServiceInternal("update collection schema with nil")
	}

	schemaBlob, err := proto.Marshal(sch)
	if err != nil {
		return err
	}

	status := C.UpdateSchema(c.ptr, unsafe.Pointer(&schemaBlob[0]), (C.int64_t)(len(schemaBlob)), (C.uint64_t)(sch.GetVersion()))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	c.schema = sch
	return nil
}

func (c *CCollection) UpdateSchemaWithRef(sch *schemapb.CollectionSchema, schemaRef *SchemaRef) error {
	defer runtime.KeepAlive(schemaRef)
	if sch == nil {
		return merr.WrapErrServiceInternal("update collection schema with nil")
	}
	if schemaRef == nil || schemaRef.rawPointer() == nil {
		return merr.WrapErrServiceInternalMsg("schema reference is released")
	}

	status := C.UpdateSchemaWithHandle(c.ptr, schemaRef.rawPointer())
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	c.schema = sch
	return nil
}

// Release releases the underlying collection
func (c *CCollection) Release() {
	C.DeleteCollection(c.ptr)
	c.ptr = nil
}

func PutOrRefPluginContext(ez *hookutil.EZ, key string) error {
	mlog.Info(context.TODO(), "PutOrRefPluginContext",
		mlog.Int64("ez_id", ez.EzID),
		mlog.Int64("collection_id", ez.CollectionID))
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	pluginContext := C.CPluginContext{
		ez_id:         C.int64_t(ez.EzID),
		collection_id: C.int64_t(ez.CollectionID),
		key:           ckey,
	}
	cstatus := C.PutOrRefPluginContext(pluginContext)
	if err := ConsumeCStatusIntoError(&cstatus); err != nil {
		return err
	}
	return nil
}

func UnRefPluginContext(ez *hookutil.EZ) error {
	mlog.Info(context.TODO(), "UnRefPluginContext",
		mlog.Int64("ez_id", ez.EzID),
		mlog.Int64("collection_id", ez.CollectionID))
	pluginContext := C.CPluginContext{
		ez_id:         C.int64_t(ez.EzID),
		collection_id: C.int64_t(ez.CollectionID),
	}
	cstatus := C.UnRefPluginContext(pluginContext)
	if err := ConsumeCStatusIntoError(&cstatus); err != nil {
		return err
	}
	return nil
}

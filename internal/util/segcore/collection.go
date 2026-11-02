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
	"sync"
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
	IndexMeta     *segcorepb.CollectionIndexMeta
	LoadFieldList []int64
	// ShareSchema is for immutable QueryView bindings; scope is passed separately.
	ShareSchema bool
}

// CreateCCollection creates a CCollection from a CreateCCollectionRequest.
func CreateCCollection(req *CreateCCollectionRequest) (*CCollection, error) {
	if req.ShareSchema {
		return createSharedSchemaCollection(req)
	}
	schemaBlob, err := proto.Marshal(req.Schema)
	if err != nil {
		return nil, merr.WrapErrSegcoreMsg("marshal schema failed")
	}
	var indexMetaBlob []byte
	if req.IndexMeta != nil {
		indexMetaBlob, err = proto.Marshal(req.IndexMeta)
		if err != nil {
			return nil, merr.WrapErrSegcoreMsg("marshal index meta failed")
		}
	}
	var ptr C.CCollection
	status := C.NewCollection(unsafe.Pointer(&schemaBlob[0]), (C.int64_t)(len(schemaBlob)), &ptr)
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
	collection := &CCollection{
		collectionID: req.CollectionID,
		ptr:          ptr,
		schema:       req.Schema,
		indexMeta:    req.IndexMeta,
	}
	// NewCollection parses the fields but leaves the native revision at zero.
	// Immutable QueryView planning collections must carry their logical version
	// so LazyCheckSchema can backfill older growing segments before reading.
	if err := collection.UpdateSchema(req.Schema, uint64(req.Schema.GetVersion())); err != nil {
		collection.Release()
		return nil, err
	}
	// Schema initialization can rebuild the native load-field selection. Apply
	// legacy preload hints afterwards so a nonzero version cannot erase them.
	if req.LoadFieldList != nil {
		status = C.UpdateLoadFields(ptr, (*C.int64_t)(unsafe.Pointer(&req.LoadFieldList[0])),
			C.int64_t(len(req.LoadFieldList)))
		if err := ConsumeCStatusIntoError(&status); err != nil {
			collection.Release()
			return nil, err
		}
	}
	return collection, nil
}

// CCollection is just a wrapper of the underlying C-structure CCollection.
// Schema snapshots follow successful native updates and remain immutable for readers.
type CCollection struct {
	ptr          C.CCollection
	collectionID int64
	schemaMu     sync.RWMutex
	// schemaVersion uses the same ordering token as the native Collection.
	// WAL callers pass a timetick, not the logical Schema.Version.
	schemaVersion uint64
	schema        *schemapb.CollectionSchema
	indexMetaMu   sync.RWMutex
	indexMeta     *segcorepb.CollectionIndexMeta
	releaseSchema func()
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
	c.schemaMu.RLock()
	defer c.schemaMu.RUnlock()
	return c.schema
}

func (c *CCollection) IndexMeta() *segcorepb.CollectionIndexMeta {
	c.indexMetaMu.RLock()
	defer c.indexMetaMu.RUnlock()
	return c.indexMeta
}

func (c *CCollection) UpdateIndexMeta(meta *segcorepb.CollectionIndexMeta) error {
	if meta == nil {
		return nil
	}
	c.indexMetaMu.Lock()
	defer c.indexMetaMu.Unlock()

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

func (c *CCollection) UpdateSchema(sch *schemapb.CollectionSchema, version uint64) error {
	if sch == nil {
		return merr.WrapErrServiceInternal("update collection schema with nil")
	}
	c.schemaMu.Lock()
	defer c.schemaMu.Unlock()
	if version <= c.schemaVersion {
		return nil
	}
	if c.releaseSchema != nil {
		return merr.WrapErrServiceInternalMsg("cannot mutate a QueryView schema binding")
	}

	schemaBlob, err := proto.Marshal(sch)
	if err != nil {
		return err
	}

	status := C.UpdateSchema(c.ptr, unsafe.Pointer(&schemaBlob[0]), (C.int64_t)(len(schemaBlob)), (C.uint64_t)(version))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	c.schema = sch
	c.schemaVersion = version
	return nil
}

// Release releases the underlying collection
func (c *CCollection) Release() {
	C.DeleteCollection(c.ptr)
	c.ptr = nil
	if c.releaseSchema != nil {
		c.releaseSchema()
		c.releaseSchema = nil
	}
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

// A parsed Schema is shared by immutable QueryView collection bindings. The
// pool owns one native handle while any binding exists. Published native layouts
// retain the Schema itself, so their lifetime does not depend on this registry.
type sharedSchemaKey struct {
	collectionID int64
	version      int32
}
type sharedCollectionSchema struct {
	collection *CCollection
	refs       int
}

var readCollectionSchemas = struct {
	sync.Mutex
	entries map[sharedSchemaKey]*sharedCollectionSchema
}{entries: make(map[sharedSchemaKey]*sharedCollectionSchema)}

func createSharedSchemaCollection(req *CreateCCollectionRequest) (*CCollection, error) {
	key := sharedSchemaKey{req.CollectionID, req.Schema.GetVersion()}
	readCollectionSchemas.Lock()
	entry := readCollectionSchemas.entries[key]
	if entry == nil {
		owner, err := CreateCCollection(&CreateCCollectionRequest{CollectionID: req.CollectionID, Schema: req.Schema})
		if err != nil {
			readCollectionSchemas.Unlock()
			return nil, err
		}
		entry = &sharedCollectionSchema{collection: owner}
		readCollectionSchemas.entries[key] = entry
	}
	var ptr C.CCollection
	status := C.NewCollectionWithSharedSchema(entry.collection.ptr, &ptr)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		if entry.refs == 0 {
			delete(readCollectionSchemas.entries, key)
			entry.collection.Release()
		}
		readCollectionSchemas.Unlock()
		return nil, err
	}
	entry.refs++
	readCollectionSchemas.Unlock()
	binding := &CCollection{
		ptr: ptr, collectionID: req.CollectionID, schema: req.Schema,
		schemaVersion: uint64(req.Schema.GetVersion()), releaseSchema: func() {
			readCollectionSchemas.Lock()
			entry.refs--
			last := entry.refs == 0
			if last {
				delete(readCollectionSchemas.entries, key)
			}
			readCollectionSchemas.Unlock()
			if last {
				entry.collection.Release()
			}
		},
	}
	if err := binding.UpdateIndexMeta(req.IndexMeta); err != nil {
		binding.Release()
		return nil, err
	}
	return binding, nil
}

package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type schemaEntry struct {
	helper   *typeutil.SchemaHelper
	refCount int64 // >= 0: available, < 0: marked as deleted
}

var (
	schemaMap sync.Map // map[int64]*schemaEntry
	nextID    int64    // atomic increment, starts from 0, first ID is 1
)

// acquireRef tries to acquire a reference to the schema entry.
// Returns true if successful, false if the schema is deleted or being deleted.
func (e *schemaEntry) acquireRef() bool {
	for {
		old := atomic.LoadInt64(&e.refCount)
		if old < 0 {
			return false
		}
		if atomic.CompareAndSwapInt64(&e.refCount, old, old+1) {
			return true
		}
	}
}

// releaseRef releases a reference to the schema entry.
func (e *schemaEntry) releaseRef() {
	atomic.AddInt64(&e.refCount, -1)
}

// tryMarkDeleted tries to mark the schema entry as deleted.
// Returns 0 if successful, 1 if in use, 2 if already deleted.
func (e *schemaEntry) tryMarkDeleted() int {
	if atomic.CompareAndSwapInt64(&e.refCount, 0, -1) {
		return 0 // success
	}
	current := atomic.LoadInt64(&e.refCount)
	if current < 0 {
		return 2 // already deleted
	}
	return 1 // in use
}

//export RegisterSchema
func RegisterSchema(protoBlob unsafe.Pointer, length C.int, errMsg **C.char) C.longlong {
	blob := C.GoBytes(protoBlob, length)
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(blob, schema); err != nil {
		*errMsg = C.CString("failed to unmarshal schema: " + err.Error())
		return 0
	}

	helper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		*errMsg = C.CString("failed to create schema helper: " + err.Error())
		return 0
	}

	id := atomic.AddInt64(&nextID, 1)
	entry := &schemaEntry{helper: helper, refCount: 0}
	schemaMap.Store(id, entry)

	return C.longlong(id)
}

//export UnregisterSchema
func UnregisterSchema(schemaID C.longlong, errMsg **C.char) C.int {
	id := int64(schemaID)

	val, ok := schemaMap.Load(id)
	if !ok {
		*errMsg = C.CString("schema not found")
		return -1
	}
	entry := val.(*schemaEntry)

	switch entry.tryMarkDeleted() {
	case 0: // success
		schemaMap.Delete(id)
		return 0
	case 1: // in use
		*errMsg = C.CString("schema is in use")
		return -1
	case 2: // already deleted
		*errMsg = C.CString("schema already unregistered")
		return -1
	}

	return -1
}

//export Parse
func Parse(schemaID C.longlong, exprStr *C.char, length *C.int, errMsg **C.char) unsafe.Pointer {
	id := int64(schemaID)
	goExprStr := C.GoString(exprStr)

	val, ok := schemaMap.Load(id)
	if !ok {
		*errMsg = C.CString("schema not found")
		return nil
	}
	entry := val.(*schemaEntry)

	if !entry.acquireRef() {
		*errMsg = C.CString("schema has been unregistered")
		return nil
	}
	defer entry.releaseRef()

	planNode, err := planparserv2.CreateRetrievePlan(entry.helper, goExprStr, nil)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return nil
	}

	bytes, err := proto.Marshal(planNode)
	if err != nil {
		*errMsg = C.CString("failed to marshal plan node: " + err.Error())
		return nil
	}

	*length = C.int(len(bytes))
	return C.CBytes(bytes)
}

//export Free
func Free(ptr unsafe.Pointer) {
	C.free(ptr)
}

func main() {}

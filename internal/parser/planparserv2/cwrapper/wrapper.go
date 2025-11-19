package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	schemaMap  sync.Map
	lastError  string
	errorMutex sync.Mutex
)

func setLastError(err error) {
	errorMutex.Lock()
	defer errorMutex.Unlock()
	if err != nil {
		lastError = err.Error()
	} else {
		lastError = ""
	}
}

//export RegisterSchema
func RegisterSchema(protoBlob unsafe.Pointer, len C.int) *C.char {
	blob := C.GoBytes(protoBlob, len)
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(blob, schema); err != nil {
		setLastError(fmt.Errorf("failed to unmarshal schema: %w", err))
		return nil
	}

	helper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		setLastError(fmt.Errorf("failed to create schema helper: %w", err))
		return nil
	}

	schemaMap.Store(schema.Name, helper)
	return C.CString(schema.Name)
}

//export UnregisterSchema
func UnregisterSchema(name *C.char) {
	goName := C.GoString(name)
	schemaMap.Delete(goName)
}

//export Parse
func Parse(schemaName *C.char, exprStr *C.char) unsafe.Pointer {
	goSchemaName := C.GoString(schemaName)
	goExprStr := C.GoString(exprStr)

	val, ok := schemaMap.Load(goSchemaName)
	if !ok {
		setLastError(fmt.Errorf("schema not found: %s", goSchemaName))
		return nil
	}
	helper := val.(*typeutil.SchemaHelper)

	planNode, err := planparserv2.CreateRetrievePlan(helper, goExprStr, nil)
	if err != nil {
		setLastError(err)
		return nil
	}

	bytes, err := proto.Marshal(planNode)
	if err != nil {
		setLastError(fmt.Errorf("failed to marshal plan node: %w", err))
		return nil
	}

	return C.CBytes(bytes)
}

//export ParseProto
func ParseProto(schemaName *C.char, exprStr *C.char, length *C.int) unsafe.Pointer {
	goSchemaName := C.GoString(schemaName)
	goExprStr := C.GoString(exprStr)

	val, ok := schemaMap.Load(goSchemaName)
	if !ok {
		setLastError(fmt.Errorf("schema not found: %s", goSchemaName))
		return nil
	}
	helper := val.(*typeutil.SchemaHelper)

	planNode, err := planparserv2.CreateRetrievePlan(helper, goExprStr, nil)
	if err != nil {
		setLastError(err)
		return nil
	}

	bytes, err := proto.Marshal(planNode)
	if err != nil {
		setLastError(fmt.Errorf("failed to marshal plan node: %w", err))
		return nil
	}

	*length = C.int(len(bytes))
	return C.CBytes(bytes)
}

//export GetLastErrorMessage
func GetLastErrorMessage() *C.char {
	errorMutex.Lock()
	defer errorMutex.Unlock()
	if lastError == "" {
		return nil
	}
	return C.CString(lastError)
}

//export Free
func Free(ptr unsafe.Pointer) {
	C.free(ptr)
}

func main() {}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/schema_c.h"
*/
import "C"

import (
	"runtime"
	"sync"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SchemaRef owns one reference to an immutable schema in segcore's process-wide
// schema cache. Clone before handing the schema to another independently-lived
// Go owner, and release each clone exactly once.
type SchemaRef struct {
	handle C.CSchemaHandle
	once   sync.Once
}

// AcquireSchemaRef gets the cached schema identified by
// (collectionID, schema.Version).
func AcquireSchemaRef(collectionID int64, schema *schemapb.CollectionSchema) (*SchemaRef, error) {
	if schema == nil {
		return nil, merr.WrapErrParameterInvalidMsg("schema is nil")
	}

	blob, err := proto.Marshal(schema)
	if err != nil {
		return nil, merr.Wrap(err, "failed to marshal schema")
	}
	if len(blob) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("marshaled schema is empty")
	}

	var handle C.CSchemaHandle
	status := C.AcquireSchemaHandle(
		C.int64_t(collectionID),
		unsafe.Pointer(&blob[0]),
		C.int64_t(len(blob)),
		&handle,
	)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	if handle == nil {
		return nil, merr.WrapErrServiceInternalMsg("segcore returned an empty schema handle")
	}

	ref := &SchemaRef{handle: handle}
	runtime.SetFinalizer(ref, (*SchemaRef).Release)
	return ref, nil
}

// Clone creates an independent owner of the same cached schema.
func (r *SchemaRef) Clone() (*SchemaRef, error) {
	if r == nil || r.handle == nil {
		return nil, merr.WrapErrServiceInternalMsg("clone released schema reference")
	}
	defer runtime.KeepAlive(r)

	var handle C.CSchemaHandle
	status := C.CloneSchemaHandle(r.handle, &handle)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	if handle == nil {
		return nil, merr.WrapErrServiceInternalMsg("segcore returned an empty cloned schema handle")
	}

	clone := &SchemaRef{handle: handle}
	runtime.SetFinalizer(clone, (*SchemaRef).Release)
	return clone, nil
}

// Release drops this owner. It is safe to call more than once.
func (r *SchemaRef) Release() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.handle != nil {
			C.ReleaseSchemaHandle(r.handle)
			r.handle = nil
		}
		runtime.SetFinalizer(r, nil)
	})
}

func (r *SchemaRef) rawPointer() C.CSchemaHandle {
	if r == nil {
		return nil
	}
	return r.handle
}

// AcquireLoadSchemaRef builds one immutable, non-cached native schema carrying
// QueryCoord's effective mmap/warmup policy and the selected load fields.
func AcquireLoadSchemaRef(loadSchema *schemapb.CollectionSchema, loadFields []int64) (*SchemaRef, error) {
	if loadSchema == nil {
		return nil, merr.WrapErrParameterInvalidMsg("load schema is nil")
	}
	blob, err := proto.Marshal(loadSchema)
	if err != nil {
		return nil, merr.Wrap(err, "failed to marshal load schema")
	}
	if len(blob) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("marshaled load schema is empty")
	}

	var fields *C.int64_t
	if len(loadFields) > 0 {
		fields = (*C.int64_t)(unsafe.Pointer(&loadFields[0]))
	}
	var handle C.CSchemaHandle
	status := C.AcquireLoadSchemaHandle(
		unsafe.Pointer(&blob[0]),
		C.int64_t(len(blob)),
		fields,
		C.int64_t(len(loadFields)),
		&handle,
	)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	if handle == nil {
		return nil, merr.WrapErrServiceInternalMsg("segcore returned an empty load schema handle")
	}

	ref := &SchemaRef{handle: handle}
	runtime.SetFinalizer(ref, (*SchemaRef).Release)
	return ref, nil
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build cgo

package pyudf

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "pyudf/pyudf_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func embeddedBuildCapability() BuildCapability {
	if C.PyUDFRuntimeBuildEnabled() {
		return BuildCapability{Available: true}
	}
	return BuildCapability{
		Reason: "embedded PyUDF runtime is not compiled into this binary",
	}
}

func initializeNativeRuntime() error {
	status := C.InitializePyUDFRuntime()
	return consumePyUDFCStatus(&status)
}

type nativeResource struct {
	handle C.CPyUDFResource
}

func loadNativeResource(serializedRequest []byte) (*nativeResource, error) {
	var requestPtr *C.uint8_t
	if len(serializedRequest) > 0 {
		requestPtr = (*C.uint8_t)(unsafe.Pointer(&serializedRequest[0]))
	}
	var handle C.CPyUDFResource
	status := C.LoadPyUDFResource(requestPtr, C.uint64_t(len(serializedRequest)), &handle)
	if err := consumePyUDFCStatus(&status); err != nil {
		if handle != nil {
			deleteStatus := C.DeletePyUDFResource(handle)
			_ = consumePyUDFCStatus(&deleteStatus)
		}
		return nil, err
	}
	if handle == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: native load returned nil resource")
	}
	return &nativeResource{handle: handle}, nil
}

func (resource *nativeResource) Close() error {
	if resource == nil || resource.handle == nil {
		return nil
	}
	status := C.DeletePyUDFResource(resource.handle)
	resource.handle = nil
	return consumePyUDFCStatus(&status)
}

package cgo

/*
#cgo pkg-config: milvus_core

#include "common/type_c.h"
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func ConsumeCStatusIntoError(status *C.CStatus) error {
	return consumeCStatusIntoError(status, nil)
}

func consumeCStatusIntoError(status *C.CStatus, mapper ErrorMapper) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorMsg := C.GoString(status.error_msg)
	getCGOCaller().call("free", func() {
		C.free(unsafe.Pointer(status.error_msg))
	})
	if mapper != nil {
		return mapper(int32(errorCode), errorMsg)
	}
	return merr.SegcoreError(int32(errorCode), errorMsg)
}

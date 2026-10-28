package indexcgowrapper

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>	// free
#include "storage/storage_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// HandleCStatus deal with the error returned from CGO
func HandleCStatus(status *C.CStatus, extraInfo string) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := int(status.error_code)
	errorMsg := C.GoString(status.error_msg)
	defer C.free(unsafe.Pointer(status.error_msg))

	logMsg := fmt.Sprintf("%s, C Runtime Exception: %s\n", extraInfo, errorMsg)
	mlog.Warn(context.TODO(), logMsg)
	if merr.IsSegcoreSignal(int32(errorCode)) {
		mlog.Info(context.TODO(), "fake finished the task")
	}
	// Pass the raw errorMsg (not the polluted logMsg) so the merr reason stays
	// clean; the extraInfo breadcrumb lives in the log above.
	return merr.SegcoreError(int32(errorCode), errorMsg)
}

func GetLocalUsedSize(path string) (int64, error) {
	var availableSize int64
	cSize := (*C.int64_t)(&availableSize)
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	status := C.GetLocalUsedSize(cPath, cSize)
	err := HandleCStatus(&status, "get local used size failed")
	if err != nil {
		return 0, err
	}

	return availableSize, nil
}

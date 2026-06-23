package canalyzer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "common/type_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
	log.Warn(logMsg)
	if merr.IsSegcoreSignal(int32(errorCode)) {
		log.Info("fake finished the task")
	}
	// Pass the raw errorMsg (not the polluted logMsg) so the merr reason stays
	// clean; the extraInfo breadcrumb lives in the log above.
	return merr.SegcoreError(int32(errorCode), errorMsg)
}

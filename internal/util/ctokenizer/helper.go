package ctokenizer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "common/type_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	if errorCode == 2003 {
		return merr.WrapErrSegcoreUnsupported(int32(errorCode), logMsg)
	}
	if errorCode == 2033 {
		log.Info("fake finished the task")
		return merr.ErrSegcorePretendFinished
	}
	return merr.WrapErrSegcore(int32(errorCode), logMsg)
}

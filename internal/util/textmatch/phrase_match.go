package textmatch

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>
#include "segcore/phrase_match_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ComputePhraseMatchSlop computes the minimum slop required for a phrase match
// between query and data texts using the specified analyzer.
// Returns the slop value if match is possible, or error if terms are missing.
func ComputePhraseMatchSlop(analyzerParams string, query string, data string) (int32, error) {
	cParams := C.CString(analyzerParams)
	defer C.free(unsafe.Pointer(cParams))

	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	cData := C.CString(data)
	defer C.free(unsafe.Pointer(cData))

	var slop C.uint32_t

	status := C.compute_phrase_match_slop_c(cParams, cQuery, cData, &slop)
	if err := handleCStatus(&status, "failed to compute phrase match slop"); err != nil {
		return 0, err
	}

	return int32(slop), nil
}

// handleCStatus deals with the error returned from CGO
func handleCStatus(status *C.CStatus, extraInfo string) error {
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

package canalyzer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "segcore/tokenizer_c.h"
#include "segcore/token_stream_c.h"
*/
import "C"

import (
	"unsafe"
)

func NewAnalyzer(param string) (*CAnalyzer, error) {
	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	var ptr C.CTokenizer
	status := C.create_tokenizer(paramPtr, &ptr)
	if err := HandleCStatus(&status, "failed to create analyzer"); err != nil {
		return nil, err
	}

	return NewCAnalyzer(ptr), nil
}

func ValidateAnalyzer(param string) error {
	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	status := C.validate_tokenizer(paramPtr)
	if err := HandleCStatus(&status, "failed to create analyzer"); err != nil {
		return err
	}
	return nil
}

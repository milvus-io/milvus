package ctokenizer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "segcore/tokenizer_c.h"
#include "segcore/token_stream_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/tokenizerapi"
)

func NewTokenizer(param string) (tokenizerapi.Tokenizer, error) {
	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	var ptr C.CTokenizer
	status := C.create_tokenizer(paramPtr, &ptr)
	if err := HandleCStatus(&status, "failed to create tokenizer"); err != nil {
		return nil, err
	}

	return NewCTokenizer(ptr), nil
}

func ValidateTokenizer(param string) error {
	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	status := C.validate_tokenizer(paramPtr)
	if err := HandleCStatus(&status, "failed to create tokenizer"); err != nil {
		return err
	}
	return nil
}

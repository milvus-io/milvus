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

	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
)

var _ interfaces.Analyzer = (*CAnalyzer)(nil)

type CAnalyzer struct {
	ptr C.CTokenizer
}

func NewCAnalyzer(ptr C.CTokenizer) *CAnalyzer {
	return &CAnalyzer{
		ptr: ptr,
	}
}

func (impl *CAnalyzer) NewTokenStream(text string) interfaces.TokenStream {
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))
	ptr := C.create_token_stream(impl.ptr, cText, (C.uint32_t)(len(text)))
	return NewCTokenStream(ptr)
}

func (impl *CAnalyzer) Clone() (interfaces.Analyzer, error) {
	var newptr C.CTokenizer
	status := C.clone_tokenizer(&impl.ptr, &newptr)
	if err := HandleCStatus(&status, "failed to clone tokenizer"); err != nil {
		return nil, err
	}
	return NewCAnalyzer(newptr), nil
}

func (impl *CAnalyzer) Destroy() {
	C.free_tokenizer(impl.ptr)
}

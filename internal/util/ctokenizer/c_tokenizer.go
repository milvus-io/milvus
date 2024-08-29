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

var _ tokenizerapi.Tokenizer = (*CTokenizer)(nil)

type CTokenizer struct {
	ptr C.CTokenizer
}

func NewCTokenizer(ptr C.CTokenizer) *CTokenizer {
	return &CTokenizer{
		ptr: ptr,
	}
}

func (impl *CTokenizer) NewTokenStream(text string) tokenizerapi.TokenStream {
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))
	ptr := C.create_token_stream(impl.ptr, cText, (C.uint32_t)(len(text)))
	return NewCTokenStream(ptr)
}

func (impl *CTokenizer) Destroy() {
	C.free_tokenizer(impl.ptr)
}

package ctokenizer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "segcore/token_stream_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/tokenizerapi"
)

var _ tokenizerapi.TokenStream = (*CTokenStream)(nil)

type CTokenStream struct {
	ptr C.CTokenStream
}

func NewCTokenStream(ptr C.CTokenStream) *CTokenStream {
	return &CTokenStream{
		ptr: ptr,
	}
}

func (impl *CTokenStream) Advance() bool {
	return bool(C.token_stream_advance(impl.ptr))
}

func (impl *CTokenStream) Token() string {
	token := C.token_stream_get_token(impl.ptr)
	defer C.free_token(unsafe.Pointer(token))
	return C.GoString(token)
}

func (impl *CTokenStream) Destroy() {
	C.free_token_stream(impl.ptr)
}

package ctokenizer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "segcore/tokenizer_c.h"
#include "segcore/token_stream_c.h"
*/
import "C"

import (
	"github.com/milvus-io/milvus/internal/util/tokenizerapi"
)

func NewTokenizer(m map[string]string) (tokenizerapi.Tokenizer, error) {
	mm := NewCMap()
	defer mm.Destroy()
	mm.From(m)

	var ptr C.CTokenizer
	status := C.create_tokenizer(mm.GetPointer(), &ptr)
	if err := HandleCStatus(&status, "failed to create tokenizer"); err != nil {
		return nil, err
	}

	return NewCTokenizer(ptr), nil
}

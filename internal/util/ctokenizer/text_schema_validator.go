package ctokenizer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "segcore/tokenizer_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func ValidateTextSchema(fieldSchema *schemapb.FieldSchema, EnableBM25 bool) error {
	h := typeutil.CreateFieldSchemaHelper(fieldSchema)
	if !h.EnableMatch() && !EnableBM25 {
		return nil
	}

	if !h.EnableAnalyzer() {
		return fmt.Errorf("field %s is set to enable match or bm25 function but not enable analyzer", fieldSchema.Name)
	}

	bs, err := proto.Marshal(fieldSchema)
	if err != nil {
		return fmt.Errorf("failed to marshal field schema: %w", err)
	}

	status := C.validate_text_schema((*C.uint8_t)(unsafe.Pointer(&bs[0])), (C.uint64_t)(len(bs)))
	return HandleCStatus(&status, "failed to validate text schema")
}

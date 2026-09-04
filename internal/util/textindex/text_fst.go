// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package textindex

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "textindex/fst/text_fst_c.h"
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"slices"
	"sort"
	"unsafe"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MilvusTextFstFormat identifies the FST format shared by writers and loaders.
const MilvusTextFstFormat = "milvus_text_fst_v1"

type FstArtifact struct {
	Data      []byte
	TermCount int64
}

// BuildTextFst builds one field-level FST from the union of all terms emitted
// for that field. Multi-analyzer row dispatch does not partition the keys.
func BuildTextFst(terms [][]byte) (*FstArtifact, error) {
	keys := slices.Clone(terms)
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	keys = slices.CompactFunc(keys, bytes.Equal)

	var encoded bytes.Buffer
	encodedSize := 8
	for _, key := range keys {
		encodedSize += 8 + len(key)
	}
	encoded.Grow(encodedSize)
	if err := binary.Write(&encoded, binary.LittleEndian, uint64(len(keys))); err != nil {
		return nil, merr.Wrap(err, "encode text FST term count")
	}
	for _, key := range keys {
		if err := binary.Write(&encoded, binary.LittleEndian, uint64(len(key))); err != nil {
			return nil, merr.Wrap(err, "encode text FST term length")
		}
		if _, err := encoded.Write(key); err != nil {
			return nil, merr.Wrap(err, "encode text FST term")
		}
	}
	input := encoded.Bytes()
	result := C.BuildTextFst(
		(*C.uint8_t)(unsafe.Pointer(&input[0])),
		C.int64_t(len(input)),
	)
	if result.status.error_code != 0 {
		errorCode := int32(result.status.error_code)
		errorMessage := C.GoString(result.status.error_msg)
		C.free(unsafe.Pointer(result.status.error_msg))
		return nil, merr.SegcoreError(errorCode, errorMessage)
	}
	defer C.DeleteTextFst(result.handle)
	if result.handle == nil {
		return nil, merr.WrapErrServiceInternal("built text FST returned no owner")
	}
	if result.data_size < 0 || uint64(result.data_size) > uint64(^uint(0)>>1) {
		return nil, merr.WrapErrServiceInternal("built text FST size does not fit Go memory")
	}
	if result.data_size > 0 && result.data == nil {
		return nil, merr.WrapErrServiceInternal("built text FST returned null data")
	}
	if result.term_count != C.int64_t(len(keys)) {
		return nil, merr.WrapErrServiceInternal("built text FST returned an unexpected term count")
	}
	data := bytes.Clone(unsafe.Slice((*byte)(unsafe.Pointer(result.data)), int(result.data_size)))
	return &FstArtifact{
		Data:      data,
		TermCount: int64(result.term_count),
	}, nil
}

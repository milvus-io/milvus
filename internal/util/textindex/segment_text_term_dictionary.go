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
#include "textindex/segment_text_term_dictionary_c.h"
*/
import "C"

import (
	"runtime"
	"unicode/utf8"
	"unsafe"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type SegmentTextTermTrieStats struct {
	TermCount  int64
	MemorySize int64
}

func textFstHandles(readers []*FstReader) (*C.CTextFstHandle, error) {
	for _, reader := range readers {
		if reader == nil || reader.handle == nil {
			return nil, merr.WrapErrServiceInternalMsg("use closed text FST reader")
		}
	}
	if len(readers) == 0 {
		return nil, nil
	}

	handleSize := unsafe.Sizeof(C.CTextFstHandle(nil))
	if uint64(len(readers)) > uint64(^uintptr(0))/uint64(handleSize) {
		return nil, merr.WrapErrServiceInternalMsg("too many text FST readers")
	}
	handles := (*C.CTextFstHandle)(C.malloc(C.size_t(uintptr(len(readers)) * handleSize)))
	if handles == nil {
		return nil, merr.Wrap(merr.ErrServiceResourceInsufficient, "allocate text FST handle array")
	}
	handleSlice := unsafe.Slice(handles, len(readers))
	for i, reader := range readers {
		handleSlice[i] = reader.handle
	}
	return handles, nil
}

// UpdateSegmentTextTermTrie adds terms to the mutable Trie owned by a segcore
// segment. segment must remain pinned for the duration of the call.
func UpdateSegmentTextTermTrie(segment unsafe.Pointer, fieldID int64, terms [][]byte) (SegmentTextTermTrieStats, error) {
	if segment == nil {
		return SegmentTextTermTrieStats{}, merr.WrapErrServiceInternalMsg("update text term Trie on nil segment")
	}
	for _, term := range terms {
		if len(term) == 0 {
			return SegmentTextTermTrieStats{}, merr.WrapErrDataIntegrityMsg("fuzzy BM25 term is empty")
		}
		if !utf8.Valid(term) {
			return SegmentTextTermTrieStats{}, merr.WrapErrDataIntegrityMsg("fuzzy BM25 term is not valid UTF-8")
		}
	}
	encoded, err := encodeTextTerms(terms)
	if err != nil {
		return SegmentTextTermTrieStats{}, err
	}
	result := C.UpdateSegmentTextTermTrie(
		C.CSegmentInterface(segment),
		C.int64_t(fieldID),
		(*C.uint8_t)(unsafe.Pointer(&encoded[0])),
		C.int64_t(len(encoded)),
	)
	stats := SegmentTextTermTrieStats{
		TermCount:  int64(result.term_count),
		MemorySize: int64(result.memory_size),
	}
	if result.status.error_code != 0 {
		errorCode := int32(result.status.error_code)
		errorMessage := C.GoString(result.status.error_msg)
		C.free(unsafe.Pointer(result.status.error_msg))
		return stats, merr.SegcoreError(errorCode, errorMessage)
	}
	if stats.TermCount < 0 || stats.MemorySize < 0 {
		return SegmentTextTermTrieStats{}, merr.WrapErrServiceInternalMsg("segment text term Trie returned invalid stats")
	}
	return stats, nil
}

// AddTextFstsToSegmentTextTermTrie streams persisted FST terms into the
// mutable Trie owned by a growing segcore segment. The readers remain owned by
// the caller and must stay alive until this call returns.
func AddTextFstsToSegmentTextTermTrie(segment unsafe.Pointer, fieldID int64, readers []*FstReader) (SegmentTextTermTrieStats, error) {
	if segment == nil {
		return SegmentTextTermTrieStats{}, merr.WrapErrServiceInternalMsg("import text FSTs into nil segment")
	}
	handles, err := textFstHandles(readers)
	if err != nil {
		return SegmentTextTermTrieStats{}, err
	}
	if handles != nil {
		defer C.free(unsafe.Pointer(handles))
	}

	result := C.AddSegmentTextTermFstsToTrie(
		C.CSegmentInterface(segment),
		C.int64_t(fieldID),
		handles,
		C.int64_t(len(readers)),
	)
	runtime.KeepAlive(readers)
	stats := SegmentTextTermTrieStats{
		TermCount:  int64(result.term_count),
		MemorySize: int64(result.memory_size),
	}
	if result.status.error_code != 0 {
		errorCode := int32(result.status.error_code)
		errorMessage := C.GoString(result.status.error_msg)
		C.free(unsafe.Pointer(result.status.error_msg))
		if bool(result.is_data_integrity_error) {
			return stats, merr.WrapErrDataIntegrityMsg("%s", errorMessage)
		}
		return stats, merr.SegcoreError(errorCode, errorMessage)
	}
	if stats.TermCount < 0 || stats.MemorySize < 0 {
		return SegmentTextTermTrieStats{}, merr.WrapErrServiceInternalMsg("segment text term Trie returned invalid stats")
	}
	return stats, nil
}

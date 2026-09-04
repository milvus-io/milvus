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
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"unicode/utf8"
	"unsafe"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	maxFuzzySearchTermCount       = 512
	maxFuzzySearchTermCodePoints  = 256
	maxFuzzySearchTotalCodePoints = 512
)

type SegmentTextTermTrieStats struct {
	TermCount  int64
	MemorySize int64
}

type FuzzyMatch struct {
	Term         []byte
	EditDistance uint32
}

// PreparedFuzzySearch owns one immutable native query DFA. It may be reused
// sequentially across every segment and FST/Trie component in one request.
type PreparedFuzzySearch struct {
	handle    C.CTextTermFuzzyQueryHandle
	closeOnce sync.Once
}

func validateFuzzySearchTerm(term []byte) (int, error) {
	if !utf8.Valid(term) {
		return 0, merr.WrapErrServiceInternalMsg("fuzzy BM25 query term is not valid UTF-8")
	}
	codePoints := utf8.RuneCount(term)
	if codePoints > maxFuzzySearchTermCodePoints {
		return 0, merr.WrapErrParameterTooLarge(fmt.Sprintf(
			"fuzzy BM25 source term has %d code points, maximum is %d",
			codePoints, maxFuzzySearchTermCodePoints))
	}
	return codePoints, nil
}

// ValidateFuzzySearchTerms bounds dense native DFA construction before the
// first Cgo call. The limits cover one request after source-term deduplication.
func ValidateFuzzySearchTerms(terms [][]byte) error {
	if len(terms) > maxFuzzySearchTermCount {
		return merr.WrapErrParameterTooLarge(fmt.Sprintf(
			"fuzzy BM25 source term count %d exceeds maximum %d",
			len(terms), maxFuzzySearchTermCount))
	}
	totalCodePoints := 0
	for _, term := range terms {
		codePoints, err := validateFuzzySearchTerm(term)
		if err != nil {
			return err
		}
		totalCodePoints += codePoints
		if totalCodePoints > maxFuzzySearchTotalCodePoints {
			return merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"fuzzy BM25 source terms have %d code points, maximum is %d",
				totalCodePoints, maxFuzzySearchTotalCodePoints))
		}
	}
	return nil
}

func PrepareFuzzySearch(
	term []byte,
	maxEditDistance, prefixLength uint32,
) (*PreparedFuzzySearch, error) {
	if maxEditDistance > 2 {
		return nil, merr.WrapErrServiceInternalMsg("fuzzy max edit distance must be in [0, 2]")
	}
	if _, err := validateFuzzySearchTerm(term); err != nil {
		return nil, err
	}
	var query *C.uint8_t
	if len(term) > 0 {
		query = (*C.uint8_t)(unsafe.Pointer(&term[0]))
	}
	result := C.PrepareSegmentTextTermFuzzyQuery(
		query,
		C.int64_t(len(term)),
		C.uint32_t(maxEditDistance),
		C.uint32_t(prefixLength),
	)
	runtime.KeepAlive(term)
	if result.status.error_code != 0 {
		errorCode := int32(result.status.error_code)
		errorMessage := C.GoString(result.status.error_msg)
		C.free(unsafe.Pointer(result.status.error_msg))
		return nil, merr.SegcoreError(errorCode, errorMessage)
	}
	if result.handle == nil {
		return nil, merr.WrapErrServiceInternalMsg("prepared fuzzy query returned a nil handle")
	}
	prepared := &PreparedFuzzySearch{handle: result.handle}
	runtime.SetFinalizer(prepared, (*PreparedFuzzySearch).Close)
	return prepared, nil
}

// Close releases the native DFA. It must not run concurrently with searches.
func (q *PreparedFuzzySearch) Close() {
	if q == nil {
		return
	}
	q.closeOnce.Do(func() {
		runtime.SetFinalizer(q, nil)
		if q.handle != nil {
			C.DeleteSegmentTextTermFuzzyQuery(q.handle)
			q.handle = nil
		}
	})
}

// PrepareFuzzySearchTerms builds exactly one DFA for each source term.
func PrepareFuzzySearchTerms(
	terms [][]byte,
	maxEditDistance, prefixLength uint32,
) ([]*PreparedFuzzySearch, error) {
	if err := ValidateFuzzySearchTerms(terms); err != nil {
		return nil, err
	}
	prepared := make([]*PreparedFuzzySearch, 0, len(terms))
	closePrepared := func() {
		for _, query := range prepared {
			query.Close()
		}
	}
	for _, term := range terms {
		query, err := PrepareFuzzySearch(term, maxEditDistance, prefixLength)
		if err != nil {
			closePrepared()
			return nil, err
		}
		prepared = append(prepared, query)
	}
	return prepared, nil
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

// FuzzySearchSegmentTextTermsPrepared reuses a request-scoped DFA.
func FuzzySearchSegmentTextTermsPrepared(
	segment unsafe.Pointer,
	fieldID int64,
	readers []*FstReader,
	prepared *PreparedFuzzySearch,
	maxExpansions uint32,
) ([]FuzzyMatch, error) {
	if segment == nil {
		return nil, merr.WrapErrServiceInternalMsg("search text terms on nil segment")
	}
	if prepared == nil || prepared.handle == nil {
		return nil, merr.WrapErrServiceInternalMsg("search text terms with a closed prepared fuzzy query")
	}
	if maxExpansions == 0 {
		return nil, merr.WrapErrServiceInternalMsg("fuzzy max expansions must be positive")
	}
	handles, err := textFstHandles(readers)
	if err != nil {
		return nil, err
	}
	if handles != nil {
		defer C.free(unsafe.Pointer(handles))
	}

	result := C.FuzzySearchSegmentTextTermsPrepared(
		C.CSegmentInterface(segment),
		C.int64_t(fieldID),
		handles,
		C.int64_t(len(readers)),
		prepared.handle,
		C.uint32_t(maxExpansions),
	)
	runtime.KeepAlive(readers)
	runtime.KeepAlive(prepared)
	defer C.FreeTextFstFuzzyResult(&result)
	if result.status.error_code != 0 {
		errorCode := int32(result.status.error_code)
		errorMessage := C.GoString(result.status.error_msg)
		C.free(unsafe.Pointer(result.status.error_msg))
		return nil, merr.SegcoreError(errorCode, errorMessage)
	}
	if result.match_count < 0 || uint64(result.match_count) > uint64(^uint(0)>>1) ||
		(result.match_count > 0 && result.matches == nil) {
		return nil, merr.WrapErrServiceInternalMsg("segment text term fuzzy search returned invalid matches")
	}
	matches := unsafe.Slice(result.matches, int(result.match_count))
	output := make([]FuzzyMatch, 0, len(matches))
	for _, match := range matches {
		if match.term_size < 0 || uint64(match.term_size) > uint64(^uint(0)>>1) ||
			(match.term_size > 0 && match.term == nil) {
			return nil, merr.WrapErrServiceInternalMsg("segment text term fuzzy search returned invalid term")
		}
		output = append(output, FuzzyMatch{
			Term:         bytes.Clone(unsafe.Slice((*byte)(unsafe.Pointer(match.term)), int(match.term_size))),
			EditDistance: uint32(match.edit_distance),
		})
	}
	return output, nil
}

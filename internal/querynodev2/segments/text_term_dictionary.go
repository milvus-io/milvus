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

package segments

import (
	"os"
	"sort"
	"sync"
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"

	"github.com/milvus-io/milvus/internal/util/textindex"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadedTextTermDictionary struct {
	readers   map[int64][]*textindex.FstReader
	cacheDir  string
	heapBytes int64
}

func (d *loadedTextTermDictionary) close() {
	if d == nil {
		return
	}
	for _, readers := range d.readers {
		for _, reader := range readers {
			reader.Close()
		}
	}
	if d.cacheDir != "" {
		_ = os.RemoveAll(d.cacheDir)
	}
}

type segmentTextTermDictionary struct {
	mu            sync.RWMutex
	statsMu       sync.Mutex
	nativeSegment unsafe.Pointer
	loaded        *loadedTextTermDictionary
	termCount     int64
	memorySize    int64
}

func newSegmentTextTermDictionary(nativeSegment unsafe.Pointer) *segmentTextTermDictionary {
	return &segmentTextTermDictionary{
		nativeSegment: nativeSegment,
	}
}

func (d *segmentTextTermDictionary) add(batches []*msgpb.TextTermBatch) error {
	if d == nil || len(batches) == 0 {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, batch := range batches {
		if batch == nil {
			continue
		}
		stats, err := textindex.UpdateSegmentTextTermTrie(
			d.nativeSegment,
			batch.GetInputFieldId(),
			batch.GetTerms(),
		)
		d.statsMu.Lock()
		if stats.TermCount >= d.termCount && stats.MemorySize >= d.memorySize {
			d.termCount = stats.TermCount
			d.memorySize = stats.MemorySize
		}
		d.statsMu.Unlock()
		if err != nil {
			// Native insertion can make partial monotonic progress before an
			// allocation failure. Preserve any usable post-failure accounting;
			// validation failures return zero values and leave the prior totals.
			return err
		}
	}
	return nil
}

func (d *segmentTextTermDictionary) replaceLoaded(loaded *loadedTextTermDictionary) {
	if d == nil {
		if loaded != nil {
			loaded.close()
		}
		return
	}
	d.mu.Lock()
	old := d.loaded
	d.loaded = loaded
	d.mu.Unlock()
	old.close()
}

// importLoaded streams all persisted FST terms into the segment-owned Trie and
// releases the FST readers before returning. It is used only while recovering
// an unpublished growing segment, so a failed monotonic import is discarded
// with that segment.
func (d *segmentTextTermDictionary) importLoaded(loaded *loadedTextTermDictionary) error {
	if loaded == nil {
		return nil
	}
	if d == nil {
		loaded.close()
		return merr.WrapErrServiceInternalMsg("import text terms into nil segment dictionary")
	}
	defer loaded.close()

	d.mu.Lock()
	defer d.mu.Unlock()

	fieldIDs := make([]int64, 0, len(loaded.readers))
	for fieldID := range loaded.readers {
		fieldIDs = append(fieldIDs, fieldID)
	}
	sort.Slice(fieldIDs, func(i, j int) bool { return fieldIDs[i] < fieldIDs[j] })
	for _, fieldID := range fieldIDs {
		stats, err := textindex.AddTextFstsToSegmentTextTermTrie(
			d.nativeSegment,
			fieldID,
			loaded.readers[fieldID],
		)
		// Native insertion is monotonic and may make partial progress before an
		// allocation or persisted-data failure. Keep accounting synchronized so
		// the unpublished segment can still release/report its actual footprint.
		if stats.TermCount >= d.termCount && stats.MemorySize >= d.memorySize {
			d.termCount = stats.TermCount
			d.memorySize = stats.MemorySize
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *segmentTextTermDictionary) memoryBytes() int64 {
	if d == nil {
		return 0
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	result := d.memorySize
	if d.loaded != nil {
		result += d.loaded.heapBytes
	}
	return result
}

func (d *segmentTextTermDictionary) expandPrepared(
	fieldID int64,
	prepared []*textindex.PreparedFuzzySearch,
	maxExpansions uint32,
) ([][]textindex.FuzzyMatch, error) {
	if d == nil {
		return make([][]textindex.FuzzyMatch, len(prepared)), nil
	}
	if maxExpansions == 0 {
		return nil, merr.WrapErrServiceInternalMsg("fuzzy max expansions must be positive")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	var readers []*textindex.FstReader
	if d.loaded != nil {
		readers = d.loaded.readers[fieldID]
	}
	result := make([][]textindex.FuzzyMatch, len(prepared))
	for sourceIndex, query := range prepared {
		matches, err := textindex.FuzzySearchSegmentTextTermsPrepared(
			d.nativeSegment,
			fieldID,
			readers,
			query,
			maxExpansions,
		)
		if err != nil {
			return nil, err
		}
		result[sourceIndex] = matches
	}
	return result, nil
}

func (d *segmentTextTermDictionary) close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	loaded := d.loaded
	d.loaded = nil
	d.nativeSegment = nil
	d.termCount = 0
	d.memorySize = 0
	d.mu.Unlock()
	loaded.close()
}

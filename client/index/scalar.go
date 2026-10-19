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

package index

import "strconv"

// Scalar index build param keys. The server rejects unknown or out-of-range
// values, so these mirror the checkers in internal/util/indexparamcheck.
const (
	ngramMinGramKey = `min_gram`
	ngramMaxGramKey = `max_gram`

	fmIndexSaSampleRateKey = `fm_sa_sample_rate`
	fmIndexBlockBytesKey   = `fm_block_bytes`
)

type scalarIndex struct {
	name      string
	indexType IndexType
	// extra build params; nil for the index types that take none
	params map[string]string
}

func (idx scalarIndex) Name() string {
	return idx.name
}

func (idx scalarIndex) IndexType() IndexType {
	return idx.indexType
}

func (idx scalarIndex) Params() map[string]string {
	result := map[string]string{
		IndexTypeKey: string(idx.indexType),
	}
	for k, v := range idx.params {
		result[k] = v
	}
	return result
}

var _ Index = scalarIndex{}

func NewTrieIndex() Index {
	return scalarIndex{
		indexType: Trie,
	}
}

func NewInvertedIndex() Index {
	return scalarIndex{
		indexType: Inverted,
	}
}

func NewSortedIndex() Index {
	return scalarIndex{
		indexType: Sorted,
	}
}

func NewBitmapIndex() Index {
	return scalarIndex{
		indexType: BITMAP,
	}
}

// NewNgramIndex creates an NGRAM index for VARCHAR fields and JSON paths. By
// indexing every n-gram in [minGram, maxGram] it accelerates the substring
// operators — LIKE prefix / infix / suffix and regex match. TEXT_MATCH is a
// different index (TextMatchIndex) and is not served by this one.
// Both bounds are mandatory: the server rejects an NGRAM index that omits
// either, so there is no default to fall back to.
func NewNgramIndex(minGram, maxGram int) Index {
	return scalarIndex{
		indexType: NGRAM,
		params: map[string]string{
			ngramMinGramKey: strconv.Itoa(minGram),
			ngramMaxGramKey: strconv.Itoa(maxGram),
		},
	}
}

var _ Index = &FMIndex{}

// FMIndex is the pre-defined index model for the FM-index scalar index: an
// exact byte-level substring index for VARCHAR that answers anchored LIKE
// (prefix / infix / suffix) with no candidate recheck.
//
// Both build params are optional; leave them unset to take the server defaults.
type FMIndex struct {
	scalarIndex

	// Recorded verbatim once a setter is called. Gating on a positive value
	// instead would make an explicitly invalid argument indistinguishable from
	// an omitted one, and quietly build with the default rather than letting
	// the server reject it.
	buildParams map[string]string
}

// WithIndexName setup the index name of FMIndex.
func (idx *FMIndex) WithIndexName(name string) *FMIndex {
	idx.name = name
	return idx
}

// WithSaSampleRate sets the suffix-array sampling rate, trading index size
// against locate latency (it does not affect count-only queries). Must be in
// [4, 256]; the server default is 8.
func (idx *FMIndex) WithSaSampleRate(rate int) *FMIndex {
	idx.buildParams[fmIndexSaSampleRateKey] = strconv.Itoa(rate)
	return idx
}

// WithBlockBytes sets the rank-directory block granularity in bytes. Must be a
// power of two in [8, 128]; the server default is 64. Larger blocks shrink the
// resident directory at no throughput cost up to ~64.
func (idx *FMIndex) WithBlockBytes(blockBytes int) *FMIndex {
	idx.buildParams[fmIndexBlockBytesKey] = strconv.Itoa(blockBytes)
	return idx
}

// Params implements Index interface
// returns the create index related parameters.
func (idx *FMIndex) Params() map[string]string {
	result := map[string]string{
		IndexTypeKey: string(idx.indexType),
	}
	// A param the caller never set stays absent so the server applies its own
	// default; one the caller did set is forwarded as given, valid or not, so
	// the server's range check is what reports the mistake.
	for k, v := range idx.buildParams {
		result[k] = v
	}
	return result
}

// NewFMIndex creates an `FMIndex` with the server's default build params.
func NewFMIndex() *FMIndex {
	return &FMIndex{
		scalarIndex: scalarIndex{
			indexType: FMINDEX,
		},
		buildParams: make(map[string]string),
	}
}

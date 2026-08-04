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

import (
	"strconv"
)

const (
	hnswMKey           = `M`
	hsnwEfConstruction = `efConstruction`
	hnswEfKey          = `ef`
)

var _ Index = hnswIndex{}

type hnswIndex struct {
	baseIndex

	m              int
	efConstruction int // exploratory factor when building index
}

func (idx hnswIndex) Params() map[string]string {
	return map[string]string{
		MetricTypeKey:      string(idx.metricType),
		IndexTypeKey:       string(HNSW),
		hnswMKey:           strconv.Itoa(idx.m),
		hsnwEfConstruction: strconv.Itoa(idx.efConstruction),
	}
}

func NewHNSWIndex(metricType MetricType, m int, efConstruction int) Index {
	return hnswIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  HNSW,
		},
		m:              m,
		efConstruction: efConstruction,
	}
}

type hsnwAnnParam struct {
	baseAnnParam
	ef int
}

func NewHNSWAnnParam(ef int) hsnwAnnParam {
	return hsnwAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		ef: ef,
	}
}

func (ap hsnwAnnParam) Params() map[string]any {
	result := ap.baseAnnParam.params
	result[hnswEfKey] = ap.ef
	return result
}

// WithSeedEf sets the ef used to seed an iterator search (knowhere
// FaissHnswConfig::seed_ef). Server default 40.
func (ap hsnwAnnParam) WithSeedEf(seedEf int) hsnwAnnParam {
	ap.params[hnswSeedEfKey] = seedEf
	return ap
}

// The quantized HNSW variants share M / efConstruction with plain HNSW and add
// their own quantizer params. Note that `M` (graph degree) and `m` (number of
// PQ sub-quantizers) are two different params — the case is significant.
//
// refine is only valid on these three. Plain HNSW maps to knowhere's
// FaissHnswFlatConfig, whose CheckAndAdjust rejects the build outright with
// "refine is not supported for this index" if refine or refine_type is set.
const (
	hnswSQTypeKey  = `sq_type`
	hnswPQMKey     = `m`
	hnswPQNbitsKey = `nbits`
	hnswPRQNrqKey  = `nrq`

	hnswRefineKey     = `refine`
	hnswRefineTypeKey = `refine_type`
	hnswRefineKKey    = `refine_k`
	hnswSeedEfKey     = `seed_ef`
)

// hnswQuantIndex carries what the three quantized HNSW variants have in common:
// the graph params and the optional refine index. Refine keeps full-precision
// vectors alongside the quantized ones and re-ranks with them, trading storage
// for recall.
type hnswQuantIndex struct {
	baseIndex

	m              int
	efConstruction int
	refine         bool
	refineType     string
}

func (idx hnswQuantIndex) commonParams(indexType IndexType) map[string]string {
	result := map[string]string{
		MetricTypeKey:      string(idx.metricType),
		IndexTypeKey:       string(indexType),
		hnswMKey:           strconv.Itoa(idx.m),
		hsnwEfConstruction: strconv.Itoa(idx.efConstruction),
	}
	// Only emitted once the caller opts in, so an index built without refine
	// does not carry an empty refine_type the server would reject.
	if idx.refine {
		result[hnswRefineKey] = strconv.FormatBool(idx.refine)
		result[hnswRefineTypeKey] = idx.refineType
	}
	return result
}

var _ Index = &hnswSQIndex{}

type hnswSQIndex struct {
	hnswQuantIndex

	sqType string
}

func (idx *hnswSQIndex) Params() map[string]string {
	result := idx.commonParams(HNSWSQ)
	result[hnswSQTypeKey] = idx.sqType
	return result
}

// WithRefineType enables the refine index and sets its precision. knowhere
// accepts sq4u / sq6 / sq8 / fp16 / bf16 / fp32 / flat.
func (idx *hnswSQIndex) WithRefineType(refineType string) *hnswSQIndex {
	idx.refine = true
	idx.refineType = refineType
	return idx
}

// NewHNSWSQIndex creates an HNSW index whose vectors are scalar-quantized.
// sqType is the quantizer: knowhere accepts sq4u / sq6 / sq8 / fp16 / bf16,
// and its default is SQ8.
func NewHNSWSQIndex(metricType MetricType, m int, efConstruction int, sqType string) *hnswSQIndex {
	return &hnswSQIndex{
		hnswQuantIndex: hnswQuantIndex{
			baseIndex: baseIndex{
				metricType: metricType,
				indexType:  HNSWSQ,
			},
			m:              m,
			efConstruction: efConstruction,
		},
		sqType: sqType,
	}
}

var _ Index = &hnswPQIndex{}

type hnswPQIndex struct {
	hnswQuantIndex

	pqM   int
	nbits int
}

func (idx *hnswPQIndex) Params() map[string]string {
	result := idx.commonParams(HNSWPQ)
	result[hnswPQMKey] = strconv.Itoa(idx.pqM)
	result[hnswPQNbitsKey] = strconv.Itoa(idx.nbits)
	return result
}

// WithRefineType enables the refine index and sets its precision. knowhere
// accepts sq4u / sq6 / sq8 / fp16 / bf16 / fp32 / flat.
func (idx *hnswPQIndex) WithRefineType(refineType string) *hnswPQIndex {
	idx.refine = true
	idx.refineType = refineType
	return idx
}

// NewHNSWPQIndex creates an HNSW index whose vectors are product-quantized.
// pqM is the number of sub-quantizers (server default 32) and nbits the bits
// per sub-quantizer, in [1, 24] (server default 8).
func NewHNSWPQIndex(metricType MetricType, m int, efConstruction int, pqM int, nbits int) *hnswPQIndex {
	return &hnswPQIndex{
		hnswQuantIndex: hnswQuantIndex{
			baseIndex: baseIndex{
				metricType: metricType,
				indexType:  HNSWPQ,
			},
			m:              m,
			efConstruction: efConstruction,
		},
		pqM:   pqM,
		nbits: nbits,
	}
}

var _ Index = &hnswPRQIndex{}

type hnswPRQIndex struct {
	hnswQuantIndex

	pqM   int
	nrq   int
	nbits int
}

func (idx *hnswPRQIndex) Params() map[string]string {
	result := idx.commonParams(HNSWPRQ)
	result[hnswPQMKey] = strconv.Itoa(idx.pqM)
	result[hnswPRQNrqKey] = strconv.Itoa(idx.nrq)
	result[hnswPQNbitsKey] = strconv.Itoa(idx.nbits)
	return result
}

// WithRefineType enables the refine index and sets its precision. knowhere
// accepts sq4u / sq6 / sq8 / fp16 / bf16 / fp32 / flat.
func (idx *hnswPRQIndex) WithRefineType(refineType string) *hnswPRQIndex {
	idx.refine = true
	idx.refineType = refineType
	return idx
}

// NewHNSWPRQIndex creates an HNSW index whose vectors are quantized with a
// product-residual quantizer. pqM is the number of splits (server default 2),
// nrq the number of residual quantizers, in [1, 16] (server default 2), and
// nbits the bits per sub-quantizer, in [1, 24] (server default 8).
func NewHNSWPRQIndex(metricType MetricType, m int, efConstruction int, pqM int, nrq int, nbits int) *hnswPRQIndex {
	return &hnswPRQIndex{
		hnswQuantIndex: hnswQuantIndex{
			baseIndex: baseIndex{
				metricType: metricType,
				indexType:  HNSWPRQ,
			},
			m:              m,
			efConstruction: efConstruction,
		},
		pqM:   pqM,
		nrq:   nrq,
		nbits: nbits,
	}
}

// hnswQuantAnnParam is the search-time param set the three quantized variants
// share: `ef` as for plain HNSW, plus refine_k (how many candidates the refine
// index re-ranks) and seed_ef (used by the iterator).
type hnswQuantAnnParam struct {
	baseAnnParam
	ef int
}

func (ap *hnswQuantAnnParam) Params() map[string]any {
	result := ap.baseAnnParam.params
	result[hnswEfKey] = ap.ef
	return result
}

// WithRefineK sets how many candidates the refine index re-ranks, as a
// multiple of the requested top-k: knowhere types it CFG_FLOAT and passes it
// straight through as faiss::IndexRefineSearchParameters::k_factor, so
// fractional values such as 1.5 are meaningful. Only useful on an index built
// with WithRefineType.
func (ap *hnswQuantAnnParam) WithRefineK(refineK float64) *hnswQuantAnnParam {
	ap.params[hnswRefineKKey] = refineK
	return ap
}

// WithSeedEf sets the ef used to seed an iterator search.
func (ap *hnswQuantAnnParam) WithSeedEf(seedEf int) *hnswQuantAnnParam {
	ap.params[hnswSeedEfKey] = seedEf
	return ap
}

func newHNSWQuantAnnParam(ef int) *hnswQuantAnnParam {
	return &hnswQuantAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		ef: ef,
	}
}

// NewHNSWSQAnnParam creates the search params for an HNSW_SQ index.
func NewHNSWSQAnnParam(ef int) *hnswQuantAnnParam { return newHNSWQuantAnnParam(ef) }

// NewHNSWPQAnnParam creates the search params for an HNSW_PQ index.
func NewHNSWPQAnnParam(ef int) *hnswQuantAnnParam { return newHNSWQuantAnnParam(ef) }

// NewHNSWPRQAnnParam creates the search params for an HNSW_PRQ index.
func NewHNSWPRQAnnParam(ef int) *hnswQuantAnnParam { return newHNSWQuantAnnParam(ef) }

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

	hnswSQTypeKey     = `sq_type`
	hnswRefineKey     = `refine`
	hnswRefineTypeKey = `refine_type`
	hnswRefineKKey    = `refine_k`
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

var _ Index = (*hnswSQIndex)(nil)

// hnswSQIndex is the HNSW index combined with scalar quantization (HNSW_SQ).
// It compresses the raw float vectors stored in the HNSW graph using a scalar
// quantizer, trading a small amount of recall for a large reduction in memory
// footprint. An optional refinement step re-ranks candidates using a
// higher-precision representation to recover recall.
type hnswSQIndex struct {
	baseIndex

	m              int
	efConstruction int
	sqType         string

	refine     bool
	refineType string
}

func (idx *hnswSQIndex) Params() map[string]string {
	result := map[string]string{
		MetricTypeKey:      string(idx.metricType),
		IndexTypeKey:       string(HNSWSQ),
		hnswMKey:           strconv.Itoa(idx.m),
		hsnwEfConstruction: strconv.Itoa(idx.efConstruction),
		hnswSQTypeKey:      idx.sqType,
	}
	if idx.refine {
		result[hnswRefineKey] = strconv.FormatBool(idx.refine)
		result[hnswRefineTypeKey] = idx.refineType
	}
	return result
}

// WithRefineType enables the refinement step and sets the precision of the data
// used during refinement (for example "SQ6", "SQ8", "BF16", "FP16" or "FP32").
// The refine type must be of higher precision than sqType.
func (idx *hnswSQIndex) WithRefineType(refineType string) *hnswSQIndex {
	idx.refine = true
	idx.refineType = refineType
	return idx
}

// NewHNSWSQIndex creates an HNSW_SQ index. sqType is the scalar quantizer type,
// for example "SQ6", "SQ8", "BF16" or "FP16".
func NewHNSWSQIndex(metricType MetricType, m int, efConstruction int, sqType string) *hnswSQIndex {
	return &hnswSQIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  HNSWSQ,
		},
		m:              m,
		efConstruction: efConstruction,
		sqType:         sqType,
	}
}

type hnswSQAnnParam struct {
	hsnwAnnParam
}

// WithRefineK sets the refinement magnification factor used at search time. When
// the index was built with refinement enabled, the top (refineK * limit)
// candidates are re-ranked using the higher-precision data.
func (ap *hnswSQAnnParam) WithRefineK(refineK float64) *hnswSQAnnParam {
	ap.params[hnswRefineKKey] = refineK
	return ap
}

func NewHNSWSQAnnParam(ef int) *hnswSQAnnParam {
	return &hnswSQAnnParam{
		hsnwAnnParam: NewHNSWAnnParam(ef),
	}
}

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
	hnswSQTypeKey      = `sq_type`
	hnswRefineKey      = `refine`
	hnswRefineTypeKey  = `refine_type`
	hnswRefineKKey     = `refine_k`
	hnswPQMKey         = `m`
	hnswPQNbitsKey     = `nbits`
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

// HNSW_SQ Index
var _ Index = (*hnswSQIndex)(nil)

type hnswSQIndex struct {
	baseIndex

	m              int
	efConstruction int
	sqType         string
	refine         bool
	refineType     string
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

func (idx *hnswSQIndex) WithRefine(refineType string) *hnswSQIndex {
	idx.refine = true
	idx.refineType = refineType
	return idx
}

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

// HNSW_PQ Index
var _ Index = (*hnswPQIndex)(nil)

type hnswPQIndex struct {
	baseIndex

	m              int
	efConstruction int
	pqM            int
	nbits          int
	refine         bool
	refineType     string
}

func (idx *hnswPQIndex) Params() map[string]string {
	result := map[string]string{
		MetricTypeKey:      string(idx.metricType),
		IndexTypeKey:       string(HNSWPQ),
		hnswMKey:           strconv.Itoa(idx.m),
		hsnwEfConstruction: strconv.Itoa(idx.efConstruction),
		hnswPQMKey:         strconv.Itoa(idx.pqM),
		hnswPQNbitsKey:     strconv.Itoa(idx.nbits),
	}
	if idx.refine {
		result[hnswRefineKey] = strconv.FormatBool(idx.refine)
		result[hnswRefineTypeKey] = idx.refineType
	}
	return result
}

func (idx *hnswPQIndex) WithRefine(refineType string) *hnswPQIndex {
	idx.refine = true
	idx.refineType = refineType
	return idx
}

func NewHNSWPQIndex(metricType MetricType, m int, efConstruction int, pqM int, nbits int) *hnswPQIndex {
	return &hnswPQIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  HNSWPQ,
		},
		m:              m,
		efConstruction: efConstruction,
		pqM:            pqM,
		nbits:          nbits,
	}
}

// HNSW_PRQ Index
var _ Index = (*hnswPRQIndex)(nil)

type hnswPRQIndex struct {
	baseIndex

	m              int
	efConstruction int
	pqM            int
	nbits          int
	refine         bool
	refineType     string
}

func (idx *hnswPRQIndex) Params() map[string]string {
	result := map[string]string{
		MetricTypeKey:      string(idx.metricType),
		IndexTypeKey:       string(HNSWPRQ),
		hnswMKey:           strconv.Itoa(idx.m),
		hsnwEfConstruction: strconv.Itoa(idx.efConstruction),
		hnswPQMKey:         strconv.Itoa(idx.pqM),
		hnswPQNbitsKey:     strconv.Itoa(idx.nbits),
	}
	if idx.refine {
		result[hnswRefineKey] = strconv.FormatBool(idx.refine)
		result[hnswRefineTypeKey] = idx.refineType
	}
	return result
}

func (idx *hnswPRQIndex) WithRefine(refineType string) *hnswPRQIndex {
	idx.refine = true
	idx.refineType = refineType
	return idx
}

func NewHNSWPRQIndex(metricType MetricType, m int, efConstruction int, pqM int, nbits int) *hnswPRQIndex {
	return &hnswPRQIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  HNSWPRQ,
		},
		m:              m,
		efConstruction: efConstruction,
		pqM:            pqM,
		nbits:          nbits,
	}
}

// HNSW_SQ/PQ/PRQ Ann Param with refine_k support
type hnswQuantAnnParam struct {
	baseAnnParam
	ef      int
	refineK float64
}

func NewHNSWQuantAnnParam(ef int) *hnswQuantAnnParam {
	return &hnswQuantAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		ef: ef,
	}
}

func (ap *hnswQuantAnnParam) WithRefineK(refineK float64) *hnswQuantAnnParam {
	ap.refineK = refineK
	return ap
}

func (ap *hnswQuantAnnParam) Params() map[string]any {
	result := ap.baseAnnParam.params
	result[hnswEfKey] = ap.ef
	if ap.refineK > 0 {
		result[hnswRefineKKey] = ap.refineK
	}
	return result
}

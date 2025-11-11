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
	hnswEfConstruction = `efConstruction`
	hnswEfKey          = `ef`
	hnswRefineKey      = `refine`
	hnswRefineTypeKey  = `refine_type`
	hnswSQTypeKey      = `sq_type`
	hnswPQMKey         = `m`
	hnswPQNbitsKey     = `nbits`

	hnswRefineKKey = `refine_k`
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
		hnswEfConstruction: strconv.Itoa(idx.efConstruction),
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

var _ Index = &hnswSQIndex{}

type hnswSQIndex struct {
	baseIndex

	m              int
	efConstruction int // exploratory factor when building index
	sqType         string
	refine         bool
	refineType     string
}

func (idx *hnswSQIndex) Params() map[string]string {
	result := map[string]string{
		MetricTypeKey:      string(idx.metricType),
		IndexTypeKey:       string(HNSWSQ),
		hnswMKey:           strconv.Itoa(idx.m),
		hnswEfConstruction: strconv.Itoa(idx.efConstruction),
		hnswSQTypeKey:      idx.sqType,
	}

	if idx.refine {
		result[hnswRefineKey] = strconv.FormatBool(idx.refine)
		result[hnswRefineTypeKey] = idx.refineType
	}
	return result
}

func (idx *hnswSQIndex) WithRefineType(refineType string) *hnswSQIndex {
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

var _ Index = &hnswSQIndex{}

type hnswPQIndex struct {
	baseIndex

	m              int
	efConstruction int // exploratory factor when building index
	pqM            int // number of sub-vectors (used for quantization) to divide each high-dimensional vector into during the quantization process
	nbits          int
	refine         bool
	refineType     string
}

func (idx *hnswPQIndex) Params() map[string]string {
	result := map[string]string{
		MetricTypeKey:      string(idx.metricType),
		IndexTypeKey:       string(HNSWPQ),
		hnswMKey:           strconv.Itoa(idx.m),
		hnswEfConstruction: strconv.Itoa(idx.efConstruction),
		hnswPQMKey:         strconv.Itoa(idx.pqM),
		hnswPQNbitsKey:     strconv.Itoa(idx.nbits),
	}

	if idx.refine {
		result[hnswRefineKey] = strconv.FormatBool(idx.refine)
		result[hnswRefineTypeKey] = idx.refineType
	}
	return result
}

func (idx *hnswPQIndex) WithRefineType(refineType string) *hnswPQIndex {
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

type hnswAnnParam struct {
	baseAnnParam
	ef int
}

func NewHNSWAnnParam(ef int) hnswAnnParam {
	return hnswAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		ef: ef,
	}
}

func (ap hnswAnnParam) Params() map[string]any {
	result := ap.baseAnnParam.params
	result[hnswEfKey] = ap.ef
	return result
}

type hnswSQAnnParam struct {
	hnswAnnParam
}

func NewHNSWSQAnnParam(ef int) *hnswSQAnnParam {
	return &hnswSQAnnParam{
		hnswAnnParam: NewHNSWAnnParam(ef),
	}
}

func (ap *hnswSQAnnParam) WithRefineK(refineK int) *hnswSQAnnParam {
	ap.params[hnswRefineKKey] = refineK
	return ap
}

func (ap *hnswSQAnnParam) Params() map[string]any {
	return ap.hnswAnnParam.Params()
}

type hnswPQAnnParam struct {
	hnswAnnParam
}

func NewHNSWPQAnnParam(ef int) *hnswPQAnnParam {
	return &hnswPQAnnParam{
		hnswAnnParam: NewHNSWAnnParam(ef),
	}
}

func (ap *hnswPQAnnParam) WithRefineK(refineK int) *hnswPQAnnParam {
	ap.params[hnswRefineKKey] = refineK
	return ap
}

func (ap *hnswPQAnnParam) Params() map[string]any {
	return ap.hnswAnnParam.Params()
}

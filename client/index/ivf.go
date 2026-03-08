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

const (
	ivfNlistKey      = `nlist`
	ivfPQMKey        = `m`
	ivfPQNbits       = `nbits`
	ivfNprobeKey     = `nprobe`
	ivfRefineKey     = `refine`
	ivfRefineTypeKey = `refine_type`

	ivfRbqQueryBitsKey = `rbq_query_bits`
	ivfRbqRefineKKey   = `refine_k`
)

var _ Index = ivfFlatIndex{}

type ivfFlatIndex struct {
	baseIndex

	nlist int
}

func (idx ivfFlatIndex) Params() map[string]string {
	return map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(IvfFlat),
		ivfNlistKey:   strconv.Itoa(idx.nlist),
	}
}

func NewIvfFlatIndex(metricType MetricType, nlist int) Index {
	return ivfFlatIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  IvfFlat,
		},

		nlist: nlist,
	}
}

var _ Index = ivfPQIndex{}

type ivfPQIndex struct {
	baseIndex

	nlist int
	m     int
	nbits int
}

func (idx ivfPQIndex) Params() map[string]string {
	return map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(IvfPQ),
		ivfNlistKey:   strconv.Itoa(idx.nlist),
		ivfPQMKey:     strconv.Itoa(idx.m),
		ivfPQNbits:    strconv.Itoa(idx.nbits),
	}
}

func NewIvfPQIndex(metricType MetricType, nlist int, m int, nbits int) Index {
	return ivfPQIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  IvfPQ,
		},

		nlist: nlist,
		m:     m,
		nbits: nbits,
	}
}

var _ Index = ivfSQ8Index{}

type ivfSQ8Index struct {
	baseIndex

	nlist int
}

func (idx ivfSQ8Index) Params() map[string]string {
	return map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(IvfSQ8),
		ivfNlistKey:   strconv.Itoa(idx.nlist),
	}
}

func NewIvfSQ8Index(metricType MetricType, nlist int) Index {
	return ivfSQ8Index{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  IvfSQ8,
		},

		nlist: nlist,
	}
}

type ivfRabitQIndex struct {
	baseIndex

	nlist      int
	refine     bool
	refineType string
}

func (idx *ivfRabitQIndex) Params() map[string]string {
	result := map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(IvfRabitQ),
		ivfNlistKey:   strconv.Itoa(idx.nlist),
	}

	if idx.refine {
		result[ivfRefineKey] = strconv.FormatBool(idx.refine)
		result[ivfRefineTypeKey] = idx.refineType
	}
	return result
}

func (idx *ivfRabitQIndex) WithRefineType(refineType string) *ivfRabitQIndex {
	idx.refine = true
	idx.refineType = refineType
	return idx
}

func NewIvfRabitQIndex(metricType MetricType, nlist int) *ivfRabitQIndex {
	return &ivfRabitQIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  BinIvfFlat,
		},

		nlist: nlist,
	}
}

var _ Index = binIvfFlat{}

type binIvfFlat struct {
	baseIndex

	nlist int
}

func (idx binIvfFlat) Params() map[string]string {
	return map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(BinIvfFlat),
		ivfNlistKey:   strconv.Itoa(idx.nlist),
	}
}

func NewBinIvfFlatIndex(metricType MetricType, nlist int) Index {
	return binIvfFlat{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  BinIvfFlat,
		},

		nlist: nlist,
	}
}

type ivfAnnParam struct {
	baseAnnParam
	nprobe int
}

func (ap ivfAnnParam) Params() map[string]any {
	result := ap.baseAnnParam.Params()
	result[ivfNprobeKey] = ap.nprobe
	return result
}

func NewIvfAnnParam(nprobe int) ivfAnnParam {
	return ivfAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		nprobe: nprobe,
	}
}

type ivfRabitQAnnParam struct {
	ivfAnnParam
}

func (ap *ivfRabitQAnnParam) Params() map[string]any {
	return ap.ivfAnnParam.Params()
}

func (ap *ivfRabitQAnnParam) WithRabitQueryBits(rbqQueryBits int) *ivfRabitQAnnParam {
	ap.params[ivfRbqQueryBitsKey] = rbqQueryBits
	return ap
}

func (ap *ivfRabitQAnnParam) WithRefineK(refineK int) *ivfRabitQAnnParam {
	ap.params[ivfRbqRefineKKey] = refineK
	return ap
}

func NewIvfRabitQAnnParam(nprobe int) *ivfRabitQAnnParam {
	return &ivfRabitQAnnParam{
		ivfAnnParam: NewIvfAnnParam(nprobe),
	}
}

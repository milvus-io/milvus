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

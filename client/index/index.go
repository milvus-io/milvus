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

import "encoding/json"

// Index represent index definition in milvus.
type Index interface {
	Name() string
	IndexType() IndexType
	Params() map[string]string
}

type baseIndex struct {
	name       string
	metricType MetricType
	indexType  IndexType
	params     map[string]string
}

func (idx baseIndex) Name() string {
	return idx.name
}

func (idx baseIndex) IndexType() IndexType {
	return idx.indexType
}

func (idx baseIndex) Params() map[string]string {
	return idx.params
}

func (idx baseIndex) getExtraParams(params map[string]any) string {
	bs, _ := json.Marshal(params)
	return string(bs)
}

var _ Index = GenericIndex{}

type GenericIndex struct {
	baseIndex
	params map[string]string
}

// Params implements Index
func (gi GenericIndex) Params() map[string]string {
	m := make(map[string]string)
	if gi.baseIndex.indexType != "" {
		m[IndexTypeKey] = string(gi.IndexType())
	}
	for k, v := range gi.params {
		m[k] = v
	}
	return m
}

func (gi GenericIndex) WithMetricType(metricType MetricType) {
	gi.baseIndex.metricType = metricType
}

// NewGenericIndex create generic index instance
func NewGenericIndex(name string, params map[string]string) GenericIndex {
	return GenericIndex{
		baseIndex: baseIndex{
			name: name,
		},
		params: params,
	}
}

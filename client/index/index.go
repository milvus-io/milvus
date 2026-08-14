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
			name:      name,
			indexType: IndexType(params[IndexTypeKey]),
		},
		params: params,
	}
}

var _ Index = extraParamIndex{}

// Keys the extra-param wrapper refuses to touch: they define which index is
// being built, and letting them be overridden would put Params() and
// IndexType() into disagreement.
var reservedIndexParamKeys = map[string]struct{}{
	IndexTypeKey:  {},
	MetricTypeKey: {},
}

type extraParamIndex struct {
	Index
	extra map[string]string
}

func (idx extraParamIndex) Params() map[string]string {
	result := idx.Index.Params()
	for k, v := range idx.extra {
		if _, reserved := reservedIndexParamKeys[k]; reserved {
			continue
		}
		result[k] = v
	}
	return result
}

// WithExtraIndexParams merges additional raw build params into whatever `idx`
// produces. index_type and metric_type are reserved and silently ignored:
// overriding them would make Params() disagree with IndexType(), so use the
// constructor for the index you actually want. Any other key overrides.
//
// The typed constructors model the params each index most needs, not every one
// the engine accepts, and they necessarily trail the engine as it gains new
// ones. This is the escape hatch for the rest — the build-side counterpart of
// baseAnnParam.WithExtraParam — so reaching for a param the constructor does
// not expose does not mean giving up the constructor for NewGenericIndex and a
// hand-built map.
//
// Params are forwarded verbatim; the server validates names and ranges.
func WithExtraIndexParams(idx Index, extra map[string]string) Index {
	return extraParamIndex{
		Index: idx,
		extra: extra,
	}
}

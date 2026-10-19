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
	diskANNSearchListKey = `search_list`
)

var _ Index = diskANNIndex{}

type diskANNIndex struct {
	baseIndex
}

func (idx diskANNIndex) Params() map[string]string {
	return map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(DISKANN),
	}
}

func NewDiskANNIndex(metricType MetricType) Index {
	return &diskANNIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  DISKANN,
		},
	}
}

type diskANNParam struct {
	baseAnnParam
	searchList int
}

func NewDiskAnnParam(searchList int) diskANNParam {
	return diskANNParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		searchList: searchList,
	}
}

func (ap diskANNParam) Params() map[string]any {
	result := ap.baseAnnParam.params
	result[diskANNSearchListKey] = ap.searchList
	return result
}

// WithBeamwidth sets the maximum number of IO requests issued per search
// iteration.
func (ap diskANNParam) WithBeamwidth(beamwidth int) diskANNParam {
	ap.params[aisaqBeamwidthKey] = beamwidth
	return ap
}

// AISAQ-specific params (knowhere AisaqConfig). Everything AISAQ inherits from
// DiskANNConfig keeps its server-side default, exactly as NewDiskANNIndex does.
const (
	aisaqInlinePQKey            = `inline_pq`
	aisaqPQCacheSizeKey         = `pq_cache_size`
	aisaqRearrangeKey           = `rearrange`
	aisaqPQReadIOEngineKey      = `pq_read_io_engine`
	aisaqNumEntryPointsKey      = `num_entry_points`
	aisaqPQReadPageCacheSizeKey = `pq_read_page_cache_size`
	aisaqBeamwidthKey           = `beamwidth`
	aisaqVectorsBeamwidthKey    = `vectors_beamwidth`
)

var _ Index = &aisaqIndex{}

// aisaqIndex is a DiskANN variant that keeps PQ codes inline with the graph to
// cut the number of random reads per hop. Every build param has a server-side
// default, so the constructor takes only the metric type and the rest are
// opt-in.
type aisaqIndex struct {
	baseIndex

	params map[string]string
}

func (idx *aisaqIndex) Params() map[string]string {
	result := map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(AISAQ),
	}
	// An unset param must stay absent: every one of these is range-checked
	// server-side, so sending a zero would be rejected rather than treated as
	// "use the default".
	for k, v := range idx.params {
		result[k] = v
	}
	return result
}

// WithInlinePQ sets how many compressed vectors are stored inline in a node.
// Capped by the graph's max degree; range [0, 2048].
func (idx *aisaqIndex) WithInlinePQ(inlinePQ int) *aisaqIndex {
	idx.params[aisaqInlinePQKey] = strconv.Itoa(inlinePQ)
	return idx
}

// WithPQCacheSize sets the compressed-vector cache size in bytes.
func (idx *aisaqIndex) WithPQCacheSize(bytes int) *aisaqIndex {
	idx.params[aisaqPQCacheSizeKey] = strconv.Itoa(bytes)
	return idx
}

// WithRearrange enables the compressed-vector reordering search optimization.
func (idx *aisaqIndex) WithRearrange(rearrange bool) *aisaqIndex {
	idx.params[aisaqRearrangeKey] = strconv.FormatBool(rearrange)
	return idx
}

// WithPQReadIOEngine selects the IO engine used to read PQ vectors, either
// "aio" (server default) or "uring".
func (idx *aisaqIndex) WithPQReadIOEngine(engine string) *aisaqIndex {
	idx.params[aisaqPQReadIOEngineKey] = engine
	return idx
}

// WithNumEntryPoints sets how many entry points are generated and stored when
// the graph is built. This is a build param (knowhere marks it for_train), not
// a per-search knob.
func (idx *aisaqIndex) WithNumEntryPoints(numEntryPoints int) *aisaqIndex {
	idx.params[aisaqNumEntryPointsKey] = strconv.Itoa(numEntryPoints)
	return idx
}

// WithPQReadPageCacheSize sets the per-thread read-page cache size in bytes.
// This one is honored at both build and search time.
func (idx *aisaqIndex) WithPQReadPageCacheSize(bytes int) *aisaqIndex {
	idx.params[aisaqPQReadPageCacheSizeKey] = strconv.Itoa(bytes)
	return idx
}

func NewAISAQIndex(metricType MetricType) *aisaqIndex {
	return &aisaqIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  AISAQ,
		},
		params: make(map[string]string),
	}
}

// aisaqAnnParam carries DiskANN's search_list plus the two beam widths AISAQ
// adds on top of it.
type aisaqAnnParam struct {
	baseAnnParam
	searchList int
}

func (ap *aisaqAnnParam) Params() map[string]any {
	result := ap.baseAnnParam.params
	result[diskANNSearchListKey] = ap.searchList
	return result
}

// WithBeamwidth sets the maximum number of IO requests issued per search
// iteration.
func (ap *aisaqAnnParam) WithBeamwidth(beamwidth int) *aisaqAnnParam {
	ap.params[aisaqBeamwidthKey] = beamwidth
	return ap
}

// WithVectorsBeamwidth sets the beam width used for the compressed vectors.
func (ap *aisaqAnnParam) WithVectorsBeamwidth(beamwidth int) *aisaqAnnParam {
	ap.params[aisaqVectorsBeamwidthKey] = beamwidth
	return ap
}

// WithPQReadPageCacheSize sets the per-thread read-page cache size in bytes for
// this search.
func (ap *aisaqAnnParam) WithPQReadPageCacheSize(bytes int) *aisaqAnnParam {
	ap.params[aisaqPQReadPageCacheSizeKey] = bytes
	return ap
}

func NewAISAQAnnParam(searchList int) *aisaqAnnParam {
	return &aisaqAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		searchList: searchList,
	}
}

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

package taskresource

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Build memory models, one per index family.
//
// knowhere and segcore expose a load-time resource estimate per index type
// (IndexStaticFaced::EstimateLoadResource) but nothing for build time, so the
// models below are derived from each index's layout: the input the build holds
// (the whole field, loaded by segcore), plus the structure it builds beside it.
// Where a layout depends on a build parameter the request carries it, merged
// with the knowhere build defaults of this node's configuration, which is what
// the build itself will run with. Each model states its terms so it can be
// checked against a measured build and tuned in one place.
//
// An index type with no model keeps DataCoord's expansion factor, applied to
// the exact input bytes.

const (
	indexTypeKey = common.IndexTypeKey

	// tantivyMemoryBudget mirrors DEFAULT_OVERALL_MEMORY_BUDGET_IN_BYTES in
	// internal/core/thirdparty/tantivy/tantivy-wrapper.h: the most a tantivy
	// index writer buffers before it flushes a segment.
	tantivyMemoryBudget = 500 << 20

	// defaultHNSWM is knowhere's HnswConfig default for M, used when neither
	// the request nor the node's knowhere configuration sets it.
	defaultHNSWM = 30
	// defaultMaxDegree is the knowhere.DISKANN.build.max_degree default.
	defaultMaxDegree = 56
	// defaultPQCodeBudgetRatio is the knowhere.DISKANN.build.pq_code_budget_gb_ratio default.
	defaultPQCodeBudgetRatio = 0.125
	// vamanaDegreeSlack is the extra room a Vamana build keeps per node while it
	// prunes candidate neighbor lists down to max_degree.
	vamanaDegreeSlack = 1.3
	// idBytes is the per-vector id an IVF inverted list stores.
	idBytes = 8
	// hnswLockBytes is the std::mutex hnswlib keeps per element for concurrent
	// insertion; hnswLabelBytes and hnswPointerBytes are its label and its link
	// list pointer.
	hnswLockBytes    = 40
	hnswLabelBytes   = 8
	hnswPointerBytes = 8
)

type indexInput struct {
	raw      int64 // exact bytes of the indexed field
	rows     int64
	dim      int64
	dataType schemapb.DataType
	params   map[string]string
}

func (in indexInput) bytesPerRow() int64 {
	if in.rows <= 0 {
		return 0
	}
	return in.raw / in.rows
}

type buildModel func(in indexInput) int64

var buildModels = map[string]buildModel{}

func registerBuildModel(model buildModel, indexTypes ...string) {
	for _, indexType := range indexTypes {
		buildModels[indexType] = model
	}
}

func init() {
	registerBuildModel(flatBuild,
		"FLAT", "BIN_FLAT", "GPU_BRUTE_FORCE", "GPU_FAISS_FLAT", "GPU_CUVS_BRUTE_FORCE", "SVS_FLAT")
	registerBuildModel(ivfFlatBuild,
		"IVF_FLAT", "BIN_IVF_FLAT", "IVF_FLAT_CC", "GPU_IVF_FLAT", "GPU_FAISS_IVF_FLAT", "GPU_CUVS_IVF_FLAT",
		"SVS_IVF", "SVS_IVF_LEANVEC")
	registerBuildModel(ivfSQBuild, "IVF_SQ8", "IVF_SQ_CC")
	registerBuildModel(ivfPQBuild, "IVF_PQ", "GPU_IVF_PQ", "GPU_FAISS_IVF_PQ", "GPU_CUVS_IVF_PQ")
	registerBuildModel(ivfRaBitQBuild, "IVF_RABITQ", "IVF_RABITQ_FASTSCAN")
	registerBuildModel(scannBuild, "SCANN", "SCANN_DVR")
	registerBuildModel(hnswBuild, "HNSW", "HNSW_SQ", "HNSW_PQ", "HNSW_PRQ")
	registerBuildModel(vamanaBuild,
		"DISKANN", "AISAQ", "SVS_VAMANA", "SVS_VAMANA_LVQ", "SVS_VAMANA_LEANVEC")
	registerBuildModel(copyBuild,
		"SPARSE_INVERTED_INDEX", "SPARSE_WAND", "SPARSE_INVERTED_INDEX_CC", "SPARSE_WAND_CC", "MINHASH_LSH",
		"Trie", "RTREE")
	registerBuildModel(sortBuild, "STL_SORT")
	registerBuildModel(invertedBuild, "INVERTED", "NGRAM")
	registerBuildModel(bitmapBuild, "BITMAP")
	registerBuildModel(hybridBuild, "HYBRID")
}

// indexBuildMemory is the build memory of indexType over in.
func indexBuildMemory(indexType string, in indexInput) int64 {
	if model, ok := buildModels[indexType]; ok {
		return model(in)
	}
	return scaled(in.raw, paramtable.Get().DataCoordCfg.TaskResourceIndexMemoryFactor.GetAsFloat())
}

// indexBuildParams is the build's parameters as the build will see them: type
// and index params from the request, with this node's knowhere build defaults
// for anything the request leaves out.
func indexBuildParams(req *workerpb.CreateJobRequest) map[string]string {
	params := make(map[string]string, len(req.GetTypeParams())+len(req.GetIndexParams()))
	for _, kv := range req.GetTypeParams() {
		params[kv.GetKey()] = kv.GetValue()
	}
	for _, kv := range req.GetIndexParams() {
		params[kv.GetKey()] = kv.GetValue()
	}
	if paramtable.Get().KnowhereConfig.Enable.GetAsBool() {
		params, _ = paramtable.Get().KnowhereConfig.MergeIndexParams(params[indexTypeKey], paramtable.BuildStage, params)
	}
	return params
}

// flatBuild: the index keeps a full copy of the input.
func flatBuild(in indexInput) int64 {
	return 2 * in.raw
}

// ivfLists is what every IVF variant adds regardless of its code: an id per
// vector in the inverted lists and nlist centroids.
func ivfLists(in indexInput) int64 {
	return in.rows*idBytes + intParam(in.params, "nlist", 0)*in.bytesPerRow()
}

// ivfFlatBuild: inverted lists holding a full copy of every vector.
func ivfFlatBuild(in indexInput) int64 {
	return 2*in.raw + ivfLists(in)
}

// ivfSQBuild: inverted lists of scalar-quantized codes.
func ivfSQBuild(in indexInput) int64 {
	return in.raw + in.rows*sqCodeBytes(in) + ivfLists(in)
}

func sqCodeBytes(in indexInput) int64 {
	switch in.params["sq_type"] {
	case "SQ6":
		return (in.dim*6 + 7) / 8
	case "SQ4", "SQ4U":
		return (in.dim + 1) / 2
	case "FP16", "BF16":
		return in.dim * 2
	default:
		return in.dim // SQ8
	}
}

// ivfPQBuild: inverted lists of m x nbits product-quantization codes, and the
// m codebooks of 2^nbits sub-vector centroids. Without m the code is priced as
// the whole vector.
func ivfPQBuild(in indexInput) int64 {
	nbits := intParam(in.params, "nbits", 8)
	codeBytes := in.bytesPerRow()
	if m := intParam(in.params, "m", 0); m > 0 {
		codeBytes = (m*nbits + 7) / 8
	}
	codebooks := (int64(1) << min(nbits, 24)) * in.bytesPerRow()
	return in.raw + in.rows*codeBytes + ivfLists(in) + codebooks
}

// ivfRaBitQBuild: inverted lists of one bit per dimension plus per-vector
// correction factors, and the raw vectors again when a refine is requested.
func ivfRaBitQBuild(in indexInput) int64 {
	memory := in.raw + in.rows*((in.dim+7)/8+8) + ivfLists(in)
	if boolParam(in.params, "refine", false) {
		memory += in.raw
	}
	return memory
}

// scannBuild: 4-bit codes per dimension, and the raw vectors kept for
// reranking unless with_raw_data is off.
func scannBuild(in indexInput) int64 {
	memory := in.raw + in.rows*((in.dim+1)/2) + ivfLists(in)
	if boolParam(in.params, "with_raw_data", true) {
		memory += in.raw
	}
	return memory
}

// hnswBuild: the graph stores every vector (priced as raw even for the SQ/PQ
// variants, which store fewer bytes, so the model is an upper bound), 2M level-0
// links and M upper-level links per element with their counts, a label, a link
// list pointer and an insertion lock.
func hnswBuild(in indexInput) int64 {
	m := intParam(in.params, "M", defaultHNSWM)
	perElement := (3*m+2)*4 + hnswLabelBytes + hnswPointerBytes + hnswLockBytes
	return 2*in.raw + in.rows*perElement
}

// vamanaBuild: the input, its PQ codes (pq_code_budget_gb_ratio of the raw
// data) and the in-memory graph of max_degree neighbors per node with the
// slack a build keeps while pruning.
func vamanaBuild(in indexInput) int64 {
	degree := intParam(in.params, "max_degree", 0)
	if degree <= 0 {
		degree = intParam(in.params, "svs_graph_max_degree", defaultMaxDegree)
	}
	pqRatio := floatParam(in.params, "pq_code_budget_gb_ratio", defaultPQCodeBudgetRatio)
	graph := in.rows * (int64(float64(degree*4)*vamanaDegreeSlack) + 16)
	return in.raw + scaled(in.raw, pqRatio) + graph
}

// copyBuild: an index whose structure holds about as much as its input
// (posting lists of a sparse vector's (id, value) pairs, a trie of the keys,
// an R-tree of the geometries, MinHash bands).
func copyBuild(in indexInput) int64 {
	return 2 * in.raw
}

// sortBuild: the sorted copy of the values, a row offset per value and the
// validity bitmap.
func sortBuild(in indexInput) int64 {
	return 2*in.raw + in.rows*4 + in.rows/8 + 1
}

// invertedBuild: the input and a tantivy writer, which buffers postings in
// proportion to its input until its memory budget forces a flush.
func invertedBuild(in indexInput) int64 {
	return tantivyBuild(in.raw)
}

func tantivyBuild(raw int64) int64 {
	return raw + min(int64(tantivyMemoryBudget), 2*raw)
}

// bitmapBuild: the input and one row bitmap per distinct value, up to the
// cardinality limit beyond which the build gives up on bitmaps.
func bitmapBuild(in indexInput) int64 {
	limit := intParam(in.params, common.BitmapCardinalityLimitKey,
		paramtable.Get().AutoIndexConfig.BitmapCardinalityLimit.GetAsInt64())
	return in.raw + (in.rows/8+1)*max(limit, 1)
}

// hybridBuild builds a bitmap or an inverted index depending on cardinality;
// it is priced as the larger of the two.
func hybridBuild(in indexInput) int64 {
	return max(bitmapBuild(in), invertedBuild(in))
}

func intParam(params map[string]string, key string, fallback int64) int64 {
	if v, err := strconv.ParseInt(params[key], 10, 64); err == nil && v > 0 {
		return v
	}
	return fallback
}

func floatParam(params map[string]string, key string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(params[key], 64); err == nil && v > 0 {
		return v
	}
	return fallback
}

func boolParam(params map[string]string, key string, fallback bool) bool {
	if v, err := strconv.ParseBool(params[key]); err == nil {
		return v
	}
	return fallback
}

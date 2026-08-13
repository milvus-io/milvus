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
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// indexBuildFactors are build-stage memory multipliers over the raw field size.
//
// They cover the raw vectors the builder must hold plus the structure it
// produces: HNSW additionally holds a graph of M*2*4 bytes per point and the
// build-time candidate sets, while DiskANN builds in batches and spills, so it
// stays below one. These are documented starting points, not measurements;
// every entry is overridable by config, and the estimate_ratio metric is the
// signal for adjusting them.
var indexBuildFactors = map[string]float64{
	"FLAT":                  1.2,
	"IVF_FLAT":              1.2,
	"IVF_SQ8":               1.3,
	"IVF_PQ":                1.3,
	"HNSW":                  2.0,
	"SCANN":                 1.8,
	"DISKANN":               0.5,
	"AISAQ":                 0.5,
	"INVERTED":              0.8,
	"BITMAP":                0.8,
	"STL_SORT":              0.8,
	"TRIE":                  0.8,
	"SPARSE_INVERTED_INDEX": 1.5,
	"SPARSE_WAND":           1.5,
}

// IndexBuildMemoryFactor returns the build-stage multiplier for an index type,
// falling back to the configured default for types not in the table. Returning
// a conservative default rather than zero matters: an unknown type must not be
// estimated as free.
func IndexBuildMemoryFactor(indexType string) float64 {
	if f, ok := indexBuildFactors[strings.ToUpper(indexType)]; ok {
		return f
	}
	return paramtable.Get().DataCoordCfg.ResourceIndexBuildFactorDefault.GetAsFloat()
}

// IndexInput carries everything the index-build estimator needs.
type IndexInput struct {
	IndexType string
	// FieldMemorySize is the uncompressed size of the indexed field.
	FieldMemorySize int64
	StorageVersion  int64
}

// EstimateIndexBuild returns the peak footprint of one index build.
//
// CPU is deliberately independent of data volume. knowhere's build parallelism
// is fixed by its own pool, so scaling CPU with size does not reflect any real
// consumption; the previous scalar scheme did scale it, which is how a 3.33GiB
// field became "384 slots" on a 128-slot node (issue #52180 incident 2).
func EstimateIndexBuild(in IndexInput) Requirement {
	cfg := &paramtable.Get().DataCoordCfg

	mem := int64(float64(in.FieldMemorySize) * IndexBuildMemoryFactor(in.IndexType))

	// Storage v3 reads through an in-flight decode window. For the accumulating
	// caller the window is pure extra peak on top of the retained column.
	if in.StorageVersion >= 3 {
		mem += cfg.ResourceIndexDecodeWindow.GetAsInt64()
	}

	return Requirement{
		CPU:    cfg.ResourceIndexBuildCPU.GetAsFloat(),
		Memory: atLeast(mem, 64*mib),
	}
}

// StatsInput carries everything the stats-sub-job estimator needs.
type StatsInput struct {
	SubJobType indexpb.StatsSubJob
	// FieldMemorySize is the uncompressed size of the fields this sub-job
	// touches, not of the whole segment. Today the coord charges whole-segment
	// size for every sub-job, which over-charges text and json-key builds.
	FieldMemorySize int64
}

// EstimateStats returns the peak footprint of one stats sub-job.
func EstimateStats(in StatsInput) Requirement {
	cfg := &paramtable.Get().DataCoordCfg

	factor := cfg.ResourceTextIndexFactor.GetAsFloat()
	if in.SubJobType == indexpb.StatsSubJob_JsonKeyIndexJob {
		factor = cfg.ResourceJSONKeyIndexFactor.GetAsFloat()
	}

	mem := int64(float64(in.FieldMemorySize) * factor)
	return Requirement{CPU: 1.0, Memory: atLeast(mem, 64*mib)}
}

// EstimateAnalyze sizes the kmeans training set. The task currently derives it
// from the node (GetMemoryCount x MaxTrainSizeRatio); phase 2 makes it read the
// grant returned here instead.
func EstimateAnalyze(totalMemorySize int64) Requirement {
	cfg := &paramtable.Get().DataCoordCfg

	mem := int64(float64(totalMemorySize) * cfg.ResourceAnalyzeFactor.GetAsFloat())
	if maxMem := cfg.ResourceAnalyzeMaxMemory.GetAsInt64(); mem > maxMem {
		mem = maxMem
	}
	return Requirement{CPU: 1.0, Memory: atLeast(mem, 64*mib)}
}

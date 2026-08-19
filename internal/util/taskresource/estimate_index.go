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
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
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
//
// It is valid for the three sub-job types DataCoord actually submits today
// (internal/datacoord/stats_inspector.go): StatsSubJob_TextIndexJob,
// StatsSubJob_BM25Job, and StatsSubJob_JsonKeyIndexJob.
// StatsSubJob_Sort is out of scope by design: sort is no longer run as a
// stats sub-job, it is CompactionType_SortCompaction, estimated by
// EstimateCompaction (estimate_compaction.go) instead.
func EstimateStats(in StatsInput) Requirement {
	cfg := &paramtable.Get().DataCoordCfg

	textFactor := cfg.ResourceTextIndexFactor.GetAsFloat()
	jsonKeyFactor := cfg.ResourceJSONKeyIndexFactor.GetAsFloat()

	var factor float64
	switch in.SubJobType {
	case indexpb.StatsSubJob_TextIndexJob, indexpb.StatsSubJob_BM25Job:
		factor = textFactor
	case indexpb.StatsSubJob_JsonKeyIndexJob:
		factor = jsonKeyFactor
	default:
		// Only Text/BM25/JsonKey are submitted today; Sort is deliberately
		// excluded here because it is estimated as a compaction
		// (CompactionType_SortCompaction) via EstimateCompaction, not as a
		// stats job. An unrecognized or future sub-job (including the Sort
		// and None enum values, which have no submitter on this branch)
		// deliberately errs high rather than silently falling through to
		// whichever factor happened to be declared first: under-provisioning
		// is the direction that causes OOM.
		factor = textFactor
		if jsonKeyFactor > factor {
			factor = jsonKeyFactor
		}
	}

	mem := int64(float64(in.FieldMemorySize) * factor)
	// CPU is a flat charge, not yet read from config: EstimateStats and
	// EstimateAnalyze do not have a dedicated CPU config key in the current
	// config table (see the field/key/default table for this task).
	return Requirement{CPU: 1.0, Memory: atLeast(mem, 64*mib)}
}

// EstimateAnalyze sizes the kmeans training set.
//
// It is deliberately a bound rather than a model: analyze is close to absent
// from production today -- 24h across both large index pools (2026-08-18)
// shows no Analyze tasks at all -- so it does not earn the modelling effort the
// compaction and index estimators get. What it earns is a charge that cannot
// under-count, because when it does run it is the single largest memory
// consumer of any DataNode task.
//
// The bound is simply what the task allocates.
// internal/datanode/index/task_analyze.go sets
// TrainSize = hardware.GetMemoryCount() x trainSizeRatio -- 0.8 of the whole
// node by default, 51GiB on the 64GiB node from issue #52180 -- regardless of
// how much data the task actually has, and the training set cannot exceed the
// data that exists. So the charge is min(dataset, node x ratio), with an
// unknown dataset (0) not bounding it downwards: the buffer is allocated
// either way.
//
// Charging the real allocation rather than a smaller "grant" is what keeps
// phase 0 from being a live regression. Until this branch, analyze was
// serialized by dataCoord.slot.analyzeTaskSlotUsage=65535: the node reported
// zero slots free and ran exactly one at a time. Phase 0 reroutes
// AvailableSlots onto the ledger, so that constant no longer serializes
// anything. Charging 0.8 of the node makes the task oversized against the 0.75
// memoryRatio budget, and the guard answers oversized with exclusive execution
// -- the same one-at-a-time behavior, now expressed through the mechanism that
// replaced the constant. (Clustering's 0.3 does NOT reach that threshold and is
// bounded rather than serialized; see estimateClusteringCompaction.)
//
// An earlier version of this function also multiplied the dataset by a factor
// and capped it, describing a phase-2 grant. Those two knobs could only ever
// change the answer on a node with less than about 5GiB of RAM; on any real
// DataNode the allocation term dominated both. They were dead config, so they
// are gone.
//
// trainSizeRatio is AnalyzeRequest.MaxTrainSizeRatio, the ratio the task will
// actually apply; a non-positive value (a legacy request that never filled it)
// falls back to the config DataCoord fills it from.
func EstimateAnalyze(totalMemorySize int64, trainSizeRatio float64) Requirement {
	if trainSizeRatio <= 0 {
		trainSizeRatio = paramtable.Get().DataCoordCfg.ClusteringCompactionMaxTrainSizeRatio.GetAsFloat()
	}

	mem := int64(float64(hardware.GetMemoryCount()) * trainSizeRatio)
	if totalMemorySize > 0 && totalMemorySize < mem {
		mem = totalMemorySize
	}

	// CPU is a flat charge; see EstimateStats.
	return Requirement{CPU: 1.0, Memory: atLeast(mem, 64*mib)}
}

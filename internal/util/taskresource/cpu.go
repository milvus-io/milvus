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

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// What Requirement.CPU is for
//
// Nothing enforces it. The DataNode's guard never refuses a task for want of
// CPU (Requirement.MemoryFitsIn), and no cgroup quota is derived from it. Two
// things read it:
//
//   - legacyAvailableSlots (internal/datanode/services.go) folds it, together
//     with memory utilization, into the scalar the node reports to DataCoord,
//     taking whichever of the two is worse. That scalar is what DataCoord's
//     placement water-fills over, so the CPU request is how a node says "I am
//     already running as much of this kind of work as I usefully can, put the
//     next one somewhere else".
//   - Operators, through the resource metrics.
//
// So the request's job is to SPREAD, not to admit. That is what makes it safe
// to state honestly -- and honesty here matters, because before this the whole
// DataNode charged a flat 1.0 core for every task whatever it was, which said
// that four concurrent HNSW builds and four concurrent L0 compactions load a
// node identically. They do not, and the consequence was that CPU-bound builds
// piled onto whichever worker happened to have memory free.
//
// The rule below is: a class bounded by a thread pool charges the node's CPU
// capacity divided by that pool's width, so the CPU dimension is exactly full
// when the pool is exactly full. A class with no pool of its own charges a flat
// nominal core.
//
// Note on where this is computed: NodeCapacity reads the local machine, so a
// DataCoord evaluating an estimator sizes against ITS cores, not the worker's.
// That is harmless today because DataCoord consumes only Requirement.Memory
// (it folds it to slots and discards the rest), and the DataNode recomputes
// every requirement locally from the request rather than trusting the number
// the coordinator attached -- see the comment at the top of request.go.

// scalarIndexTypes are the index types an index build of which is NOT routed
// through the DataNode's vector-index build pool.
//
// It mirrors indexparamcheck.IsScalarIndexType, plus RTREE, which that function
// omits but which is not a vector index either. The list is repeated here rather
// than imported because internal/util/indexparamcheck pulls in
// internal/util/vecindexmgr, which is cgo-bound, and this package is compiled
// into DataCoord.
//
// The predicate that actually routes at runtime is
// vecindexmgr.GetVecIndexMgrInstance().IsVecIndex, called from
// indexBuildTask.IsVectorIndex (internal/datanode/index/task_index.go): it
// answers true for any type registered in knowhere's feature map. The two must
// stay in step, and this list is the side that has to be maintained by hand when
// a scalar index type is added.
var scalarIndexTypes = map[string]struct{}{
	"STL_SORT": {},
	"TRIE":     {},
	"BITMAP":   {},
	"HYBRID":   {},
	"INVERTED": {},
	"NGRAM":    {},
	"FMINDEX":  {},
	"RTREE":    {},
}

// IsVectorIndexType reports whether a build of this index type will occupy a
// slot of the DataNode's bounded vector-index build pool.
//
// An unrecognized type is treated as a vector index. That is the conservative
// direction for the thing this decides -- an unknown type charges the larger
// request, so the node reports itself full sooner and the work is spread rather
// than concentrated. An empty type is not: knowhere's own predicate answers
// false for it, so the runtime would not route it to the pool, and claiming a
// pool slot the task will not take would hold capacity back for nothing.
func IsVectorIndexType(indexType string) bool {
	if indexType == "" {
		return false
	}
	_, scalar := scalarIndexTypes[strings.ToUpper(indexType)]
	return !scalar
}

// PoolShareCPU is the CPU request for one task of a class whose concurrency is
// bounded by a pool of poolWidth workers: the share of the node that one of
// those workers represents.
//
// Sizing it this way makes the local cap and the global placement decision
// agree by construction. The node's CPU dimension reaches full at exactly the
// moment the pool does, so it reports itself full to DataCoord at the moment it
// would otherwise start queueing work internally -- and task poolWidth+1 is
// placed on another worker instead of sitting behind a pool it cannot enter.
// Any other number breaks that in one of two ways: too small and the
// coordinator keeps feeding a worker whose pool is already saturated, too large
// and the worker reports itself full while its pool still has slots.
func PoolShareCPU(poolWidth int) float64 {
	if poolWidth <= 0 {
		poolWidth = 1
	}
	share := NodeCapacity().CPU / float64(poolWidth)
	if share <= 0 {
		// A capacity of zero cores (cpuRatio set to 0, or an unreadable core
		// count) would make every class free and the CPU dimension inert. Fall
		// back to the flat nominal charge so the arm keeps some meaning.
		return NominalCPU()
	}
	return share
}

// VectorIndexBuildCPU is the CPU request for one vector index build.
//
// internal/datanode/index/scheduler.go routes every vector build through
// GetVecIndexBuildPool(), whose width is dataNode.index.maxVecIndexBuildConcurrency
// (default 4); scalar builds bypass the pool entirely and run inline on the
// scheduler's goroutine. So vector builds are the one DataNode class with a hard
// concurrency bound of its own, and PoolShareCPU is exactly the charge that
// makes the reported availability agree with it.
//
// Underneath both sits one process-wide knowhere thread pool, sized
// GetCPUNum() x DefaultKnowhereThreadPoolNumRatioInBuild and shared by every
// concurrent build (internal/datanode/index/init_segcore.go). That is why
// concurrency here costs build SPEED rather than cores: builds do not
// oversubscribe the machine, they divide one pool between them. Spreading them
// over workers is the only lever that makes each one faster, and it is the lever
// this charge pulls.
func VectorIndexBuildCPU() float64 {
	return PoolShareCPU(paramtable.Get().DataNodeCfg.MaxVecIndexBuildConcurrency.GetAsInt())
}

// NominalCPU is the flat charge for a class with no concurrency bound of its
// own: scalar index builds, stats sub-jobs, analyze, and the compaction
// families. It is dataCoord.resource.indexBuildCPU, one core by default -- the
// value every task in the DataNode charged before this file existed.
//
// These classes keep a nominal charge on purpose rather than being given a
// derived one. They are governed by MEMORY: sizing them from their pool widths
// too (compaction's pool is 10 wide, so a tenth of the node each) would let the
// CPU arm of the reported utilization bind before the memory arm does, and the
// memory arm is the one with an incident behind it and a derivation behind
// every term. A nominal core is small enough that memory reaches full first in
// every configuration these classes are run in, so the CPU request stays what
// it is meant to be here: a tiebreak, not a limit.
func NominalCPU() float64 {
	return paramtable.Get().DataCoordCfg.ResourceIndexBuildCPU.GetAsFloat()
}

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

package taskcost

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

func NowMs() int64 {
	return time.Now().UnixMilli()
}

func CalcCostTimeMs(startMs, endMs int64) int64 {
	if startMs <= 0 || endMs <= 0 || endMs < startMs {
		return 0
	}
	return endMs - startMs
}

// EstimateIndexBuildCPUNum returns the approximate number of CPU threads an
// index build task consumes during execution.
//
// Vector indexes go through knowhere's build thread pool (sized to NumCPU
// in cluster mode, see internal/datanode/index/init_segcore.go:74), so they
// report hardware.GetCPUNum(). All current scalar indexes build
// single-threaded: the tantivy wrapper hardcodes 1 thread
// (internal/core/thirdparty/tantivy/tantivy-wrapper.h:23 — covers INVERTED /
// NGRAM / TEXT_MATCH / JSON_INVERTED), and the rest (BITMAP / STL_SORT /
// STRING_SORT / MARISA / HYBRID / RTREE) are plain loops with no thread
// pool / OpenMP / std::async. They therefore report 1.
func EstimateIndexBuildCPUNum(isVectorIndex bool) int64 {
	if isVectorIndex {
		return int64(hardware.GetCPUNum())
	}
	return 1
}

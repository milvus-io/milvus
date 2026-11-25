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

package compaction

import (
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// CanMergeSort reports whether this plan is eligible for storage.MergeSort,
// which merges without sorting and so requires every input to already be
// ordered by the plan's merge key. A plan that is not eligible falls back to
// mergeSplit, which does not assume ordering.
func CanMergeSort(plan *datapb.CompactionPlan, params Params) bool {
	if !params.UseMergeSort {
		return false
	}
	// Inputs must match this plan's order, not just have either sorted flag.
	namespaceSorted := common.UsesNamespacePartitionKey(plan.GetSchema())
	for _, segment := range plan.GetSegmentBinlogs() {
		sortedByMergeKey := segment.GetIsSorted()
		if namespaceSorted {
			sortedByMergeKey = segment.GetIsSortedByNamespace()
		}
		if !sortedByMergeKey {
			return false
		}
	}
	// Each reader holds a live record, so memory grows with the reader count. A
	// single segment is allowed: merge sort keeps the output flagged sorted,
	// whereas mergeSplit emits it unsorted and needs a follow-up sort compaction.
	return len(plan.GetSegmentBinlogs()) <= params.MaxSegmentMergeSort
}

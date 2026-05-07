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

package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/plan_c.h"
#include "segcore/search_result_export_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PrepareSearchResultsForExport runs the pre-export reduce phase on all
// per-segment SearchResults: filter invalid rows, optionally apply Global
// Refine (truncate + refine), then fill primary keys. Call this before
// ExportSearchResultAsArrowStream so the exported DataFrames reflect the
// refined scores and truncated candidate count.
//
// Internally wraps the C++ ReduceHelper::PreReduce. When global refine is
// enabled via QueryInfo.search_topk_ratio/refine_topk_ratio and at least one
// segment's index supports refine, this runs truncate + CalcDistByIDs using
// the query vectors in placeholderGroup. Otherwise behaves as filter +
// fill_pk only.
//
// Mutates the passed SearchResults in place and returns the sum of their
// total_data_cnt_ values, matching legacy C++ reduce's all_search_count.
func PrepareSearchResultsForExport(
	ctx context.Context,
	plan *SearchPlan,
	placeholderGroup unsafe.Pointer,
	searchResults []*SearchResult,
	sliceNQs []int64,
	sliceTopKs []int64,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if plan == nil || plan.cSearchPlan == nil {
		return 0, merr.WrapErrParameterInvalidMsg("nil search plan")
	}
	if placeholderGroup == nil {
		return 0, merr.WrapErrParameterInvalidMsg("nil placeholder group")
	}
	if len(searchResults) == 0 {
		return 0, merr.WrapErrParameterInvalidMsg("empty search results")
	}
	if len(sliceNQs) == 0 || len(sliceNQs) != len(sliceTopKs) {
		return 0, merr.WrapErrParameterInvalidMsg("unaligned slice nqs (%d) and topks (%d)",
			len(sliceNQs), len(sliceTopKs))
	}

	cResults := make([]C.CSearchResult, len(searchResults))
	for i, r := range searchResults {
		if r == nil {
			return 0, merr.WrapErrParameterInvalidMsg("nil search result at index %d", i)
		}
		cResults[i] = r.cSearchResult
	}

	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)

	var allSearchCount C.int64_t
	status := C.PrepareSearchResultsForExport(
		traceCtx.ctx,
		plan.cSearchPlan,
		C.CPlaceholderGroup(placeholderGroup),
		&cResults[0],
		C.int64_t(len(searchResults)),
		(*C.int64_t)(unsafe.Pointer(&sliceNQs[0])),
		C.int64_t(len(sliceNQs)),
		(*C.int64_t)(unsafe.Pointer(&sliceTopKs[0])),
		&allSearchCount,
	)
	runtime.KeepAlive(cResults)
	runtime.KeepAlive(searchResults)
	runtime.KeepAlive(plan)
	runtime.KeepAlive(sliceNQs)
	runtime.KeepAlive(sliceTopKs)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return 0, err
	}
	return int64(allSearchCount), nil
}

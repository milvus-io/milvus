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

package delegator

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// shouldUseTwoStageSearch determines if two-stage search should be used for this request
func (sd *shardDelegator) shouldUseTwoStageSearch(req *querypb.SearchRequest, sealedNum int) bool {
	// not enabled, skip
	if !paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.GetAsBool() {
		return false
	}

	// num_of_segments < min_num_segments and topk < min_topk, skip
	if sealedNum < paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.GetAsInt() && req.GetReq().GetTopk() < paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.GetAsInt64() {
		return false
	}

	// only pure ann search with filter is supported
	return req.GetReq().GetSearchType() == internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER
}

// executeFilterStage executes the filter-only stage of two-stage search.
// It sets FilterOnly=true on the request, executes search subtasks, and returns filter statistics.
func (sd *shardDelegator) executeFilterStage(
	ctx context.Context,
	req *querypb.SearchRequest,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) ([]int64, error) {
	req.FilterOnly = true
	// filteronly stage only need to search sealed segments
	filterResults, err := sd.executeSearchSubTasks(ctx, req, sealed, []SegmentEntry{}, sealedRowCount)
	if err != nil {
		log := sd.getLogger(ctx)
		log.Warn("Two-stage search: filter stage failed", zap.Error(err))
		return nil, err
	}

	validCounts := make([]int64, 0, len(sealedRowCount))
	for _, result := range filterResults {
		validCounts = append(validCounts, result.GetFilterValidCounts()...)
	}
	return validCounts, nil
}

// twoStageSearch implements the two-stage search flow:
// Stage 1: Execute filter-only on all segments to get actual filter statistics
// Stage 2: Optimize search params using actual filter stats, execute normal search (filter re-executed)
// Note: Bitsets are NOT cached since filtering is lightweight and can be re-executed in stage 2
func (sd *shardDelegator) twoStageSearch(
	ctx context.Context,
	req *querypb.SearchRequest,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) ([]*internalpb.SearchResults, error) {
	log := sd.getLogger(ctx)

	// ==================== STAGE 1: Filter Only (get statistics) ====================
	validCounts, err := sd.executeFilterStage(ctx, req, sealed, growing, sealedRowCount)
	if err != nil {
		return nil, err
	}

	effectiveSegmentNum := optimizers.CalculateEffectiveSegmentNum(validCounts, req.GetReq().GetTopk())

	log.Debug("Two-stage search: filter stage completed",
		zap.Int("effectiveSegmentNum", effectiveSegmentNum),
	)

	// ==================== Optimize with actual stats ====================
	log.Debug("Optimizing search params with actual stats")
	req.FilterOnly = false
	optimizedReq, err := optimizers.OptimizeSearchParams(ctx, req, sd.queryHook, effectiveSegmentNum, true)
	if err != nil {
		log.Warn("Two-stage search: failed to optimize search params", zap.Error(err))
		return nil, err
	}

	// ==================== STAGE 2: Normal Search with optimized params ====================
	// Filter is re-executed (lightweight) since we could enable expr cache
	log.Debug("Starting vector search stage with optimized params")
	// vector search stage need to search both sealed and growing segments
	results, err := sd.executeSearchSubTasks(ctx, optimizedReq, sealed, growing, sealedRowCount)
	if err != nil {
		log.Warn("Two-stage search: vector search stage failed", zap.Error(err))
		return nil, err
	}

	log.Info("Two-stage search completed", zap.Int("results", len(results)))
	return results, nil
}

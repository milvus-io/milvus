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
	"fmt"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// executeFilterStage executes the filter-only stage of two-stage search.
// It sets FilterOnly=true on the request, executes search subtasks, and returns filter statistics.
func (sd *shardDelegator) executeFilterStage(
	ctx context.Context,
	req *querypb.SearchRequest,
	sealed []SnapshotItem,
	sealedRowCount map[int64]int64,
) ([]int64, error) {
	originalFilterOnly := req.FilterOnly
	originalEnableExprCache := req.EnableExprCache
	req.FilterOnly = true
	req.EnableExprCache = true
	defer func() {
		req.FilterOnly = originalFilterOnly
		req.EnableExprCache = originalEnableExprCache
	}()
	// filteronly stage only need to search sealed segments.
	// filterResults are proto messages whose slices (e.g. FilterValidCounts,
	// SegmentIDs) are owned by the proto and may share backing arrays across
	// merged worker responses. Treat all slices on these results as read-only;
	// copy before any mutation (append/sort/dedup).
	filterResults, err := sd.executeSearchSubTasks(ctx, req, sealed, []SegmentEntry{}, sealedRowCount)
	if err != nil {
		log := sd.getLogger(ctx)
		log.Warn("Two-stage search: filter stage failed", zap.Error(err))
		return nil, err
	}

	// NOTE: validCounts ordering is non-deterministic because filterResults
	// come from concurrent worker goroutines. This is safe for the current
	// consumer (CalculateEffectiveSegmentNum) which only cares about the
	// aggregate distribution, not per-segment identity.
	// TODO: if a future consumer needs per-segment association, pair each
	// count with its SegmentID before returning.
	validCounts := make([]int64, 0, len(sealedRowCount))
	for _, result := range filterResults {
		for _, vc := range result.GetFilterValidCounts() {
			if vc < 0 {
				return nil, fmt.Errorf("filter stage returned negative valid_count %d, segment may not support filter-only search", vc)
			}
			validCounts = append(validCounts, vc)
		}
	}
	return validCounts, nil
}

// twoStageSearch implements the two-stage search flow:
// Stage 1: Execute filter-only on all segments to get actual filter statistics
// Stage 2: Optimize search params using actual filter stats, execute normal search (filter re-executed)
// Note: Filter bitsets are cached via the process-level ExprResCacheManager
// (Stage 1 writes, Stage 2 reads). Cross-query reuse comes for free when the
// same predicate runs on the same sealed segment.
// twoStageSearch returns (results, fallback, error). When fallback is true,
// the caller should continue with the normal single-stage search path;
// results will be nil in that case.
func (sd *shardDelegator) twoStageSearch(
	ctx context.Context,
	req *querypb.SearchRequest,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) ([]*internalpb.SearchResults, bool, error) {
	log := sd.getLogger(ctx)

	// ==================== STAGE 1: Filter Only (get statistics) ====================
	nodeID := paramtable.GetStringNodeID()
	collectionID := fmt.Sprint(sd.collectionID)

	stage1Start := time.Now()
	validCounts, err := sd.executeFilterStage(ctx, req, sealed, sealedRowCount)
	stage1Dur := float64(time.Since(stage1Start).Milliseconds())
	metrics.QueryNodeTwoStageFilterLatency.WithLabelValues(nodeID, collectionID).Observe(stage1Dur)
	if err != nil {
		return nil, false, err
	}

	// In a mixed-version cluster, old workers may not populate
	// FilterValidCounts for some or all segments. When the count is
	// incomplete, the optimizer would work with wrong data, so signal the
	// caller to fall back to normal single-stage search. executeFilterStage
	// restores request flags before returning so fallback sees the original mode.
	expectedSegments := lo.SumBy(sealed, func(item SnapshotItem) int { return len(item.Segments) })
	if len(validCounts) != expectedSegments {
		log.Debug("Two-stage search: FilterValidCounts incomplete, falling back to normal search",
			zap.Int("expected", expectedSegments),
			zap.Int("got", len(validCounts)),
		)
		metrics.QueryNodeTwoStageSearchFallbackCount.WithLabelValues(nodeID, collectionID, "incomplete_filter_counts").Inc()
		return nil, true, nil
	}

	effectiveSegmentNum := optimizers.CalculateEffectiveSegmentNum(sd.queryHook, validCounts, req.GetReq().GetTopk())

	log.Debug("Two-stage search: filter stage completed",
		zap.Int64s("validCounts", validCounts),
		zap.Int("effectiveSegmentNum", effectiveSegmentNum),
	)

	// ==================== Optimize with actual stats ====================
	log.Debug("Optimizing search params with actual stats")
	const isSecondStageSearch = true
	optimizedReq, err := optimizers.OptimizeSearchParams(ctx, req, sd.queryHook, effectiveSegmentNum, isSecondStageSearch, sd.getVectorFieldDim)
	if err != nil {
		log.Warn("Two-stage search: failed to optimize search params", zap.Error(err))
		return nil, false, err
	}

	// ==================== STAGE 2: Normal Search with optimized params ====================
	// Filter bitset is read from ExprResCacheManager (cached in Stage 1), skipping re-execution
	log.Debug("Starting vector search stage with optimized params")
	optimizedReq.EnableExprCache = true
	// vector search stage need to search both sealed and growing segments
	stage2Start := time.Now()
	results, err := sd.executeSearchSubTasks(ctx, optimizedReq, sealed, growing, sealedRowCount)
	stage2Dur := float64(time.Since(stage2Start).Milliseconds())
	metrics.QueryNodeTwoStageSearchLatency.WithLabelValues(nodeID, collectionID).Observe(stage2Dur)
	if err != nil {
		log.Warn("Two-stage search: vector search stage failed", zap.Error(err))
		return nil, false, err
	}

	log.Debug("Two-stage search completed", zap.Int("results", len(results)))
	return results, false, nil
}

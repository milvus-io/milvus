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
	"strconv"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Reasons a sub-request could not join a shared-filter group. Reported on the
// fallback counter so a disappointing group-size histogram can be explained.
//
// Why a sub-request is unshareable -- no predicate, or an iterative filter --
// is known to the proxy, which collapses both into group 0. Recovering the
// distinction here would mean unmarshalling the plan, the very cost the hint
// exists to avoid.
const (
	sharedFilterFallbackUnshareable = "unshareable"
	sharedFilterFallbackNoPeer      = "no_matching_peer"
	sharedFilterFallbackVectorPrune = "vector_prune"
	sharedFilterFallbackNQBudget    = "nq_budget"
)

// sharedFilterKey identifies sub-requests whose filter evaluation can be
// shared. The predicate decides the bitset; IgnoreGrowing decides whether
// growing segments are searched at all, and a group travels as one request
// with one segment list, so it must agree too.
//
// PartitionIDs is deliberately NOT part of the key. It cannot differ once the
// predicates match (outside partition-key mode every sub-request carries the
// same list; inside it, the list is derived from the partition-key predicate),
// and it has no effect on this path anyway: the delegator pins by the
// top-level union and the worker's validate() ignores its partitionIDs
// argument entirely, selecting segments purely from the delegator-computed
// segment IDs.
type sharedFilterKey struct {
	filterSharingGroup int32
	ignoreGrowing      bool
}

// groupSubReqsBySharedFilter partitions sub-request indexes into groups that
// may share one filter evaluation. Order is preserved: groups appear in the
// order of their first member, and members in their original order, so a
// group's branch positions map back to sub-request indexes by position.
//
// A multi-branch group is also one scheduler task, so its summed NQ may not
// exceed the scheduler's existing MaxGroupNQ work-unit budget. A large
// predicate bucket is split into stable, ordered chunks; each chunk evaluates
// the filter once. Individually oversized branches stay singletons. This
// bounds one grouped task's branch fan-out, not its result bytes or GPU kernels.
//
// Every sub-request always lands in exactly one group; a sub-request that
// cannot share ends up alone, which is byte-for-byte today's behavior.
func groupSubReqsBySharedFilter(
	ctx context.Context,
	collectionID int64,
	schema *schemapb.CollectionSchema,
	subReqs []*internalpb.SubSearchRequest,
) [][]int {
	singletons := func(reason string) [][]int {
		groups := make([][]int, len(subReqs))
		for i := range subReqs {
			groups[i] = []int{i}
		}
		if reason != "" && len(subReqs) > 1 {
			observeSharedFilterFallback(collectionID, reason, len(subReqs))
		}
		return groups
	}

	if len(subReqs) < 2 {
		return singletons("")
	}
	if !paramtable.Get().QueryNodeCfg.HybridSearchSharedFilterEnabled.GetAsBool() {
		// Not a fallback worth counting: nothing was attempted.
		return singletons("")
	}
	if hasVectorClusteringKey(schema) {
		return singletons(sharedFilterFallbackVectorPrune)
	}
	maxGroupNQ := paramtable.Get().QueryNodeCfg.MaxGroupNQ.GetAsInt64()

	type bucket struct {
		indexes       []int
		nq            int64
		groupable     bool
		budgetLimited bool
	}
	buckets := make([]*bucket, 0, len(subReqs))
	byKey := make(map[sharedFilterKey]*bucket, len(subReqs))
	unshareable := 0

	for i, subReq := range subReqs {
		key, _, ok := sharedFilterKeyOf(subReq)
		if !ok {
			// Not groupable at all: its own bucket, unreachable by key so
			// nothing joins it. Track the reason here for one batched metric
			// update below, and do not also count it as "no peer".
			unshareable++
			buckets = append(buckets, &bucket{indexes: []int{i}})
			continue
		}

		nq := subReq.GetNq()
		if maxGroupNQ <= 0 || nq <= 0 || nq > maxGroupNQ {
			// A non-positive NQ cannot serve as a grouped-work estimate, so leave
			// it to the ordinary singleton path. An individually oversized branch
			// likewise keeps that path.
			buckets = append(buckets, &bucket{
				indexes:       []int{i},
				groupable:     true,
				budgetLimited: true,
			})
			continue
		}

		if b, exists := byKey[key]; exists {
			// Both operands are positive and at most maxGroupNQ. Subtracting
			// before comparing avoids overflowing an int64 sum near MaxInt64.
			if b.nq > maxGroupNQ-nq {
				b.budgetLimited = true
				b = &bucket{
					indexes:       []int{i},
					nq:            nq,
					groupable:     true,
					budgetLimited: true,
				}
				byKey[key] = b
				buckets = append(buckets, b)
				continue
			}
			b.indexes = append(b.indexes, i)
			b.nq += nq
			continue
		}
		b := &bucket{indexes: []int{i}, nq: nq, groupable: true}
		byKey[key] = b
		buckets = append(buckets, b)
	}

	groups := make([][]int, 0, len(buckets))
	noPeer := 0
	nqBudget := 0
	for _, b := range buckets {
		if b.groupable && len(b.indexes) == 1 {
			if b.budgetLimited {
				nqBudget++
			} else {
				noPeer++
			}
		}
		groups = append(groups, b.indexes)
	}
	observeSharedFilterFallback(collectionID, sharedFilterFallbackUnshareable, unshareable)
	observeSharedFilterFallback(collectionID, sharedFilterFallbackNoPeer, noPeer)
	observeSharedFilterFallback(collectionID, sharedFilterFallbackNQBudget, nqBudget)

	mlog.Debug(ctx, "grouped hybrid sub-requests by shared filter",
		mlog.Int("subRequests", len(subReqs)),
		mlog.Int("groups", len(groups)),
	)
	return groups
}

// sharedFilterKeyOf reads the proxy's grouping hint rather than deriving one.
//
// The proxy parsed each sub-request's (Dsl, expr_template_values) into a plan
// already, so marking identical predicates costs it a string compare. Deriving
// the same answer here would mean unmarshalling a multi-kilobyte plan,
// re-marshaling its predicate and hashing it, once per sub-request and
// repeated on every shard the request touches.
func sharedFilterKeyOf(subReq *internalpb.SubSearchRequest) (sharedFilterKey, string, bool) {
	group := subReq.GetFilterSharingGroup()
	if group == 0 {
		// No predicate, an iterative filter, or a proxy that leaves the hint
		// unset. All three mean the same thing here: nothing to share.
		return sharedFilterKey{}, sharedFilterFallbackUnshareable, false
	}
	return sharedFilterKey{
		filterSharingGroup: group,
		ignoreGrowing:      subReq.GetIgnoreGrowing(),
	}, "", true
}

func observeSharedFilterFallback(collectionID int64, reason string, count int) {
	if reason == "" || count <= 0 {
		return
	}
	metrics.QueryNodeSharedFilterFallbackTotal.WithLabelValues(
		paramtable.GetStringNodeID(),
		strconv.FormatInt(collectionID, 10),
		reason,
	).Add(float64(count))
}

// demuxSharedFilterResults splits each worker's grouped response back into one
// per-branch result list, ready for the ordinary per-branch reduce.
//
// Cost aggregation and the topk-reduce flags are attached to branch 0 only:
// every branch searched the same segments through the same worker call, so
// replicating them across branches would multiply the shard's reported cost.
func demuxSharedFilterResults(
	workerResults []*internalpb.SearchResults,
	branchCount int,
) ([][]*internalpb.SearchResults, error) {
	perBranch := make([][]*internalpb.SearchResults, branchCount)
	for _, workerResult := range workerResults {
		if workerResult == nil {
			continue
		}
		subResults := workerResult.GetSubResults()
		if len(subResults) != branchCount {
			return nil, merr.WrapErrServiceInternalMsg(
				"shared-filter worker returned %d sub-results for %d branches",
				len(subResults), branchCount)
		}
		for _, subResult := range subResults {
			index := int(subResult.GetReqIndex())
			if index < 0 || index >= branchCount {
				return nil, merr.WrapErrServiceInternalMsg(
					"shared-filter sub-result carries out-of-range branch index %d (branches=%d)",
					index, branchCount)
			}
			branchResult := &internalpb.SearchResults{
				MetricType:     subResult.GetMetricType(),
				NumQueries:     subResult.GetNumQueries(),
				TopK:           subResult.GetTopK(),
				SlicedBlob:     subResult.GetSlicedBlob(),
				ResultData:     subResult.GetResultData(),
				SlicedNumCount: subResult.GetSlicedNumCount(),
				SlicedOffset:   subResult.GetSlicedOffset(),
				ChannelsMvcc:   workerResult.GetChannelsMvcc(),
				IsAdvanced:     false,
			}
			if index == 0 {
				branchResult.CostAggregation = workerResult.GetCostAggregation()
				branchResult.IsTopkReduce = workerResult.GetIsTopkReduce()
				branchResult.IsRecallEvaluation = workerResult.GetIsRecallEvaluation()
				branchResult.ScannedRemoteBytes = workerResult.GetScannedRemoteBytes()
				branchResult.ScannedTotalBytes = workerResult.GetScannedTotalBytes()
			} else {
				// Zeroed, but never nil: TotalRelatedDataSize is summed across
				// sub-results, so replicating branch 0's cost would multiply the
				// shard's reported size by the branch count -- while
				// mergeRequestCost dereferences every entry without a nil check
				// (segments/utils.go:158).
				branchResult.CostAggregation = &internalpb.CostAggregation{}
			}
			perBranch[index] = append(perBranch[index], branchResult)
		}
	}
	return perBranch, nil
}

// buildSharedFilterSearchRequest flattens one group into a single worker
// request. The group's first member becomes `req` itself -- byte-for-byte what
// an ungrouped sub-request would have carried -- and the rest ride in
// ExtraFilterSharingReqs. That asymmetry is deliberate: it keeps every
// existing reader of `req` working untouched on the regular path.
func buildSharedFilterSearchRequest(
	req *querypb.SearchRequest,
	subReqs []*internalpb.SubSearchRequest,
	group []int,
	tSafe uint64,
) *querypb.SearchRequest {
	base := req.GetReq()
	head := subReqs[group[0]]

	flattened := &internalpb.SearchRequest{
		Base:                    base.GetBase(),
		ReqID:                   base.GetReqID(),
		DbID:                    base.GetDbID(),
		CollectionID:            base.GetCollectionID(),
		PartitionIDs:            head.GetPartitionIDs(),
		Dsl:                     head.GetDsl(),
		PlaceholderGroup:        head.GetPlaceholderGroup(),
		DslType:                 head.GetDslType(),
		SerializedExprPlan:      head.GetSerializedExprPlan(),
		OutputFieldsId:          base.GetOutputFieldsId(),
		MvccTimestamp:           base.GetMvccTimestamp(),
		GuaranteeTimestamp:      base.GetGuaranteeTimestamp(),
		TimeoutTimestamp:        base.GetTimeoutTimestamp(),
		Nq:                      head.GetNq(),
		Topk:                    head.GetTopk(),
		MetricType:              head.GetMetricType(),
		IgnoreGrowing:           head.GetIgnoreGrowing(),
		Username:                base.GetUsername(),
		IsAdvanced:              false,
		GroupByFieldId:          head.GetGroupByFieldId(),
		GroupSize:               head.GetGroupSize(),
		FieldId:                 head.GetFieldId(),
		GroupByFieldIds:         base.GetGroupByFieldIds(),
		IsTopkReduce:            base.GetIsTopkReduce(),
		IsIterator:              base.GetIsIterator(),
		CollectionTtlTimestamps: base.GetCollectionTtlTimestamps(),
		EntityTtlPhysicalTime:   base.GetEntityTtlPhysicalTime(),
		AnalyzerName:            head.GetAnalyzerName(),
		PkFilter:                common.PkFilterNoPkFilter, // hybrid search sub-requests rarely have PK predicates, skip unmarshal
		SearchType:              head.GetSearchType(),
	}
	if flattened.GetMvccTimestamp() == 0 {
		flattened.MvccTimestamp = tSafe
	}

	searchReq := &querypb.SearchRequest{
		Req:             flattened,
		DmlChannels:     req.GetDmlChannels(),
		TotalChannelNum: req.GetTotalChannelNum(),
	}
	for _, subReqIdx := range group[1:] {
		searchReq.ExtraFilterSharingReqs = append(searchReq.ExtraFilterSharingReqs,
			shallowCopySubSearchRequest(subReqs[subReqIdx]))
	}
	return searchReq
}

// shallowCopySubSearchRequest returns a new message with the same field
// values, sharing the byte slices.
//
// A copy is needed because sd.search rewrites a branch's placeholder group
// and plan (BM25 IDF, AutoIndex params) by assigning new values to those
// fields; sharing the caller's SubSearchRequest pointers would leak those
// rewrites back into the top-level request and into any later ungrouped
// retry. A shallow copy is enough because the rewrites replace the slices
// rather than mutating their contents, and the placeholder group -- the query
// vectors, usually most of the request -- would otherwise be duplicated for
// every branch by proto.Clone.
func shallowCopySubSearchRequest(sub *internalpb.SubSearchRequest) *internalpb.SubSearchRequest {
	return &internalpb.SubSearchRequest{
		Dsl:                sub.GetDsl(),
		PlaceholderGroup:   sub.GetPlaceholderGroup(),
		DslType:            sub.GetDslType(),
		SerializedExprPlan: sub.GetSerializedExprPlan(),
		Nq:                 sub.GetNq(),
		PartitionIDs:       sub.GetPartitionIDs(),
		Topk:               sub.GetTopk(),
		Offset:             sub.GetOffset(),
		MetricType:         sub.GetMetricType(),
		GroupByFieldId:     sub.GetGroupByFieldId(),
		GroupSize:          sub.GetGroupSize(),
		FieldId:            sub.GetFieldId(),
		IgnoreGrowing:      sub.GetIgnoreGrowing(),
		AnalyzerName:       sub.GetAnalyzerName(),
		SearchType:         sub.GetSearchType(),
		FilterSharingGroup: sub.GetFilterSharingGroup(),
	}
}

// errSharedFilterUngroupable signals that a group cannot be answered as a
// group after all, and its branches must be retried one at a time. Three things
// raise it:
//
//   - a branch has to be skipped entirely (a BM25 field with no data yet).
//     One worker response carries one status for all branches, so "this one
//     branch returns nothing" has nowhere to go.
//   - a worker failed and the partial-result evaluator absorbed the failure.
//     One worker response also carries one result set for all branches, so
//     every branch would inherit the gap, where ungrouped only the branch
//     that hit the failing worker did.
//   - refreshable config selected a vector clustering key after the group was
//     formed. Vector pruning reads branch 0's query vector, so every branch
//     must be retried alone before pruning can proceed.
//
// The retry costs a second round of worker calls, but it only runs on the
// failure path: a skipped branch, a worker failure while partial results are
// enabled, or a clustering-key config change. Avoiding it would mean a
// per-branch status on the wire.
var errSharedFilterUngroupable = errors.New("shared-filter group must be executed ungrouped")

// prepareSharedFilterBranchFunctions runs the managed-function preparation
// (BM25 IDF, MinHash) for every branch of a request.
//
// prepareSearchFunction only looks at req.Req -- branch 0. The extra branches
// therefore use scratch requests and copy their rewritten plan and placeholder
// back after preparation. Without this, an extra BM25 branch keeps the raw
// VARCHAR placeholder the client sent and segcore rejects it outright:
//
//	check_data_type(...) => vector type must be the same,
//	field sparse - type VECTOR_SPARSE_U32_F32, search ph type VARCHAR
//
// The caller handles a true skip result: "this one branch returns nothing"
// cannot be expressed inside a group, so grouped requests are retried as
// singletons. For an ungrouped request this preserves the ordinary empty result.
//
// Grouped branches, including branch 0, run concurrently under one bounded
// errgroup. A BM25 branch unmarshals its placeholder group, runs the analyzer,
// builds the IDF vector and marshals the result; serializing branch 0 ahead of
// the extras would add a full head-of-line preparation step. Each branch reads
// its own request and writes only its own result slot. prepareSearchFunction ->
// RunWithRunner -> buildBM25IDF is already entered concurrently by ungrouped
// sub-requests, so the grouped path preserves that concurrency envelope.
func (sd *shardDelegator) prepareSharedFilterBranchFunctions(ctx context.Context, req *querypb.SearchRequest) (float64, bool, error) {
	base := req.GetReq()
	extras := req.GetExtraFilterSharingReqs()
	if len(extras) == 0 {
		return sd.prepareSearchFunction(ctx, base)
	}

	collectionID := base.GetCollectionID()
	branchCount := len(extras) + 1
	skips := make([]bool, branchCount)
	var avgdl float64
	branchGroup, branchCtx := errgroup.WithContext(ctx)
	branchGroup.SetLimit(min(branchCount, hardware.GetCPUNum()))
	branchGroup.Go(func() error {
		var err error
		avgdl, skips[0], err = sd.prepareSearchFunction(branchCtx, base)
		return err
	})
	for i := range extras {
		i, sub := i, extras[i]
		branchGroup.Go(func() error {
			branchReq := &internalpb.SearchRequest{
				CollectionID:       collectionID,
				PartitionIDs:       sub.GetPartitionIDs(),
				SerializedExprPlan: sub.GetSerializedExprPlan(),
				PlaceholderGroup:   sub.GetPlaceholderGroup(),
				Nq:                 sub.GetNq(),
				Topk:               sub.GetTopk(),
				MetricType:         sub.GetMetricType(),
				FieldId:            sub.GetFieldId(),
				AnalyzerName:       sub.GetAnalyzerName(),
			}
			_, skipSearch, err := sd.prepareSearchFunction(branchCtx, branchReq)
			if err != nil {
				return err
			}
			if skipSearch {
				skips[i+1] = true
				return nil
			}
			// buildBM25IDF / SetBM25Params rewrote these in place.
			extras[i].PlaceholderGroup = branchReq.GetPlaceholderGroup()
			extras[i].SerializedExprPlan = branchReq.GetSerializedExprPlan()
			return nil
		})
	}
	if err := branchGroup.Wait(); err != nil {
		return 0, false, err
	}
	for _, skip := range skips {
		if skip {
			return avgdl, true, nil
		}
	}
	return avgdl, false, nil
}

// optimizeSearchParams runs the AutoIndex query hook over every branch of a
// request: `req` itself (branch 0) and, for a shared-filter group, each entry
// of ExtraFilterSharingReqs. For an ungrouped request this is exactly
// optimizers.OptimizeSearchParams.
//
// OptimizeSearchParams only ever looks at req.Req. Without the extra pass the
// other branches would reach segcore with untuned search params (`ef` and
// friends straight from the user request) and nothing would report an error --
// recall and latency would just quietly differ between branch 0 and the rest.
//
// Two things about the hook's contract shape this function:
//
//   - IsTopkReduce and IsRecallEvaluation are input *and* output on the same
//     fields. As input they say what the request permits; the hook overwrites
//     them with what it actually did. Every branch must be asked with the
//     request's input, so the input is captured before branch 0 runs, and the
//     outputs are OR-ed back once at the end -- the same way
//     ReduceAdvancedSearchResults combines those flags across sub-results.
//   - The effective segment count depends on topk, which is per branch.
//
// branch0Stage2 is whether branch 0 takes stage-2 semantics (WithFilterKey
// false, topk-reduce suppressed); isSecondStageSearch gates the same decision
// for the extras, each of which must also qualify on its own -- see
// branchQualifiesForTwoStage.
// indexTypeFunc resolves the loaded index type for a branch's vector field;
// Knowhere search defaults are index-specific, so branch 0 and every extra
// must resolve it from their own field ID.
//
// Grouped branches, including branch 0, run concurrently under one bounded
// errgroup, for the same reason prepareSharedFilterBranchFunctions does: each
// one unmarshals a plan, calls the hook and marshals the plan again. Extras get
// scratch requests and write only their own indexes; shared envelope fields
// and input flags are snapshotted before branch 0 mutates req.Req. queryHook.Run
// is already called concurrently by the ungrouped path, where each sub-request
// ran this from its own sd.search future.
func (sd *shardDelegator) optimizeSearchParams(
	ctx context.Context,
	req *querypb.SearchRequest,
	queryHook optimizers.QueryHook,
	rowCounts []int64,
	effectiveSegmentNum int,
	branch0Stage2 bool,
	isSecondStageSearch bool,
	dimFunc func(fieldID int64) int64,
	indexTypeFunc func(fieldID int64) string,
) (*querypb.SearchRequest, error) {
	extras := req.GetExtraFilterSharingReqs()
	if len(extras) == 0 {
		indexType := indexTypeFunc(req.GetReq().GetFieldId())
		return optimizers.OptimizeSearchParams(
			ctx, req, queryHook, effectiveSegmentNum, branch0Stage2, dimFunc, indexType)
	}

	inputTopkReduce := req.GetReq().GetIsTopkReduce()
	inputRecallEvaluation := req.GetReq().GetIsRecallEvaluation()
	collectionID := req.GetReq().GetCollectionID()
	branch0FieldID := req.GetReq().GetFieldId()
	totalChannelNum := req.GetTotalChannelNum()

	branchCount := len(extras) + 1
	branchTopkReduce := make([]bool, branchCount)
	branchRecallEvaluation := make([]bool, branchCount)
	var optimized *querypb.SearchRequest
	branchGroup, branchCtx := errgroup.WithContext(ctx)
	branchGroup.SetLimit(min(branchCount, hardware.GetCPUNum()))
	branchGroup.Go(func() error {
		branch0IndexType := indexTypeFunc(branch0FieldID)
		branchOptimized, err := optimizers.OptimizeSearchParams(
			branchCtx, req, queryHook, effectiveSegmentNum, branch0Stage2, dimFunc, branch0IndexType)
		if err != nil {
			return err
		}
		optimized = branchOptimized
		branchTopkReduce[0] = branchOptimized.GetReq().GetIsTopkReduce()
		branchRecallEvaluation[0] = branchOptimized.GetReq().GetIsRecallEvaluation()
		return nil
	})
	for i := range extras {
		i, sub := i, extras[i]
		branchGroup.Go(func() error {
			branchReq := &querypb.SearchRequest{
				Req: &internalpb.SearchRequest{
					CollectionID:       collectionID,
					SerializedExprPlan: sub.GetSerializedExprPlan(),
					Nq:                 sub.GetNq(),
					Topk:               sub.GetTopk(),
					MetricType:         sub.GetMetricType(),
					SearchType:         sub.GetSearchType(),
					IsTopkReduce:       inputTopkReduce,
					IsRecallEvaluation: inputRecallEvaluation,
				},
				TotalChannelNum: totalChannelNum,
			}
			// Stage 2 tells the query hook the filter's selectivity is
			// already known (WithFilterKey=false) and suppresses
			// topk-reduce. That is only true of a branch two-stage search
			// was built for; a co-grouped branch that would not have
			// qualified on its own -- a BM25 branch next to a dense one --
			// must keep its ordinary parameters even though it shares the
			// group's stage-1 bitset.
			branchStage2 := isSecondStageSearch &&
				branchQualifiesForTwoStage(sub.GetTopk(), sub.GetSearchType())
			branchSegmentNum := optimizers.CalculateEffectiveSegmentNum(queryHook, rowCounts, sub.GetTopk())
			branchIndexType := indexTypeFunc(sub.GetFieldId())
			branchOptimized, err := optimizers.OptimizeSearchParams(
				branchCtx, branchReq, queryHook, branchSegmentNum, branchStage2, dimFunc, branchIndexType)
			if err != nil {
				return err
			}
			extras[i].SerializedExprPlan = branchOptimized.GetReq().GetSerializedExprPlan()
			branchTopkReduce[i+1] = branchOptimized.GetReq().GetIsTopkReduce()
			branchRecallEvaluation[i+1] = branchOptimized.GetReq().GetIsRecallEvaluation()
			return nil
		})
	}
	if err := branchGroup.Wait(); err != nil {
		return nil, err
	}

	topkReduce := false
	recallEvaluation := false
	for i := range branchTopkReduce {
		topkReduce = topkReduce || branchTopkReduce[i]
		recallEvaluation = recallEvaluation || branchRecallEvaluation[i]
	}
	optimized.Req.IsTopkReduce = topkReduce
	optimized.Req.IsRecallEvaluation = recallEvaluation
	return optimized, nil
}

// branchQualifiesForTwoStage is the branch-intrinsic half of
// optimizers.ShouldUseTwoStageSearch. The global switch and the segment count
// are properties of the whole group and were already settled when stage 1
// ran; topk and search type are each branch's own.
func branchQualifiesForTwoStage(topk int64, searchType internalpb.SearchType) bool {
	return topk >= paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.GetAsInt64() &&
		searchType == internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER
}

// hasVectorClusteringKey reports whether this collection's clustering key is a
// vector field, in which case sub-requests are not grouped at all.
//
// PruneSegments on a vector clustering key reads the query vector out of the
// request's placeholder group and keeps only the segments whose centroids are
// near it. That is inherently per branch: a group would either prune every
// branch by branch 0's vector -- wrong rows for the others -- or skip pruning
// and search every segment, trading the pruning ratio for the saved filter.
// Neither is an improvement on running the branches separately, each pruned
// by its own vector, so such collections keep today's per-sub-request path.
// Scalar-key pruning reads the predicate, which a group shares by
// construction, and is unaffected.
//
// EnableSegmentPrune is deliberately NOT consulted: it is refreshable and
// sd.search re-reads it per group, long after this decision was made. Reading
// it here would let "pruning is off, so grouping is free" be decided, the
// branches be grouped, and the flag be turned on before execution reaches
// PruneSegments -- at which point every sibling is pruned by branch 0's
// vector and silently loses its own nearest segments.
func hasVectorClusteringKey(schema *schemapb.CollectionSchema) bool {
	key := clustering.GetClusteringKeyField(schema)
	return key != nil && typeutil.IsVectorType(key.GetDataType())
}

// shouldUseTwoStageSearchForGroup extends ShouldUseTwoStageSearch to a
// shared-filter group: it qualifies if any branch does.
//
// The gate reads topk and search type, both of which are per-branch, so branch
// 0 alone is not a defensible answer for the group. Taking the union is the
// right call rather than a lenient one: stage 1 is a single filter-only pass
// shared by every branch, so adding a branch to a group that already qualifies
// costs nothing extra in stage 1.
func shouldUseTwoStageSearchForGroup(req *querypb.SearchRequest, effectiveSegmentNum int) bool {
	if optimizers.ShouldUseTwoStageSearch(req, effectiveSegmentNum) {
		return true
	}
	for _, sub := range req.GetExtraFilterSharingReqs() {
		branchReq := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				CollectionID: req.GetReq().GetCollectionID(),
				Topk:         sub.GetTopk(),
				SearchType:   sub.GetSearchType(),
			},
			TotalChannelNum: req.GetTotalChannelNum(),
		}
		if optimizers.ShouldUseTwoStageSearch(branchReq, effectiveSegmentNum) {
			return true
		}
	}
	return false
}

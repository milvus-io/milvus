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
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const iterativeFilterHint = "iterative_filter"

// Reasons a sub-request could not join a shared-filter group. Reported on the
// fallback counter so a disappointing group-size histogram can be explained.
const (
	sharedFilterFallbackNoPlan      = "plan_unmarshal_failed"
	sharedFilterFallbackNoPredicate = "no_predicate"
	sharedFilterFallbackIterative   = "iterative_filter"
	sharedFilterFallbackNoPeer      = "no_matching_peer"
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
	predicateDigest string
	ignoreGrowing   bool
}

func newSharedFilterKey(predicate []byte, ignoreGrowing bool) sharedFilterKey {
	digest := sha256.Sum256(predicate)
	return sharedFilterKey{
		predicateDigest: hex.EncodeToString(digest[:]),
		ignoreGrowing:   ignoreGrowing,
	}
}

// groupSubReqsBySharedFilter partitions sub-request indexes into groups that
// may share one filter evaluation. Order is preserved: groups appear in the
// order of their first member, and members in their original order, so a
// group's branch positions map back to sub-request indexes by position.
//
// Every sub-request always lands in exactly one group; a sub-request that
// cannot share ends up alone, which is byte-for-byte today's behavior.
func groupSubReqsBySharedFilter(
	ctx context.Context,
	collectionID int64,
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

	type bucket struct {
		indexes   []int
		groupable bool
	}
	buckets := make([]*bucket, 0, len(subReqs))
	byKey := make(map[sharedFilterKey]*bucket, len(subReqs))

	for i, subReq := range subReqs {
		key, reason, ok := sharedFilterKeyOf(subReq)
		if !ok {
			// Not groupable at all: its own bucket, unreachable by key so
			// nothing joins it. The specific reason is already counted here,
			// so it must not also count as "no peer" below.
			observeSharedFilterFallback(collectionID, reason, 1)
			buckets = append(buckets, &bucket{indexes: []int{i}})
			continue
		}
		if b, exists := byKey[key]; exists {
			b.indexes = append(b.indexes, i)
			continue
		}
		b := &bucket{indexes: []int{i}, groupable: true}
		byKey[key] = b
		buckets = append(buckets, b)
	}

	groups := make([][]int, 0, len(buckets))
	noPeer := 0
	for _, b := range buckets {
		if b.groupable && len(b.indexes) == 1 {
			noPeer++
		}
		groups = append(groups, b.indexes)
	}
	observeSharedFilterFallback(collectionID, sharedFilterFallbackNoPeer, noPeer)

	mlog.Debug(ctx, "grouped hybrid sub-requests by shared filter",
		mlog.Int("subRequests", len(subReqs)),
		mlog.Int("groups", len(groups)),
	)
	return groups
}

// sharedFilterKeyOf derives a sub-request's grouping key, or reports why it
// cannot be grouped at all.
func sharedFilterKeyOf(subReq *internalpb.SubSearchRequest) (sharedFilterKey, string, bool) {
	plan := &planpb.PlanNode{}
	if err := proto.Unmarshal(subReq.GetSerializedExprPlan(), plan); err != nil {
		return sharedFilterKey{}, sharedFilterFallbackNoPlan, false
	}
	anns := plan.GetVectorAnns()
	if anns == nil || anns.GetPredicates() == nil {
		// No filter to share. A search without a predicate has no
		// FilterBitsNode, so there is nothing to save.
		return sharedFilterKey{}, sharedFilterFallbackNoPredicate, false
	}
	if usesIterativeFilter(anns.GetQueryInfo()) {
		// The iterative path applies the filter row-by-row after the vector
		// search; there is no prefix subtree to share.
		return sharedFilterKey{}, sharedFilterFallbackIterative, false
	}
	predicate, err := proto.Marshal(anns.GetPredicates())
	if err != nil {
		return sharedFilterKey{}, sharedFilterFallbackNoPlan, false
	}
	return newSharedFilterKey(predicate, subReq.GetIgnoreGrowing()), "", true
}

func usesIterativeFilter(queryInfo *planpb.QueryInfo) bool {
	if queryInfo == nil {
		return false
	}
	if queryInfo.GetHints() == iterativeFilterHint {
		return true
	}
	params := queryInfo.GetSearchParams()
	if params == "" {
		return false
	}
	return gjson.Get(params, common.HintsKey).String() == iterativeFilterHint
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
		// Clone: sd.search rewrites a branch's placeholder group and plan in
		// place (BM25 IDF, AutoIndex params). Sharing the caller's
		// SubSearchRequest pointers would leak those rewrites back into the
		// top-level request and into any later ungrouped retry.
		searchReq.ExtraFilterSharingReqs = append(searchReq.ExtraFilterSharingReqs,
			proto.Clone(subReqs[subReqIdx]).(*internalpb.SubSearchRequest))
	}
	return searchReq
}

// errSharedFilterUngroupable signals that a group cannot be executed as a
// group after all, and its branches must be retried one at a time.
var errSharedFilterUngroupable = errors.New("shared-filter group must be executed ungrouped")

// prepareSharedFilterBranchFunctions runs the managed-function preparation
// (BM25 IDF, MinHash) for the extra branches of a grouped request.
//
// prepareSearchFunction only looks at req.Req -- branch 0. Without this the
// extra branches keep the raw VARCHAR placeholder the client sent for a BM25
// field and segcore rejects them outright:
//
//	check_data_type(...) => vector type must be the same,
//	field sparse - type VECTOR_SPARSE_U32_F32, search ph type VARCHAR
//
// Returns errSharedFilterUngroupable when a branch would have to be skipped
// entirely (BM25 field with no data): "this one branch returns nothing" cannot
// be expressed inside a group, so the caller retries the branches separately.
func (sd *shardDelegator) prepareSharedFilterBranchFunctions(ctx context.Context, req *querypb.SearchRequest) error {
	base := req.GetReq()
	for i, sub := range req.GetExtraFilterSharingReqs() {
		branchReq := &internalpb.SearchRequest{
			CollectionID:       base.GetCollectionID(),
			PartitionIDs:       sub.GetPartitionIDs(),
			SerializedExprPlan: sub.GetSerializedExprPlan(),
			PlaceholderGroup:   sub.GetPlaceholderGroup(),
			Nq:                 sub.GetNq(),
			Topk:               sub.GetTopk(),
			MetricType:         sub.GetMetricType(),
			FieldId:            sub.GetFieldId(),
			AnalyzerName:       sub.GetAnalyzerName(),
		}
		_, skipSearch, err := sd.prepareSearchFunction(ctx, branchReq)
		if err != nil {
			return err
		}
		if skipSearch {
			return errSharedFilterUngroupable
		}
		// buildBM25IDF / SetBM25Params rewrote these in place.
		req.ExtraFilterSharingReqs[i].PlaceholderGroup = branchReq.GetPlaceholderGroup()
		req.ExtraFilterSharingReqs[i].SerializedExprPlan = branchReq.GetSerializedExprPlan()
	}
	return nil
}

// optimizeSharedFilterBranches runs the AutoIndex query hook over the extra
// branches of a grouped request.
//
// OptimizeSearchParams only ever looks at req.Req, i.e. branch 0. Without this
// the extra branches would reach segcore with untuned search params (`ef` and
// friends straight from the user request) and nothing would report an error --
// recall and latency would just quietly differ between branch 0 and the rest.
func (sd *shardDelegator) optimizeSharedFilterBranches(
	ctx context.Context,
	req *querypb.SearchRequest,
	queryHook optimizers.QueryHook,
	numSegments int,
	isSecondStageSearch bool,
	dimFunc func(fieldID int64) int64,
) error {
	for i, sub := range req.GetExtraFilterSharingReqs() {
		branchReq := &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				CollectionID:       req.GetReq().GetCollectionID(),
				SerializedExprPlan: sub.GetSerializedExprPlan(),
				Nq:                 sub.GetNq(),
				Topk:               sub.GetTopk(),
				MetricType:         sub.GetMetricType(),
				SearchType:         sub.GetSearchType(),
				IsTopkReduce:       req.GetReq().GetIsTopkReduce(),
				IsRecallEvaluation: req.GetReq().GetIsRecallEvaluation(),
			},
			TotalChannelNum: req.GetTotalChannelNum(),
		}
		optimized, err := optimizers.OptimizeSearchParams(ctx, branchReq, queryHook, numSegments, isSecondStageSearch, dimFunc)
		if err != nil {
			return err
		}
		req.ExtraFilterSharingReqs[i].SerializedExprPlan = optimized.GetReq().GetSerializedExprPlan()
		// These are request-level fields written from a per-branch decision.
		// OR them in, matching how ReduceAdvancedSearchResults combines the
		// same flags across sub-results.
		if optimized.GetReq().GetIsTopkReduce() {
			req.Req.IsTopkReduce = true
		}
		if optimized.GetReq().GetIsRecallEvaluation() {
			req.Req.IsRecallEvaluation = true
		}
	}
	return nil
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

package qviews

import (
	"bytes"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// OmitUnchangedQueryPayloads consumes the planner-owned request after validation.
// Keep the small optimized parameters explicit, but do not echo unchanged
// vectors or expression plans through every shard's Phase 1 response.
func OmitUnchangedQueryPayloads(plan *viewpb.QueryPlan, original *viewpb.GetQueryPlanRequest) {
	reuse := &viewpb.QueryPayloadReuse{}
	if search := plan.GetLegacySearchRequest(); search != nil {
		input := original.GetLegacySearchRequest()
		reuse.PlaceholderGroup = omitPayload(input.GetPlaceholderGroup(), &search.PlaceholderGroup)
		reuse.SerializedExprPlan = omitPayload(input.GetSerializedExprPlan(), &search.SerializedExprPlan)
		for i, sub := range search.GetSubReqs() {
			before := input.GetSubReqs()[i]
			reuse.SubRequests = append(reuse.SubRequests, &viewpb.QueryPayloadReuse{
				PlaceholderGroup:   omitPayload(before.GetPlaceholderGroup(), &sub.PlaceholderGroup),
				SerializedExprPlan: omitPayload(before.GetSerializedExprPlan(), &sub.SerializedExprPlan),
			})
		}
	} else if query := plan.GetLegacyRetrieveRequest(); query != nil {
		reuse.SerializedExprPlan = omitPayload(original.GetLegacyRetrieveRequest().GetSerializedExprPlan(), &query.SerializedExprPlan)
	}
	plan.ReusedPayloads = reuse
}

func omitPayload(original []byte, optimized *[]byte) bool {
	if len(original) == 0 || !bytes.Equal(original, *optimized) {
		return false
	}
	*optimized = nil
	return true
}

// RestoreQueryPlanPayloads borrows immutable payloads from the original request.
// Phase 2 creates its own request before applying node-specific MVCC. This also
// preserves explicit optimizer replacements, including replacement with empty.
func RestoreQueryPlanPayloads(plan *viewpb.QueryPlan, original *viewpb.GetQueryPlanRequest) error {
	reuse := plan.GetReusedPayloads()
	if search := plan.GetLegacySearchRequest(); search != nil {
		input := original.GetLegacySearchRequest()
		if reuse.GetPlaceholderGroup() {
			search.PlaceholderGroup = input.GetPlaceholderGroup()
		}
		if reuse.GetSerializedExprPlan() {
			search.SerializedExprPlan = input.GetSerializedExprPlan()
		}
		if len(reuse.GetSubRequests()) > 0 && (len(reuse.GetSubRequests()) != len(search.GetSubReqs()) || len(search.GetSubReqs()) != len(input.GetSubReqs())) {
			return merr.WrapErrServiceInternalMsg("query plan payload reuse does not match sub-search count")
		}
		for i, subReuse := range reuse.GetSubRequests() {
			sub, before := search.GetSubReqs()[i], input.GetSubReqs()[i]
			if subReuse.GetPlaceholderGroup() {
				sub.PlaceholderGroup = before.GetPlaceholderGroup()
			}
			if subReuse.GetSerializedExprPlan() {
				sub.SerializedExprPlan = before.GetSerializedExprPlan()
			}
		}
	} else if query := plan.GetLegacyRetrieveRequest(); query != nil && reuse.GetSerializedExprPlan() {
		query.SerializedExprPlan = original.GetLegacyRetrieveRequest().GetSerializedExprPlan()
	}
	plan.ReusedPayloads = nil
	return nil
}

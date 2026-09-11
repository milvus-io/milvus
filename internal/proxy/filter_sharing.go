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

package proxy

import (
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// Recognising which of a hybrid search's sub-requests carry the same filter
// belongs here rather than at the query node.
//
// Deciding whether to actually share the evaluation stays at the delegator --
// it owns the segment distribution and the MVCC pin, and it splits further on
// ignore_growing. But it should not have to *discover* the equality: doing so
// there means unmarshalling every sub-request's serialized plan, re-marshalling
// its predicate and hashing it, repeated on every shard the request touches,
// for predicates that are multiple kilobytes in the workloads this targets.
//
// The proxy already parses each sub-request's (Dsl, expr_template_values) into
// a plan, so equality of those inputs is available for a string compare. Same
// inputs, same parser, same schema => same predicate.

// filterSharingCandidate is what the proxy holds for one sub-request once its
// plan has been built.
type filterSharingCandidate struct {
	dsl       string
	templates map[string]*schemapb.TemplateValue
	// False when there is nothing to share: no predicate at all, or an
	// iterative filter, which applies the predicate after the vector search
	// and so leaves no prefix subtree to reuse.
	shareable bool
}

// assignFilterSharingGroups returns one group number per sub-request, in the
// caller's order. Equal non-zero numbers mean identical predicates; 0 means the
// sub-request cannot share a filter at all.
//
// Singletons get a number of their own rather than 0, so the query node can
// still tell "nothing to share" from "nothing matched", which its fallback
// metric reports separately.
//
// The pairwise scan is quadratic, which is the right shape here: a hybrid
// search carries a handful of sub-requests, and an exact comparison avoids both
// the cost and the collision handling that digests would bring.
func assignFilterSharingGroups(candidates []filterSharingCandidate) []int32 {
	groups := make([]int32, len(candidates))
	var assigned int32
	for i := range candidates {
		if !candidates[i].shareable {
			continue
		}
		for j := 0; j < i; j++ {
			if groups[j] != 0 && sameFilterInput(candidates[j], candidates[i]) {
				groups[i] = groups[j]
				break
			}
		}
		if groups[i] == 0 {
			assigned++
			groups[i] = assigned
		}
	}
	return groups
}

func sameFilterInput(a, b filterSharingCandidate) bool {
	if a.dsl != b.dsl || len(a.templates) != len(b.templates) {
		return false
	}
	for name, av := range a.templates {
		bv, ok := b.templates[name]
		if !ok || !proto.Equal(av, bv) {
			return false
		}
	}
	return true
}

// planUsesIterativeFilter reports whether the plan applies its predicate after
// the vector search instead of before it. Such a plan has no filter prefix to
// share.
//
// This reads the same QueryInfo that gets serialized into the plan, so it
// answers exactly what the query node would have concluded from the serialized
// form.
func planUsesIterativeFilter(queryInfo *planpb.QueryInfo) bool {
	if queryInfo == nil {
		return false
	}
	if queryInfo.GetHints() == iterativeFilterKey {
		return true
	}
	params := queryInfo.GetSearchParams()
	if params == "" {
		return false
	}
	return gjson.Get(params, common.HintsKey).String() == iterativeFilterKey
}

// filterSharingCandidateOf builds the candidate for one sub-request from the
// plan and query info the proxy has just produced for it.
func filterSharingCandidateOf(
	dsl string,
	templates map[string]*schemapb.TemplateValue,
	plan *planpb.PlanNode,
	queryInfo *planpb.QueryInfo,
) filterSharingCandidate {
	anns := plan.GetVectorAnns()
	return filterSharingCandidate{
		dsl:       dsl,
		templates: templates,
		shareable: anns != nil && anns.GetPredicates() != nil && !planUsesIterativeFilter(queryInfo),
	}
}

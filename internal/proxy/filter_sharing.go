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
	"crypto/sha256"
	"encoding/binary"
	"io"
	"sort"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// Recognizing which of a hybrid search's sub-requests carry the same filter
// belongs here rather than at the query node.
//
// Deciding whether to actually share the evaluation stays at the delegator --
// it owns the segment distribution and the MVCC pin, and it splits further on
// ignore_growing. But it should not have to *discover* the equality: doing so
// there means unmarshalling every sub-request's serialized plan, re-marshaling
// its predicate and hashing it, repeated on every shard the request touches,
// for predicates that are multiple kilobytes in the workloads this targets.
//
// The proxy already parses each sub-request's (Dsl, expr_template_values) into
// a plan, so equality of those inputs is available for a string compare. Same
// inputs, same parser, same schema => same predicate.
//
// None of this runs unless the query node's sharing flag is on; the caller
// gates on it before building candidates at all.

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
// Candidates are bucketed by a fingerprint of their whole filter input, and a
// bucket keeps only the first member of each group it holds. A hybrid search
// may carry up to a thousand sub-requests with multi-kilobyte predicates, so a
// pairwise scan would compare those long strings -- and walk every template
// value -- hundreds of thousands of times. Equal fingerprints are in practice
// equal inputs, so a bucket holds a single group and the exact comparison runs
// once per candidate rather than once per pair. That comparison stays as the
// correctness guard: the fingerprint selects what is worth comparing, it does
// not decide.
func assignFilterSharingGroups(candidates []filterSharingCandidate) []int32 {
	groups := make([]int32, len(candidates))
	var assigned int32
	byFingerprint := make(map[[sha256.Size]byte][]int, len(candidates))
	for i := range candidates {
		if !candidates[i].shareable {
			continue
		}
		key, keyed := filterSharingFingerprint(candidates[i])
		if keyed {
			for _, j := range byFingerprint[key] {
				if sameFilterInput(candidates[j], candidates[i]) {
					groups[i] = groups[j]
					break
				}
			}
		}
		if groups[i] == 0 {
			assigned++
			groups[i] = assigned
			if keyed {
				byFingerprint[key] = append(byFingerprint[key], i)
			}
		}
	}
	return groups
}

// filterSharingFingerprint streams a deterministic, fixed-size key over a
// candidate's (Dsl, expr_template_values). Equal inputs always produce equal
// keys: the template names are sorted, because Go randomizes map iteration,
// and each value is marshaled with protobuf's deterministic option, without
// which proto.Marshal is free to emit the same message as different bytes.
// Every part has a fixed-width length prefix, so component boundaries are
// unambiguous without retaining an encoded copy of the whole filter input.
//
// A value that fails to marshal reports false. Such a candidate is left out of
// the buckets and gets a group of its own: it is still shareable, its plan does
// have a reusable prefix, it simply finds no peer, which the query node already
// reports as a no-peer fallback. Falling back to a pairwise scan for it would
// reintroduce exactly the quadratic comparison the buckets exist to avoid.
func filterSharingFingerprint(candidate filterSharingCandidate) ([sha256.Size]byte, bool) {
	names := make([]string, 0, len(candidate.templates))
	for name := range candidate.templates {
		names = append(names, name)
	}
	sort.Strings(names)

	digest := sha256.New()
	var length [8]byte
	writeLength := func(size int) {
		binary.BigEndian.PutUint64(length[:], uint64(size))
		_, _ = digest.Write(length[:])
	}
	writeStringPart := func(part string) {
		writeLength(len(part))
		_, _ = io.WriteString(digest, part)
	}
	writeBytesPart := func(part []byte) {
		writeLength(len(part))
		_, _ = digest.Write(part)
	}
	writeStringPart(candidate.dsl)
	marshal := proto.MarshalOptions{Deterministic: true}
	for _, name := range names {
		value, err := marshal.Marshal(candidate.templates[name])
		if err != nil {
			return [sha256.Size]byte{}, false
		}
		writeStringPart(name)
		writeBytesPart(value)
	}
	var key [sha256.Size]byte
	copy(key[:], digest.Sum(key[:0]))
	return key, true
}

func sameFilterInput(a, b filterSharingCandidate) bool {
	return a.dsl == b.dsl && sameTemplates(a.templates, b.templates)
}

func sameTemplates(a, b map[string]*schemapb.TemplateValue) bool {
	if len(a) != len(b) {
		return false
	}
	for name, av := range a {
		bv, ok := b[name]
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
// This reads the same QueryInfo that gets serialized into the plan, and mirrors
// the precedence of the segcore plan builder (ParseSearchInfo in
// PlanProto.cpp) rule for rule:
//
//  1. A group-by search is always emitted as a pre-filter plan, whatever the
//     hint says, so it keeps a shareable prefix.
//  2. Iterative filtering does not support range search: a radius in the search
//     params turns it off before any hint is read.
//  3. An explicit QueryInfo.Hints decides on its own, and "disable" means
//     disable even when the search params ask for an iterative filter. Only
//     when Hints is empty does the hints key inside the search params apply.
//     Any other value is rejected by segcore, so a request carrying one never
//     reaches a plan and reading it as non-iterative costs nothing.
func planUsesIterativeFilter(queryInfo *planpb.QueryInfo) bool {
	if queryInfo == nil {
		return false
	}
	if len(queryInfo.GetGroupByFieldIds()) > 0 || queryInfo.GetGroupByFieldId() > 0 {
		return false
	}
	params := queryInfo.GetSearchParams()
	if gjson.Get(params, radiusKey).Exists() {
		return false
	}
	if hints := queryInfo.GetHints(); hints != "" {
		return hints == iterativeFilterKey
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

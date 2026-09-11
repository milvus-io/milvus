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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func tmpl(v int64) map[string]*schemapb.TemplateValue {
	return map[string]*schemapb.TemplateValue{
		"v": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: v}},
	}
}

// shareable builds a candidate the proxy would consider groupable.
func shareable(dsl string, templates map[string]*schemapb.TemplateValue) filterSharingCandidate {
	return filterSharingCandidate{dsl: dsl, templates: templates, shareable: true}
}

func TestAssignFilterSharingGroups(t *testing.T) {
	cases := []struct {
		name       string
		candidates []filterSharingCandidate
		want       []int32
	}{
		{
			name:       "identical Dsl shares a group",
			candidates: []filterSharingCandidate{shareable("a > 1", nil), shareable("a > 1", nil)},
			want:       []int32{1, 1},
		},
		{
			name:       "different Dsl does not",
			candidates: []filterSharingCandidate{shareable("a > 1", nil), shareable("a > 2", nil)},
			want:       []int32{1, 2},
		},
		{
			name:       "same Dsl but different template values does not",
			candidates: []filterSharingCandidate{shareable("a > {v}", tmpl(1)), shareable("a > {v}", tmpl(2))},
			want:       []int32{1, 2},
		},
		{
			name:       "same Dsl and equal template values does",
			candidates: []filterSharingCandidate{shareable("a > {v}", tmpl(1)), shareable("a > {v}", tmpl(1))},
			want:       []int32{1, 1},
		},
		{
			name: "unshareable sub-requests get 0, even with identical Dsl",
			candidates: []filterSharingCandidate{
				{dsl: "a > 1", shareable: false},
				{dsl: "a > 1", shareable: false},
			},
			want: []int32{0, 0},
		},
		{
			name: "an unshareable sub-request does not absorb a shareable one",
			candidates: []filterSharingCandidate{
				{dsl: "a > 1", shareable: false},
				shareable("a > 1", nil),
				shareable("a > 1", nil),
			},
			want: []int32{0, 1, 1},
		},
		{
			name: "numbering follows first appearance, not sorted order",
			candidates: []filterSharingCandidate{
				shareable("b", nil), shareable("a", nil), shareable("b", nil), shareable("a", nil),
			},
			want: []int32{1, 2, 1, 2},
		},
		{
			name:       "three on one predicate",
			candidates: []filterSharingCandidate{shareable("a", nil), shareable("a", nil), shareable("a", nil)},
			want:       []int32{1, 1, 1},
		},
		{
			name:       "single sub-request still gets a number",
			candidates: []filterSharingCandidate{shareable("a", nil)},
			want:       []int32{1},
		},
		{
			name:       "no sub-requests",
			candidates: nil,
			want:       []int32{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, assignFilterSharingGroups(tc.candidates))
		})
	}
}

// A template map with the same contents but a different iteration order must
// still compare equal: Go map ordering is unspecified, and the delegator would
// otherwise see two groups where the predicates are identical.
func TestSameFilterInputIgnoresMapOrder(t *testing.T) {
	build := func() map[string]*schemapb.TemplateValue {
		return map[string]*schemapb.TemplateValue{
			"a": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 1}},
			"b": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 2}},
			"c": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 3}},
		}
	}
	assert.True(t, sameFilterInput(shareable("x", build()), shareable("x", build())))

	missing := build()
	delete(missing, "c")
	assert.False(t, sameFilterInput(shareable("x", build()), shareable("x", missing)))
}

func TestFilterSharingCandidateOf(t *testing.T) {
	withPredicate := func(qi *planpb.QueryInfo) *planpb.PlanNode {
		return &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					Predicates: &planpb.Expr{},
					QueryInfo:  qi,
				},
			},
		}
	}
	plain := &planpb.QueryInfo{SearchParams: "{}"}

	t.Run("a plain predicate is shareable", func(t *testing.T) {
		got := filterSharingCandidateOf("a > 1", nil, withPredicate(plain), plain)
		assert.True(t, got.shareable)
		assert.Equal(t, "a > 1", got.dsl)
	})

	t.Run("no predicate is not", func(t *testing.T) {
		noPredicate := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{QueryInfo: plain}},
		}
		assert.False(t, filterSharingCandidateOf("", nil, noPredicate, plain).shareable)
	})

	t.Run("a non-ANNS plan is not", func(t *testing.T) {
		assert.False(t, filterSharingCandidateOf("", nil, &planpb.PlanNode{}, plain).shareable)
	})

	t.Run("an iterative filter is not: the predicate runs after the search", func(t *testing.T) {
		viaHints := &planpb.QueryInfo{Hints: iterativeFilterKey, SearchParams: "{}"}
		assert.False(t, filterSharingCandidateOf("a > 1", nil, withPredicate(viaHints), viaHints).shareable)

		viaParams := &planpb.QueryInfo{SearchParams: `{"hints": "iterative_filter"}`}
		assert.False(t, filterSharingCandidateOf("a > 1", nil, withPredicate(viaParams), viaParams).shareable)
	})
}

func TestPlanUsesIterativeFilter(t *testing.T) {
	assert.False(t, planUsesIterativeFilter(nil))
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{}))
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{SearchParams: `{"hints": "none"}`}))
	assert.True(t, planUsesIterativeFilter(&planpb.QueryInfo{Hints: iterativeFilterKey}))
	assert.True(t, planUsesIterativeFilter(&planpb.QueryInfo{SearchParams: `{"hints": "iterative_filter"}`}))
}

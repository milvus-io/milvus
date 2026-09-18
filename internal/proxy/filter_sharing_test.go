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
	"context"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
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

	t.Run("a range search is: it is emitted as a pre-filter plan despite the hint", func(t *testing.T) {
		rangeSearch := &planpb.QueryInfo{Hints: iterativeFilterKey, SearchParams: `{"radius": 0.5}`}
		assert.True(t, filterSharingCandidateOf("a > 1", nil, withPredicate(rangeSearch), rangeSearch).shareable)
	})
}

func TestPlanUsesIterativeFilter(t *testing.T) {
	assert.False(t, planUsesIterativeFilter(nil))
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{}))
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{SearchParams: `{"hints": "none"}`}))
	assert.True(t, planUsesIterativeFilter(&planpb.QueryInfo{Hints: iterativeFilterKey}))
	assert.True(t, planUsesIterativeFilter(&planpb.QueryInfo{SearchParams: `{"hints": "iterative_filter"}`}))

	// QueryInfo.Hints is read before the search params and settles the question
	// on its own, so "disable" disables even when the params ask for iterative
	// filtering. The plan is a pre-filter one, and its prefix is shareable.
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{
		Hints:        "disable",
		SearchParams: `{"hints": "iterative_filter"}`,
	}))
	// Any other value fails the plan builder, so no plan is ever emitted for it.
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{Hints: "bogus"}))

	// Iterative filtering does not support range search: a radius in the search
	// params stops the plan builder before it reads a hint at all.
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{
		Hints:        iterativeFilterKey,
		SearchParams: `{"radius": 0.5}`,
	}))
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{
		SearchParams: `{"radius": 0.5, "range_filter": 0.1, "hints": "iterative_filter"}`,
	}))

	// The plan builder ignores the hint for a group-by search and emits a
	// pre-filter plan, whose prefix is shareable.
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{Hints: iterativeFilterKey, GroupByFieldIds: []int64{101}}))
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{Hints: iterativeFilterKey, GroupByFieldId: 101}))
	assert.False(t, planUsesIterativeFilter(&planpb.QueryInfo{
		Hints:          iterativeFilterKey,
		GroupByFieldId: 101,
		SearchParams:   `{"radius": 0.5}`,
	}))
	assert.True(t, planUsesIterativeFilter(&planpb.QueryInfo{Hints: iterativeFilterKey, GroupByFieldId: -1}),
		"-1 is how the proxy spells 'no group-by'")
}

// Grouping must stay exact when many sub-requests share a Dsl but differ in
// template values, and must not compare sub-requests whose Dsl differs at all.
func TestAssignFilterSharingGroupsManyCandidates(t *testing.T) {
	const n = 1024
	candidates := make([]filterSharingCandidate, 0, n)
	want := make([]int32, 0, n)
	for i := 0; i < n; i++ {
		// 4 distinct Dsl strings x 4 distinct template values = 16 groups,
		// each hit 64 times.
		dsl := "a > {v} && b == " + strings.Repeat("x", 100) + string(rune('a'+i%4))
		candidates = append(candidates, shareable(dsl, tmpl(int64(i/4%4))))
		want = append(want, int32(i%16+1))
	}
	assert.Equal(t, want, assignFilterSharingGroups(candidates))
}

// The fingerprint exists so that a bucket holds one group and the exact
// comparison runs once per candidate instead of once per pair. Group numbers
// alone cannot show that -- a pairwise scan produces the same ones -- so count
// the comparisons.
func TestAssignFilterSharingGroupsComparesOncePerCandidate(t *testing.T) {
	const n = 1024
	// Long enough that a pairwise scan would be visibly expensive, which is the
	// case the fingerprint exists for.
	dsl := "a > {v} && b == " + strings.Repeat("x", 4096)

	// The hook repeats what sameFilterInput does rather than calling through to
	// it, which would recurse into the hook.
	countCompares := func(candidates []filterSharingCandidate) (int, []int32) {
		mock := mockey.Mock(sameFilterInput).To(func(a, b filterSharingCandidate) bool {
			return a.dsl == b.dsl && sameTemplates(a.templates, b.templates)
		}).Build()
		defer mock.UnPatch()
		groups := assignFilterSharingGroups(candidates)
		return mock.Times(), groups
	}

	t.Run("distinct template values under one Dsl never reach the comparison", func(t *testing.T) {
		candidates := make([]filterSharingCandidate, 0, n)
		want := make([]int32, 0, n)
		for i := 0; i < n; i++ {
			candidates = append(candidates, shareable(dsl, tmpl(int64(i))))
			want = append(want, int32(i+1))
		}
		compares, groups := countCompares(candidates)
		assert.Equal(t, want, groups)
		// Every fingerprint differs, so every bucket is empty on arrival. A
		// pairwise scan would have run n*(n-1)/2 = 523776 comparisons here.
		assert.Equal(t, 0, compares)
	})

	t.Run("identical candidates compare against one representative each", func(t *testing.T) {
		candidates := make([]filterSharingCandidate, 0, n)
		want := make([]int32, 0, n)
		for i := 0; i < n; i++ {
			candidates = append(candidates, shareable(dsl, tmpl(7)))
			want = append(want, 1)
		}
		compares, groups := countCompares(candidates)
		assert.Equal(t, want, groups)
		// Only the first member of the group is kept in the bucket, so each of
		// the remaining candidates compares exactly once.
		assert.Equal(t, n-1, compares)
	})
}

// Go randomizes map iteration, so a fingerprint that walked the template map in
// range order would differ between two runs over the very same map. Sorting the
// names is what makes the key usable as a bucket key at all.
func TestFilterSharingFingerprintIgnoresMapOrder(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	build := func(order []string) map[string]*schemapb.TemplateValue {
		templates := make(map[string]*schemapb.TemplateValue, len(order))
		for _, name := range order {
			// The value follows the name, so the two maps differ only in the
			// order their entries were written.
			templates[name] = &schemapb.TemplateValue{Val: &schemapb.TemplateValue_StringVal{StringVal: name}}
		}
		return templates
	}
	forward := build(names)
	backward := build(lo.Reverse(append([]string{}, names...)))

	want, ok := filterSharingFingerprint(shareable("x", forward))
	require.True(t, ok)
	for i := 0; i < 32; i++ {
		again, ok := filterSharingFingerprint(shareable("x", forward))
		require.True(t, ok)
		assert.Equal(t, want, again)
	}

	// Same names, same values, inserted in the opposite order: still one group.
	assert.Equal(t, []int32{1, 1}, assignFilterSharingGroups([]filterSharingCandidate{
		shareable("x", forward), shareable("x", backward),
	}))
}

// The fingerprint covers the marshaled value, not just the template name, so
// two sub-requests whose only difference is buried in a nested array must not
// be told to share a bitset.
func TestAssignFilterSharingGroupsSeesNestedTemplateValues(t *testing.T) {
	nested := func(values ...int64) map[string]*schemapb.TemplateValue {
		return map[string]*schemapb.TemplateValue{
			"v": {Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: &schemapb.TemplateArrayValue{
				Data: &schemapb.TemplateArrayValue_ArrayData{ArrayData: &schemapb.TemplateArrayValueArray{
					Data: []*schemapb.TemplateArrayValue{{
						Data: &schemapb.TemplateArrayValue_LongData{LongData: &schemapb.LongArray{Data: values}},
					}},
				}},
			}}},
		}
	}
	assert.Equal(t, []int32{1, 2, 1}, assignFilterSharingGroups([]filterSharingCandidate{
		shareable("a in {v}", nested(1, 2, 3)),
		shareable("a in {v}", nested(1, 2, 4)),
		shareable("a in {v}", nested(1, 2, 3)),
	}))
}

// A candidate whose template value cannot be marshaled keeps a group of its
// own: it stays shareable, it just cannot be matched against anything.
func TestAssignFilterSharingGroupsUnfingerprintableCandidate(t *testing.T) {
	unusedKey, _ := filterSharingFingerprint(filterSharingCandidate{})
	mock := mockey.Mock(filterSharingFingerprint).Return(unusedKey, false).Build()
	defer mock.UnPatch()

	assert.Equal(t, []int32{1, 2, 3}, assignFilterSharingGroups([]filterSharingCandidate{
		shareable("a > {v}", tmpl(1)),
		shareable("a > {v}", tmpl(1)),
		shareable("a > {v}", tmpl(1)),
	}))
}

// The query node reads the hint only when its own sharing flag is on, so the
// proxy must not pay for the grouping while the flag is off: no candidate is
// built and every hint stays at 0.
func TestInitAdvancedSearchRequestGatesFilterSharing(t *testing.T) {
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{
		Name: "test_filter_sharing",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
			},
			{FieldID: 102, Name: "g", DataType: schemapb.DataType_Int64},
		},
	}
	placeholderGroup, err := proto.Marshal(constructPlaceholderGroup(1, 4))
	require.NoError(t, err)
	schemaInfo, err := newSchemaInfo(schema)
	require.NoError(t, err)

	newTask := func() *searchTask {
		subReq := func() *milvuspb.SubSearchRequest {
			return &milvuspb.SubSearchRequest{
				Dsl:              "g > 3",
				DslType:          commonpb.DslType_BoolExprV1,
				Nq:               1,
				PlaceholderGroup: placeholderGroup,
				SearchParams: []*commonpb.KeyValuePair{
					{Key: AnnsFieldKey, Value: "vec"},
					{Key: TopKKey, Value: "10"},
					{Key: common.MetricTypeKey, Value: metric.L2},
					{Key: ParamsKey, Value: `{}`},
				},
			}
		}
		return &searchTask{
			ctx:            ctx,
			collectionName: schema.GetName(),
			SearchRequest:  &internalpb.SearchRequest{},
			request: &milvuspb.SearchRequest{
				CollectionName: schema.GetName(),
				SubReqs:        []*milvuspb.SubSearchRequest{subReq(), subReq()},
				SearchParams:   []*commonpb.KeyValuePair{{Key: LimitKey, Value: "10"}},
			},
			schema: schemaInfo,
			tr:     timerecord.NewTimeRecorder("test-filter-sharing"),
		}
	}
	hints := func(task *searchTask) []int32 {
		return lo.Map(task.SubReqs, func(subReq *internalpb.SubSearchRequest, _ int) int32 {
			return subReq.GetFilterSharingGroup()
		})
	}

	key := Params.QueryNodeCfg.HybridSearchSharedFilterEnabled.Key
	t.Run("flag off leaves every hint unset", func(t *testing.T) {
		paramtable.Get().Save(key, "false")
		defer paramtable.Get().Reset(key)
		task := newTask()
		require.NoError(t, task.initAdvancedSearchRequest(ctx))
		assert.Equal(t, []int32{0, 0}, hints(task))
	})

	t.Run("flag on groups the identical predicates", func(t *testing.T) {
		paramtable.Get().Save(key, "true")
		defer paramtable.Get().Reset(key)
		task := newTask()
		require.NoError(t, task.initAdvancedSearchRequest(ctx))
		assert.Equal(t, []int32{1, 1}, hints(task))
	})
}

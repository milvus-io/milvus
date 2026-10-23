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

package planparserv2

import (
	"encoding/base64"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func explainSearch(t *testing.T, helper *typeutil.SchemaHelper, expr string, info *planpb.QueryInfo) *PlanExplanation {
	t.Helper()
	plan, err := CreateSearchPlan(helper, expr, "FloatVectorField", info, nil, nil)
	require.NoError(t, err, expr)
	got, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	return got
}

func mustRetrievePlan(t *testing.T, helper *typeutil.SchemaHelper, expr string) *planpb.PlanNode {
	t.Helper()
	plan, err := CreateRetrievePlan(helper, expr, nil)
	require.NoError(t, err, expr)
	return plan
}

func explainRetrieve(t *testing.T, helper *typeutil.SchemaHelper, expr string) *PlanExplanation {
	t.Helper()
	got, err := ExplainPlan(mustRetrievePlan(t, helper, expr), helper, nil)
	require.NoError(t, err)
	return got
}

func fieldIDByName(t *testing.T, helper *typeutil.SchemaHelper, name string) int64 {
	t.Helper()
	field, err := helper.GetFieldFromName(name)
	require.NoError(t, err)
	return field.GetFieldID()
}

// One small search plan is pinned byte for byte, key order included;
// TestExplainPlan_ContractKeys covers the keys this plan does not carry.
func TestExplainPlan_VectorSearchJSONContract(t *testing.T) {
	helper := newTestSchemaHelper(t)
	vecID := fieldIDByName(t, helper, "FloatVectorField")
	intID := fieldIDByName(t, helper, "Int64Field")

	plan, err := CreateSearchPlan(helper, "Int64Field > 1", "FloatVectorField", &planpb.QueryInfo{
		Topk:           10,
		MetricType:     "L2",
		SearchParams:   `{"nprobe": 8}`,
		RoundDecimal:   -1,
		GroupByFieldId: -1,
	}, nil, nil)
	require.NoError(t, err)
	plan.OutputFieldIds = []int64{intID}
	// Set explicitly: the parser's default comes from paramtable.
	plan.PlanOptions = &planpb.PlanOption{ExprUseJsonStats: true}

	got, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	js, err := got.JSON()
	require.NoError(t, err)

	want := fmt.Sprintf(`{"version":1,"plan_type":"vector_search",`+
		`"vector_search":{"field":{"id":%d,"name":"FloatVectorField","type":"FloatVector"},`+
		`"vector_type":"FloatVector","metric_type":"L2","topk":10,"search_params":{"nprobe":8},`+
		`"round_decimal":-1,"group_by":null,"iterator":null},`+
		`"query":null,`+
		`"predicates":{"node":"unary_range","op":"GreaterThan",`+
		`"field":{"id":%d,"name":"Int64Field","type":"Int64"},"value":1},`+
		`"output_fields":[{"id":%d,"name":"Int64Field","type":"Int64"}],`+
		`"dynamic_fields":[],"scorers":[],"options":{"expr_use_json_stats":true},"unknown_node_count":0}`, vecID, intID, intID)
	assert.Equal(t, want, string(js), "key order is part of the contract")
}

func TestExplainPlan_QueryPlan(t *testing.T) {
	helper := newTestSchemaHelper(t)
	got := explainRetrieve(t, helper, `Int64Field in [1, 2]`)

	assert.Equal(t, PlanTypeQuery, got.PlanType)
	assert.Nil(t, got.VectorSearch)
	require.NotNil(t, got.Query)
	assert.False(t, got.Query.IsCount)
	assert.Equal(t, int64(0), got.Query.Limit)
	require.NotNil(t, got.Predicates)
	assert.Equal(t, NodeTerm, got.Predicates.Node)
	assert.Equal(t, "Int64Field", got.Predicates.Field.Name)
	assert.Equal(t, []any{int64(1), int64(2)}, got.Predicates.Values)
	assert.Equal(t, 0, got.UnknownNodeCount)
}

// One row per node kind the parser can emit from an expression string. Rows
// assert the root node and operator; nested shapes are covered separately.
func TestExplainPlan_PredicateNodes(t *testing.T) {
	helper := newTestSchemaHelper(t)
	cases := []struct {
		name      string
		expr      string
		wantNode  string
		wantOp    string
		wantField string
	}{
		{"term", `Int64Field in [1, 2]`, NodeTerm, "", "Int64Field"},
		{"unary_range", `Int64Field > 1`, NodeUnaryRange, "GreaterThan", "Int64Field"},
		{"binary_range", `1 < Int64Field < 10`, NodeBinaryRange, "", "Int64Field"},
		{"binary_or", `Int64Field > 1 || VarCharField == "a"`, NodeBinary, "LogicalOr", ""},
		{"compare", `Int8Field < Int16Field`, NodeCompare, "LessThan", "Int8Field"},
		{"arith_compare", `Int64Field + 1 == 5`, NodeArithCompare, "Equal", "Int64Field"},
		{"json_contains", `json_contains(JSONField["a"], 1)`, NodeJSONContains, "Contains", "JSONField"},
		{"json_contains_all", `json_contains_all(ArrayField, [1, 2])`, NodeJSONContains, "ContainsAll", "ArrayField"},
		{"like_prefix", `VarCharField like "ab%"`, NodeUnaryRange, "PrefixMatch", "VarCharField"},
		{"not", `not (Int64Field in [1, 2])`, NodeUnary, "Not", ""},
		{"exists", `exists JSONField["key"]`, NodeExists, "", "JSONField"},
		{"always_true", `true`, NodeAlwaysTrue, "", ""},
		{"always_false", `false`, NodeUnary, "Not", ""}, // the parser spells false as not(always_true)
		{"is_null", `Int64Field is null`, NodeNull, "IsNull", "Int64Field"},
		{"is_not_null", `Int64Field is not null`, NodeNull, "IsNotNull", "Int64Field"},
		{"random_sample", `random_sample(0.01)`, NodeRandomSample, "", ""},
		{"array_length", `array_length(ArrayField) == 3`, NodeArithCompare, "Equal", "ArrayField"},
		{"gis", `st_equals(GeometryField, "POINT(0 0)")`, NodeGIS, "Equals", "GeometryField"},
		{"timestamptz_arith_compare", `TimestamptzField + INTERVAL 'P1D' > ISO '2025-01-01T00:00:00Z'`, NodeTimestamptzArithCompare, "GreaterThan", "TimestamptzField"},
		{"element_filter", `element_filter(struct_array, $[sub_int] > 1)`, NodeElementFilter, "", ""},
		{"match_least", `match_least(struct_array, $[sub_int] > 1, threshold=2)`, NodeMatch, "MatchLeast", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := explainRetrieve(t, helper, tc.expr)
			require.NotNil(t, got.Predicates, tc.expr)
			assert.Equal(t, tc.wantNode, got.Predicates.Node)
			assert.Equal(t, tc.wantOp, got.Predicates.Op)
			if tc.wantField != "" {
				require.NotNil(t, got.Predicates.Field)
				assert.Equal(t, tc.wantField, got.Predicates.Field.Name)
			}
			assert.Equal(t, 0, got.UnknownNodeCount, tc.expr)
		})
	}
}

func TestExplainPlan_NestedTree(t *testing.T) {
	helper := newTestSchemaHelper(t)
	got := explainRetrieve(t, helper, `Int64Field > 1 && VarCharField == "a"`)

	root := got.Predicates
	require.NotNil(t, root)
	assert.Equal(t, NodeBinary, root.Node)
	assert.Equal(t, "LogicalAnd", root.Op)
	require.NotNil(t, root.Left)
	require.NotNil(t, root.Right)

	// The rewriter may reorder AND operands, so match children by field.
	byField := map[string]*PredicateNode{
		root.Left.Field.Name:  root.Left,
		root.Right.Field.Name: root.Right,
	}
	intSide, ok := byField["Int64Field"]
	require.True(t, ok)
	assert.Equal(t, NodeUnaryRange, intSide.Node)
	assert.Equal(t, "GreaterThan", intSide.Op)
	assert.Equal(t, int64(1), intSide.Value)

	strSide, ok := byField["VarCharField"]
	require.True(t, ok)
	assert.Equal(t, NodeUnaryRange, strSide.Node)
	assert.Equal(t, "Equal", strSide.Op)
	assert.Equal(t, "a", strSide.Value)
	assert.Equal(t, 0, got.UnknownNodeCount)
}

func TestExplainPlan_UnknownNodesAreRepresented(t *testing.T) {
	helper := newTestSchemaHelper(t)
	known, err := ParseExpr(helper, "Int64Field > 1", nil)
	require.NoError(t, err)

	t.Run("empty expr inside a tree", func(t *testing.T) {
		plan := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
			Predicates: &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
				Op:    planpb.BinaryExpr_LogicalAnd,
				Left:  known,
				Right: &planpb.Expr{},
			}}},
		}}}
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		require.NotNil(t, got.Predicates)
		assert.Equal(t, NodeUnaryRange, got.Predicates.Left.Node)
		assert.Equal(t, NodeUnknown, got.Predicates.Right.Node)
		assert.Equal(t, "expression not set", got.Predicates.Right.Detail)
		assert.Equal(t, 1, got.UnknownNodeCount)
	})

	t.Run("missing child", func(t *testing.T) {
		plan := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
			Predicates: &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{
				Op: planpb.UnaryExpr_Not,
			}}},
		}}}
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		require.NotNil(t, got.Predicates.Child)
		assert.Equal(t, NodeUnknown, got.Predicates.Child.Node)
		assert.Equal(t, 1, got.UnknownNodeCount)
	})

	t.Run("plan without node", func(t *testing.T) {
		got, err := ExplainPlan(&planpb.PlanNode{}, helper, nil)
		require.NoError(t, err)
		assert.Equal(t, PlanTypeUnknown, got.PlanType)
		assert.Equal(t, "plan node not set", got.Detail)
		assert.Nil(t, got.Predicates)
		assert.Equal(t, 1, got.UnknownNodeCount)
		assert.True(t, strings.HasPrefix(got.Text(), "plan: unknown (plan node not set)\n"), got.Text())
	})

	t.Run("nil plan is an error", func(t *testing.T) {
		// A caller bug, not request content, so it is a system error.
		_, err := ExplainPlan(nil, helper, nil)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}

func TestExplainPlan_NoFilterAndNoSchema(t *testing.T) {
	helper := newTestSchemaHelper(t)
	plan, err := CreateSearchPlan(helper, "", "FloatVectorField", &planpb.QueryInfo{Topk: 5, GroupByFieldId: -1}, nil, nil)
	require.NoError(t, err)

	got, err := ExplainPlan(plan, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, got.Predicates)
	assert.Equal(t, "", got.VectorSearch.Field.Name, "no schema: name cannot be resolved")
	assert.Equal(t, fieldIDByName(t, helper, "FloatVectorField"), got.VectorSearch.Field.ID)
	assert.Nil(t, got.VectorSearch.SearchParams)
	assert.Contains(t, got.Text(), "filter: none")
}

func TestExplainPlan_RedactsTemplateValuesOnly(t *testing.T) {
	helper := newTestSchemaHelper(t)
	templates := map[string]*schemapb.TemplateValue{"ids": {
		Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}},
		}},
	}}
	plan, err := CreateRetrievePlan(helper, "Int64Field in {ids} && VarCharField == \"literal\"", templates)
	require.NoError(t, err)

	verbatim, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	redacted, err := ExplainPlan(plan, helper, &ExplainOptions{RedactTemplateValues: true})
	require.NoError(t, err)

	pick := func(p *PlanExplanation, node string) *PredicateNode {
		for _, side := range []*PredicateNode{p.Predicates.Left, p.Predicates.Right} {
			if side.Node == node {
				return side
			}
		}
		t.Fatalf("no %s node in %s", node, p.Text())
		return nil
	}
	assert.Equal(t, []any{int64(1), int64(2)}, pick(verbatim, NodeTerm).Values)
	assert.Equal(t, "ids", pick(verbatim, NodeTerm).Template)
	assert.Equal(t, []any{"<template ids elided, 2 values>"}, pick(redacted, NodeTerm).Values)
	assert.Equal(t, "literal", pick(redacted, NodeUnaryRange).Value, "literals stay visible")
}

func TestExplainPlan_SummarizesMembershipBlob(t *testing.T) {
	helper := newTestSchemaHelper(t)
	tv, _ := bloomBytesTemplate(t, 0.001, 21, 22)
	plan, err := CreateSearchPlan(helper, "membership_match(Int64Field, {bf}, type=bloom)", "FloatVectorField",
		&planpb.QueryInfo{Topk: 10, MetricType: "L2", GroupByFieldId: -1},
		map[string]*schemapb.TemplateValue{"bf": tv}, nil)
	require.NoError(t, err)
	blob := plan.GetVectorAnns().GetPredicates().GetBloomFilterExpr().GetFilterBlob()
	require.NotEmpty(t, blob)

	got, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, got.Predicates)
	assert.Equal(t, NodeBloomFilter, got.Predicates.Node)
	require.NotNil(t, got.Predicates.Blob)
	assert.Equal(t, len(blob), got.Predicates.Blob.Bytes)

	js, err := got.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(js), fmt.Sprintf(`"blob":{"bytes":%d}`, len(blob)))
	assert.NotContains(t, string(js), base64.StdEncoding.EncodeToString(blob), "the blob itself must not be serialized")
	assert.Contains(t, got.Text(), fmt.Sprintf("  bloom_filter Int64Field blob=<%d bytes>\n", len(blob)))
}

// Scorer params decide the scores (the random scorer hashes each row with its
// seed), so the text view carries them too, with sorted keys so the line is
// stable.
func TestExplainPlan_Scorers(t *testing.T) {
	helper := newTestSchemaHelper(t)
	intID := fieldIDByName(t, helper, "Int64Field")
	fs := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{
		{Params: []*commonpb.KeyValuePair{
			{Key: rerank.WeightKey, Value: "1.5"},
			{Key: "reranker", Value: rerank.BoostName},
			{Key: rerank.FilterKey, Value: "Int64Field > 0"},
		}},
		{Params: []*commonpb.KeyValuePair{
			{Key: rerank.WeightKey, Value: "2"},
			{Key: "reranker", Value: rerank.BoostName},
			{Key: BoostRandomScoreKey, Value: `{"seed": 42, "field": "Int64Field"}`},
		}},
	}}
	plan, err := CreateSearchPlan(helper, "", "FloatVectorField", &planpb.QueryInfo{GroupByFieldId: -1}, nil, fs)
	require.NoError(t, err)
	require.Len(t, plan.GetScorers(), 2)

	got, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	require.Len(t, got.Scorers, 2)
	assert.Equal(t, planpb.FunctionType_FunctionTypeWeight.String(), got.Scorers[0].Type)
	assert.InDelta(t, 1.5, float64(got.Scorers[0].Weight), 1e-6)
	require.NotNil(t, got.Scorers[0].Filter)
	assert.Equal(t, NodeUnaryRange, got.Scorers[0].Filter.Node)
	assert.Nil(t, got.Scorers[0].Params)
	assert.Equal(t, planpb.FunctionType_FunctionTypeRandom.String(), got.Scorers[1].Type)
	assert.Equal(t, map[string]string{RandomScoreSeedKey: "42", RandomScoreFileIdKey: strconv.FormatInt(intID, 10)}, got.Scorers[1].Params)
	assert.Nil(t, got.Scorers[1].Filter)
	assert.Equal(t, 0, got.UnknownNodeCount)

	js, err := got.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(js), `"weight":1.5`)
	text := got.Text()
	assert.Contains(t, text, "scorer[0]: FunctionTypeWeight weight=1.5\n  unary_range GreaterThan Int64Field 0\n")
	assert.Contains(t, text, fmt.Sprintf(`scorer[1]: FunctionTypeRandom weight=2.0 params={"field_id":"%d","seed":"42"}`, intID)+"\n")
}

// Values typed into the expression stay visible under redaction even when
// they sit next to a template value in the same node.
func TestExplainPlan_RedactsTemplateOperandOnly(t *testing.T) {
	helper := newTestSchemaHelper(t)
	templates := map[string]*schemapb.TemplateValue{"x": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 1}}}
	plan, err := CreateRetrievePlan(helper, "Int64Field + {x} == 5", templates)
	require.NoError(t, err)

	verbatim, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	require.Equal(t, NodeArithCompare, verbatim.Predicates.Node)
	assert.Equal(t, int64(1), verbatim.Predicates.Operand)
	assert.Equal(t, int64(5), verbatim.Predicates.Value)
	assert.Equal(t, "x", verbatim.Predicates.OperandTemplate)
	assert.Equal(t, "", verbatim.Predicates.Template)

	redacted, err := ExplainPlan(plan, helper, &ExplainOptions{RedactTemplateValues: true})
	require.NoError(t, err)
	assert.Equal(t, "<template x elided>", redacted.Predicates.Operand)
	assert.Equal(t, int64(5), redacted.Predicates.Value)
	assert.Contains(t, redacted.Text(), `arith_compare Equal Int64Field arith=Add operand="<template x elided>" 5 operand_template=x`+"\n")

	t.Run("template on the value side", func(t *testing.T) {
		templates := map[string]*schemapb.TemplateValue{"y": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 5}}}
		plan, err := CreateRetrievePlan(helper, "Int64Field + 1 == {y}", templates)
		require.NoError(t, err)
		redacted, err := ExplainPlan(plan, helper, &ExplainOptions{RedactTemplateValues: true})
		require.NoError(t, err)
		require.Equal(t, NodeArithCompare, redacted.Predicates.Node)
		assert.Equal(t, "y", redacted.Predicates.Template)
		assert.Equal(t, "", redacted.Predicates.OperandTemplate)
		assert.Equal(t, int64(1), redacted.Predicates.Operand)
		assert.Equal(t, "<template y elided>", redacted.Predicates.Value)
	})
}

// A GIS template is the one template the plan does not name: the parser keeps
// the placeholder in wkt_string and template filling overwrites it with the
// caller's WKT, so only Expr.is_template is left behind. Redaction must still
// elide it. RedactPlanForLog keys on template variable names and cannot, so
// this is checked directly rather than through parity.
func TestExplainPlan_RedactsTemplateWKT(t *testing.T) {
	helper := newTestSchemaHelper(t)
	templates := map[string]*schemapb.TemplateValue{
		"wkt": generateTemplateValue(schemapb.DataType_VarChar, "POINT(1 2)"),
	}
	for _, tc := range []struct{ op, expr string }{
		{"Contains", `st_contains(GeometryField, {wkt})`},
		{"DWithin", `st_dwithin(GeometryField, {wkt}, 2.5)`},
	} {
		t.Run(tc.op, func(t *testing.T) {
			plan, err := CreateRetrievePlan(helper, tc.expr, templates)
			require.NoError(t, err)
			verbatim, err := ExplainPlan(plan, helper, nil)
			require.NoError(t, err)
			require.Equal(t, NodeGIS, verbatim.Predicates.Node)
			assert.Equal(t, "POINT(1 2)", verbatim.Predicates.WKT)
			assert.Empty(t, verbatim.Predicates.Template, "a filled GIS template keeps no variable name")

			redacted, err := ExplainPlan(plan, helper, &ExplainOptions{RedactTemplateValues: true})
			require.NoError(t, err)
			assert.Equal(t, "<template elided>", redacted.Predicates.WKT)
			js, err := redacted.JSON()
			require.NoError(t, err)
			assert.NotContains(t, string(js), "POINT(1 2)")
			assert.NotContains(t, redacted.Text(), "POINT(1 2)")
			assert.Contains(t, redacted.Text(), "gis "+tc.op+" GeometryField wkt=<template elided>")
			if tc.op == "DWithin" {
				require.NotNil(t, redacted.Predicates.Distance)
				assert.Equal(t, Float64(2.5), *redacted.Predicates.Distance, "the literal distance stays visible")
			}
		})
	}

	t.Run("literal WKT stays visible", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper, `st_contains(GeometryField, "POINT(1 2)")`, nil)
		require.NoError(t, err)
		literal, err := ExplainPlan(plan, helper, &ExplainOptions{RedactTemplateValues: true})
		require.NoError(t, err)
		assert.Equal(t, "POINT(1 2)", literal.Predicates.WKT)
	})
}

// Redaction can only elide what the plan still marks as template supplied.
// The rewriter rebuilds some nodes without a template variable name, and then
// neither this explainer nor RedactPlanForLog can tell the value from a
// literal. This pins parity between the two rather than the leak itself, so
// it keeps passing if the rewriter learns to carry provenance. With
// common.enabledOptimizeExpr off the optimizer's rewrites stop and those
// shapes must keep their names; term normalization (a mixed-kind JSON list
// split by kind, a list of arrays lowered to one equality per array) runs
// regardless, so those shapes are held to parity only.
func TestExplainPlan_RedactionParityAfterRewrite(t *testing.T) {
	helper := newTestSchemaHelper(t)
	const secret = "template-secret"
	const number = "424242" // an int64 secret for the Int64Field shapes
	str := func(s string) *schemapb.TemplateValue { return generateTemplateValue(schemapb.DataType_VarChar, s) }
	i64 := func(v int64) *schemapb.TemplateValue { return generateTemplateValue(schemapb.DataType_Int64, v) }
	i64s := func(vs ...int64) *schemapb.TemplateValue {
		return generateTemplateValue(schemapb.DataType_Array, generateTemplateArrayValue(schemapb.DataType_Int64, vs))
	}
	cases := []struct {
		expr          string
		values        map[string]*schemapb.TemplateValue
		secret        string
		namesSurvive  bool // with the optimizer off
		rewrittenByOp string
	}{
		// Optimizer rewrites: names survive when it is off.
		{`not (VarCharField == {s})`, map[string]*schemapb.TemplateValue{"s": str(secret)}, secret, true, "f != v"},
		{`VarCharField in {s}`, map[string]*schemapb.TemplateValue{
			"s": generateTemplateValue(schemapb.DataType_Array, generateTemplateArrayValue(schemapb.DataType_VarChar, []string{secret})),
		}, secret, true, "single-element in becomes =="},
		{`VarCharField == {a} || VarCharField == {b}`, map[string]*schemapb.TemplateValue{"a": str(secret), "b": str(secret + "-2")}, secret, true, "or of equals becomes one term"},
		{`Int64Field > {lo} && Int64Field < {hi}`, map[string]*schemapb.TemplateValue{"lo": i64(424242), "hi": i64(424252)}, number, true, "two ranges become one binary range"},
		// Both lists stay multi-element: a single-element list becomes == first
		// and would take the equals-merge paths above instead.
		{`Int64Field in {a} || Int64Field in {b}`, map[string]*schemapb.TemplateValue{"a": i64s(424242, 424243), "b": i64s(424244, 424245)}, number, true, "in lists are merged"},
		{`Int64Field in {a} && Int64Field in {b}`, map[string]*schemapb.TemplateValue{"a": i64s(424242, 424243, 424244), "b": i64s(424242, 424243, 424245)}, number, true, "in lists are intersected"},
		// Term normalization: runs whatever the option says.
		{`JSONField["a"] in {s}`, map[string]*schemapb.TemplateValue{
			"s": generateTemplateValue(schemapb.DataType_Array, generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{generateJSONData(int64(1)), generateJSONData(secret)})),
		}, secret, false, "mixed-kind JSON list is split by kind"},
		{`ArrayField in {t}`, map[string]*schemapb.TemplateValue{
			"t": generateTemplateValue(schemapb.DataType_Array, generateTemplateArrayValue(schemapb.DataType_Array, []*schemapb.TemplateArrayValue{
				generateTemplateArrayValue(schemapb.DataType_Int64, []int64{424242, 1}),
				generateTemplateArrayValue(schemapb.DataType_Int64, []int64{424243, 2}),
			})),
		}, number, false, "a list of arrays becomes one equality per array"},
	}
	for _, optimize := range []string{"false", "true"} {
		t.Run("optimize="+optimize, func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(optimize)
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
			for _, tc := range cases {
				plan, err := CreateRetrievePlan(helper, tc.expr, tc.values)
				require.NoError(t, err, tc.expr)
				got, err := ExplainPlan(plan, helper, &ExplainOptions{RedactTemplateValues: true})
				require.NoError(t, err)
				js, err := got.JSON()
				require.NoError(t, err)
				leaked := strings.Contains(RedactPlanForLog(plan).String(), tc.secret)
				assert.Equal(t, leaked, strings.Contains(string(js), tc.secret), tc.expr)
				assert.Equal(t, leaked, strings.Contains(got.Text(), tc.secret), tc.expr)
				if optimize == "false" && tc.namesSurvive {
					assert.False(t, leaked, "%s (%s) must keep its template names with the optimizer off", tc.expr, tc.rewrittenByOp)
				}
			}
		})
	}
}

// The plan carries no index metadata, so candidate indexes come from the
// caller and attach to the fields the plan actually reads: the vector field
// and predicate columns, not output fields.
func TestExplainPlan_CandidateIndexes(t *testing.T) {
	helper := newTestSchemaHelper(t)
	vecID := fieldIDByName(t, helper, "FloatVectorField")
	intID := fieldIDByName(t, helper, "Int64Field")
	strID := fieldIDByName(t, helper, "VarCharField")
	plan, err := CreateSearchPlan(helper, "Int64Field > 1", "FloatVectorField",
		&planpb.QueryInfo{Topk: 10, MetricType: "L2", GroupByFieldId: -1}, nil, nil)
	require.NoError(t, err)
	plan.OutputFieldIds = []int64{intID, strID}
	indexes := map[int64][]IndexCandidate{
		vecID: {{Name: "vec_idx", Type: "HNSW", Params: map[string]string{"M": "16"}}},
		intID: {{Name: "int_idx", Type: "INVERTED"}, {Name: "int_bitmap"}},
		strID: {{Name: "str_idx", Type: "INVERTED"}},
	}

	got, err := ExplainPlan(plan, helper, &ExplainOptions{FieldIndexes: indexes})
	require.NoError(t, err)
	assert.Equal(t, indexes[vecID], got.VectorSearch.Field.CandidateIndexes)
	assert.Equal(t, indexes[intID], got.Predicates.Field.CandidateIndexes)
	for _, f := range got.OutputFields {
		assert.Nil(t, f.CandidateIndexes, "output fields are read, not filtered, so no candidates: %s", f.Name)
	}

	js, err := got.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(js), `"candidate_indexes":[{"name":"int_idx","type":"INVERTED"},{"name":"int_bitmap"}]`)
	text := got.Text()
	assert.Contains(t, text, "search: FloatVectorField (FloatVector) metric=L2 topk=10 round_decimal=0 indexes=[vec_idx:HNSW]\n")
	assert.Contains(t, text, "  unary_range GreaterThan Int64Field indexes=[int_idx:INVERTED,int_bitmap] 1\n")

	// The explanation owns its copies: editing it must not reach the caller's
	// map, neither the candidate structs nor the params maps inside them.
	got.Predicates.Field.CandidateIndexes[0].Name = "edited"
	assert.Equal(t, "int_idx", indexes[intID][0].Name)
	got.VectorSearch.Field.CandidateIndexes[0].Params["M"] = "32"
	assert.Equal(t, "16", indexes[vecID][0].Params["M"])

	t.Run("no index metadata", func(t *testing.T) {
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		assert.Nil(t, got.VectorSearch.Field.CandidateIndexes)
		assert.Nil(t, got.Predicates.Field.CandidateIndexes)
		assert.NotContains(t, got.Text(), "indexes=")
	})

	// A typed JSON path index serves one path, so a candidate scoped to a path
	// is attached only to columns reading exactly that path; a flat one
	// (CoversSubpaths) also serves the paths below its own, the way the query
	// node routes it; an unscoped one goes everywhere. An empty, non-nil path
	// is the field's root value.
	t.Run("json path candidates", func(t *testing.T) {
		jsonID := fieldIDByName(t, helper, "JSONField")
		indexes := map[int64][]IndexCandidate{jsonID: {
			{Name: "on_a", Type: "INVERTED", NestedPath: []string{"a"}},
			{Name: "on_a_b", Type: "INVERTED", NestedPath: []string{"a", "b"}},
			{Name: "on_root", Type: "INVERTED", NestedPath: []string{}},
			{Name: "flat_a", Type: "INVERTED", NestedPath: []string{"a"}, CoversSubpaths: true},
			{Name: "flat_root", Type: "INVERTED", NestedPath: []string{}, CoversSubpaths: true},
			{Name: "unscoped"},
		}}
		names := func(expr string) []string {
			got, err := ExplainPlan(mustRetrievePlan(t, helper, expr), helper, &ExplainOptions{FieldIndexes: indexes})
			require.NoError(t, err)
			var out []string
			for _, c := range got.Predicates.Field.CandidateIndexes {
				out = append(out, c.Name)
			}
			return out
		}
		assert.Equal(t, []string{"on_a", "flat_a", "flat_root", "unscoped"}, names(`JSONField["a"] > 1`))
		assert.Equal(t, []string{"on_a_b", "flat_a", "flat_root", "unscoped"}, names(`JSONField["a"]["b"] > 1`))
		assert.Equal(t, []string{"flat_root", "unscoped"}, names(`JSONField["b"] > 1`))
		assert.Equal(t, []string{"on_root", "flat_root", "unscoped"}, names(`JSONField > 1`))
		// A prefix is matched by whole path elements, and the candidate list
		// does not reproduce the query node's finer checks (it skips a flat
		// index for an array element, for instance), so both stay candidates.
		assert.Equal(t, []string{"flat_root", "unscoped"}, names(`JSONField["ab"] > 1`))
		assert.Equal(t, []string{"flat_a", "flat_root", "unscoped"}, names(`JSONField["a"][0] > 1`))

		// The root path stays visible as [] so a root-scoped candidate can be
		// told from an unscoped one, which has no path at all.
		got, err := ExplainPlan(mustRetrievePlan(t, helper, `JSONField["a"] > 1`), helper, &ExplainOptions{FieldIndexes: indexes})
		require.NoError(t, err)
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), `"candidate_indexes":[{"name":"on_a","type":"INVERTED","nested_path":["a"]},{"name":"flat_a","type":"INVERTED","nested_path":["a"],"covers_subpaths":true},{"name":"flat_root","type":"INVERTED","nested_path":[],"covers_subpaths":true},{"name":"unscoped"}]`)
		// The path is copied, not aliased.
		got.Predicates.Field.CandidateIndexes[0].NestedPath[0] = "edited"
		assert.Equal(t, "a", indexes[jsonID][0].NestedPath[0])
	})
}

// Column flags are copied from the plan's ColumnInfo, which is what segcore
// evaluates against, rather than re-derived from the schema.
func TestExplainPlan_ColumnFlags(t *testing.T) {
	helper := newTestSchemaHelper(t)
	intID := fieldIDByName(t, helper, "Int64Field")
	expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
		ColumnInfo: &planpb.ColumnInfo{
			FieldId: intID, DataType: schemapb.DataType_Int64,
			IsPrimaryKey: true, IsAutoID: true, IsPartitionKey: true, IsClusteringKey: true, Nullable: true,
		},
		Op:    planpb.OpType_Equal,
		Value: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 3}},
	}}}
	got, err := ExplainPlan(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: expr}}}, helper, nil)
	require.NoError(t, err)
	field := got.Predicates.Field
	require.NotNil(t, field)
	assert.True(t, field.PrimaryKey)
	assert.True(t, field.AutoID)
	assert.True(t, field.PartitionKey)
	assert.True(t, field.ClusteringKey)
	assert.True(t, field.Nullable)
	assert.False(t, field.ElementLevel)
	js, err := got.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(js), `"primary_key":true,"auto_id":true,"partition_key":true,"clustering_key":true,"nullable":true`)

	// The parser copies the schema's AutoID onto every column it builds.
	schema := newTestSchema(true)
	for _, f := range schema.Fields {
		if f.Name == "Int64Field" {
			f.AutoID = true
		}
	}
	autoHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	auto := explainRetrieve(t, autoHelper, `Int64Field > 1`)
	assert.True(t, auto.Predicates.Field.AutoID)

	// $[sub] access inside element_filter is marked element-level by the parser.
	elem := explainRetrieve(t, helper, `element_filter(struct_array, $[sub_int] > 1)`)
	require.NotNil(t, elem.Predicates.Element)
	require.NotNil(t, elem.Predicates.Element.Field)
	assert.True(t, elem.Predicates.Element.Field.ElementLevel)

	// Flags are omitted from JSON when false, so the common case stays short.
	js, err = explainRetrieve(t, helper, `Int64Field > 1`).JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(js), "primary_key")
	assert.NotContains(t, string(js), "auto_id")
	assert.NotContains(t, string(js), "nullable")
}

func TestExplainPlan_ExtraValues(t *testing.T) {
	schema := newTestSchema(true)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	got := explainRetrieve(t, helper, `text_match_fuzzy(VarCharField, "query", max_edit_distance=2)`)
	require.Equal(t, NodeUnaryRange, got.Predicates.Node)
	assert.Equal(t, "TextMatchFuzzy", got.Predicates.Op)
	assert.Equal(t, "query", got.Predicates.Value)
	assert.Equal(t, []any{int64(2)}, got.Predicates.ExtraValues)
	assert.Equal(t, 0, got.UnknownNodeCount)
	assert.Contains(t, got.Text(), `  unary_range TextMatchFuzzy VarCharField "query" extra=[2]`+"\n")
}

func TestExplainPlan_DoesNotMutatePlan(t *testing.T) {
	helper := newTestSchemaHelper(t)
	templates := map[string]*schemapb.TemplateValue{"x": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 7}}}
	plan, err := CreateSearchPlan(helper, "Int64Field > {x}", "FloatVectorField",
		&planpb.QueryInfo{Topk: 10, MetricType: "L2", GroupByFieldId: -1}, templates, nil)
	require.NoError(t, err)
	before := proto.Clone(plan)

	_, err = ExplainPlan(plan, helper, &ExplainOptions{RedactTemplateValues: true})
	require.NoError(t, err)
	_, err = ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	assert.True(t, proto.Equal(before, plan), "explain must be read-only")
}

// Every member of the planpb.Expr oneof must map to a named node. Each member
// is built empty through protoreflect, so a type added to plan.proto fails
// here until the explainer learns it, instead of silently becoming "unknown".
func TestExplainPlan_CoversEveryExprType(t *testing.T) {
	oneof := (&planpb.Expr{}).ProtoReflect().Descriptor().Oneofs().ByName("expr")
	require.NotNil(t, oneof)
	members := oneof.Fields()
	for i := 0; i < members.Len(); i++ {
		fd := members.Get(i)
		t.Run(string(fd.Name()), func(t *testing.T) {
			expr := &planpb.Expr{}
			msg := expr.ProtoReflect()
			msg.Set(fd, protoreflect.ValueOfMessage(msg.NewField(fd).Message()))
			plan := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: expr}}}

			got, err := ExplainPlan(plan, nil, nil)
			require.NoError(t, err)
			require.NotNil(t, got.Predicates)
			assert.NotEqual(t, NodeUnknown, got.Predicates.Node, "Expr.%s is not explained", fd.Name())
		})
	}
}

// Some children are absent by construction rather than malformed: the parser
// folds `X && random_sample(f)` and `X && element_filter(...)` into one node
// whose predicate is X and leaves it nil for the bare forms, and it emits
// array_length as a BinaryArithExpr with no right operand. None of these may
// be counted as unknown.
func TestExplainPlan_OptionalChildren(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("bare random_sample", func(t *testing.T) {
		got := explainRetrieve(t, helper, `random_sample(0.01)`)
		require.Equal(t, NodeRandomSample, got.Predicates.Node)
		assert.InDelta(t, 0.01, float64(got.Predicates.SampleFactor), 1e-6)
		assert.Nil(t, got.Predicates.Child, "no predicate means every row is a candidate")
		assert.Equal(t, 0, got.UnknownNodeCount)
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), `"sample_factor":0.01`, "float32 values keep their float32 spelling")
	})
	t.Run("random_sample with folded predicate", func(t *testing.T) {
		got := explainRetrieve(t, helper, `Int64Field > 1 && random_sample(0.01)`)
		require.Equal(t, NodeRandomSample, got.Predicates.Node)
		require.NotNil(t, got.Predicates.Child)
		assert.Equal(t, NodeUnaryRange, got.Predicates.Child.Node)
		assert.Equal(t, 0, got.UnknownNodeCount)
	})
	t.Run("bare element_filter", func(t *testing.T) {
		got := explainRetrieve(t, helper, `element_filter(struct_array, $[sub_int] > 1)`)
		require.Equal(t, NodeElementFilter, got.Predicates.Node)
		assert.Equal(t, "struct_array", got.Predicates.StructName)
		require.NotNil(t, got.Predicates.Element)
		assert.Equal(t, NodeUnaryRange, got.Predicates.Element.Node)
		assert.Nil(t, got.Predicates.Child)
		assert.Equal(t, 0, got.UnknownNodeCount)
	})
	t.Run("element_filter with folded doc predicate", func(t *testing.T) {
		got := explainRetrieve(t, helper, `Int64Field > 1 && element_filter(struct_array, $[sub_int] > 1)`)
		require.Equal(t, NodeElementFilter, got.Predicates.Node)
		require.NotNil(t, got.Predicates.Child)
		assert.Equal(t, NodeUnaryRange, got.Predicates.Child.Node)
		assert.Equal(t, 0, got.UnknownNodeCount)
	})

	arrayID := fieldIDByName(t, helper, "ArrayField")
	column := &planpb.Expr{Expr: &planpb.Expr_ColumnExpr{ColumnExpr: &planpb.ColumnExpr{Info: &planpb.ColumnInfo{FieldId: arrayID}}}}
	explainArith := func(t *testing.T, op planpb.ArithOpType) *PlanExplanation {
		arith := &planpb.Expr{Expr: &planpb.Expr_BinaryArithExpr{BinaryArithExpr: &planpb.BinaryArithExpr{Op: op, Left: column}}}
		got, err := ExplainPlan(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: arith}}}, helper, nil)
		require.NoError(t, err)
		require.Equal(t, NodeArith, got.Predicates.Node)
		require.NotNil(t, got.Predicates.Left)
		assert.Equal(t, NodeColumn, got.Predicates.Left.Node)
		return got
	}
	t.Run("array_length has no right operand", func(t *testing.T) {
		// The parser folds `array_length(f) == 3` into arith_compare, so the
		// standalone arith node is built directly.
		got := explainArith(t, planpb.ArithOpType_ArrayLength)
		assert.Equal(t, "ArrayLength", got.Predicates.Op)
		assert.Nil(t, got.Predicates.Right)
		assert.Equal(t, 0, got.UnknownNodeCount)
	})
	t.Run("binary arith op without right operand is malformed", func(t *testing.T) {
		got := explainArith(t, planpb.ArithOpType_Add)
		require.NotNil(t, got.Predicates.Right)
		assert.Equal(t, NodeUnknown, got.Predicates.Right.Node)
		assert.Equal(t, 1, got.UnknownNodeCount)
	})
}

func TestExplainPlan_SummarizesRoaringBlob(t *testing.T) {
	helper := newTestSchemaHelper(t)
	tv, _ := roaringBytesTemplate(t, 1, 2, 3)
	plan, err := CreateRetrievePlan(helper, "membership_match(Int64Field, {ids}, type=roaring)",
		map[string]*schemapb.TemplateValue{"ids": tv})
	require.NoError(t, err)
	blob := plan.GetQuery().GetPredicates().GetRoaringFilterExpr().GetBitmapBlob()
	require.NotEmpty(t, blob)

	got, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	require.NotNil(t, got.Predicates)
	assert.Equal(t, NodeRoaringFilter, got.Predicates.Node)
	assert.Equal(t, "Int64Field", got.Predicates.Field.Name)
	require.NotNil(t, got.Predicates.Blob)
	assert.Equal(t, len(blob), got.Predicates.Blob.Bytes)
	assert.Equal(t, 0, got.UnknownNodeCount)

	js, err := got.JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(js), base64.StdEncoding.EncodeToString(blob))
}

// ValueExpr and ColumnExpr occur only as children (for example the parameters
// of a not-yet-materialized membership_match call), never as a parser root,
// so the call is built directly, together with the value kinds the parser
// cannot produce from an expression string.
func TestExplainPlan_IntermediateNodesAndValueKinds(t *testing.T) {
	helper := newTestSchemaHelper(t)
	intID := fieldIDByName(t, helper, "Int64Field")
	value := func(v *planpb.GenericValue) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_ValueExpr{ValueExpr: &planpb.ValueExpr{Value: v}}}
	}
	call := &planpb.Expr{Expr: &planpb.Expr_CallExpr{CallExpr: &planpb.CallExpr{
		FunctionName: MembershipMatchFunctionName,
		FunctionParameters: []*planpb.Expr{
			{Expr: &planpb.Expr_ColumnExpr{ColumnExpr: &planpb.ColumnExpr{Info: &planpb.ColumnInfo{FieldId: intID, DataType: schemapb.DataType_Int64}}}},
			{Expr: &planpb.Expr_ValueExpr{ValueExpr: &planpb.ValueExpr{TemplateVariableName: "ids"}}},
			value(&planpb.GenericValue{Val: &planpb.GenericValue_BytesVal{BytesVal: []byte{1, 2, 3}}}),
			value(&planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 1.5}}),
			value(&planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: true}}),
			value(&planpb.GenericValue{Val: &planpb.GenericValue_ArrayVal{ArrayVal: &planpb.Array{Array: []*planpb.GenericValue{
				{Val: &planpb.GenericValue_StringVal{StringVal: "a"}},
				{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
			}}}}),
			value(&planpb.GenericValue{}),
		},
	}}}

	got, err := ExplainPlan(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: call}}}, helper, nil)
	require.NoError(t, err)
	root := got.Predicates
	require.NotNil(t, root)
	assert.Equal(t, NodeCall, root.Node)
	assert.Equal(t, MembershipMatchFunctionName, root.Function)
	require.Len(t, root.Children, 7)
	assert.Equal(t, NodeColumn, root.Children[0].Node)
	assert.Equal(t, "Int64Field", root.Children[0].Field.Name)
	assert.Equal(t, NodeValue, root.Children[1].Node)
	assert.Nil(t, root.Children[1].Value, "unfilled template has no value yet")
	assert.Equal(t, "ids", root.Children[1].Template)
	assert.Equal(t, &BlobSummary{Bytes: 3}, root.Children[2].Value)
	assert.Equal(t, Float64(1.5), root.Children[3].Value)
	assert.Equal(t, true, root.Children[4].Value)
	assert.Equal(t, []any{"a", int64(2)}, root.Children[5].Value)
	assert.Equal(t, NodeValue, root.Children[6].Node)
	assert.Nil(t, root.Children[6].Value, "a GenericValue with no val explains as null")
	assert.Equal(t, 0, got.UnknownNodeCount)

	text := got.Text()
	assert.Contains(t, text, "  call membership_match\n")
	assert.Contains(t, text, "    column Int64Field\n")
	assert.Contains(t, text, "    value template=ids\n")
	assert.Contains(t, text, "    value <3 bytes>\n")
}

func TestExplainPlan_SearchOptions(t *testing.T) {
	helper := newTestSchemaHelper(t)
	intID := fieldIDByName(t, helper, "Int64Field")
	lastBound := float32(0.5)
	got := explainSearch(t, helper, "", &planpb.QueryInfo{
		Topk:                 10,
		MetricType:           "IP",
		SearchParams:         `{not json`,
		GroupByFieldIds:      []int64{intID},
		GroupSize:            3,
		StrictGroupSize:      true,
		SearchIteratorV2Info: &planpb.SearchIteratorV2Info{Token: "session-token", BatchSize: 100, LastBound: &lastBound},
		JsonPath:             "/a/b",
		JsonType:             schemapb.DataType_Float,
		StrictCast:           true,
		Hints:                "iterative_filter",
	})
	vs := got.VectorSearch
	require.NotNil(t, vs)
	assert.Equal(t, `{not json`, vs.SearchParams, "undecodable params are kept verbatim")
	require.NotNil(t, vs.GroupBy)
	assert.Equal(t, []string{"Int64Field"}, fieldLabels(vs.GroupBy.Fields))
	assert.Equal(t, int64(3), vs.GroupBy.GroupSize)
	assert.True(t, vs.GroupBy.StrictGroupSize)
	require.NotNil(t, vs.Iterator)
	assert.Equal(t, uint32(100), vs.Iterator.BatchSize)
	require.NotNil(t, vs.Iterator.LastBound)
	assert.InDelta(t, 0.5, float64(*vs.Iterator.LastBound), 1e-6)
	require.NotNil(t, vs.JSONPath)
	assert.Equal(t, JSONPathExplanation{Path: "/a/b", Type: "Float", StrictCast: true}, *vs.JSONPath)
	assert.Equal(t, "iterative_filter", vs.Hints)

	js, err := got.JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(js), "session-token", "the iterator token is not part of the explanation")
	assert.Contains(t, string(js), `"last_bound":0.5`)
	text := got.Text()
	assert.Contains(t, text, "json_path: \"/a/b\" type=Float strict_cast=true\n")
	assert.Contains(t, text, "group_by: Int64Field group_size=3 strict=true\n")
	assert.Contains(t, text, "iterator: batch_size=100 last_bound=0.5\n")
	assert.Contains(t, text, "hints: iterative_filter\n")

	t.Run("legacy single group_by field", func(t *testing.T) {
		got := explainSearch(t, helper, "", &planpb.QueryInfo{Topk: 10, GroupByFieldId: intID})
		require.NotNil(t, got.VectorSearch.GroupBy)
		assert.Equal(t, []string{"Int64Field"}, fieldLabels(got.VectorSearch.GroupBy.Fields))
	})
	t.Run("iterator without bound", func(t *testing.T) {
		got := explainSearch(t, helper, "", &planpb.QueryInfo{
			Topk: 10, GroupByFieldId: -1,
			SearchIteratorV2Info: &planpb.SearchIteratorV2Info{BatchSize: 7},
		})
		require.NotNil(t, got.VectorSearch.Iterator)
		assert.Nil(t, got.VectorSearch.Iterator.LastBound)
		assert.Contains(t, got.Text(), "iterator: batch_size=7\n")
		assert.NotContains(t, got.Text(), "json_path:")
		assert.NotContains(t, got.Text(), "hints:")
	})
}

// search_params is caller-written JSON. Decoding it through float64 would
// round integers above 2^53, so numbers are carried as written.
func TestExplainPlan_SearchParamsKeepNumbersVerbatim(t *testing.T) {
	helper := newTestSchemaHelper(t)
	got := explainSearch(t, helper, "", &planpb.QueryInfo{
		Topk: 10, MetricType: "L2", GroupByFieldId: -1,
		SearchParams: `{"nprobe": 8, "ef": 9007199254740993, "radius": 0.10, "nested": {"ids": [9007199254740993, 1.50], "level": 3}}`,
	})
	js, err := got.JSON()
	require.NoError(t, err)
	const want = `{"ef":9007199254740993,"nested":{"ids":[9007199254740993,1.50],"level":3},"nprobe":8,"radius":0.10}`
	assert.Contains(t, string(js), `"search_params":`+want)
	assert.Contains(t, got.Text(), "search_params: "+want+"\n")

	t.Run("trailing content is not JSON", func(t *testing.T) {
		// A stream decoder stops after the first value; the whole string
		// must be one document or it is kept verbatim like any other non-JSON.
		got := explainSearch(t, helper, "", &planpb.QueryInfo{
			Topk: 10, MetricType: "L2", GroupByFieldId: -1,
			SearchParams: `{"nprobe": 8} trailing`,
		})
		assert.Equal(t, `{"nprobe": 8} trailing`, got.VectorSearch.SearchParams)
	})
}

func TestExplainPlan_QueryOptions(t *testing.T) {
	helper := newTestSchemaHelper(t)
	intID := fieldIDByName(t, helper, "Int64Field")
	strID := fieldIDByName(t, helper, "VarCharField")
	plan, err := CreateRetrievePlan(helper, "Int64Field > 1", nil)
	require.NoError(t, err)
	q := plan.GetQuery()
	q.IsCount = true
	q.Limit = 10
	q.GroupByFieldIds = []int64{intID}
	q.Aggregates = []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: intID}}
	q.OrderByFields = []*planpb.OrderByField{{FieldId: strID, Ascending: true, NullsFirst: true}}
	q.QueryIteratorCursor = &planpb.QueryIteratorCursor{LastIntPk: proto.Int64(42), LastElementOffset: 3}
	plan.Namespace = proto.String("tenant-a")
	plan.ScoreOption = &planpb.ScoreOption{BoostMode: planpb.BoostMode_BoostModeSum, FunctionMode: planpb.FunctionMode_FunctionModeSum}
	plan.OutputFieldIds = []int64{intID, 9999}
	plan.DynamicFields = []string{"a", "b"}

	got, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	assert.Equal(t, []FieldRef{{ID: intID, Name: "Int64Field", Type: "Int64"}, {ID: 9999}}, got.OutputFields,
		"an id the schema does not know is still reported, by id only")
	assert.Equal(t, []string{"a", "b"}, got.DynamicFields)
	require.NotNil(t, got.Query)
	assert.True(t, got.Query.IsCount)
	assert.Equal(t, int64(10), got.Query.Limit)
	assert.Equal(t, []string{"Int64Field"}, fieldLabels(got.Query.GroupBy))
	require.Len(t, got.Query.Aggregates, 1)
	assert.Equal(t, "count", got.Query.Aggregates[0].Op)
	assert.Equal(t, "Int64Field", got.Query.Aggregates[0].Field.Name)
	require.Len(t, got.Query.OrderBy, 1)
	assert.Equal(t, "VarCharField", got.Query.OrderBy[0].Field.Name)
	assert.True(t, got.Query.OrderBy[0].Ascending)
	assert.True(t, got.Query.OrderBy[0].NullsFirst)
	require.NotNil(t, got.Query.Cursor)
	require.NotNil(t, got.Query.Cursor.LastIntPK)
	assert.Equal(t, int64(42), *got.Query.Cursor.LastIntPK)
	assert.Nil(t, got.Query.Cursor.LastStrPK)
	assert.Equal(t, int64(3), got.Query.Cursor.LastElementOffset)
	require.NotNil(t, got.Namespace)
	assert.Equal(t, "tenant-a", *got.Namespace)
	require.NotNil(t, got.ScoreOption)
	assert.Equal(t, "BoostModeSum", got.ScoreOption.BoostMode)
	assert.Equal(t, "FunctionModeSum", got.ScoreOption.FunctionMode)

	text := got.Text()
	assert.Contains(t, text, "plan: query\nnamespace: \"tenant-a\"\n")
	assert.Contains(t, text, "query: is_count=true limit=10\n")
	assert.Contains(t, text, "group_by: Int64Field\n")
	assert.Contains(t, text, "aggregate: count(Int64Field)\n")
	assert.Contains(t, text, "order_by: VarCharField ascending=true nulls_first=true\n")
	assert.Contains(t, text, "cursor: last_pk=42 last_element_offset=3\n")
	assert.Contains(t, text, "output_fields: Int64Field, #9999\n")
	assert.Contains(t, text, `dynamic_fields: "a", "b"`+"\n")
	assert.Contains(t, text, "score_option: boost_mode=BoostModeSum function_mode=FunctionModeSum\n")

	t.Run("string primary key cursor", func(t *testing.T) {
		q.QueryIteratorCursor = &planpb.QueryIteratorCursor{LastStrPk: proto.String("k9"), LastElementOffset: 1}
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		require.NotNil(t, got.Query.Cursor)
		assert.Nil(t, got.Query.Cursor.LastIntPK)
		require.NotNil(t, got.Query.Cursor.LastStrPK)
		assert.Equal(t, "k9", *got.Query.Cursor.LastStrPK)
		assert.Contains(t, got.Text(), "cursor: last_pk=\"k9\" last_element_offset=1\n")
	})
	t.Run("cursor without primary key", func(t *testing.T) {
		q.QueryIteratorCursor = &planpb.QueryIteratorCursor{LastElementOffset: 5}
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		assert.Contains(t, got.Text(), "cursor: last_element_offset=5\n")
	})
}

// PlanNode.namespace is proto3 optional and segcore keys on its presence
// (has_namespace_), adding a `$namespace == value` predicate even for "".
// A present empty namespace is therefore a different plan from an absent
// one and must not explain the same way.
func TestExplainPlan_NamespacePresence(t *testing.T) {
	helper := newTestSchemaHelper(t)
	plan, err := CreateRetrievePlan(helper, "Int64Field > 1", nil)
	require.NoError(t, err)

	t.Run("absent", func(t *testing.T) {
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		assert.Nil(t, got.Namespace)
		js, err := got.JSON()
		require.NoError(t, err)
		assert.NotContains(t, string(js), `"namespace"`)
		assert.NotContains(t, got.Text(), "namespace:")
	})
	t.Run("present but empty", func(t *testing.T) {
		plan.Namespace = proto.String("")
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		require.NotNil(t, got.Namespace)
		assert.Equal(t, "", *got.Namespace)
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), `"namespace":""`)
		assert.Contains(t, got.Text(), "plan: query\nnamespace: \"\"\n")
	})
	t.Run("present", func(t *testing.T) {
		plan.Namespace = proto.String("tenant-a")
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		require.NotNil(t, got.Namespace)
		assert.Equal(t, "tenant-a", *got.Namespace)
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), `"namespace":"tenant-a"`)
		assert.Contains(t, got.Text(), "plan: query\nnamespace: \"tenant-a\"\n")
	})
}

// Grouping by a bare JSON field leaves QueryInfo.json_path empty while
// json_type and strict_cast still apply (segcore reads the whole value and
// casts it), so an empty path must not hide those options.
func TestExplainPlan_RootJSONGroupingOptions(t *testing.T) {
	helper := newTestSchemaHelper(t)
	jsonID := fieldIDByName(t, helper, "JSONField")

	got := explainSearch(t, helper, "", &planpb.QueryInfo{
		Topk: 10, MetricType: "L2", GroupByFieldId: jsonID, GroupSize: 1,
		JsonType: schemapb.DataType_Int64, StrictCast: true,
	})
	require.NotNil(t, got.VectorSearch.JSONPath)
	assert.Equal(t, JSONPathExplanation{Path: "", Type: "Int64", StrictCast: true}, *got.VectorSearch.JSONPath)
	js, err := got.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(js), `"json_path":{"path":"","type":"Int64","strict_cast":true}`)
	assert.Contains(t, got.Text(), `json_path: "" type=Int64 strict_cast=true`+"\n")

	t.Run("nothing to show without a path, type or strict cast", func(t *testing.T) {
		got := explainSearch(t, helper, "", &planpb.QueryInfo{Topk: 10, MetricType: "L2", GroupByFieldId: jsonID, GroupSize: 1})
		assert.Nil(t, got.VectorSearch.JSONPath)
		assert.NotContains(t, got.Text(), "json_path:")
	})
}

// Nested JSON keys are quoted in the text view so one key containing "]["
// does not read like two keys.
func TestExplainPlan_TextQuotesNestedPath(t *testing.T) {
	helper := newTestSchemaHelper(t)
	a := explainRetrieve(t, helper, `JSONField["a\u005d\u005bb"] > 1`)
	b := explainRetrieve(t, helper, `JSONField["a"]["b"] > 1`)
	require.Equal(t, []string{"a][b"}, a.Predicates.Field.NestedPath)
	require.Equal(t, []string{"a", "b"}, b.Predicates.Field.NestedPath)
	assert.Contains(t, a.Text(), `  unary_range GreaterThan JSONField["a][b"] 1`+"\n")
	assert.Contains(t, b.Text(), `  unary_range GreaterThan JSONField["a"]["b"] 1`+"\n")
	assert.NotEqual(t, a.Text(), b.Text())
}

// An ARRAY field can only be subscripted by an integer (the parser rejects
// ArrayField["0"]), so the text view keeps that subscript bare. A JSON path
// component is stored as a string whether it was written [0] or ["0"], so the
// plan cannot tell the two apart and the text view quotes it either way.
func TestExplainPlan_TextArraySubscriptsBare(t *testing.T) {
	helper := newTestSchemaHelper(t)

	arr := explainRetrieve(t, helper, `ArrayField[0] > 1`)
	require.Equal(t, "Array", arr.Predicates.Field.Type)
	require.Equal(t, []string{"0"}, arr.Predicates.Field.NestedPath, "the plan stores the index as a string")
	assert.Contains(t, arr.Text(), "  unary_range GreaterThan ArrayField[0] 1\n")

	js := explainRetrieve(t, helper, `JSONField["a"][0] > 1`)
	require.Equal(t, []string{"a", "0"}, js.Predicates.Field.NestedPath)
	assert.Contains(t, js.Text(), `  unary_range GreaterThan JSONField["a"]["0"] 1`+"\n")

	t.Run("non-integer component on an array stays quoted", func(t *testing.T) {
		// Not producible by the parser; the label must still not reintroduce
		// the "][" ambiguity for a hand-built plan.
		assert.Equal(t, `ArrayField["a][b"]`, fieldLabel(FieldRef{Name: "ArrayField", Type: "Array", NestedPath: []string{"a][b"}}))
	})
}

// On a JSON field the numeric kind of a constant decides whether segcore runs
// integer or floating-point arithmetic (BinaryArithOpEvalRangeExpr dispatches
// on the GenericValue kind), so `f / 2 == 1` and `f / 2.0 == 1` are different
// plans: for {"n": 3} the first matches (3 / 2 == 1) and the second does not
// (1.5). Floats therefore always carry a decimal point or an exponent, in JSON
// and in text, so 2.0 never reads as 2.
func TestExplainPlan_NumericKindsSurvive(t *testing.T) {
	helper := newTestSchemaHelper(t)
	ints := explainRetrieve(t, helper, `JSONField["n"] / 2 == 1`)
	floats := explainRetrieve(t, helper, `JSONField["n"] / 2.0 == 1`)
	require.IsType(t, int64(0), ints.Predicates.Operand)
	require.IsType(t, int64(0), ints.Predicates.Value)
	require.IsType(t, Float64(0), floats.Predicates.Operand)
	require.IsType(t, Float64(0), floats.Predicates.Value, "the parser casts the comparison constant to the arithmetic type")

	a, err := ints.JSON()
	require.NoError(t, err)
	b, err := floats.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(a), `"value":1,"arith_op":"Div","operand":2}`)
	assert.Contains(t, string(b), `"value":1.0,"arith_op":"Div","operand":2.0}`)
	assert.NotEqual(t, string(a), string(b))
	assert.Contains(t, ints.Text(), `  arith_compare Equal JSONField["n"] arith=Div operand=2 1`+"\n")
	assert.Contains(t, floats.Text(), `  arith_compare Equal JSONField["n"] arith=Div operand=2.0 1.0`+"\n")

	t.Run("exponent forms are left alone", func(t *testing.T) {
		got := explainRetrieve(t, helper, `JSONField["n"] > 1e21`)
		require.IsType(t, Float64(0), got.Predicates.Value)
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), `"value":1e+21}`)
		assert.Contains(t, got.Text(), `  unary_range GreaterThan JSONField["n"] 1e+21`+"\n")
	})
	t.Run("spelling is the same in JSON and text", func(t *testing.T) {
		cases := []struct {
			in   any
			want string
		}{
			{Float64(2), "2.0"},
			{Float32(2), "2.0"},
			{Float64(0), "0.0"},
			{Float64(math.Copysign(0, -1)), "-0.0"},
			{Float32(0.1), "0.1"}, // float32 values keep their float32 spelling
			{Float64(0.1), "0.1"},
			{Float64(1234567), "1234567.0"},
			{Float64(0.000001), "0.000001"},
			{Float64(1e-7), "1e-7"},
			{Float64(1e21), "1e+21"},
			{Float64(-2.5e-9), "-2.5e-9"},
			{Float32(1e21), "1e+21"},
		}
		for _, c := range cases {
			js, err := json.Marshal(c.in)
			require.NoError(t, err)
			assert.Equal(t, c.want, string(js), "%v", c.in)
			// Only Float64 reaches the text view as a predicate value.
			if f, ok := c.in.(Float64); ok {
				assert.Equal(t, c.want, textValue(f), "%v", c.in)
			}
		}
		assert.Equal(t, `[1, 2.0, [3.5, "4"]]`, textValue([]any{int64(1), Float64(2), []any{Float64(3.5), "4"}}))
	})
}

// A binary range carries one template name per bound (lower.template and
// upper.template in JSON); the text view shows both, and a bound whose value
// has not been filled yet prints as null rather than Go's <nil>.
func TestExplainPlan_BinaryRangeTemplateText(t *testing.T) {
	helper := newTestSchemaHelper(t)
	templates := map[string]*schemapb.TemplateValue{
		"lo": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 1}},
		"hi": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 10}},
	}
	plan, err := CreateRetrievePlan(helper, "{lo} < Int64Field < {hi}", templates)
	require.NoError(t, err)

	got, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	require.Equal(t, NodeBinaryRange, got.Predicates.Node)
	assert.Equal(t, "lo", got.Predicates.Lower.Template)
	assert.Equal(t, "hi", got.Predicates.Upper.Template)
	assert.Contains(t, got.Text(),
		"  binary_range Int64Field lower=1 inclusive=false upper=10 inclusive=false lower_template=lo upper_template=hi\n")

	redacted, err := ExplainPlan(plan, helper, &ExplainOptions{RedactTemplateValues: true})
	require.NoError(t, err)
	assert.Contains(t, redacted.Text(),
		`  binary_range Int64Field lower="<template lo elided>" inclusive=false upper="<template hi elided>" inclusive=false lower_template=lo upper_template=hi`+"\n")

	t.Run("one templated bound", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper, "{lo} < Int64Field < 10", templates)
		require.NoError(t, err)
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		assert.Equal(t, "lo", got.Predicates.Lower.Template)
		assert.Empty(t, got.Predicates.Upper.Template)
		line := describeNode(got.Predicates)
		assert.True(t, strings.HasSuffix(line, " upper=10 inclusive=false lower_template=lo"), line)
		assert.NotContains(t, line, "upper_template")
	})
	t.Run("unfilled template bounds print as null", func(t *testing.T) {
		expr, err := ParseExprTemplate(helper, "{lo} < Int64Field < {hi}", nil)
		require.NoError(t, err)
		got, err := ExplainPlan(&planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: expr}}, helper, nil)
		require.NoError(t, err)
		assert.Nil(t, got.Predicates.Lower.Value)
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), `"lower":{"value":null,"inclusive":false,"template":"lo"}`)
		assert.Contains(t, got.Text(),
			"  binary_range Int64Field lower=null inclusive=false upper=null inclusive=false lower_template=lo upper_template=hi\n")
		assert.NotContains(t, got.Text(), "<nil>")
	})
}

// Dynamic field names are JSON keys and may contain the list separator.
func TestExplainPlan_TextQuotesDynamicFields(t *testing.T) {
	helper := newTestSchemaHelper(t)
	plan, err := CreateRetrievePlan(helper, "Int64Field > 1", nil)
	require.NoError(t, err)

	plan.DynamicFields = []string{"a, b"}
	one, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	plan.DynamicFields = []string{"a", "b"}
	two, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)

	assert.Contains(t, one.Text(), `dynamic_fields: "a, b"`+"\n")
	assert.Contains(t, two.Text(), `dynamic_fields: "a", "b"`+"\n")
	assert.NotEqual(t, one.Text(), two.Text())
}

// The bare-predicate PlanNode form is deprecated but still accepted by
// segcore, so it explains as a query without options.
func TestExplainPlan_DeprecatedPredicatesNode(t *testing.T) {
	helper := newTestSchemaHelper(t)
	expr, err := ParseExpr(helper, "Int64Field > 1", nil)
	require.NoError(t, err)

	got, err := ExplainPlan(&planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: expr}}, helper, nil)
	require.NoError(t, err)
	assert.Equal(t, PlanTypeQuery, got.PlanType)
	require.NotNil(t, got.Query)
	assert.Equal(t, QueryExplanation{}, *got.Query)
	require.NotNil(t, got.Predicates)
	assert.Equal(t, NodeUnaryRange, got.Predicates.Node)
	assert.Nil(t, got.Options)
	assert.Equal(t, 0, got.UnknownNodeCount)
}

func TestExplainPlan_TextNodeDetails(t *testing.T) {
	helper := newTestSchemaHelper(t)
	cases := []struct{ expr, want string }{
		{`Int64Field in [1, 2]`, "term Int64Field [1, 2]"},
		{`1 < Int64Field < 10`, "binary_range Int64Field lower=1 inclusive=false upper=10 inclusive=false"},
		{`Int64Field + 1 == 5`, "arith_compare Equal Int64Field arith=Add operand=1 5"},
		{`Int8Field < Int16Field`, "compare LessThan Int8Field Int16Field"},
		{`exists JSONField["key"]`, `exists JSONField["key"]`},
		{`Int64Field is null`, "null IsNull Int64Field"},
		{`match_least(struct_array, $[sub_int] > 1, threshold=2)`, "match MatchLeast struct=struct_array count=2"},
		{`match_any(struct_array, $[sub_int] > 1)`, "match MatchAny struct=struct_array"},
		{`random_sample(0.01)`, "random_sample sample_factor=0.01"},
		{`st_equals(GeometryField, "POINT(0 0)")`, "gis Equals GeometryField wkt=POINT(0 0)"},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			got := explainRetrieve(t, helper, tc.expr)
			assert.Contains(t, got.Text(), "  "+tc.want+"\n", got.Text())
		})
	}

	// Strings are quoted so element boundaries survive: ["a b", "c"] and
	// ["a", "b c"] are different sets and must not read the same.
	t.Run("strings are quoted", func(t *testing.T) {
		a := explainRetrieve(t, helper, `VarCharField in ["a b", "c"]`)
		b := explainRetrieve(t, helper, `VarCharField in ["a", "b c"]`)
		assert.Contains(t, a.Text(), `  term VarCharField ["a b", "c"]`+"\n")
		assert.Contains(t, b.Text(), `  term VarCharField ["a", "b c"]`+"\n")
		assert.NotEqual(t, a.Text(), b.Text())

		got := explainRetrieve(t, helper, `"a" < VarCharField <= "b c"`)
		assert.Contains(t, got.Text(), `  binary_range VarCharField lower="a" inclusive=false upper="b c" inclusive=true`+"\n")
	})

	t.Run("dwithin distance", func(t *testing.T) {
		got := explainRetrieve(t, helper, `st_dwithin(GeometryField, "POINT(0 0)", 2.5)`)
		line := describeNode(got.Predicates)
		assert.True(t, strings.HasPrefix(line, "gis DWithin GeometryField wkt="), line)
		assert.True(t, strings.HasSuffix(line, " distance=2.5"), line)
	})
	t.Run("interval as ISO 8601", func(t *testing.T) {
		got := explainRetrieve(t, helper, `TimestamptzField + INTERVAL 'P1DT12H' > ISO '2025-01-01T00:00:00Z'`)
		line := describeNode(got.Predicates)
		assert.True(t, strings.HasPrefix(line, "timestamptz_arith_compare GreaterThan TimestamptzField arith=Add interval=P1DT12H "), line)
	})
	t.Run("reversed membership", func(t *testing.T) {
		// The Go parser never emits is_in_field, so the node is built directly.
		arrayID := fieldIDByName(t, helper, "ArrayField")
		term := &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
			ColumnInfo: &planpb.ColumnInfo{FieldId: arrayID, DataType: schemapb.DataType_Array},
			Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}},
			IsInField:  true,
		}}}
		got, err := ExplainPlan(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: term}}}, helper, nil)
		require.NoError(t, err)
		assert.True(t, got.Predicates.InField)
		assert.Contains(t, got.Text(), "  term ArrayField in_field [1]\n")
	})

	t.Run("template marker", func(t *testing.T) {
		templates := map[string]*schemapb.TemplateValue{"x": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 7}}}
		plan, err := CreateRetrievePlan(helper, "Int64Field > {x}", templates)
		require.NoError(t, err)
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		assert.Contains(t, got.Text(), "  unary_range GreaterThan Int64Field 7 template=x\n")
	})
	t.Run("unknown marker", func(t *testing.T) {
		plan := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
			Predicates: &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{Op: planpb.UnaryExpr_Not}}},
		}}}
		got, err := ExplainPlan(plan, helper, nil)
		require.NoError(t, err)
		assert.Contains(t, got.Text(), "    unknown missing child expression\n")
		assert.Contains(t, got.Text(), "unknown_nodes: 1\n")
	})
}

// match_most and match_exact accept threshold=0 and st_dwithin accepts a zero
// radius, so a zero must reach the JSON instead of being dropped as empty,
// while node kinds that take no such value must not grow a spurious zero.
func TestExplainPlan_ZeroThresholdsAreKept(t *testing.T) {
	helper := newTestSchemaHelper(t)

	got := explainRetrieve(t, helper, `match_exact(struct_array, $[sub_int] > 1, threshold=0)`)
	require.Equal(t, NodeMatch, got.Predicates.Node)
	require.NotNil(t, got.Predicates.Count)
	assert.Equal(t, int64(0), *got.Predicates.Count)
	js, err := got.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(js), `"struct_name":"struct_array","count":0`)
	assert.Contains(t, got.Text(), "  match MatchExact struct=struct_array count=0\n")

	got = explainRetrieve(t, helper, `match_any(struct_array, $[sub_int] > 1)`)
	assert.Nil(t, got.Predicates.Count, "match_any takes no threshold")
	js, err = got.JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(js), `"count"`)

	got = explainRetrieve(t, helper, `st_dwithin(GeometryField, "POINT(0 0)", 0)`)
	require.Equal(t, NodeGIS, got.Predicates.Node)
	assert.Equal(t, "DWithin", got.Predicates.Op)
	require.NotNil(t, got.Predicates.Distance)
	assert.Equal(t, Float64(0), *got.Predicates.Distance)
	js, err = got.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(js), `"distance":0.0`)
	assert.True(t, strings.HasSuffix(describeNode(got.Predicates), " distance=0.0"))

	got = explainRetrieve(t, helper, `st_equals(GeometryField, "POINT(0 0)")`)
	assert.Nil(t, got.Predicates.Distance, "only DWithin takes a radius")
	js, err = got.JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(js), `"distance"`)
}

// The parser lets NaN and ±Inf through in several places: a scorer weight of
// "NaN" parses, constant folding turns `(-1.0) ** 0.5` into NaN and
// `1e308 * 10` into +Inf, and the random_sample and st_dwithin range checks
// are comparisons that NaN never fails. JSON cannot carry any of them, so the
// explanation emits a {"non_finite": ...} object rather than failing to
// marshal. Not a bare string: a JSON field compared with the string "NaN" is a
// different plan from one compared with the number NaN.
func TestExplainPlan_NonFiniteFloatsStillMarshal(t *testing.T) {
	helper := newTestSchemaHelper(t)
	fs := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{Params: []*commonpb.KeyValuePair{
		{Key: rerank.WeightKey, Value: "NaN"},
		{Key: "reranker", Value: rerank.BoostName},
	}}}}
	plan, err := CreateSearchPlan(helper, `DoubleField > (-1.0) ** 0.5 && random_sample((-1.0) ** 0.5)`, "FloatVectorField",
		&planpb.QueryInfo{Topk: 10, MetricType: "L2", GroupByFieldId: -1}, nil, fs)
	require.NoError(t, err)

	got, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	require.Len(t, got.Scorers, 1)
	assert.True(t, math.IsNaN(float64(got.Scorers[0].Weight)))
	require.Equal(t, NodeRandomSample, got.Predicates.Node)
	assert.True(t, math.IsNaN(float64(got.Predicates.SampleFactor)))
	require.NotNil(t, got.Predicates.Child)
	assert.Equal(t, NodeUnaryRange, got.Predicates.Child.Node)
	value, ok := got.Predicates.Child.Value.(Float64)
	require.True(t, ok, "%T", got.Predicates.Child.Value)
	assert.True(t, math.IsNaN(float64(value)))

	js, err := got.JSON()
	require.NoError(t, err, "one odd value must not fail the whole explanation")
	assert.Contains(t, string(js), `"weight":{"non_finite":"NaN"}`)
	assert.Contains(t, string(js), `"sample_factor":{"non_finite":"NaN"}`)
	assert.Contains(t, string(js), `"value":{"non_finite":"NaN"}`)
	assert.Contains(t, got.Text(), "scorer[0]: FunctionTypeWeight weight=NaN\n")

	t.Run("numeric NaN is not the string NaN", func(t *testing.T) {
		numeric := explainRetrieve(t, helper, `JSONField["n"] == (-1.0) ** 0.5`)
		literal := explainRetrieve(t, helper, `JSONField["n"] == "NaN"`)
		_, isFloat := numeric.Predicates.Value.(Float64)
		require.True(t, isFloat, "%T", numeric.Predicates.Value)
		require.Equal(t, "NaN", literal.Predicates.Value)
		a, err := numeric.JSON()
		require.NoError(t, err)
		b, err := literal.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(a), `"value":{"non_finite":"NaN"}`)
		assert.Contains(t, string(b), `"value":"NaN"`)
		assert.NotEqual(t, string(a), string(b))
		assert.Contains(t, numeric.Text(), `  unary_range Equal JSONField["n"] NaN`+"\n")
		assert.Contains(t, literal.Text(), `  unary_range Equal JSONField["n"] "NaN"`+"\n")
	})

	t.Run("infinite iterator bound", func(t *testing.T) {
		// Scorers and iterators are mutually exclusive in a search plan.
		lastBound := float32(math.Inf(1))
		got := explainSearch(t, helper, "", &planpb.QueryInfo{
			Topk: 10, MetricType: "L2", GroupByFieldId: -1,
			SearchIteratorV2Info: &planpb.SearchIteratorV2Info{BatchSize: 1, LastBound: &lastBound},
		})
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), `"last_bound":{"non_finite":"+Inf"}`)
		assert.Contains(t, got.Text(), "iterator: batch_size=1 last_bound=+Inf\n")
	})
	t.Run("infinite distance", func(t *testing.T) {
		got := explainRetrieve(t, helper, `st_dwithin(GeometryField, "POINT(0 0)", 1e308 * 10)`)
		require.NotNil(t, got.Predicates.Distance)
		assert.True(t, math.IsInf(float64(*got.Predicates.Distance), 1))
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), `"distance":{"non_finite":"+Inf"}`)
	})
	t.Run("negative infinity", func(t *testing.T) {
		js, err := json.Marshal(struct {
			A Float32 `json:"a"`
			B Float64 `json:"b"`
		}{Float32(math.Inf(-1)), Float64(math.Inf(-1))})
		require.NoError(t, err)
		assert.Equal(t, `{"a":{"non_finite":"-Inf"},"b":{"non_finite":"-Inf"}}`, string(js))
	})
}

func TestIsoDuration(t *testing.T) {
	cases := []struct {
		iv   IntervalExplanation
		want string
	}{
		{IntervalExplanation{}, "PT0S"},
		{IntervalExplanation{Days: 1}, "P1D"},
		{IntervalExplanation{Minutes: 30}, "PT30M"},
		{IntervalExplanation{Years: 1, Months: 2, Days: 3, Hours: 4, Minutes: 5, Seconds: 6}, "P1Y2M3DT4H5M6S"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, isoDuration(&tc.iv))
	}
}

// A list the parser leaves empty is still a list: `f in []` matches nothing,
// `json_contains_all(f, [])` has nothing to require, and `f()` is a call with
// no arguments. All three keep their (empty) list in JSON, so they do not read
// like a node whose list was never reported. In text, values print as [] and
// a call's arguments are its nested lines, so `f()` simply has none.
func TestExplainPlan_EmptyListsStayVisible(t *testing.T) {
	helper := newTestSchemaHelper(t)
	cases := []struct {
		expr string
		json string
		text string
	}{
		{`Int64Field in []`, `"values":[]}`, "  term Int64Field []\n"},
		{`Int64Field not in []`, `"values":[]}`, "    term Int64Field []\n"},
		{`JSONField["a"] in []`, `"values":[]}`, `  term JSONField["a"] []` + "\n"},
		{`json_contains_all(ArrayField, [])`, `"values":[]}`, "  json_contains ContainsAll ArrayField []\n"},
		{`f()`, `{"node":"call","children":[],"function":"f"}`, "  call f\n"},
	}
	for _, tc := range cases {
		got := explainRetrieve(t, helper, tc.expr)
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), tc.json, tc.expr)
		assert.Contains(t, got.Text(), tc.text, tc.expr)
		assert.Equal(t, 0, got.UnknownNodeCount, tc.expr)
	}

	// Node kinds without a list still omit the keys, so the common case stays short.
	js, err := explainRetrieve(t, helper, `Int64Field > 1`).JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(js), `"values"`)
	assert.NotContains(t, string(js), `"children"`)

	t.Run("unfilled template list keeps its name", func(t *testing.T) {
		expr, err := ParseExprTemplate(helper, `Int64Field in {ids}`, nil)
		require.NoError(t, err)
		got, err := ExplainPlan(&planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: expr}}, helper, nil)
		require.NoError(t, err)
		js, err := got.JSON()
		require.NoError(t, err)
		assert.Contains(t, string(js), `"values":[],"template":"ids"}`)
		assert.Contains(t, got.Text(), "  term Int64Field [] template=ids\n")
	})
}

// Before FillExpressionValue runs, a GIS template node holds its variable name
// in wkt_string. The explainer reports it as the template name, as it does for
// an unfilled range bound, rather than as a geometry.
func TestExplainPlan_UnfilledGISTemplateIsNamed(t *testing.T) {
	helper := newTestSchemaHelper(t)
	for _, expr := range []string{`st_contains(GeometryField, {wkt})`, `st_dwithin(GeometryField, {wkt}, 2.5)`} {
		parsed, err := ParseExprTemplate(helper, expr, nil)
		require.NoError(t, err)
		require.True(t, parsed.GetIsTemplate())
		require.Equal(t, "wkt", parsed.GetGisfunctionFilterExpr().GetWktString(), "the parser stores the name as the placeholder")
		for _, redact := range []bool{false, true} {
			got, err := ExplainPlan(&planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: parsed}}, helper, &ExplainOptions{RedactTemplateValues: redact})
			require.NoError(t, err)
			require.Equal(t, NodeGIS, got.Predicates.Node, expr)
			assert.Equal(t, "wkt", got.Predicates.Template, expr)
			assert.Empty(t, got.Predicates.WKT, expr)
			js, err := got.JSON()
			require.NoError(t, err)
			assert.NotContains(t, string(js), `"wkt":`, expr, "the name is not reported as a geometry")
			assert.Contains(t, string(js), `"template":"wkt"`, expr)
			assert.Contains(t, got.Text(), " template=wkt\n", expr)
			assert.NotContains(t, got.Text(), "wkt=", expr)
		}
	}
}

// Query-node rerank chains are the other way a plan reorders its results
// (mutually exclusive with scorers), so they are summarized: name, stage and
// operation names. The expressions inside the operations are out of scope.
func TestExplainPlan_FunctionChains(t *testing.T) {
	helper := newTestSchemaHelper(t)
	plan, err := CreateSearchPlan(helper, "", "FloatVectorField", &planpb.QueryInfo{Topk: 10, MetricType: "L2", GroupByFieldId: -1}, nil, nil)
	require.NoError(t, err)
	plan.QuerynodeFunctionChains = []*schemapb.FunctionChain{
		{Name: "rerank", Stage: schemapb.FunctionChainStage_FunctionChainStageL0Rerank, Ops: []*schemapb.FunctionChainOp{{Op: "map"}, {Op: "sort"}, {Op: "limit"}}},
		{Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank, Ops: []*schemapb.FunctionChainOp{{Op: "limit"}}},
	}
	got, err := ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	require.Equal(t, []FunctionChainExplanation{
		{Name: "rerank", Stage: "FunctionChainStageL0Rerank", Ops: []string{"map", "sort", "limit"}},
		{Stage: "FunctionChainStageL1Rerank", Ops: []string{"limit"}},
	}, got.FunctionChains)

	js, err := got.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(js), `"function_chains":[{"name":"rerank","stage":"FunctionChainStageL0Rerank","ops":["map","sort","limit"]},{"stage":"FunctionChainStageL1Rerank","ops":["limit"]}]`)
	text := got.Text()
	assert.Contains(t, text, `function_chain[0]: name="rerank" stage=FunctionChainStageL0Rerank ops=["map", "sort", "limit"]`+"\n")
	assert.Contains(t, text, `function_chain[1]: name="" stage=FunctionChainStageL1Rerank ops=["limit"]`+"\n")

	// A plan without chains omits the key and the line.
	plan.QuerynodeFunctionChains = nil
	got, err = ExplainPlan(plan, helper, nil)
	require.NoError(t, err)
	js, err = got.JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(js), "function_chains")
	assert.NotContains(t, got.Text(), "function_chain")
}

// Every JSON key of the contract, with its presence rule, in one place. The
// byte-exact pin above covers one search plan; this covers the key set of
// every explanation type, so renaming or removing any key (which needs a
// version bump) fails here, and adding one is a visible edit to this list.
func TestExplainPlan_ContractKeys(t *testing.T) {
	want := strings.Fields(`
AggregateExplanation.field
AggregateExplanation.op
BlobSummary.bytes
FieldRef.auto_id,omitempty
FieldRef.candidate_indexes,omitempty
FieldRef.clustering_key,omitempty
FieldRef.element_level,omitempty
FieldRef.element_type,omitempty
FieldRef.id
FieldRef.name,omitempty
FieldRef.nested_path,omitempty
FieldRef.nullable,omitempty
FieldRef.partition_key,omitempty
FieldRef.primary_key,omitempty
FieldRef.type,omitempty
FunctionChainExplanation.name,omitempty
FunctionChainExplanation.ops
FunctionChainExplanation.stage
GroupByExplanation.fields
GroupByExplanation.group_size
GroupByExplanation.strict_group_size
IndexCandidate.covers_subpaths,omitempty
IndexCandidate.name
IndexCandidate.nested_path,omitzero
IndexCandidate.params,omitempty
IndexCandidate.type,omitempty
IntervalExplanation.days,omitempty
IntervalExplanation.hours,omitempty
IntervalExplanation.minutes,omitempty
IntervalExplanation.months,omitempty
IntervalExplanation.seconds,omitempty
IntervalExplanation.years,omitempty
IteratorExplanation.batch_size
IteratorExplanation.last_bound
JSONPathExplanation.path
JSONPathExplanation.strict_cast
JSONPathExplanation.type
NonFinite.non_finite
OrderByExplanation.ascending
OrderByExplanation.field
OrderByExplanation.nulls_first
PlanExplanation.detail,omitempty
PlanExplanation.dynamic_fields
PlanExplanation.function_chains,omitempty
PlanExplanation.namespace,omitempty
PlanExplanation.options,omitempty
PlanExplanation.output_fields
PlanExplanation.plan_type
PlanExplanation.predicates
PlanExplanation.query
PlanExplanation.score_option,omitempty
PlanExplanation.scorers
PlanExplanation.unknown_node_count
PlanExplanation.vector_search
PlanExplanation.version
PlanOptionsExplanation.expr_use_json_stats
PredicateNode.arith_op,omitempty
PredicateNode.blob,omitempty
PredicateNode.child,omitempty
PredicateNode.children,omitzero
PredicateNode.count,omitempty
PredicateNode.detail,omitempty
PredicateNode.distance,omitempty
PredicateNode.element,omitempty
PredicateNode.extra_values,omitempty
PredicateNode.field,omitempty
PredicateNode.function,omitempty
PredicateNode.in_field,omitempty
PredicateNode.interval,omitempty
PredicateNode.left,omitempty
PredicateNode.lower,omitempty
PredicateNode.node
PredicateNode.op,omitempty
PredicateNode.operand,omitempty
PredicateNode.operand_template,omitempty
PredicateNode.right,omitempty
PredicateNode.right_field,omitempty
PredicateNode.sample_factor,omitempty
PredicateNode.struct_name,omitempty
PredicateNode.template,omitempty
PredicateNode.upper,omitempty
PredicateNode.value,omitempty
PredicateNode.values,omitzero
PredicateNode.wkt,omitempty
QueryCursorExplanation.last_element_offset
QueryCursorExplanation.last_int_pk,omitempty
QueryCursorExplanation.last_str_pk,omitempty
QueryExplanation.aggregates,omitempty
QueryExplanation.cursor,omitempty
QueryExplanation.group_by,omitempty
QueryExplanation.is_count
QueryExplanation.limit
QueryExplanation.order_by,omitempty
RangeBound.inclusive
RangeBound.template,omitempty
RangeBound.value
ScoreOptionExplanation.boost_mode
ScoreOptionExplanation.function_mode
ScorerExplanation.filter
ScorerExplanation.params,omitempty
ScorerExplanation.type
ScorerExplanation.weight
VectorSearchExplanation.field
VectorSearchExplanation.group_by
VectorSearchExplanation.hints,omitempty
VectorSearchExplanation.iterator
VectorSearchExplanation.json_path,omitempty
VectorSearchExplanation.metric_type
VectorSearchExplanation.round_decimal
VectorSearchExplanation.search_params
VectorSearchExplanation.topk
VectorSearchExplanation.vector_type
`)
	slices.Sort(want)
	got := contractKeys(reflect.TypeOf(PlanExplanation{}), reflect.TypeOf(BlobSummary{}), reflect.TypeOf(NonFinite{}))
	assert.Equal(t, want, got, "the JSON contract changed; renaming or removing a key needs an ExplainVersion bump")
}

// contractKeys walks the struct types reachable from the roots through
// pointers, slices and maps and lists every JSON key as Type.key[,option].
func contractKeys(roots ...reflect.Type) []string {
	seen := map[reflect.Type]bool{}
	var keys []string
	var walk func(reflect.Type)
	walk = func(t reflect.Type) {
		for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Map {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct || seen[t] {
			return
		}
		seen[t] = true
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if tag, ok := f.Tag.Lookup("json"); ok {
				keys = append(keys, t.Name()+"."+tag)
			}
			walk(f.Type)
		}
	}
	for _, root := range roots {
		walk(root)
	}
	slices.Sort(keys)
	return keys
}

func ExampleExplainPlan() {
	helper, err := typeutil.CreateSchemaHelper(newTestSchema(true))
	if err != nil {
		panic(err)
	}
	plan, err := CreateSearchPlan(helper, `Int64Field > 1 && VarCharField in ["a", "b"]`, "FloatVectorField",
		&planpb.QueryInfo{Topk: 10, MetricType: "L2", SearchParams: `{"nprobe": 8}`, GroupByFieldId: -1}, nil, nil)
	if err != nil {
		panic(err)
	}
	// Pinned so the example output does not depend on configuration.
	plan.PlanOptions = &planpb.PlanOption{ExprUseJsonStats: true}

	explanation, err := ExplainPlan(plan, helper, &ExplainOptions{
		FieldIndexes: map[int64][]IndexCandidate{plan.GetVectorAnns().GetFieldId(): {{Name: "vec_idx", Type: "HNSW"}}},
	})
	if err != nil {
		panic(err)
	}
	fmt.Print(explanation.Text())
	// Output:
	// plan: vector_search
	// search: FloatVectorField (FloatVector) metric=L2 topk=10 round_decimal=0 indexes=[vec_idx:HNSW]
	// search_params: {"nprobe":8}
	// filter:
	//   binary LogicalAnd
	//     unary_range GreaterThan Int64Field 1
	//     term VarCharField ["a", "b"]
	// options: expr_use_json_stats=true
	// unknown_nodes: 0
}

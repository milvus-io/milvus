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

package featureusage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func exprTestSchema(t *testing.T) *typeutil.SchemaHelper {
	schema := &schemapb.CollectionSchema{
		Name:               "expr_walk",
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "Int64Field", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "VarCharField", DataType: schemapb.DataType_VarChar, Nullable: true, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "256"}, {Key: "enable_match", Value: "true"}, {Key: common.EnableAnalyzerKey, Value: "true"},
			}},
			{FieldID: 102, Name: "JSONField", DataType: schemapb.DataType_JSON},
			{FieldID: 103, Name: "ArrayField", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}}},
			{FieldID: 104, Name: "GeometryField", DataType: schemapb.DataType_Geometry},
			{FieldID: 105, Name: "TimestamptzField", DataType: schemapb.DataType_Timestamptz},
			{FieldID: 106, Name: "FloatVectorField", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
			{FieldID: 107, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func featuresOf(t *testing.T, helper *typeutil.SchemaHelper, expr string) []string {
	parsed, err := planparserv2.ParseExpr(helper, expr, nil)
	require.NoError(t, err, "expr %q", expr)
	var set FeatureSet
	CollectExprFeatures(parsed, &set)
	names := make([]string, 0)
	for _, f := range set.Features() {
		names = append(names, f.Name())
	}
	return names
}

func TestCollectExprFeatures_ParsedExpressions(t *testing.T) {
	helper := exprTestSchema(t)
	cases := []struct {
		expr string
		want []string
	}{
		{`Int64Field > 3 and Int64Field < 10`, []string{}},
		{`Int64Field in [1, 2, 3]`, []string{}},
		{`VarCharField == "a" or VarCharField != "b"`, []string{}},
		{`text_match(VarCharField, "hello world")`, []string{"text_match"}},
		{`phrase_match(VarCharField, "hello world", 2)`, []string{"phrase_match"}},
		{`VarCharField like "abc%"`, []string{"like"}},
		{`VarCharField like "%abc"`, []string{"like"}},
		{`VarCharField like "%abc%"`, []string{"like"}},
		{`VarCharField is null`, []string{"is_null"}},
		{`VarCharField is not null`, []string{"is_not_null"}},
		{`exists JSONField["a"]`, []string{"exists", "json_identifier"}},
		{`exists $meta["dyn"]`, []string{"exists", "json_identifier"}},
		{`JSONField["a"] > 1`, []string{"json_identifier"}},
		{`JSONField["a"]["b"] == "x" and Int64Field > 0`, []string{"json_identifier"}},
		{`json_contains(JSONField["tags"], 1)`, []string{"json_contains", "json_identifier"}},
		{`json_contains_all(JSONField["tags"], [1, 2])`, []string{"json_contains", "json_identifier"}},
		{`json_contains_any(JSONField["tags"], [1, 2])`, []string{"json_contains", "json_identifier"}},
		{`array_contains(ArrayField, 1)`, []string{"array_contains"}},
		{`array_contains_any(ArrayField, [1, 2])`, []string{"array_contains"}},
		{`array_length(ArrayField) == 3`, []string{"array_length"}},
		{`ArrayField[0] > 1`, []string{}},
		{`st_contains(GeometryField, "POINT(1 1)")`, []string{"st_contains"}},
		{`st_within(GeometryField, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")`, []string{"st_within"}},
		{`st_intersects(GeometryField, "LINESTRING(0 0, 1 1)")`, []string{"st_intersects"}},
		{`st_equals(GeometryField, "POINT(0 0)")`, []string{"st_equals"}},
		{`st_touches(GeometryField, "POINT(0 0)")`, []string{"st_touches"}},
		{`st_overlaps(GeometryField, "POINT(0 0)")`, []string{"st_overlaps"}},
		{`st_crosses(GeometryField, "POINT(0 0)")`, []string{"st_crosses"}},
		{`st_dwithin(GeometryField, "POINT(0 0)", 10.0)`, []string{"st_dwithin"}},
		{`TimestamptzField > ISO '2025-01-01T00:00:00Z'`, []string{"timestamptz_compare"}},
		{`random_sample(0.1)`, []string{"random_sample"}},
		{`Int64Field > 5 and random_sample(0.5)`, []string{"random_sample"}},
		// Several features in one predicate: each is reported once.
		{
			`text_match(VarCharField, "x") and text_match(VarCharField, "y") and JSONField["a"] > 1 and exists JSONField["b"]`,
			[]string{"text_match", "exists", "json_identifier"},
		},
	}
	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			got := featuresOf(t, helper, c.expr)
			assert.ElementsMatch(t, c.want, got, "expr %q", c.expr)
		})
	}
}

func TestCollectExprFeatures_HandBuiltNodes(t *testing.T) {
	// Struct-array nodes, a regex op, a Timestamptz arith compare and nil.
	var set FeatureSet
	CollectExprFeatures(nil, &set)
	assert.Empty(t, set.Features())

	tree := &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
		Op: planpb.BinaryExpr_LogicalAnd,
		Left: &planpb.Expr{Expr: &planpb.Expr_ElementFilterExpr{ElementFilterExpr: &planpb.ElementFilterExpr{
			StructName: "s",
			Predicate: &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 1, DataType: schemapb.DataType_VarChar, IsElementLevel: true},
				Op:         planpb.OpType_RegexMatch,
			}}},
		}}},
		Right: &planpb.Expr{Expr: &planpb.Expr_MatchExpr{MatchExpr: &planpb.MatchExpr{
			StructName: "s", MatchType: planpb.MatchType_MatchAny,
			Predicate: &planpb.Expr{Expr: &planpb.Expr_TimestamptzArithCompareExpr{TimestamptzArithCompareExpr: &planpb.TimestamptzArithCompareExpr{
				TimestamptzColumn: &planpb.ColumnInfo{FieldId: 2, DataType: schemapb.DataType_Timestamptz},
			}}},
		}}},
	}}}
	set = FeatureSet{}
	CollectExprFeatures(tree, &set)
	names := make([]string, 0)
	for _, f := range set.Features() {
		names = append(names, f.Name())
	}
	assert.ElementsMatch(t, []string{"element_filter", "regex_match", "struct_match", "timestamptz_compare"}, names)

	// Unknown GIS op and unknown null op create nothing.
	set = FeatureSet{}
	CollectExprFeatures(&planpb.Expr{Expr: &planpb.Expr_GisfunctionFilterExpr{GisfunctionFilterExpr: &planpb.GISFunctionFilterExpr{Op: planpb.GISFunctionFilterExpr_Invalid}}}, &set)
	CollectExprFeatures(&planpb.Expr{Expr: &planpb.Expr_NullExpr{NullExpr: &planpb.NullExpr{Op: planpb.NullExpr_Invalid}}}, &set)
	assert.Empty(t, set.Features())
}

func TestRecordExpr(t *testing.T) {
	SetEnabled(true)
	helper := exprTestSchema(t)
	parsed, err := planparserv2.ParseExpr(helper, `text_match(VarCharField, "a") and text_match(VarCharField, "b")`, nil)
	require.NoError(t, err)

	before := index(Snapshot())
	RecordExpr(parsed, map[string]*schemapb.TemplateValue{
		"limit":                    {},
		common.ExprUseJSONStatsKey: {},
	})
	after := index(Snapshot())
	assert.Equal(t, before["text_match"].Value+1, after["text_match"].Value, "two occurrences count once per request")
	assert.Equal(t, before["expr_template_values"].Value+1, after["expr_template_values"].Value)
	assert.Equal(t, before["expr_use_json_stats"].Value+1, after["expr_use_json_stats"].Value)
	assert.Equal(t, before["like"].Value, after["like"].Value)

	// Only the json-stats hint: not a template request.
	before = after
	RecordExpr(nil, map[string]*schemapb.TemplateValue{common.ExprUseJSONStatsKey: {}})
	after = index(Snapshot())
	assert.Equal(t, before["expr_template_values"].Value, after["expr_template_values"].Value)
	assert.Equal(t, before["expr_use_json_stats"].Value+1, after["expr_use_json_stats"].Value)

	// Disabled: nothing moves.
	SetEnabled(false)
	defer SetEnabled(true)
	before = after
	RecordExpr(parsed, map[string]*schemapb.TemplateValue{"x": {}})
	after = index(Snapshot())
	assert.Equal(t, before["text_match"].Value, after["text_match"].Value)
	assert.Equal(t, before["expr_template_values"].Value, after["expr_template_values"].Value)
}

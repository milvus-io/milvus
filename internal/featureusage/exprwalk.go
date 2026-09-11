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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// FeatureSet is a fixed-size bitmap over the counter id set. It lives on the
// stack of the caller: walking an expression allocates nothing.
type FeatureSet [numFeatures]bool

// Set marks f.
func (s *FeatureSet) Set(f Feature) {
	if f >= 0 && f < numFeatures {
		s[f] = true
	}
}

// Has reports whether f is marked.
func (s *FeatureSet) Has(f Feature) bool {
	return f >= 0 && f < numFeatures && s[f]
}

// Features returns the marked features in id order.
func (s *FeatureSet) Features() []Feature {
	out := make([]Feature, 0, 8)
	for f := Feature(0); f < numFeatures; f++ {
		if s[f] {
			out = append(out, f)
		}
	}
	return out
}

// RecordExpr counts the expression-language features used by one request:
// the parsed predicate tree is walked once and each feature found is hit
// once, however many times it occurs in the tree. It runs on the output of
// the parser, after its expression cache, so a repeated expression string is
// counted on every request and not only on cache misses.
//
// exprTemplateValues is the request's template map: any key other than the
// expr_use_json_stats hint counts as expr_template_values; the hint itself
// counts as expr_use_json_stats.
func RecordExpr(expr *planpb.Expr, exprTemplateValues map[string]*schemapb.TemplateValue) {
	if !enabled.Load() {
		return
	}
	var set FeatureSet
	CollectExprFeatures(expr, &set)
	for key := range exprTemplateValues {
		if key == common.ExprUseJSONStatsKey {
			set.Set(FeatureExprUseJSONStats)
		} else {
			set.Set(FeatureExprTemplateValues)
		}
	}
	for f := Feature(0); f < numFeatures; f++ {
		if set[f] {
			defaultCounters.Hit(f)
		}
	}
}

// CollectExprFeatures walks expr and marks every expression feature it
// contains. Nil is a valid (empty) expression.
func CollectExprFeatures(expr *planpb.Expr, set *FeatureSet) {
	if expr == nil {
		return
	}
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		CollectExprFeatures(e.BinaryExpr.GetLeft(), set)
		CollectExprFeatures(e.BinaryExpr.GetRight(), set)
	case *planpb.Expr_UnaryExpr:
		CollectExprFeatures(e.UnaryExpr.GetChild(), set)
	case *planpb.Expr_RandomSampleExpr:
		set.Set(FeatureRandomSample)
		CollectExprFeatures(e.RandomSampleExpr.GetPredicate(), set)
	case *planpb.Expr_ElementFilterExpr:
		set.Set(FeatureElementFilter)
		CollectExprFeatures(e.ElementFilterExpr.GetElementExpr(), set)
		CollectExprFeatures(e.ElementFilterExpr.GetPredicate(), set)
	case *planpb.Expr_MatchExpr:
		set.Set(FeatureStructMatch)
		CollectExprFeatures(e.MatchExpr.GetPredicate(), set)

	case *planpb.Expr_TermExpr:
		markColumn(e.TermExpr.GetColumnInfo(), set)
	case *planpb.Expr_UnaryRangeExpr:
		markColumn(e.UnaryRangeExpr.GetColumnInfo(), set)
		markOp(e.UnaryRangeExpr.GetOp(), set)
	case *planpb.Expr_BinaryRangeExpr:
		markColumn(e.BinaryRangeExpr.GetColumnInfo(), set)
	case *planpb.Expr_CompareExpr:
		markColumn(e.CompareExpr.GetLeftColumnInfo(), set)
		markColumn(e.CompareExpr.GetRightColumnInfo(), set)
	case *planpb.Expr_BinaryArithOpEvalRangeExpr:
		markColumn(e.BinaryArithOpEvalRangeExpr.GetColumnInfo(), set)
		if e.BinaryArithOpEvalRangeExpr.GetArithOp() == planpb.ArithOpType_ArrayLength {
			set.Set(FeatureArrayLength)
		}
		markOp(e.BinaryArithOpEvalRangeExpr.GetOp(), set)
	case *planpb.Expr_BinaryArithExpr:
		CollectExprFeatures(e.BinaryArithExpr.GetLeft(), set)
		CollectExprFeatures(e.BinaryArithExpr.GetRight(), set)
	case *planpb.Expr_ColumnExpr:
		markColumn(e.ColumnExpr.GetInfo(), set)
	case *planpb.Expr_ExistsExpr:
		set.Set(FeatureExists)
		markColumn(e.ExistsExpr.GetInfo(), set)
	case *planpb.Expr_JsonContainsExpr:
		markColumn(e.JsonContainsExpr.GetColumnInfo(), set)
		if e.JsonContainsExpr.GetColumnInfo().GetDataType() == schemapb.DataType_Array {
			set.Set(FeatureArrayContains)
		} else {
			set.Set(FeatureJSONContains)
		}
	case *planpb.Expr_NullExpr:
		markColumn(e.NullExpr.GetColumnInfo(), set)
		switch e.NullExpr.GetOp() {
		case planpb.NullExpr_IsNull:
			set.Set(FeatureIsNull)
		case planpb.NullExpr_IsNotNull:
			set.Set(FeatureIsNotNull)
		}
	case *planpb.Expr_GisfunctionFilterExpr:
		markColumn(e.GisfunctionFilterExpr.GetColumnInfo(), set)
		if f, ok := gisFeature(e.GisfunctionFilterExpr.GetOp()); ok {
			set.Set(f)
		}
	case *planpb.Expr_TimestamptzArithCompareExpr:
		set.Set(FeatureTimestamptzCompare)
	case *planpb.Expr_BloomFilterExpr:
		markColumn(e.BloomFilterExpr.GetColumnInfo(), set)
	case *planpb.Expr_RoaringFilterExpr:
		markColumn(e.RoaringFilterExpr.GetColumnInfo(), set)
	case *planpb.Expr_ValueExpr, *planpb.Expr_AlwaysTrueExpr, *planpb.Expr_CallExpr:
		// No user-facing feature of its own. CallExpr names are not counted:
		// they are a string the parser accepted and the catalog has no row.
	}
}

// markColumn records the column-level features: access to a JSON path (a
// JSON column with a nested path, which is also how dynamic fields are
// addressed) and any comparison involving a Timestamptz column.
func markColumn(col *planpb.ColumnInfo, set *FeatureSet) {
	if col == nil {
		return
	}
	switch col.GetDataType() {
	case schemapb.DataType_JSON:
		if len(col.GetNestedPath()) > 0 {
			set.Set(FeatureJSONIdentifier)
		}
	case schemapb.DataType_Timestamptz:
		set.Set(FeatureTimestamptzCompare)
	}
}

// markOp records the operator-level features of a range expression.
func markOp(op planpb.OpType, set *FeatureSet) {
	switch op {
	case planpb.OpType_TextMatch, planpb.OpType_TextMatchFuzzy:
		set.Set(FeatureTextMatch)
	case planpb.OpType_PhraseMatch:
		set.Set(FeaturePhraseMatch)
	case planpb.OpType_PrefixMatch, planpb.OpType_PostfixMatch, planpb.OpType_Match, planpb.OpType_InnerMatch:
		set.Set(FeatureLike)
	case planpb.OpType_RegexMatch:
		set.Set(FeatureRegexMatch)
	}
}

func gisFeature(op planpb.GISFunctionFilterExpr_GISOp) (Feature, bool) {
	switch op {
	case planpb.GISFunctionFilterExpr_Equals:
		return FeatureGeoEquals, true
	case planpb.GISFunctionFilterExpr_Touches:
		return FeatureGeoTouches, true
	case planpb.GISFunctionFilterExpr_Overlaps:
		return FeatureGeoOverlaps, true
	case planpb.GISFunctionFilterExpr_Crosses:
		return FeatureGeoCrosses, true
	case planpb.GISFunctionFilterExpr_Contains:
		return FeatureGeoContains, true
	case planpb.GISFunctionFilterExpr_Intersects:
		return FeatureGeoIntersects, true
	case planpb.GISFunctionFilterExpr_Within:
		return FeatureGeoWithin, true
	case planpb.GISFunctionFilterExpr_DWithin:
		return FeatureGeoDWithin, true
	case planpb.GISFunctionFilterExpr_STIsValid:
		return FeatureGeoIsValid, true
	default:
		return 0, false
	}
}

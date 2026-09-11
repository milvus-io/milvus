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
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// This file holds the request-level feature counter hooks. Each hook sits at
// a place that already parses the field it reads, adds one branch and one
// atomic add per counted feature, and never allocates. Counting rules follow
// docs/design-docs/design_docs/20260902-feature-usage-reporting.md:
//
//   - fields the SDKs populate unconditionally are counted on their effective
//     value, never on presence;
//   - per-value counters (consistency level, function score type, highlighter
//     type) only recognize a fixed set and fold the rest into _other;
//   - guarantee_timestamp is not counted: pymilvus sets it on every request.

// requestFeatureSource is the subset of getters shared by Search, Query and
// HybridSearch requests that the common hooks read.
type requestFeatureSource interface {
	GetUseDefaultConsistency() bool
	GetConsistencyLevel() commonpb.ConsistencyLevel
	GetNotReturnAllMeta() bool
	GetTravelTimestamp() uint64
	GetNamespace() string
}

// recordCommonRequestFeatures counts the features present on every read
// request type.
func recordCommonRequestFeatures(r requestFeatureSource) {
	if !featureusage.Enabled() {
		return
	}
	// use_default_consistency=false is the only request-side signal that the
	// client chose a level; the level itself is an enum, so one slot per value.
	if !r.GetUseDefaultConsistency() {
		if f, ok := featureusage.ConsistencyLevelFeature(r.GetConsistencyLevel()); ok {
			featureusage.Hit(f)
		}
	}
	if r.GetNotReturnAllMeta() {
		featureusage.Hit(featureusage.FeatureNotReturnAllMeta)
	}
	// The Proxy no longer reads travel_timestamp; the counter measures how many
	// clients still send the removed field.
	if r.GetTravelTimestamp() > 0 {
		featureusage.Hit(featureusage.FeatureDeprecatedTravelTimestamp)
	}
	if r.GetNamespace() != "" {
		featureusage.Hit(featureusage.FeatureNamespace)
	}
}

// recordSearchRequestFeatures counts the Search-only request fields, then the
// common ones. HybridSearch is folded into a SearchRequest before it reaches
// the search task, so this covers both.
func recordSearchRequestFeatures(req *milvuspb.SearchRequest) {
	if !featureusage.Enabled() {
		return
	}
	recordCommonRequestFeatures(req)
	if req.GetSearchByPrimaryKeys() {
		featureusage.Hit(featureusage.FeatureSearchByPrimaryKeys)
	}
	recordFunctionScoreFeatures(req.GetFunctionScore())
	if h := req.GetHighlighter(); h != nil {
		if f, ok := featureusage.HighlighterFeature(h.GetType()); ok {
			featureusage.Hit(f)
		}
		for _, kv := range h.GetParams() {
			switch kv.GetKey() {
			case FragmentSizeKey:
				featureusage.Hit(featureusage.FeatureFragmentSize)
			case FragmentNumKey:
				featureusage.Hit(featureusage.FeatureNumOfFragments)
			}
		}
	}
}

// recordFunctionScoreFeatures counts one slot per rerank function in a
// function_score. The function name is a lowercased user string at this
// point, so FunctionScoreFeature folds anything unrecognized into _other.
func recordFunctionScoreFeatures(fs *schemapb.FunctionScore) {
	for _, fn := range fs.GetFunctions() {
		featureusage.Hit(featureusage.FunctionScoreFeature(rerank.GetRerankName(fn)))
	}
}

// recordSearchInfoFeatures counts the search_params features that
// parseSearchInfo has just parsed. It runs once per search and once per
// hybrid sub-request.
func recordSearchInfoFeatures(isIterator, isRangeSearch bool, groupByFieldID int64, isIteratorV2 bool) {
	if !featureusage.Enabled() {
		return
	}
	// pymilvus's v2 search iterator sends iterator=True as well as
	// search_iter_v2, so the old-protocol counter only fires without v2.
	if isIteratorV2 {
		featureusage.Hit(featureusage.FeatureSearchIterV2)
	} else if isIterator {
		featureusage.Hit(featureusage.FeatureIterator)
	}
	if isRangeSearch {
		featureusage.Hit(featureusage.FeatureRangeSearch)
	}
	if groupByFieldID > 0 {
		featureusage.Hit(featureusage.FeatureGroupByField)
	}
}

// recordLegacyRankStrategy counts the legacy hybrid-search rank_params
// "strategy" (RRFRanker / WeightedRanker in the SDKs). The value is a raw user
// string here, so unrecognized values fold into strategy=_other. Absent key:
// not a legacy rerank, nothing counted.
func recordLegacyRankStrategy(searchParams []*commonpb.KeyValuePair) {
	if !featureusage.Enabled() {
		return
	}
	strategy, err := funcutil.GetAttrByKeyFromRepeatedKV(RankTypeKey, searchParams)
	if err != nil {
		return
	}
	featureusage.Hit(featureusage.RankStrategyFeature(strategy))
	if _, err := funcutil.GetAttrByKeyFromRepeatedKV(NormScoreKey, searchParams); err == nil {
		featureusage.Hit(featureusage.FeatureNormScore)
	}
}

// recordPlanExprFeatures counts the expression-language features of a parsed
// plan (search, query or delete) by walking its predicate tree once. It runs
// on the parser's output, after the expression cache, so repeated expression
// strings are counted on every request.
func recordPlanExprFeatures(plan *planpb.PlanNode, exprTemplateValues map[string]*schemapb.TemplateValue) {
	if !featureusage.Enabled() || plan == nil {
		return
	}
	var predicates *planpb.Expr
	switch n := plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		predicates = n.VectorAnns.GetPredicates()
	case *planpb.PlanNode_Query:
		predicates = n.Query.GetPredicates()
	case *planpb.PlanNode_Predicates:
		predicates = n.Predicates
	}
	if predicates == nil && len(exprTemplateValues) == 0 {
		return
	}
	featureusage.RecordExpr(predicates, exprTemplateValues)
}

// recordSearchParamKeyFeatures counts the search_params features in one pass
// over the key/value list: group_size, strict_group_size, rank_group_scorer,
// hints and analyzer_name by presence; ignore_growing by its effective value,
// because pymilvus sends the key (as "False") on every search.
func recordSearchParamKeyFeatures(params []*commonpb.KeyValuePair) {
	if !featureusage.Enabled() {
		return
	}
	for _, kv := range params {
		switch kv.GetKey() {
		case GroupSizeKey:
			featureusage.Hit(featureusage.FeatureGroupSize)
		case StrictGroupSize:
			featureusage.Hit(featureusage.FeatureStrictGroupSize)
		case RankGroupScorer:
			featureusage.Hit(featureusage.FeatureRankGroupScorer)
		case common.HintsKey:
			featureusage.Hit(featureusage.FeatureHints)
		case AnalyzerKey:
			featureusage.Hit(featureusage.FeatureAnalyzerName)
		case IgnoreGrowingKey:
			if b, err := strconv.ParseBool(kv.GetValue()); err == nil && b {
				featureusage.Hit(featureusage.FeatureIgnoreGrowing)
			}
		}
	}
}

// recordQueryParamKeyFeatures counts the query_params features: group_by_fields
// by presence, ignore_growing by its effective value.
func recordQueryParamKeyFeatures(params []*commonpb.KeyValuePair) {
	if !featureusage.Enabled() {
		return
	}
	for _, kv := range params {
		switch kv.GetKey() {
		case GroupByFieldsKey:
			featureusage.Hit(featureusage.FeatureQueryGroupByFields)
		case IgnoreGrowingKey:
			if b, err := strconv.ParseBool(kv.GetValue()); err == nil && b {
				featureusage.Hit(featureusage.FeatureIgnoreGrowing)
			}
		}
	}
}

// recordOutputDynamicField counts a request whose output fields resolved to at
// least one dynamic field (translateOutputFields already did the resolution).
func recordOutputDynamicField(userDynamicFields []string) {
	if len(userDynamicFields) > 0 && featureusage.Enabled() {
		featureusage.Hit(featureusage.FeatureOutputDynamicField)
	}
}

// recordQueryIteratorFeature counts the old query iterator protocol.
func recordQueryIteratorFeature(isIterator bool) {
	if isIterator && featureusage.Enabled() {
		featureusage.Hit(featureusage.FeatureIterator)
	}
}

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

package dql

import (
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

// collectCommonRequestFeatures marks the features present on every read
// request type.
func collectCommonRequestFeatures(r requestFeatureSource, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	// use_default_consistency=false is the only request-side signal that the
	// client chose a level; the level itself is an enum, so one slot per value.
	if !r.GetUseDefaultConsistency() {
		if f, ok := featureusage.ConsistencyLevelFeature(r.GetConsistencyLevel()); ok {
			set.Set(f)
		}
	}
	if r.GetNotReturnAllMeta() {
		set.Set(featureusage.FeatureNotReturnAllMeta)
	}
	// The Proxy no longer reads travel_timestamp; the counter measures how many
	// clients still send the removed field.
	if r.GetTravelTimestamp() > 0 {
		set.Set(featureusage.FeatureDeprecatedTravelTimestamp)
	}
	if r.GetNamespace() != "" {
		set.Set(featureusage.FeatureNamespace)
	}
}

// recordCommonRequestFeatures counts them for a request that is counted in one
// place, as the query path is.
func recordCommonRequestFeatures(r requestFeatureSource) {
	if !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	collectCommonRequestFeatures(r, &set)
	set.HitAll()
}

// collectSearchRequestFeatures marks the Search-only request fields, then the
// common ones. HybridSearch is folded into a SearchRequest before it reaches
// the search task, so this covers both.
func collectSearchRequestFeatures(req *milvuspb.SearchRequest, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	collectCommonRequestFeatures(req, set)
	collectFunctionScoreFeatures(req.GetFunctionScore(), set)
	if req.GetSearchAggregation() != nil {
		set.Set(featureusage.FeatureSearchAggregation)
	}
	if h := req.GetHighlighter(); h != nil {
		if f, ok := featureusage.HighlighterFeature(h.GetType()); ok {
			set.Set(f)
		}
		for _, kv := range h.GetParams() {
			switch kv.GetKey() {
			case FragmentSizeKey:
				set.Set(featureusage.FeatureFragmentSize)
			case FragmentNumKey:
				set.Set(featureusage.FeatureNumOfFragments)
			}
		}
	}
}

// collectFunctionScoreFeatures marks one slot per rerank function in a
// function_score. The function name is a lowercased user string at this
// point, so FunctionScoreFeature folds anything unrecognized into _other.
func collectFunctionScoreFeatures(fs *schemapb.FunctionScore, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	for _, fn := range fs.GetFunctions() {
		set.Set(featureusage.FunctionScoreFeature(rerank.GetRerankName(fn)))
		// norm_score reaches a reranker as a function param on this path, which
		// is where parseWeightedParams and parseDecayParams read it from.
		for _, kv := range fn.GetParams() {
			if kv.GetKey() == NormScoreKey && isTrueValue(kv.GetValue()) {
				set.Set(featureusage.FeatureNormScore)
			}
		}
	}
}

// isTrueValue reports whether a parameter value asks for the feature. The
// counters record effective values, not presence, because an SDK that always
// sends a key would otherwise make its counter meaningless.
func isTrueValue(v string) bool {
	b, err := strconv.ParseBool(v)
	return err == nil && b
}

// collectSearchInfoFeatures marks the search_params features that
// parseSearchInfo has just parsed. It runs once per search and once per
// hybrid sub-request; the set makes a feature several sub-requests share
// count once for the whole request.
func collectSearchInfoFeatures(isIterator, isRangeSearch bool, groupByFieldID int64, isIteratorV2 bool, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	// pymilvus's v2 search iterator sends iterator=True as well as
	// search_iter_v2, so the old-protocol counter only fires without v2.
	if isIteratorV2 {
		set.Set(featureusage.FeatureSearchIterV2)
	} else if isIterator {
		set.Set(featureusage.FeatureIterator)
	}
	if isRangeSearch {
		set.Set(featureusage.FeatureRangeSearch)
	}
	if groupByFieldID > 0 {
		set.Set(featureusage.FeatureGroupByField)
	}
}

// collectLegacyRankStrategy marks the legacy hybrid-search rank_params
// "strategy" (RRFRanker / WeightedRanker in the SDKs). The value is a raw user
// string here, so unrecognized values fold into strategy=_other. Absent key:
// not a legacy rerank, nothing marked.
func collectLegacyRankStrategy(searchParams []*commonpb.KeyValuePair, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	strategy, err := funcutil.GetAttrByKeyFromRepeatedKV(RankTypeKey, searchParams)
	if err != nil {
		return
	}
	set.Set(featureusage.RankStrategyFeature(strategy))
	if legacyNormScoreEnabled(searchParams) {
		set.Set(featureusage.FeatureNormScore)
	}
}

// legacyNormScoreEnabled reports whether the legacy rank_params ask for score
// normalization.
//
// The value lives inside the JSON object held by the "params" key, which is
// where convertLegacyParams reads it from; that function recognizes only
// "strategy" and "params" at the top level and drops everything else, so a
// top-level norm_score key cannot change behavior and no client sends one.
// Reading the top level would leave this counter at zero for every request
// that genuinely normalizes.
//
// This is the one hook that parses: it unmarshals the same small object
// convertLegacyParams unmarshals a moment later, and only for a legacy hybrid
// search that carries rank_params.
func legacyNormScoreEnabled(searchParams []*commonpb.KeyValuePair) bool {
	raw, err := funcutil.GetAttrByKeyFromRepeatedKV(ParamsKey, searchParams)
	if err != nil || raw == "" {
		return false
	}
	// The key has to appear literally for the object to hold it, so this skips
	// the unmarshal -- the only allocation any of these hooks makes -- for
	// every request that does not ask for normalization. A substring elsewhere
	// in the object only costs the parse this would have done anyway.
	if !strings.Contains(raw, NormScoreKey) {
		return false
	}
	var params map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &params); err != nil {
		return false
	}
	switch v := params[NormScoreKey].(type) {
	case bool:
		return v
	case string:
		return isTrueValue(v)
	default:
		return false
	}
}

// collectPlanExprFeatures marks the expression-language features of a parsed
// plan (search, query or delete) by walking its predicate tree once. It runs
// on the parser's output, after the expression cache, so repeated expression
// strings are counted on every request.
func collectPlanExprFeatures(plan *planpb.PlanNode, exprTemplateValues map[string]*schemapb.TemplateValue, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	if plan == nil {
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
	featureusage.CollectExprFeatures(predicates, set)
	for key := range exprTemplateValues {
		if key == common.ExprUseJSONStatsKey {
			set.Set(featureusage.FeatureExprUseJSONStats)
		} else {
			set.Set(featureusage.FeatureExprTemplateValues)
		}
	}
}

// recordPlanExprFeatures counts them for a request that is counted in one
// place, as the query and delete paths are.
func recordPlanExprFeatures(plan *planpb.PlanNode, exprTemplateValues map[string]*schemapb.TemplateValue) {
	if !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	collectPlanExprFeatures(plan, exprTemplateValues, &set)
	set.HitAll()
}

// collectSearchParamKeyFeatures marks the search_params features in one pass
// over the key/value list: group_size, strict_group_size, rank_group_scorer,
// hints and analyzer_name by presence; ignore_growing by its effective value,
// because pymilvus sends the key (as "False") on every search.
//
// On the hybrid path this list is a sub-request's, and the caller also passes
// the request-level rank_params, which is where group_size, strict_group_size
// and rank_group_scorer live there. Marking into a shared set is what lets one
// call cover both without counting a key twice.
func collectSearchParamKeyFeatures(params []*commonpb.KeyValuePair, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	for _, kv := range params {
		switch kv.GetKey() {
		case GroupSizeKey:
			set.Set(featureusage.FeatureGroupSize)
		case StrictGroupSize:
			set.Set(featureusage.FeatureStrictGroupSize)
		case RankGroupScorer:
			set.Set(featureusage.FeatureRankGroupScorer)
		case common.HintsKey:
			// iterative_filter is the only documented hint; any other value is a
			// user string and folds into _other.
			if kv.GetValue() == iterativeFilterKey {
				set.Set(featureusage.FeatureHints)
			} else {
				set.Set(featureusage.FeatureHintsOther)
			}
		case AnalyzerKey:
			set.Set(featureusage.FeatureAnalyzerName)
		case OrderByFieldsKey:
			set.Set(featureusage.FeatureSearchOrderBy)
		case IgnoreGrowingKey:
			if isTrueValue(kv.GetValue()) {
				set.Set(featureusage.FeatureIgnoreGrowing)
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
		case OrderByFieldsKey:
			featureusage.Hit(featureusage.FeatureQueryOrderBy)
		case IgnoreGrowingKey:
			if b, err := strconv.ParseBool(kv.GetValue()); err == nil && b {
				featureusage.Hit(featureusage.FeatureIgnoreGrowing)
			}
		}
	}
}

// collectOutputFieldFeatures marks what the resolved output fields ask for:
// a dynamic field, and a vector field carrying raw vectors back to the client.
// translateOutputFields has already expanded "*" and resolved the names, so
// this walks the resolved list rather than the request.
//
// Struct array sub-fields count as well: a vector inside a struct is still a
// raw vector on the wire.
func collectOutputFieldFeatures(userDynamicFields, translatedOutputFields []string, schema *schemaInfo, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	if len(userDynamicFields) > 0 {
		set.Set(featureusage.FeatureOutputDynamicField)
	}
	if len(translatedOutputFields) == 0 || schema == nil {
		return
	}
	if outputHasVectorField(translatedOutputFields, schema.GetFields()) {
		set.Set(featureusage.FeatureOutputVectorField)
		return
	}
	for _, structField := range schema.GetStructArrayFields() {
		if outputHasVectorField(translatedOutputFields, structField.GetFields()) {
			set.Set(featureusage.FeatureOutputVectorField)
			return
		}
	}
}

// recordOutputFieldFeatures counts them for a request counted in one place,
// as the query path is.
func recordOutputFieldFeatures(userDynamicFields, translatedOutputFields []string, schema *schemaInfo) {
	if !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	collectOutputFieldFeatures(userDynamicFields, translatedOutputFields, schema, &set)
	set.HitAll()
}

// outputHasVectorField reports whether any vector field in fields is named in
// outputFields. Both slices hold a handful of entries on a real request, so
// the nested scan costs less than building a set and allocates nothing.
func outputHasVectorField(outputFields []string, fields []*schemapb.FieldSchema) bool {
	for _, field := range fields {
		if !typeutil.IsVectorType(field.GetDataType()) {
			continue
		}
		for _, name := range outputFields {
			if name == field.GetName() {
				return true
			}
		}
	}
	return false
}

// recordQueryIteratorFeature counts the old query iterator protocol.
func recordQueryIteratorFeature(isIterator bool) {
	if isIterator && featureusage.Enabled() {
		featureusage.Hit(featureusage.FeatureQueryIterator)
	}
}

// RecordPlanExprFeatures is recordPlanExprFeatures for the root proxy package,
// whose delete path builds a plan outside the dql tasks.
func RecordPlanExprFeatures(plan *planpb.PlanNode, exprTemplateValues map[string]*schemapb.TemplateValue) {
	recordPlanExprFeatures(plan, exprTemplateValues)
}

// collectSearchParamUnits records, for one ANN subrequest, the buckets of the
// ef, nprobe and limit the client asked for. searchParamStr is the "params"
// JSON; a key it does not carry falls into the omitted bucket. These are the
// requested values: the Proxy does not know which index serves the field, so
// a search on an HNSW field also reports nprobe as omitted, and the limit is
// the one before an iterator raises it.
func collectSearchParamUnits(searchParamStr string, limit int64, units *featureusage.Tally) {
	if units == nil {
		return
	}
	ef := gjson.Get(searchParamStr, "ef")
	units.Add(featureusage.EfFeature(ef.Int(), ef.Exists()))
	nprobe := gjson.Get(searchParamStr, "nprobe")
	units.Add(featureusage.NprobeFeature(nprobe.Int(), nprobe.Exists()))
	units.Add(featureusage.LimitFeature(limit))
}

// retrievalFeature classifies one ANN subrequest by what it searches: a dense
// vector field, a sparse one with vectors, a sparse one with text (full text
// search through a BM25 function), or a vector array in a struct, either as
// an embedding list or per element. The ArrayOfVector split follows
// classifyHybridSubSearch.
func retrievalFeature(schema *schemapb.CollectionSchema, fieldID int64, placeholderType commonpb.PlaceholderType) featureusage.Feature {
	switch typeutil.GetField(schema, fieldID).GetDataType() {
	case schemapb.DataType_SparseFloatVector:
		if placeholderType == commonpb.PlaceholderType_VarChar {
			return featureusage.FeatureRetrievalFullText
		}
		return featureusage.FeatureRetrievalSparse
	case schemapb.DataType_ArrayOfVector:
		if placeholderType == 0 || isEmbeddingListPlaceholderType(placeholderType) {
			return featureusage.FeatureRetrievalEmbeddingList
		}
		return featureusage.FeatureRetrievalElementLevel
	default:
		return featureusage.FeatureRetrievalDense
	}
}

// collectSubSearchUnits records the query vector count and the retrieval kind
// of one ANN subrequest, and returns the kind's hybrid family bit for the
// caller that combines subrequests.
func collectSubSearchUnits(schema *schemapb.CollectionSchema, fieldID int64, placeholderType commonpb.PlaceholderType, nq int64, units *featureusage.Tally) int {
	if units == nil {
		return 0
	}
	kind := retrievalFeature(schema, fieldID, placeholderType)
	units.Add(kind)
	units.Add(featureusage.NqFeature(nq))
	return featureusage.RetrievalFamily(kind)
}

// collectHybridShape marks, once per hybrid search, the subrequest count
// bucket and the combination of retrieval families.
func collectHybridShape(numSubReqs int, families int, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	set.Set(featureusage.HybridReqsFeature(numSubReqs))
	if f, ok := featureusage.HybridComboFeature(families); ok {
		set.Set(f)
	}
}

// recordQueryAggregationFeatures counts the aggregation operators of a query,
// once each. avg reaches the Proxy as a sum and a count sharing the original
// "avg(x)" name, so the operator is read back from that name.
func recordQueryAggregationFeatures(aggs []agg.AggregateBase) {
	if len(aggs) == 0 || !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	for _, a := range aggs {
		if _, op, _ := agg.MatchAggregationExpression(a.OriginalName()); op != "" {
			if f, ok := featureusage.QueryAggregationFeature(op); ok {
				set.Set(f)
			}
		}
	}
	set.HitAll()
}

// recordExecFeatures counts, once per user request, the execution features
// the QueryNodes report for it: the bit sets of every result, OR-ed, plus a
// cold read when any result scanned remote bytes. Only a request whose plan
// asked for the bits (see collectExecFeatureBits) is counted, so count must
// be the same decision.
func recordExecFeatures(count bool, bits uint64, scannedRemoteBytes int64) {
	if !count || !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	set.SetExecBits(bits)
	if scannedRemoteBytes > 0 {
		set.Set(featureusage.FeatureTieredStorageColdRead)
	}
	set.HitAll()
}

// collectExecFeatureBits asks segcore to record the execution features of a
// plan. Only a counted request pays for recording; every other plan leaves
// the option off and segcore records nothing.
func collectExecFeatureBits(plan *planpb.PlanNode, count bool) {
	if !count || plan == nil {
		return
	}
	if plan.PlanOptions == nil {
		plan.PlanOptions = &planpb.PlanOption{}
	}
	plan.PlanOptions.CollectFeatureBits = true
}

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
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
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
	GetGuaranteeTimestamp() uint64
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
	// Level 0 (Strong) together with a guarantee timestamp is the pre-2.3
	// protocol, where the timestamp was the request and the level field did
	// not exist yet: REST v1 and old SDKs send it, the server runs the
	// timestamp (task_search.go, "Compatibility logic"), and no level was
	// chosen. pymilvus sends an explicit Strong with timestamp 0.
	legacyTimestampOnly := r.GetConsistencyLevel() == commonpb.ConsistencyLevel_Strong && r.GetGuaranteeTimestamp() > 0
	if !r.GetUseDefaultConsistency() && !legacyTimestampOnly {
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

// collectSearchRequestFeatures marks the Search-only request fields, then the
// common ones. HybridSearch is folded into a SearchRequest before it reaches
// the search task, so this covers both.
func collectSearchRequestFeatures(req *milvuspb.SearchRequest, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	collectCommonRequestFeatures(req, set)
	collectFunctionScoreFeatures(req.GetFunctionScore(), set)
	collectFunctionChainFeatures(req.GetFunctionChains(), set)
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
		name := rerank.GetRerankName(fn)
		set.Set(featureusage.FunctionScoreFeature(name))
		// norm_score reaches a reranker as a function param on this path, which
		// is where parseWeightedParams and parseDecayParams read it from. The
		// model reranker's provider is read the way rerank.newModelFunction
		// reads it: key matched case-insensitively.
		for _, kv := range fn.GetParams() {
			switch {
			case kv.GetKey() == NormScoreKey && isTrueValue(kv.GetValue()):
				set.Set(featureusage.FeatureNormScore)
			case name == rerankModelName && strings.EqualFold(kv.GetKey(), rerankProviderKey):
				set.Set(featureusage.RerankProviderFeature(kv.GetValue()))
			}
		}
	}
}

// collectFunctionChainFeatures marks the reranker counters a function_chains
// request uses. A chain is the third client shape for reranking (after
// rank_params and function_score) and both searches and hybrid searches carry
// it on the request, so it is counted here with the other request fields
// rather than where each path builds its rerankMeta. The proto is read
// directly: the counting happens before validation, like the other hooks, and
// a chain the server then rejects still counts as asked for.
//
// A merge operator counts its strategy (rrf, weighted; max/sum/avg fold into
// _other); a map or filter operator counts the rerank function it evaluates
// (decay, rerank_model, boost_score); helper functions (num_combine,
// round_decimal) are arithmetic, not rerankers, and count nothing on their
// own. A chain that names no recognized reranker counts once as _other, so no
// chain goes uncounted.
func collectFunctionChainFeatures(chains []*schemapb.FunctionChain, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	for _, c := range chains {
		recognized := false
		for _, op := range c.GetOps() {
			var f featureusage.Feature
			var ok bool
			switch strings.TrimSpace(op.GetOp()) {
			case chaintypes.OpTypeMerge:
				f, ok = featureusage.MergeStrategyFeature(op.GetParams()[chain.MergeParamStrategy].GetStringValue())
			case chaintypes.OpTypeMap, chaintypes.OpTypeFilter:
				f, ok = featureusage.ChainFunctionFeature(op.GetExpr().GetName())
				if p, present := op.GetExpr().GetParams()[rerankProviderKey]; ok && f == featureusage.FeatureFunctionScoreModel && present {
					set.Set(featureusage.RerankProviderFeature(p.GetStringValue()))
				}
			}
			if ok {
				recognized = true
				set.Set(f)
			}
		}
		if !recognized {
			set.Set(featureusage.FeatureFunctionScoreOther)
		}
	}
}

const (
	// rerankModelName is the function_score reranker that calls a provider.
	rerankModelName = "model"
	// rerankProviderKey is its provider parameter (rerank.providerParamName).
	rerankProviderKey = "provider"
)

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
func collectSearchInfoFeatures(isIterator, isRangeSearch bool, groupByFieldID int64, isIteratorV2, iterativeFilter bool, set *featureusage.FeatureSet) {
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
	// The v1 search iterator pages with radius/range_filter of its own; a
	// range search is one the client asked for.
	if isRangeSearch && !isIterator {
		set.Set(featureusage.FeatureRangeSearch)
	}
	if groupByFieldID > 0 {
		set.Set(featureusage.FeatureGroupByField)
	}
	// The hint is also honored inside the "params" JSON, which is where the
	// REST API and pymilvus's param={"params": {...}} put it; the key scan
	// sees only a top-level hints key.
	if iterativeFilter {
		set.Set(featureusage.FeatureHints)
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
	if err != nil || strategy == "" {
		// REST v2 sends the key with an empty value when no ranker was given;
		// that is no ranker, not an unrecognized one.
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

// exprFeatureObserver returns the ParserVisitorArgs.OnParsedExpr hook that
// marks the expression-language features of every expression one parse
// produces: the user's filter and, on a search, each boost scorer's filter.
// It sees the parser's output before the rewriter, so what is counted is the
// operator the user wrote; the row-level-security predicate is merged in
// afterwards and is never seen. Nil when the request is not counted, which
// also spares the parser the call.
func exprFeatureObserver(set *featureusage.FeatureSet) func(*planpb.Expr) {
	if set == nil {
		return nil
	}
	return func(expr *planpb.Expr) { featureusage.CollectExprFeatures(expr, set) }
}

// collectExprTemplateFeatures marks the template-value features: any key is
// filter templating, except the JSON-stats switch, which counts only when it
// is on (the value is what planparserv2 reads; the key alone does nothing).
func collectExprTemplateFeatures(exprTemplateValues map[string]*schemapb.TemplateValue, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	for key, value := range exprTemplateValues {
		if key == common.ExprUseJSONStatsKey {
			if value.GetBoolVal() {
				set.Set(featureusage.FeatureExprUseJSONStats)
			}
		} else {
			set.Set(featureusage.FeatureExprTemplateValues)
		}
	}
}

// searchParamScope says which keys of a search_params list the server reads,
// so that a key a client puts where it has no effect is not counted.
type searchParamScope int

const (
	// scopeSearch: a plain search's params; every key is read.
	scopeSearch searchParamScope = iota
	// scopeHybridRequest: a hybrid search's rank_params; the grouping keys and
	// order_by_fields are read here, the per-ANN keys are not.
	scopeHybridRequest
	// scopeHybridSub: a hybrid sub-request's params; the per-ANN keys (hints,
	// analyzer_name, ignore_growing) are read here, the grouping keys are not.
	scopeHybridSub
)

// collectSearchParamKeyFeatures marks the search_params features in one pass
// over the key/value list: group_size, strict_group_size, rank_group_scorer,
// hints and analyzer_name by presence; ignore_growing by its effective value,
// because pymilvus sends the key (as "False") on every search. Marking into a
// shared set is what lets the hybrid path scan the rank_params and every
// sub-request without counting a key twice.
func collectSearchParamKeyFeatures(params []*commonpb.KeyValuePair, scope searchParamScope, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	grouping, perANN := scope != scopeHybridSub, scope != scopeHybridRequest
	for _, kv := range params {
		switch kv.GetKey() {
		case GroupSizeKey:
			if grouping {
				set.Set(featureusage.FeatureGroupSize)
			}
		case StrictGroupSize:
			if grouping {
				set.Set(featureusage.FeatureStrictGroupSize)
			}
		case RankGroupScorer:
			if grouping {
				set.Set(featureusage.FeatureRankGroupScorer)
			}
		case OrderByFieldsKey:
			if grouping {
				set.Set(featureusage.FeatureSearchOrderBy)
			}
		case common.HintsKey:
			// iterative_filter is the only documented hint; any other value is a
			// user string and folds into _other.
			if !perANN {
				continue
			}
			if kv.GetValue() == iterativeFilterKey {
				set.Set(featureusage.FeatureHints)
			} else {
				set.Set(featureusage.FeatureHintsOther)
			}
		case AnalyzerKey:
			if perANN {
				set.Set(featureusage.FeatureAnalyzerName)
			}
		case IgnoreGrowingKey:
			if perANN && isTrueValue(kv.GetValue()) {
				set.Set(featureusage.FeatureIgnoreGrowing)
			}
		}
	}
}

// collectQueryParamKeyFeatures marks the query_params features: group_by_fields
// and order_by_fields by presence, ignore_growing by its effective value.
func collectQueryParamKeyFeatures(params []*commonpb.KeyValuePair, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	for _, kv := range params {
		switch kv.GetKey() {
		case GroupByFieldsKey:
			set.Set(featureusage.FeatureQueryGroupByFields)
		case OrderByFieldsKey:
			set.Set(featureusage.FeatureQueryOrderBy)
		case IgnoreGrowingKey:
			if b, err := strconv.ParseBool(kv.GetValue()); err == nil && b {
				set.Set(featureusage.FeatureIgnoreGrowing)
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

// collectQueryIteratorFeature marks the old query iterator protocol.
func collectQueryIteratorFeature(isIterator bool, set *featureusage.FeatureSet) {
	if isIterator && set != nil {
		set.Set(featureusage.FeatureQueryIterator)
	}
}

// ExprFeatureObserver and RecordExprTemplateFeatures are the expression hooks
// for the dml package, whose delete path parses a plan outside the dql tasks
// and counts in one place: the observer marks into the set while the parser
// runs, and the record call adds the template features and flushes.
func ExprFeatureObserver(set *featureusage.FeatureSet) func(*planpb.Expr) {
	if !featureusage.Enabled() {
		return nil
	}
	return exprFeatureObserver(set)
}

func RecordExprTemplateFeatures(set *featureusage.FeatureSet, exprTemplateValues map[string]*schemapb.TemplateValue) {
	if !featureusage.Enabled() {
		return
	}
	collectExprTemplateFeatures(exprTemplateValues, set)
	set.HitAll()
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

// collectQueryAggregationFeatures marks the aggregation operators of a query.
// avg reaches the Proxy as a sum and a count sharing the original "avg(x)"
// name, so the operator is read back from that name.
func collectQueryAggregationFeatures(aggs []agg.AggregateBase, set *featureusage.FeatureSet) {
	if set == nil {
		return
	}
	for _, a := range aggs {
		if _, op, _ := agg.MatchAggregationExpression(a.OriginalName()); op != "" {
			if f, ok := featureusage.QueryAggregationFeature(op); ok {
				set.Set(f)
			}
		}
	}
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

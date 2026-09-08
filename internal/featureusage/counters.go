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
	"strings"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Feature identifies one request counter. The set of features is a
// compile-time constant: no request field, parameter value, collection,
// database, user, or time period may create a counter. This is what keeps
// the memory footprint fixed for the life of the process and makes cleanup
// unnecessary. Per-value counters (consistency level, function score type,
// highlighter type) enumerate the values the code recognizes and fold the
// rest into an "_other" slot.
type Feature int

const (
	FeatureGroupByField Feature = iota
	FeatureIterator
	FeatureSearchIterV2
	FeatureRangeSearch
	FeatureSearchByPrimaryKeys
	FeatureNamespace
	FeatureNotReturnAllMeta
	FeatureDeprecatedTravelTimestamp
	FeatureGroupSize
	FeatureStrictGroupSize
	FeatureRankGroupScorer
	FeatureQueryGroupByFields
	FeatureIgnoreGrowing
	FeatureHints
	FeatureHintsOther
	FeatureQueryIterator
	FeatureAnalyzerName
	FeatureOutputDynamicField
	FeatureOutputVectorField
	FeatureNormScore
	FeatureFragmentSize
	FeatureNumOfFragments

	FeatureConsistencyStrong
	FeatureConsistencySession
	FeatureConsistencyBounded
	FeatureConsistencyEventually
	FeatureConsistencyCustomized

	FeatureFunctionScoreRRF
	FeatureFunctionScoreWeighted
	FeatureFunctionScoreDecay
	FeatureFunctionScoreModel
	FeatureFunctionScoreBoost
	FeatureFunctionScoreOther

	FeatureRankStrategyRRF
	FeatureRankStrategyWeighted
	FeatureRankStrategyOther

	FeatureHighlighterLexical
	FeatureHighlighterSemantic

	// Authentication method, counted once per authenticated request on both
	// entry points. Counted after the credential verifies, so a rejected
	// attempt does not move a counter.
	FeatureAuthAPIKey
	FeatureAuthPassword

	// Expression-language features, set by CollectExprFeatures from the parsed
	// predicate tree.
	FeatureRandomSample
	FeaturePhraseMatch
	FeatureTextMatch
	FeatureLike
	FeatureRegexMatch
	FeatureExists
	FeatureIsNull
	FeatureIsNotNull
	FeatureJSONContains
	FeatureJSONIdentifier
	FeatureArrayContains
	FeatureArrayLength
	FeatureTimestamptzCompare
	FeatureElementFilter
	FeatureStructMatch
	FeatureGeoEquals
	FeatureGeoTouches
	FeatureGeoOverlaps
	FeatureGeoCrosses
	FeatureGeoContains
	FeatureGeoIntersects
	FeatureGeoWithin
	FeatureGeoDWithin
	FeatureGeoIsValid
	FeatureExprTemplateValues
	FeatureExprUseJSONStats
	// Ordinary scalar predicates, by logical operator. Counted once per user
	// request however many predicates of the kind the expression holds.
	FeatureScalarEquality
	FeatureScalarRange
	FeatureScalarIn

	// Search parameter distributions and retrieval kind, counted once per ANN
	// subrequest through a Tally: a hybrid search with three subrequests adds
	// three. Each distribution is a run of counters sharing one name, one per
	// fixed bucket. firstTallyFeature..lastTallyFeature must stay contiguous;
	// Tally covers only this range.
	FeatureEfOmitted
	FeatureEf16
	FeatureEf64
	FeatureEf256
	FeatureEf1024
	FeatureEfOver
	FeatureNprobeOmitted
	FeatureNprobe8
	FeatureNprobe32
	FeatureNprobe128
	FeatureNprobe1024
	FeatureNprobeOver
	FeatureLimit10
	FeatureLimit100
	FeatureLimit1000
	FeatureLimit16384
	FeatureLimitOver
	FeatureNq1
	FeatureNq10
	FeatureNq100
	FeatureNq1000
	FeatureNqOver
	FeatureRetrievalDense
	FeatureRetrievalSparse
	FeatureRetrievalFullText
	FeatureRetrievalEmbeddingList
	FeatureRetrievalElementLevel

	// Hybrid search shape, once per request: how many subrequests, and which
	// retrieval families they combine (dense, sparse, full text search), the
	// seven non-empty combinations in bitmask order.
	FeatureHybridReqs2
	FeatureHybridReqs3
	FeatureHybridReqs5
	FeatureHybridReqs10
	FeatureHybridReqsOver
	FeatureHybridDense
	FeatureHybridSparse
	FeatureHybridDenseSparse
	FeatureHybridFullText
	FeatureHybridDenseFullText
	FeatureHybridSparseFullText
	FeatureHybridDenseSparseFullText

	// Query aggregation and ordering, once per request.
	FeatureQueryAggCount
	FeatureQueryAggSum
	FeatureQueryAggMin
	FeatureQueryAggMax
	FeatureQueryAggAvg
	FeatureQueryOrderBy
	FeatureSearchOrderBy
	FeatureSearchAggregation

	// Upsert and delete modes, once per request.
	FeatureUpsertOverride
	FeatureUpsertMerge
	FeatureFieldOpReplace
	FeatureFieldOpArrayAppend
	FeatureFieldOpArrayRemove
	FeatureFieldOpPathReplace
	FeatureFieldOpOther
	FeatureUpsertFields1
	FeatureUpsertFields4
	FeatureUpsertFields16
	FeatureUpsertFieldsOver
	FeatureDeleteByIDs
	FeatureDeleteByFilter

	// Execution features: what segcore actually used to serve the request, as
	// opposed to what the request asked for. The QueryNodes report them as a
	// bit set on the results (see FeatureFromExecBit), the Proxy ORs the sets
	// of one request together and counts each feature once, so these are
	// Proxy counters.
	FeatureFilterPathScalarIndex
	FeatureFilterPathPkIndex
	FeatureFilterPathTextMatchIndex
	FeatureFilterPathJSONShredding
	FeatureFilterPathNgramIndex
	FeatureFilterPathBruteForce
	FeatureScalarIndexBitmap
	FeatureScalarIndexStlSort
	FeatureScalarIndexTrie
	FeatureScalarIndexInverted
	FeatureScalarIndexHybrid
	FeatureScalarIndexRtree
	FeatureScalarIndexNgram
	FeatureScalarIndexJSONFlat
	FeatureScalarIndexFmindex
	FeatureFilterIndexDeclined
	FeatureExprCacheHit
	FeatureInterimIndexSearch
	FeatureStrictGroupSizeEffective
	FeatureTieredStorageColdRead

	// DataCoord-side features, counted where import jobs and compaction tasks are created.
	FeatureImportJSON
	FeatureImportJSONLines
	FeatureImportNumpy
	FeatureImportParquet
	FeatureImportCSV
	FeatureCompactionMix
	FeatureCompactionL0Delete
	FeatureCompactionClustering
	FeatureCompactionSort
	FeatureCompactionPartitionKeySort
	FeatureCompactionClusteringPartitionKeySort
	FeatureCompactionBumpSchemaVersion
	FeatureCompactionOther

	// QueryNode execution-path features: decisions the query node takes that
	// neither the request nor the metadata show.
	FeatureTwoStageSearch
	FeatureSegmentPrune
	FeatureRunAnalyzer

	numFeatures // must be last
)

// #nosec G101 -- these are report entry names, not credential values; one of
// them names the username/password authentication method.
//
// featureNames maps a Feature to the entry name in the report. The array
// length is the feature count, so a Feature without a name is a compile
// error when the array literal is indexed by Feature and a missing entry is
// caught by TestFeatureNamesComplete.
var featureNames = [numFeatures]string{
	FeatureGroupByField:              "grouping_search",
	FeatureIterator:                  "search_iterator=v1",
	FeatureSearchIterV2:              "search_iterator=v2",
	FeatureRangeSearch:               "range_search",
	FeatureSearchByPrimaryKeys:       "primary_key_search",
	FeatureNamespace:                 "namespace",
	FeatureNotReturnAllMeta:          "not_return_all_meta",
	FeatureDeprecatedTravelTimestamp: "deprecated_travel_timestamp",
	FeatureGroupSize:                 "group_size",
	FeatureStrictGroupSize:           "strict_group_size",
	FeatureRankGroupScorer:           "rank_group_scorer",
	FeatureQueryGroupByFields:        "group_by_fields",
	FeatureIgnoreGrowing:             "ignore_growing",
	FeatureHints:                     "hints=iterative_filter",
	FeatureHintsOther:                "hints=" + OtherValue,
	FeatureQueryIterator:             "query_iterator",
	FeatureAnalyzerName:              "analyzer_name",
	FeatureOutputDynamicField:        "output_fields=dynamic",
	FeatureOutputVectorField:         "output_fields=vector",
	FeatureNormScore:                 "norm_score",
	FeatureFragmentSize:              "fragment_size",
	FeatureNumOfFragments:            "num_of_fragments",

	FeatureConsistencyStrong:     "consistency_level=Strong",
	FeatureConsistencySession:    "consistency_level=Session",
	FeatureConsistencyBounded:    "consistency_level=Bounded",
	FeatureConsistencyEventually: "consistency_level=Eventually",
	FeatureConsistencyCustomized: "consistency_level=Customized",

	FeatureFunctionScoreRRF:      "reranker=rrf",
	FeatureFunctionScoreWeighted: "reranker=weighted",
	FeatureFunctionScoreDecay:    "reranker=decay",
	FeatureFunctionScoreModel:    "reranker=model",
	FeatureFunctionScoreBoost:    "reranker=boost",
	FeatureFunctionScoreOther:    "reranker=" + OtherValue,

	FeatureRankStrategyRRF:      "strategy=rrf",
	FeatureRankStrategyWeighted: "strategy=weighted",
	FeatureRankStrategyOther:    "strategy=" + OtherValue,

	FeatureHighlighterLexical:  "highlighter=Lexical",
	FeatureHighlighterSemantic: "highlighter=Semantic",

	FeatureAuthAPIKey:   "auth_method=api_key",
	FeatureAuthPassword: "auth_method=password",

	FeatureRandomSample:       "random_sample",
	FeaturePhraseMatch:        "phrase_match",
	FeatureTextMatch:          "text_match",
	FeatureLike:               "like",
	FeatureRegexMatch:         "regex_match",
	FeatureExists:             "exists",
	FeatureIsNull:             "is_null",
	FeatureIsNotNull:          "is_not_null",
	FeatureJSONContains:       "json_contains",
	FeatureJSONIdentifier:     "json_path",
	FeatureArrayContains:      "array_contains",
	FeatureArrayLength:        "array_length",
	FeatureTimestamptzCompare: "timestamptz_compare",
	FeatureElementFilter:      "element_filter",
	FeatureStructMatch:        "struct_array_match",
	FeatureGeoEquals:          "st_equals",
	FeatureGeoTouches:         "st_touches",
	FeatureGeoOverlaps:        "st_overlaps",
	FeatureGeoCrosses:         "st_crosses",
	FeatureGeoContains:        "st_contains",
	FeatureGeoIntersects:      "st_intersects",
	FeatureGeoWithin:          "st_within",
	FeatureGeoDWithin:         "st_dwithin",
	FeatureGeoIsValid:         "st_isvalid",
	FeatureExprTemplateValues: "filter_templating",
	FeatureExprUseJSONStats:   "expr_use_json_stats",
	FeatureScalarEquality:     "comparison_operators=equality",
	FeatureScalarRange:        "comparison_operators=relational",
	FeatureScalarIn:           "in_operator",

	FeatureEfOmitted:     "ef",
	FeatureEf16:          "ef",
	FeatureEf64:          "ef",
	FeatureEf256:         "ef",
	FeatureEf1024:        "ef",
	FeatureEfOver:        "ef",
	FeatureNprobeOmitted: "nprobe",
	FeatureNprobe8:       "nprobe",
	FeatureNprobe32:      "nprobe",
	FeatureNprobe128:     "nprobe",
	FeatureNprobe1024:    "nprobe",
	FeatureNprobeOver:    "nprobe",
	FeatureLimit10:       "limit",
	FeatureLimit100:      "limit",
	FeatureLimit1000:     "limit",
	FeatureLimit16384:    "limit",
	FeatureLimitOver:     "limit",
	FeatureNq1:           "nq",
	FeatureNq10:          "nq",
	FeatureNq100:         "nq",
	FeatureNq1000:        "nq",
	FeatureNqOver:        "nq",

	FeatureRetrievalDense:         "retrieval=dense_vector",
	FeatureRetrievalSparse:        "retrieval=sparse_vector",
	FeatureRetrievalFullText:      "retrieval=full_text_search",
	FeatureRetrievalEmbeddingList: "retrieval=embedding_list",
	FeatureRetrievalElementLevel:  "retrieval=element_level",

	FeatureHybridReqs2:               "hybrid_search_reqs",
	FeatureHybridReqs3:               "hybrid_search_reqs",
	FeatureHybridReqs5:               "hybrid_search_reqs",
	FeatureHybridReqs10:              "hybrid_search_reqs",
	FeatureHybridReqsOver:            "hybrid_search_reqs",
	FeatureHybridDense:               "hybrid_search=dense_vector",
	FeatureHybridSparse:              "hybrid_search=sparse_vector",
	FeatureHybridDenseSparse:         "hybrid_search=dense_vector+sparse_vector",
	FeatureHybridFullText:            "hybrid_search=full_text_search",
	FeatureHybridDenseFullText:       "hybrid_search=dense_vector+full_text_search",
	FeatureHybridSparseFullText:      "hybrid_search=sparse_vector+full_text_search",
	FeatureHybridDenseSparseFullText: "hybrid_search=dense_vector+sparse_vector+full_text_search",

	FeatureQueryAggCount:     "query_aggregation=count",
	FeatureQueryAggSum:       "query_aggregation=sum",
	FeatureQueryAggMin:       "query_aggregation=min",
	FeatureQueryAggMax:       "query_aggregation=max",
	FeatureQueryAggAvg:       "query_aggregation=avg",
	FeatureQueryOrderBy:      "order_by",
	FeatureSearchOrderBy:     "order_by_fields",
	FeatureSearchAggregation: "search_aggregation",

	FeatureUpsertOverride:     "upsert_mode=override",
	FeatureUpsertMerge:        "upsert_mode=merge",
	FeatureFieldOpReplace:     "field_ops=REPLACE",
	FeatureFieldOpArrayAppend: "field_ops=ARRAY_APPEND",
	FeatureFieldOpArrayRemove: "field_ops=ARRAY_REMOVE",
	FeatureFieldOpPathReplace: "field_ops=PATH_REPLACE",
	FeatureFieldOpOther:       "field_ops=" + OtherValue,
	FeatureUpsertFields1:      "upsert_fields",
	FeatureUpsertFields4:      "upsert_fields",
	FeatureUpsertFields16:     "upsert_fields",
	FeatureUpsertFieldsOver:   "upsert_fields",
	FeatureDeleteByIDs:        "delete_mode=ids",
	FeatureDeleteByFilter:     "delete_mode=filter",

	FeatureFilterPathScalarIndex:    "filter_exec_path=scalar_index",
	FeatureFilterPathPkIndex:        "filter_exec_path=pk_index",
	FeatureFilterPathTextMatchIndex: "filter_exec_path=text_match_index",
	FeatureFilterPathJSONShredding:  "filter_exec_path=json_shredding",
	FeatureFilterPathNgramIndex:     "filter_exec_path=ngram_index",
	FeatureFilterPathBruteForce:     "filter_exec_path=brute_force",
	FeatureScalarIndexBitmap:        "scalar_index_type=BITMAP",
	FeatureScalarIndexStlSort:       "scalar_index_type=STL_SORT",
	FeatureScalarIndexTrie:          "scalar_index_type=Trie",
	FeatureScalarIndexInverted:      "scalar_index_type=INVERTED",
	FeatureScalarIndexHybrid:        "scalar_index_type=HYBRID",
	FeatureScalarIndexRtree:         "scalar_index_type=RTREE",
	FeatureScalarIndexNgram:         "scalar_index_type=NGRAM",
	FeatureScalarIndexJSONFlat:      "scalar_index_type=json_flat",
	FeatureScalarIndexFmindex:       "scalar_index_type=FMINDEX",
	FeatureFilterIndexDeclined:      "filter_index_declined",
	FeatureExprCacheHit:             "expr_cache_hit",
	FeatureInterimIndexSearch:       "interim_index_search",
	FeatureStrictGroupSizeEffective: "strict_group_size_effective",
	FeatureTieredStorageColdRead:    "tiered_storage_cold_read",

	FeatureImportJSON:      "import_file_type=JSON",
	FeatureImportJSONLines: "import_file_type=JSONLine",
	FeatureImportNumpy:     "import_file_type=NumPy",
	FeatureImportParquet:   "import_file_type=Parquet",
	FeatureImportCSV:       "import_file_type=CSV",

	FeatureCompactionMix:                        "compaction=mix",
	FeatureCompactionL0Delete:                   "compaction=l0",
	FeatureCompactionClustering:                 "compaction=clustering",
	FeatureCompactionSort:                       "compaction=sort",
	FeatureCompactionPartitionKeySort:           "compaction=partition_key_sort",
	FeatureCompactionClusteringPartitionKeySort: "compaction=clustering_partition_key_sort",
	FeatureCompactionBumpSchemaVersion:          "compaction=bump_schema_version",
	FeatureCompactionOther:                      "compaction=" + OtherValue,

	FeatureTwoStageSearch: "two_stage_search",
	FeatureSegmentPrune:   "segment_prune",
	FeatureRunAnalyzer:    "run_analyzer",
}

// Role is the node role that owns a counter. Every role's GetFeatureUsage
// returns only its own counters, so in standalone (all roles in one process,
// one shared array) the same slot is not reported twice.
//
// Import and compaction counters belong to MixCoord, where DataCoord creates
// the tasks, not to the DataNode that executes them: DataNodes are shared
// across instances in pooled deployments and must not report per-instance
// usage.
type Role int

const (
	RoleProxy Role = iota
	RoleMixCoord
	RoleQueryNode
)

// String names the role as it appears in the report and in the golden
// surface file. It is not the typeutil role constant; it only has to be
// stable and readable.
func (r Role) String() string {
	switch r {
	case RoleMixCoord:
		return "mixcoord"
	case RoleQueryNode:
		return "querynode"
	default:
		return "proxy"
	}
}

// featureRoles tags the MixCoord (DataCoord-side) counters; every other
// Feature is RoleProxy (the zero value).
var featureRoles = [numFeatures]Role{
	FeatureImportJSON:      RoleMixCoord,
	FeatureImportJSONLines: RoleMixCoord,
	FeatureImportNumpy:     RoleMixCoord,
	FeatureImportParquet:   RoleMixCoord,
	FeatureImportCSV:       RoleMixCoord,

	FeatureCompactionMix:                        RoleMixCoord,
	FeatureCompactionL0Delete:                   RoleMixCoord,
	FeatureCompactionClustering:                 RoleMixCoord,
	FeatureCompactionSort:                       RoleMixCoord,
	FeatureCompactionPartitionKeySort:           RoleMixCoord,
	FeatureCompactionClusteringPartitionKeySort: RoleMixCoord,
	FeatureCompactionBumpSchemaVersion:          RoleMixCoord,
	FeatureCompactionOther:                      RoleMixCoord,

	FeatureTwoStageSearch: RoleQueryNode,
	FeatureSegmentPrune:   RoleQueryNode,
	FeatureRunAnalyzer:    RoleQueryNode,
}

// Role returns the node role that reports f.
func (f Feature) Role() Role {
	if f < 0 || f >= numFeatures {
		return RoleProxy
	}
	return featureRoles[f]
}

// ImportFileTypeFeature maps an import file type name (importutilv2.FileType
// String()) to its counter. ok is false for an unknown or invalid type; such
// files are not counted rather than given a slot.
func ImportFileTypeFeature(fileType string) (f Feature, ok bool) {
	switch fileType {
	case "JSON":
		return FeatureImportJSON, true
	case "JSONLines":
		return FeatureImportJSONLines, true
	case "Numpy":
		return FeatureImportNumpy, true
	case "Parquet":
		return FeatureImportParquet, true
	case "CSV":
		return FeatureImportCSV, true
	default:
		return 0, false
	}
}

// CompactionTypeFeature maps a compaction plan type to its counter. Types the
// code does not recognize fold into FeatureCompactionOther.
func CompactionTypeFeature(t datapb.CompactionType) Feature {
	switch t {
	case datapb.CompactionType_MixCompaction:
		return FeatureCompactionMix
	case datapb.CompactionType_Level0DeleteCompaction:
		return FeatureCompactionL0Delete
	case datapb.CompactionType_ClusteringCompaction:
		return FeatureCompactionClustering
	case datapb.CompactionType_SortCompaction:
		return FeatureCompactionSort
	case datapb.CompactionType_PartitionKeySortCompaction:
		return FeatureCompactionPartitionKeySort
	case datapb.CompactionType_ClusteringPartitionKeySortCompaction:
		return FeatureCompactionClusteringPartitionKeySort
	case datapb.CompactionType_BumpSchemaVersionCompaction:
		return FeatureCompactionBumpSchemaVersion
	default:
		return FeatureCompactionOther
	}
}

// NumFeatures returns the size of the counter id set.
func NumFeatures() int { return int(numFeatures) }

// featureBuckets gives the bucket label of a counter that is one bucket of a
// distribution (search parameter values, subrequest counts). Several counters
// share a name and differ by bucket, which is how the report carries a
// histogram without a second entry shape. Empty for a plain counter.
var featureBuckets = [numFeatures]string{
	FeatureEfOmitted: BucketOmitted,
	FeatureEf16:      "<=16",
	FeatureEf64:      "17-64",
	FeatureEf256:     "65-256",
	FeatureEf1024:    "257-1024",
	FeatureEfOver:    ">1024",

	FeatureNprobeOmitted: BucketOmitted,
	FeatureNprobe8:       "<=8",
	FeatureNprobe32:      "9-32",
	FeatureNprobe128:     "33-128",
	FeatureNprobe1024:    "129-1024",
	FeatureNprobeOver:    ">1024",

	FeatureLimit10:    "<=10",
	FeatureLimit100:   "11-100",
	FeatureLimit1000:  "101-1000",
	FeatureLimit16384: "1001-16384",
	FeatureLimitOver:  ">16384",

	FeatureNq1:    "1",
	FeatureNq10:   "2-10",
	FeatureNq100:  "11-100",
	FeatureNq1000: "101-1000",
	FeatureNqOver: ">1000",

	FeatureHybridReqs2:    "<=2",
	FeatureHybridReqs3:    "3",
	FeatureHybridReqs5:    "4-5",
	FeatureHybridReqs10:   "6-10",
	FeatureHybridReqsOver: ">10",

	FeatureUpsertFields1:    "1",
	FeatureUpsertFields4:    "2-4",
	FeatureUpsertFields16:   "5-16",
	FeatureUpsertFieldsOver: ">16",
}

// BucketOmitted is the bucket of a search parameter distribution for a
// request that did not send the parameter.
const BucketOmitted = "omitted"

// bucketFeature returns the counter of the bucket v falls in, for a run of
// counters starting at first whose inclusive upper bounds are upper; the run
// has one more counter than upper, for the open-ended top bucket.
func bucketFeature(first Feature, upper []int64, v int64) Feature {
	for i, u := range upper {
		if v <= u {
			return first + Feature(i)
		}
	}
	return first + Feature(len(upper))
}

var (
	efUpper          = []int64{16, 64, 256, 1024}
	nprobeUpper      = []int64{8, 32, 128, 1024}
	limitUpper       = []int64{10, 100, 1000, 16384}
	nqUpper          = []int64{1, 10, 100, 1000}
	hybridReqsUpper  = []int64{2, 3, 5, 10}
	upsertFieldUpper = []int64{1, 4, 16}
)

// EfFeature returns the ef bucket for a search that sent ef (ok) or not.
func EfFeature(v int64, ok bool) Feature {
	if !ok {
		return FeatureEfOmitted
	}
	return bucketFeature(FeatureEf16, efUpper, v)
}

// NprobeFeature returns the nprobe bucket for a search that sent nprobe (ok) or not.
func NprobeFeature(v int64, ok bool) Feature {
	if !ok {
		return FeatureNprobeOmitted
	}
	return bucketFeature(FeatureNprobe8, nprobeUpper, v)
}

// LimitFeature returns the bucket of the limit a search asked for.
func LimitFeature(v int64) Feature { return bucketFeature(FeatureLimit10, limitUpper, v) }

// NqFeature returns the bucket of a search's query vector count.
func NqFeature(v int64) Feature { return bucketFeature(FeatureNq1, nqUpper, v) }

// HybridReqsFeature returns the bucket of a hybrid search's subrequest count.
func HybridReqsFeature(n int) Feature {
	return bucketFeature(FeatureHybridReqs2, hybridReqsUpper, int64(n))
}

// Retrieval families a hybrid search combines, as bits of HybridComboFeature.
const (
	HybridFamilyDense = 1 << iota
	HybridFamilySparse
	HybridFamilyFullText
)

// HybridComboFeature returns the counter of a non-empty combination of
// retrieval families; ok is false for the empty set.
func HybridComboFeature(families int) (f Feature, ok bool) {
	families &= HybridFamilyDense | HybridFamilySparse | HybridFamilyFullText
	if families == 0 {
		return 0, false
	}
	return FeatureHybridDense + Feature(families-1), true
}

// RetrievalFamily returns the hybrid family bit of a retrieval-kind counter.
// Embedding-list and element-level searches are dense vector searches.
func RetrievalFamily(f Feature) int {
	switch f {
	case FeatureRetrievalSparse:
		return HybridFamilySparse
	case FeatureRetrievalFullText:
		return HybridFamilyFullText
	case FeatureRetrievalDense, FeatureRetrievalEmbeddingList, FeatureRetrievalElementLevel:
		return HybridFamilyDense
	default:
		return 0
	}
}

// QueryAggregationFeature maps an aggregation operator name as
// agg.MatchAggregationExpression returns it to its counter.
func QueryAggregationFeature(op string) (f Feature, ok bool) {
	switch op {
	case "count":
		return FeatureQueryAggCount, true
	case "sum":
		return FeatureQueryAggSum, true
	case "min":
		return FeatureQueryAggMin, true
	case "max":
		return FeatureQueryAggMax, true
	case "avg":
		return FeatureQueryAggAvg, true
	default:
		return 0, false
	}
}

// FieldOpFeature maps a partial-update operator to its counter.
func FieldOpFeature(op schemapb.FieldPartialUpdateOp_OpType) Feature {
	switch op {
	case schemapb.FieldPartialUpdateOp_REPLACE:
		return FeatureFieldOpReplace
	case schemapb.FieldPartialUpdateOp_ARRAY_APPEND:
		return FeatureFieldOpArrayAppend
	case schemapb.FieldPartialUpdateOp_ARRAY_REMOVE:
		return FeatureFieldOpArrayRemove
	case schemapb.FieldPartialUpdateOp_PATH_REPLACE:
		return FeatureFieldOpPathReplace
	default:
		return FeatureFieldOpOther
	}
}

// UpsertFieldsFeature returns the bucket of the number of fields an upsert carries.
func UpsertFieldsFeature(n int) Feature {
	return bucketFeature(FeatureUpsertFields1, upsertFieldUpper, int64(n))
}

// Bucket returns the bucket label of f, or "" for a plain counter.
func (f Feature) Bucket() string {
	if f < 0 || f >= numFeatures {
		return ""
	}
	return featureBuckets[f]
}

// Name returns the report entry name of f.
func (f Feature) Name() string {
	if f < 0 || f >= numFeatures {
		return ""
	}
	return featureNames[f]
}

// ConsistencyLevelFeature maps a per-request consistency level to its counter.
// ok is false for a level the code does not recognize; such requests are not
// counted rather than given a new slot.
func ConsistencyLevelFeature(level commonpb.ConsistencyLevel) (f Feature, ok bool) {
	switch level {
	case commonpb.ConsistencyLevel_Strong:
		return FeatureConsistencyStrong, true
	case commonpb.ConsistencyLevel_Session:
		return FeatureConsistencySession, true
	case commonpb.ConsistencyLevel_Bounded:
		return FeatureConsistencyBounded, true
	case commonpb.ConsistencyLevel_Eventually:
		return FeatureConsistencyEventually, true
	case commonpb.ConsistencyLevel_Customized:
		return FeatureConsistencyCustomized, true
	default:
		return 0, false
	}
}

// FunctionScoreFeature maps a rerank function name (as returned by
// rerank.GetRerankName, i.e. a lowercased user string) to its counter. Any
// name the code does not recognize folds into FeatureFunctionScoreOther.
func FunctionScoreFeature(name string) Feature {
	switch strings.ToLower(name) {
	case "rrf":
		return FeatureFunctionScoreRRF
	case "weighted":
		return FeatureFunctionScoreWeighted
	case "decay":
		return FeatureFunctionScoreDecay
	case "model":
		return FeatureFunctionScoreModel
	case "boost":
		return FeatureFunctionScoreBoost
	default:
		return FeatureFunctionScoreOther
	}
}

// RankStrategyFeature maps the legacy rank_params "strategy" value (a raw user
// string; the Proxy does not validate it) to its counter, folding anything
// unrecognized into FeatureRankStrategyOther.
func RankStrategyFeature(strategy string) Feature {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case "rrf":
		return FeatureRankStrategyRRF
	case "weighted":
		return FeatureRankStrategyWeighted
	default:
		return FeatureRankStrategyOther
	}
}

// HighlighterFeature maps a highlighter type to its counter.
func HighlighterFeature(t commonpb.HighlightType) (f Feature, ok bool) {
	switch t {
	case commonpb.HighlightType_Lexical:
		return FeatureHighlighterLexical, true
	case commonpb.HighlightType_Semantic:
		return FeatureHighlighterSemantic, true
	default:
		return 0, false
	}
}

// slot is one counter: the cumulative hit count and the unix second of the
// most recent hit. Both are monotonic for the life of the process.
type slot struct {
	value      atomic.Int64
	lastUsedAt atomic.Int64
}

// Counters is the fixed-size request counter array of one process.
type Counters struct {
	slots [numFeatures]slot
	now   func() int64
}

// NewCounters returns an empty counter array using the wall clock.
func NewCounters() *Counters {
	return &Counters{now: func() int64 { return time.Now().Unix() }}
}

// newCountersWithClock is for tests.
func newCountersWithClock(now func() int64) *Counters {
	return &Counters{now: now}
}

// Hit records one use of f. Cost: one atomic add, one clock read, and an
// atomic store of the timestamp only when the stored second differs from the
// current one, so under load each counter takes at most one store per second.
func (c *Counters) Hit(f Feature) {
	if f < 0 || f >= numFeatures {
		return
	}
	s := &c.slots[f]
	s.value.Add(1)
	advanceTo(&s.lastUsedAt, c.now())
}

// HitN records n uses of f at once, for a request that used a feature several
// times in a unit the report counts separately, such as one ANN subrequest of
// a hybrid search each. One atomic add and one timestamp advance, as for Hit.
func (c *Counters) HitN(f Feature, n int64) {
	if f < 0 || f >= numFeatures || n <= 0 {
		return
	}
	s := &c.slots[f]
	s.value.Add(n)
	advanceTo(&s.lastUsedAt, c.now())
}

// advanceTo raises ts to now and never lowers it. A caller that read the clock
// and was then descheduled must not overwrite a later second another hit has
// already stored, so a plain store is wrong; a compare-and-swap that only moves
// forward is not. The first comparison is also the fast path: under steady load
// every hit after the first in a given second sees ts >= now and returns
// without writing, so a counter still takes at most one store per second.
func advanceTo(ts *atomic.Int64, now int64) {
	for {
		prev := ts.Load()
		if prev >= now {
			return
		}
		if ts.CompareAndSwap(prev, now) {
			return
		}
	}
}

// Snapshot returns every counter as a GroupRequest entry. Counters that were
// never hit are present with value 0 and last_used_at 0: an entry that is
// present with zero means "this build has the counter and it was not used";
// an absent entry means "this build has no such counter". A snapshot does
// not modify any counter.
func (c *Counters) Snapshot() []*internalpb.FeatureEntry {
	return c.snapshot(func(Feature) bool { return true })
}

// SnapshotFor returns the counters owned by role, see Role.
func (c *Counters) SnapshotFor(role Role) []*internalpb.FeatureEntry {
	return c.snapshot(func(f Feature) bool { return featureRoles[f] == role })
}

func (c *Counters) snapshot(include func(Feature) bool) []*internalpb.FeatureEntry {
	entries := make([]*internalpb.FeatureEntry, 0, numFeatures)
	for f := Feature(0); f < numFeatures; f++ {
		if !include(f) {
			continue
		}
		s := &c.slots[f]
		entries = append(entries, &internalpb.FeatureEntry{
			Group:      GroupRequest,
			Name:       featureNames[f],
			Bucket:     featureBuckets[f],
			Value:      s.value.Load(),
			LastUsedAt: s.lastUsedAt.Load(),
		})
	}
	return entries
}

var (
	defaultCounters = NewCounters()
	enabled         atomic.Bool
)

func init() {
	enabled.Store(true)
}

// SetEnabled turns request counting on or off for the process. When off,
// Hit is a single load-and-branch and Snapshot still returns the (frozen)
// counters.
func SetEnabled(on bool) { enabled.Store(on) }

// Enabled reports whether request counting is on.
func Enabled() bool { return enabled.Load() }

// Hit records one use of f on the process-wide counters.
func Hit(f Feature) {
	if !enabled.Load() {
		return
	}
	defaultCounters.Hit(f)
}

// Snapshot returns the process-wide counters as report entries.
func Snapshot() []*internalpb.FeatureEntry {
	return defaultCounters.Snapshot()
}

// SnapshotFor returns the process-wide counters owned by role.
func SnapshotFor(role Role) []*internalpb.FeatureEntry {
	return defaultCounters.SnapshotFor(role)
}

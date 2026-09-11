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
	FeatureAnalyzerName
	FeatureOutputDynamicField
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
	FeatureBruteForceSearch

	numFeatures // must be last
)

// featureNames maps a Feature to the entry name in the report. The array
// length is the feature count, so a Feature without a name is a compile
// error when the array literal is indexed by Feature and a missing entry is
// caught by TestFeatureNamesComplete.
var featureNames = [numFeatures]string{
	FeatureGroupByField:              "group_by_field",
	FeatureIterator:                  "iterator",
	FeatureSearchIterV2:              "search_iter_v2",
	FeatureRangeSearch:               "radius",
	FeatureSearchByPrimaryKeys:       "search_by_primary_keys",
	FeatureNamespace:                 "namespace",
	FeatureNotReturnAllMeta:          "not_return_all_meta",
	FeatureDeprecatedTravelTimestamp: "deprecated_travel_timestamp",
	FeatureGroupSize:                 "group_size",
	FeatureStrictGroupSize:           "strict_group_size",
	FeatureRankGroupScorer:           "rank_group_scorer",
	FeatureQueryGroupByFields:        "group_by_fields",
	FeatureIgnoreGrowing:             "ignore_growing",
	FeatureHints:                     "hints",
	FeatureAnalyzerName:              "analyzer_name",
	FeatureOutputDynamicField:        "output_dynamic_field",
	FeatureNormScore:                 "norm_score",
	FeatureFragmentSize:              "fragment_size",
	FeatureNumOfFragments:            "num_of_fragments",

	FeatureConsistencyStrong:     "consistency_level=Strong",
	FeatureConsistencySession:    "consistency_level=Session",
	FeatureConsistencyBounded:    "consistency_level=Bounded",
	FeatureConsistencyEventually: "consistency_level=Eventually",
	FeatureConsistencyCustomized: "consistency_level=Customized",

	FeatureFunctionScoreRRF:      "function_score=rrf",
	FeatureFunctionScoreWeighted: "function_score=weighted",
	FeatureFunctionScoreDecay:    "function_score=decay",
	FeatureFunctionScoreModel:    "function_score=model",
	FeatureFunctionScoreBoost:    "function_score=boost",
	FeatureFunctionScoreOther:    "function_score=" + OtherValue,

	FeatureRankStrategyRRF:      "strategy=rrf",
	FeatureRankStrategyWeighted: "strategy=weighted",
	FeatureRankStrategyOther:    "strategy=" + OtherValue,

	FeatureHighlighterLexical:  "highlighter=Lexical",
	FeatureHighlighterSemantic: "highlighter=Semantic",

	FeatureRandomSample:       "random_sample",
	FeaturePhraseMatch:        "phrase_match",
	FeatureTextMatch:          "text_match",
	FeatureLike:               "like",
	FeatureRegexMatch:         "regex_match",
	FeatureExists:             "exists",
	FeatureIsNull:             "is_null",
	FeatureIsNotNull:          "is_not_null",
	FeatureJSONContains:       "json_contains",
	FeatureJSONIdentifier:     "json_identifier",
	FeatureArrayContains:      "array_contains",
	FeatureArrayLength:        "array_length",
	FeatureTimestamptzCompare: "timestamptz_compare",
	FeatureElementFilter:      "element_filter",
	FeatureStructMatch:        "struct_match",
	FeatureGeoEquals:          "st_equals",
	FeatureGeoTouches:         "st_touches",
	FeatureGeoOverlaps:        "st_overlaps",
	FeatureGeoCrosses:         "st_crosses",
	FeatureGeoContains:        "st_contains",
	FeatureGeoIntersects:      "st_intersects",
	FeatureGeoWithin:          "st_within",
	FeatureGeoDWithin:         "st_dwithin",
	FeatureGeoIsValid:         "st_isvalid",
	FeatureExprTemplateValues: "expr_template_values",
	FeatureExprUseJSONStats:   "expr_use_json_stats",

	FeatureImportJSON:      "import_file_type=JSON",
	FeatureImportJSONLines: "import_file_type=JSONLines",
	FeatureImportNumpy:     "import_file_type=Numpy",
	FeatureImportParquet:   "import_file_type=Parquet",
	FeatureImportCSV:       "import_file_type=CSV",

	FeatureCompactionMix:                        "compaction=MixCompaction",
	FeatureCompactionL0Delete:                   "compaction=Level0DeleteCompaction",
	FeatureCompactionClustering:                 "compaction=ClusteringCompaction",
	FeatureCompactionSort:                       "compaction=SortCompaction",
	FeatureCompactionPartitionKeySort:           "compaction=PartitionKeySortCompaction",
	FeatureCompactionClusteringPartitionKeySort: "compaction=ClusteringPartitionKeySortCompaction",
	FeatureCompactionBumpSchemaVersion:          "compaction=BumpSchemaVersionCompaction",
	FeatureCompactionOther:                      "compaction=" + OtherValue,

	FeatureTwoStageSearch:   "two_stage_search",
	FeatureSegmentPrune:     "segment_prune",
	FeatureRunAnalyzer:      "run_analyzer",
	FeatureBruteForceSearch: "brute_force_search",
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

	FeatureTwoStageSearch:   RoleQueryNode,
	FeatureSegmentPrune:     RoleQueryNode,
	FeatureRunAnalyzer:      RoleQueryNode,
	FeatureBruteForceSearch: RoleQueryNode,
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
	now := c.now()
	if s.lastUsedAt.Load() != now {
		s.lastUsedAt.Store(now)
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

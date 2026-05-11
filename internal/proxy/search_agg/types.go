package search_agg

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
)

const (
	ScoreFieldID    int64 = -1
	CountAllFieldID int64 = 0
)

type SearchAggregationContext struct {
	NQ int64

	Levels []LevelContext

	// UserOutputFieldIDs records the fields the user explicitly requested in
	// output_fields. Populated by the caller after construction. Only these
	// appear in the final HitResult.Fields.
	UserOutputFieldIDs map[int64]struct{}

	// DerivedTopK is the number of distinct composite keys each NQ retains
	// downstream, equal to the product of every level's Size (empty / zero
	// levels treated as 1). Mirrors ES `terms.size` nested product.
	DerivedTopK int64

	// DerivedGroupSize is the per-composite-key doc retention budget. Equal to
	// the max TopHits.Size across all levels, or 1 when TopHits is absent. This
	// keeps enough rows per full composite key for proxy-side top_hits at any
	// aggregation level.
	DerivedGroupSize int64

	// groupByFieldIDs is the union of all Levels[*].OwnFieldIDs, derived at
	// construction time. Values for these fields are read from
	// SearchResultData.group_by_field_values (written by segcore's composite
	// group-by path).
	groupByFieldIDs map[int64]struct{}

	// allGroupByFieldIDsOrdered is the full composite-key field list in
	// OwnFieldIDs order across all levels; used by proxy-side cross-shard
	// groupReduce so the composite matches what delegator used.
	allGroupByFieldIDsOrdered []int64

	// extraOutputFieldIDs are non-group-by fields the proxy needs from
	// fields_data (metric sources, top_hits sort). They must be appended to
	// SearchRequest.OutputFieldsId by the caller so segcore writes them.
	// Stored sorted for deterministic downstream behavior.
	extraOutputFieldIDs []int64
}

// IsGroupByField reports whether fieldID appears in any level's OwnFieldIDs.
func (c *SearchAggregationContext) IsGroupByField(fieldID int64) bool {
	_, ok := c.groupByFieldIDs[fieldID]
	return ok
}

// ExtraOutputFieldIDs returns non-group-by field IDs the caller must append to
// SearchRequest.OutputFieldsId.
func (c *SearchAggregationContext) ExtraOutputFieldIDs() []int64 {
	return c.extraOutputFieldIDs
}

// AllGroupByFieldIDs returns the flattened composite-key field list in the
// order defined by each level's OwnFieldIDs. Used by proxy-side cross-shard
// groupReduce to build composite keys consistent with the delegator.
func (c *SearchAggregationContext) AllGroupByFieldIDs() []int64 {
	return c.allGroupByFieldIDsOrdered
}

type LevelContext struct {
	OwnFieldIDs []int64
	Metrics     map[string]MetricSpec
	// metricPlans mirror Metrics so computeLevel can delegate state lifecycle to internal/agg.
	metricPlans []metricPlan
	TopHits     *TopHitsConfig
	Order       []OrderCriterion
	Size        int64
}

type MetricSpec struct {
	Op        string
	FieldID   int64
	FieldType schemapb.DataType
}

// metricPlan is the executable form of a MetricSpec.
type metricPlan struct {
	alias     string
	spec      MetricSpec
	aggregate agg.AggregateBase
}

type TopHitsConfig struct {
	Size int64
	Sort []SortCriterion
}

type SortCriterion struct {
	FieldID   int64
	Dir       string
	NullFirst bool
}

type OrderCriterion struct {
	Key string
	Dir string
}

type AggBucketResult struct {
	Key   map[int64]any
	Count int64
	// Metrics holds finalized aggregate values; any keeps string min/max and numeric results lossless.
	Metrics       map[string]any
	Hits          []*HitResult
	SubAggBuckets []*AggBucketResult
}

type HitResult struct {
	PK     any
	Score  float32
	Fields map[int64]any
}

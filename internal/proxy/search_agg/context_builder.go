package search_agg

import (
	"math"
	"sort"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func BuildSearchAggregationContext(
	groupBy *commonpb.SearchAggregationSpec,
	schema *schemapb.CollectionSchema,
	nq int64,
) (*SearchAggregationContext, error) {
	resolved, err := resolveAggregationSpec(groupBy, schema)
	if err != nil {
		return nil, err
	}
	topK, groupSize, err := deriveTopKAndGroupSizeChecked(resolved.levels)
	if err != nil {
		return nil, err
	}
	maxEntries := paramtable.Get().ProxyCfg.MaxSearchAggregationResultEntries.GetAsInt64()
	if err := validateSearchAggregationResultEntries(nq, topK, groupSize, maxEntries); err != nil {
		return nil, err
	}
	return NewContext(nq, resolved.levels, nil, resolved.extraOutputFieldIDs)
}

// NewContext assembles a SearchAggregationContext from already-resolved levels.
// Used by BuildSearchAggregationContext and by unit tests that want to drive
// the computer without parsing a real SearchAggregationSpec.
//
// groupByFieldIDs is derived from levels.OwnFieldIDs; extraOutputFieldIDs is
// sanitized (group-by fields removed) and sorted for deterministic ordering.
func NewContext(
	nq int64,
	levels []LevelContext,
	userOutputFieldIDs []int64,
	extraOutputFieldIDs []int64,
) (*SearchAggregationContext, error) {
	groupBy := make(map[int64]struct{})
	allGroupBy := make([]int64, 0)
	for _, level := range levels {
		for _, id := range level.OwnFieldIDs {
			if _, dup := groupBy[id]; dup {
				continue
			}
			groupBy[id] = struct{}{}
			allGroupBy = append(allGroupBy, id)
		}
	}

	extra := make([]int64, 0, len(extraOutputFieldIDs))
	seen := make(map[int64]struct{}, len(extraOutputFieldIDs))
	for _, id := range extraOutputFieldIDs {
		if _, isGroupBy := groupBy[id]; isGroupBy {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		extra = append(extra, id)
	}
	sort.Slice(extra, func(i, j int) bool { return extra[i] < extra[j] })

	userOutput := make(map[int64]struct{}, len(userOutputFieldIDs))
	for _, id := range userOutputFieldIDs {
		userOutput[id] = struct{}{}
	}

	topK, groupSize := deriveTopKAndGroupSize(levels)

	// Compile metricPlans for any level where the caller populated Metrics
	// but not metricPlans (typical for tests that handcraft Metrics).
	for i := range levels {
		if len(levels[i].Metrics) == 0 || len(levels[i].metricPlans) > 0 {
			continue
		}
		plans, err := compileMetricPlans(levels[i].Metrics)
		if err != nil {
			return nil, merr.Wrapf(err, "level %d metric compile failed", i)
		}
		levels[i].metricPlans = plans
	}

	return &SearchAggregationContext{
		NQ:                        nq,
		Levels:                    levels,
		UserOutputFieldIDs:        userOutput,
		DerivedTopK:               topK,
		DerivedGroupSize:          groupSize,
		groupByFieldIDs:           groupBy,
		allGroupByFieldIDsOrdered: allGroupBy,
		extraOutputFieldIDs:       extra,
	}, nil
}

func buildMetricPlan(alias string, spec MetricSpec) (metricPlan, error) {
	if spec.Op == "avg" {
		if err := agg.ValidateAggFieldType(spec.Op, spec.FieldType); err != nil {
			return metricPlan{}, err
		}
		return metricPlan{alias: alias, spec: spec, aggregate: agg.NewAvgAggregate(spec.FieldID, alias)}, nil
	}

	aggregates, err := agg.NewAggregate(spec.Op, spec.FieldID, alias, spec.FieldType)
	if err != nil {
		return metricPlan{}, err
	}
	return metricPlan{alias: alias, spec: spec, aggregate: aggregates[0]}, nil
}

// compileMetricPlans turns user-populated Metrics into the executable plan
// form used by the computer. Shared between BuildSearchAggregationContext and
// tests that bypass spec parsing.
func compileMetricPlans(metrics map[string]MetricSpec) ([]metricPlan, error) {
	aliases := make([]string, 0, len(metrics))
	for alias := range metrics {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	plans := make([]metricPlan, 0, len(aliases))
	for _, alias := range aliases {
		plan, err := buildMetricPlan(alias, metrics[alias])
		if err != nil {
			return nil, merr.Wrapf(err, "metric %q", alias)
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

// deriveTopKAndGroupSize maps the ES-style nested aggregation sizes into the
// (topK, groupSize) pair consumed by the Delegator/QN group-reduce algorithm:
//
//	topK      = product of every level's SearchSize
//	groupSize = max TopHits.Size across all levels, or 1 when TopHits is absent
//
// groupSize is global because upstream group-reduce keeps rows per full
// composite key before proxy-side aggregation computes top_hits at any level.
func normalizeAggregationSize(size int64) int64 {
	if size <= 0 {
		return 1
	}
	return size
}

func candidateSize(level LevelContext) int64 {
	if level.SearchSize > 0 {
		return level.SearchSize
	}
	return normalizeAggregationSize(level.Size)
}

func deriveTopKAndGroupSize(levels []LevelContext) (topK, groupSize int64) {
	topK = 1
	for _, lvl := range levels {
		topK *= candidateSize(lvl)
	}
	return topK, deriveGroupSize(levels)
}

func deriveTopKAndGroupSizeChecked(levels []LevelContext) (topK, groupSize int64, err error) {
	topK = 1
	for _, lvl := range levels {
		var ok bool
		topK, ok = checkedMulInt64(topK, candidateSize(lvl))
		if !ok {
			return 0, 0, merr.WrapErrParameterInvalidMsg("search_aggregation derived topK overflows int64")
		}
	}
	return topK, deriveGroupSize(levels), nil
}

func deriveGroupSize(levels []LevelContext) int64 {
	groupSize := int64(1)
	for _, lvl := range levels {
		if lvl.TopHits != nil {
			groupSize = max(groupSize, normalizeAggregationSize(lvl.TopHits.Size))
		}
	}
	return groupSize
}

func validateSearchAggregationResultEntries(nq, topK, groupSize, maxEntries int64) error {
	if maxEntries <= 0 {
		return nil
	}
	nqTopK, ok := checkedMulInt64(nq, topK)
	if !ok {
		return merr.WrapErrParameterInvalidMsg("number of search_aggregation result entries is too large")
	}
	entries, ok := checkedMulInt64(nqTopK, groupSize)
	if !ok {
		return merr.WrapErrParameterInvalidMsg("number of search_aggregation result entries is too large")
	}
	if entries > maxEntries {
		return merr.WrapErrParameterInvalidMsg("number of search_aggregation result entries is too large")
	}
	return nil
}

func checkedMulInt64(a, b int64) (int64, bool) {
	if a < 0 || b < 0 {
		return 0, false
	}
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > math.MaxInt64/b {
		return 0, false
	}
	return a * b, true
}

type resolvedAggregationSpec struct {
	levels          []LevelContext
	groupByFieldIDs []int64
	// extraOutputFieldIDs are non-group-by fields proxy needs from fields_data:
	// metric source fields and top_hits sort fields. These must be appended to
	// SearchRequest.OutputFieldsId so segcore writes them into fields_data.
	extraOutputFieldIDs []int64
}

// maxAggregationLevels caps SearchAggregation nesting depth to keep proxy
// validation and downstream segcore topK/groupSize derivation bounded against
// malformed or abusive specs. 4 covers every real use (ES best practice is
// <=3); increase only if a concrete use case appears.
const maxAggregationLevels = 4

func resolveAggregationSpec(groupBy *commonpb.SearchAggregationSpec, schema *schemapb.CollectionSchema) (*resolvedAggregationSpec, error) {
	if groupBy == nil {
		return nil, merr.WrapErrParameterInvalidMsg("group_by spec is nil")
	}
	if schema == nil {
		return nil, merr.WrapErrParameterInvalidMsg("collection schema is nil")
	}

	depth := 0
	for cur := groupBy; cur != nil; cur = cur.GetSubAggregation() {
		depth++
		if depth > maxAggregationLevels {
			return nil, merr.WrapErrParameterInvalidMsg("search_aggregation nesting exceeds max %d levels", maxAggregationLevels)
		}
	}

	dynamicField := findDynamicField(schema)
	groupBySeen := make(map[int64]struct{})
	extraSeen := make(map[int64]struct{})

	resolved := &resolvedAggregationSpec{}
	var walk func(spec *commonpb.SearchAggregationSpec) error
	walk = func(spec *commonpb.SearchAggregationSpec) error {
		if spec == nil {
			return nil
		}

		if len(spec.GetFields()) == 0 {
			return merr.WrapErrParameterInvalidMsg("group_by level has no fields")
		}
		if spec.GetSize() < 0 {
			return merr.WrapErrParameterInvalidMsg("search_aggregation size must be non-negative")
		}
		if spec.GetSearchSize() < 0 {
			return merr.WrapErrParameterInvalidMsg("search_aggregation search_size must be non-negative")
		}

		levelSize := normalizeAggregationSize(spec.GetSize())
		searchSize := spec.GetSearchSize()
		if searchSize == 0 {
			searchSize = levelSize
		}
		if searchSize < levelSize {
			return merr.WrapErrParameterInvalidMsg("search_aggregation search_size must be greater than or equal to size")
		}

		level := LevelContext{
			OwnFieldIDs: make([]int64, 0, len(spec.GetFields())),
			Size:        levelSize,
			SearchSize:  searchSize,
		}

		levelFieldSeen := make(map[int64]struct{})
		for _, fieldName := range spec.GetFields() {
			fieldID, err := resolveFieldID(fieldName, schema, dynamicField)
			if err != nil {
				return merr.Wrapf(err, "invalid group_by field %q", fieldName)
			}
			field, err := validateSearchAggregationFieldSupport(fieldName, fieldID, schema, "group_by field")
			if err != nil {
				return err
			}
			if field != nil {
				if field.GetDataType() == schemapb.DataType_Float || field.GetDataType() == schemapb.DataType_Double {
					return merr.WrapErrParameterInvalidMsg("group_by field %q: FLOAT / DOUBLE fields are not supported with search_aggregation (BucketKeyEntry has no float variant; equality on floats is fragile)", fieldName)
				}
			}
			if _, ok := levelFieldSeen[fieldID]; ok {
				return merr.WrapErrParameterInvalidMsg("duplicated group_by field %q in one level", fieldName)
			}
			if _, ok := groupBySeen[fieldID]; ok {
				return merr.WrapErrParameterInvalidMsg("duplicated group_by field %q across levels", fieldName)
			}
			levelFieldSeen[fieldID] = struct{}{}
			groupBySeen[fieldID] = struct{}{}
			level.OwnFieldIDs = append(level.OwnFieldIDs, fieldID)
			resolved.groupByFieldIDs = append(resolved.groupByFieldIDs, fieldID)
		}

		if len(spec.GetMetrics()) > 0 {
			level.Metrics = make(map[string]MetricSpec, len(spec.GetMetrics()))
			// Sort aliases so metricPlans order is deterministic regardless of
			// proto map iteration order — matters for reproducible finalization.
			aliases := make([]string, 0, len(spec.GetMetrics()))
			for alias := range spec.GetMetrics() {
				aliases = append(aliases, alias)
			}
			sort.Strings(aliases)
			level.metricPlans = make([]metricPlan, 0, len(aliases))
			for _, alias := range aliases {
				metric := spec.GetMetrics()[alias]
				if strings.TrimSpace(alias) == "" {
					return merr.WrapErrParameterMissingMsg("metric alias cannot be empty")
				}
				metricSpec, metricSourceFieldID, err := buildMetricSpec(metric, schema, dynamicField)
				if err != nil {
					return merr.Wrapf(err, "invalid metric %q", alias)
				}
				plan, err := buildMetricPlan(alias, metricSpec)
				if err != nil {
					return merr.Wrapf(err, "invalid metric %q", alias)
				}
				level.Metrics[alias] = metricSpec
				level.metricPlans = append(level.metricPlans, plan)
				appendUniqueFieldID(extraSeen, &resolved.extraOutputFieldIDs, metricSourceFieldID)
			}
		}

		topHits, topHitsFieldIDs, err := buildTopHitsConfig(spec.GetTopHits(), schema, dynamicField)
		if err != nil {
			return err
		}
		level.TopHits = topHits
		for _, fieldID := range topHitsFieldIDs {
			appendUniqueFieldID(extraSeen, &resolved.extraOutputFieldIDs, fieldID)
		}

		order, err := buildOrderCriteria(spec.GetOrder(), level.Metrics)
		if err != nil {
			return err
		}
		level.Order = order

		resolved.levels = append(resolved.levels, level)
		return walk(spec.GetSubAggregation())
	}

	if err := walk(groupBy); err != nil {
		return nil, err
	}

	return resolved, nil
}

func buildMetricSpec(metric *commonpb.MetricAggSpec, schema *schemapb.CollectionSchema, dynamicField *schemapb.FieldSchema) (MetricSpec, int64, error) {
	if metric == nil {
		return MetricSpec{}, 0, merr.WrapErrParameterInvalidMsg("metric spec is nil")
	}

	op := strings.ToLower(strings.TrimSpace(metric.GetOp()))
	switch op {
	case "avg", "sum", "count", "min", "max":
	default:
		return MetricSpec{}, 0, merr.WrapErrParameterInvalidMsg("unsupported metric op %q", metric.GetOp())
	}

	fieldName := strings.TrimSpace(metric.GetFieldName())
	if fieldName == "" {
		return MetricSpec{}, 0, merr.WrapErrParameterInvalidMsg("metric field_name is empty")
	}

	switch fieldName {
	case "_score":
		// _score is a float32 produced by the engine; declare as Float so
		// internal/agg's type check accepts it for sum/avg/min/max.
		return MetricSpec{Op: op, FieldID: ScoreFieldID, FieldType: schemapb.DataType_Float}, 0, nil
	case "*":
		if op != "count" {
			return MetricSpec{}, 0, merr.WrapErrParameterInvalidMsg("field_name '*' only supports count op")
		}
		// count(*) has no source field; DataType_None is what internal/agg
		// expects for the synthetic always-1 input.
		return MetricSpec{Op: op, FieldID: CountAllFieldID, FieldType: schemapb.DataType_None}, 0, nil
	default:
		fieldID, err := resolveFieldID(fieldName, schema, dynamicField)
		if err != nil {
			return MetricSpec{}, 0, err
		}
		fieldType := schemapb.DataType_None
		field, err := validateSearchAggregationFieldSupport(fieldName, fieldID, schema, "metric field")
		if err != nil {
			return MetricSpec{}, 0, err
		}
		if field != nil {
			fieldType = field.GetDataType()
		}
		return MetricSpec{Op: op, FieldID: fieldID, FieldType: fieldType}, fieldID, nil
	}
}

func buildTopHitsConfig(topHits *commonpb.TopHitsSpec, schema *schemapb.CollectionSchema, dynamicField *schemapb.FieldSchema) (*TopHitsConfig, []int64, error) {
	if topHits == nil {
		return nil, nil, nil
	}
	if topHits.GetSize() < 0 {
		return nil, nil, merr.WrapErrParameterInvalidMsg("top_hits size must be non-negative")
	}

	cfg := &TopHitsConfig{
		Size: normalizeAggregationSize(topHits.GetSize()),
		Sort: make([]SortCriterion, 0, len(topHits.GetSort())),
	}
	sortFieldIDs := make([]int64, 0, len(topHits.GetSort()))
	seen := make(map[int64]struct{})

	for _, sortSpec := range topHits.GetSort() {
		if sortSpec == nil {
			return nil, nil, merr.WrapErrParameterInvalidMsg("top_hits.sort contains nil item")
		}
		fieldName := strings.TrimSpace(sortSpec.GetFieldName())
		if fieldName == "" {
			return nil, nil, merr.WrapErrParameterInvalidMsg("top_hits.sort field_name is empty")
		}

		direction, err := normalizeDirection(sortSpec.GetDirection(), "desc")
		if err != nil {
			return nil, nil, merr.Wrapf(err, "invalid top_hits.sort direction for %q", fieldName)
		}

		if fieldName == "_score" {
			cfg.Sort = append(cfg.Sort, SortCriterion{FieldID: ScoreFieldID, Dir: direction, NullFirst: sortSpec.GetNullFirst()})
			continue
		}

		if isJSONPathFieldExpr(fieldName) {
			return nil, nil, merr.WrapErrParameterInvalidMsg("top_hits.sort JSON path is not yet supported: %q", fieldName)
		}

		fieldID, err := resolveFieldID(fieldName, schema, dynamicField)
		if err != nil {
			return nil, nil, merr.Wrapf(err, "invalid top_hits.sort field %q", fieldName)
		}
		if _, err := validateSearchAggregationFieldSupport(fieldName, fieldID, schema, "top_hits.sort field"); err != nil {
			return nil, nil, err
		}
		cfg.Sort = append(cfg.Sort, SortCriterion{FieldID: fieldID, Dir: direction, NullFirst: sortSpec.GetNullFirst()})
		appendUniqueFieldID(seen, &sortFieldIDs, fieldID)
	}

	return cfg, sortFieldIDs, nil
}

func buildOrderCriteria(orderSpecs []*commonpb.OrderSpec, metrics map[string]MetricSpec) ([]OrderCriterion, error) {
	if len(orderSpecs) == 0 {
		return nil, nil
	}

	order := make([]OrderCriterion, 0, len(orderSpecs))
	for _, orderSpec := range orderSpecs {
		if orderSpec == nil {
			return nil, merr.WrapErrParameterInvalidMsg("order contains nil item")
		}

		key := strings.TrimSpace(orderSpec.GetKey())
		if key == "" {
			return nil, merr.WrapErrParameterInvalidMsg("order key is empty")
		}

		if key != "_count" && key != "_key" {
			if _, ok := metrics[key]; !ok {
				return nil, merr.WrapErrParameterInvalidMsg("order key %q is neither reserved key nor metric alias", key)
			}
		}

		direction, err := normalizeDirection(orderSpec.GetDirection(), "desc")
		if err != nil {
			return nil, merr.Wrapf(err, "invalid order direction for key %q", key)
		}
		// ES bucket order has no null placement option; OrderSpec.NullFirst is ignored intentionally.
		order = append(order, OrderCriterion{Key: key, Dir: direction})
	}

	return order, nil
}

func normalizeDirection(direction string, defaultDir string) (string, error) {
	dir := strings.ToLower(strings.TrimSpace(direction))
	if dir == "" {
		return defaultDir, nil
	}
	switch dir {
	case "asc", "desc":
		return dir, nil
	default:
		return "", merr.WrapErrParameterInvalidMsg("direction must be asc or desc")
	}
}

func validateSearchAggregationFieldSupport(fieldName string, fieldID int64, schema *schemapb.CollectionSchema, usage string) (*schemapb.FieldSchema, error) {
	field := typeutil.GetFieldByID(schema, fieldID)
	if field == nil {
		return nil, nil
	}
	if field.GetDataType() == schemapb.DataType_JSON || field.GetIsDynamic() {
		return nil, merr.WrapErrParameterInvalidMsg("%s %q: JSON / dynamic fields are not yet supported with search_aggregation", usage, fieldName)
	}
	return field, nil
}

func isJSONPathFieldExpr(fieldExpr string) bool {
	// TODO: Preserve parsed nested path in SortCriterion and extract it in the top_hits comparator.
	return strings.Contains(fieldExpr, "[") && strings.Contains(fieldExpr, "]")
}

func resolveFieldID(fieldExpr string, schema *schemapb.CollectionSchema, dynamicField *schemapb.FieldSchema) (int64, error) {
	fieldExpr = strings.TrimSpace(fieldExpr)
	if fieldExpr == "" {
		return 0, merr.WrapErrParameterInvalidMsg("field is empty")
	}

	if field := typeutil.GetFieldByName(schema, fieldExpr); field != nil {
		return field.GetFieldID(), nil
	}

	hasBrackets := strings.Contains(fieldExpr, "[") && strings.Contains(fieldExpr, "]")
	if hasBrackets {
		baseName := strings.Split(fieldExpr, "[")[0]
		if baseName != "" {
			if baseField := typeutil.GetFieldByName(schema, baseName); baseField != nil {
				if _, err := typeutil2.ParseAndVerifyNestedPath(fieldExpr, schema, baseField.GetFieldID()); err != nil {
					return 0, err
				}
				return baseField.GetFieldID(), nil
			}
		}

		if dynamicField != nil {
			if _, err := typeutil2.ParseAndVerifyNestedPath(fieldExpr, schema, dynamicField.GetFieldID()); err != nil {
				return 0, err
			}
			return dynamicField.GetFieldID(), nil
		}
	}

	if dynamicField != nil {
		if _, err := typeutil2.ParseAndVerifyNestedPath(fieldExpr, schema, dynamicField.GetFieldID()); err == nil {
			return dynamicField.GetFieldID(), nil
		}
	}

	return 0, merr.WrapErrParameterInvalidMsg("field %q not found in schema", fieldExpr)
}

func findDynamicField(schema *schemapb.CollectionSchema) *schemapb.FieldSchema {
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetIsDynamic() {
			return field
		}
	}
	return nil
}

func appendUniqueFieldID(seen map[int64]struct{}, fields *[]int64, fieldID int64) {
	if fieldID <= 0 {
		return
	}
	if _, ok := seen[fieldID]; ok {
		return
	}
	seen[fieldID] = struct{}{}
	*fields = append(*fields, fieldID)
}

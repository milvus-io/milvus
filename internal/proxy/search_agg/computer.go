package search_agg

import (
	"context"
	"slices"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SearchAggregationComputer builds a hierarchy over the rows retained by
// cross-shard group reduce. Row indices inside this computer refer to the
// selection order, preserving stable ties even when sources are interleaved.
type SearchAggregationComputer struct {
	ctx                   *SearchAggregationContext
	topks                 []int64
	sources               []*schemapb.SearchResultData
	selection             *ReducedRows
	defaultScoreDirection string
	accessors             []map[int64]*fieldAccessor
	metricSources         [][]metricSource
	groupFieldPositions   [][]int
	groupBuckets          [][]*bucketState
	groupIDOffset         int
}

type fieldAccessor struct {
	data *schemapb.FieldData
	iter func(int) any
}

type metricSource struct {
	fieldID int64
	plans   []metricPlan
}

// NewSearchAggregationComputer accepts an already-materialized reduction.
func NewSearchAggregationComputer(data *schemapb.SearchResultData, ctx *SearchAggregationContext, metricType string) *SearchAggregationComputer {
	return newSearchAggregationComputer([]*schemapb.SearchResultData{data}, data.GetTopks(), nil, ctx, metricType)
}

// NewSearchAggregationComputerFromRows retains the original source columns;
// only fields of the final hits are materialized by the response serializer.
func NewSearchAggregationComputerFromRows(rows *ReducedRows, ctx *SearchAggregationContext, metricType string) *SearchAggregationComputer {
	return newSearchAggregationComputer(rows.Sources, rows.Topks, rows, ctx, metricType)
}

func newSearchAggregationComputer(sources []*schemapb.SearchResultData, topks []int64, selection *ReducedRows, ctx *SearchAggregationContext, metricType string) *SearchAggregationComputer {
	direction := "asc"
	if metric.PositivelyRelated(metricType) {
		direction = "desc"
	}
	c := &SearchAggregationComputer{
		ctx: ctx, sources: sources, topks: topks, selection: selection, defaultScoreDirection: direction,
		accessors: make([]map[int64]*fieldAccessor, len(sources)),
	}
	for i, data := range sources {
		fields := make(map[int64]*fieldAccessor, len(data.GetFieldsData())+len(data.GetGroupByFieldValues()))
		for _, fd := range data.GetFieldsData() {
			if fd != nil {
				fields[fd.GetFieldId()] = &fieldAccessor{data: fd}
			}
		}
		for _, fd := range data.GetGroupByFieldValues() {
			if fd != nil {
				fields[fd.GetFieldId()] = &fieldAccessor{data: fd}
			}
		}
		if selection != nil {
			for _, id := range selection.GroupFieldIDs {
				if fd := reduce.FindGroupByFieldData(data, id, len(selection.GroupFieldIDs) == 1); fd != nil {
					fields[id] = &fieldAccessor{data: fd}
				}
			}
		}
		c.accessors[i] = fields
	}
	if ctx == nil {
		return c
	}
	c.metricSources = make([][]metricSource, len(ctx.Levels))
	for i, level := range ctx.Levels {
		byField := make(map[int64]int)
		for _, plan := range level.metricPlans {
			idx, exists := byField[plan.spec.FieldID]
			if !exists {
				idx = len(c.metricSources[i])
				byField[plan.spec.FieldID] = idx
				c.metricSources[i] = append(c.metricSources[i], metricSource{fieldID: plan.spec.FieldID})
			}
			c.metricSources[i][idx].plans = append(c.metricSources[i][idx].plans, plan)
		}
	}
	if selection != nil {
		positions := make(map[int64]int, len(selection.GroupFieldIDs))
		for i, id := range selection.GroupFieldIDs {
			positions[id] = i
		}
		c.groupFieldPositions = make([][]int, len(ctx.Levels))
		for i, level := range ctx.Levels {
			for _, id := range level.OwnFieldIDs {
				c.groupFieldPositions[i] = append(c.groupFieldPositions[i], positions[id])
			}
		}
	}
	return c
}

// sourceRow resolves a logical selection index without copying source data.
func (c *SearchAggregationComputer) sourceRow(row int64) (int, int64) {
	if c.selection != nil {
		ref := c.selection.Rows[row]
		return ref.ResultIdx, ref.RowIdx
	}
	return 0, row
}

func (c *SearchAggregationComputer) score(row int64) float32 {
	source, index := c.sourceRow(row)
	score := c.sources[source].GetScores()[index]
	if c.selection != nil && c.selection.NegateScores {
		score = -score
	}
	return score
}

func (c *SearchAggregationComputer) primaryKey(row int64) any {
	source, index := c.sourceRow(row)
	return typeutil.GetPK(c.sources[source].GetIds(), index)
}

func (c *SearchAggregationComputer) Compute(ctx context.Context) ([][]*AggBucketResult, error) {
	if c.ctx == nil {
		return nil, merr.WrapErrServiceInternalMsg("search aggregation context is nil")
	}
	if len(c.ctx.Levels) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("search aggregation context has no levels")
	}

	output := make([][]*AggBucketResult, c.ctx.NQ)
	for qi := int64(0); qi < c.ctx.NQ; qi++ {
		buckets, err := c.computeForQi(ctx, qi)
		if err != nil {
			return nil, err
		}
		output[qi] = buckets
	}
	return output, nil
}

func (c *SearchAggregationComputer) computeForQi(ctx context.Context, qi int64) ([]*AggBucketResult, error) {
	if qi < 0 || qi >= int64(len(c.topks)) {
		return nil, merr.WrapErrServiceInternalMsg("invalid qi %d, topks length=%d", qi, len(c.topks))
	}
	var start int64
	for i := int64(0); i < qi; i++ {
		start += c.topks[i]
	}
	count := c.topks[qi]
	rows := make([]int64, count)
	groupCount := 0
	if c.selection != nil && count > 0 {
		// Reduce assigns contiguous group IDs per query in first-seen order.
		c.groupIDOffset = c.selection.GroupIDs[start]
	}
	for i := int64(0); i < count; i++ {
		rows[i] = start + i
		if c.selection != nil {
			groupCount = max(groupCount, c.selection.GroupIDs[rows[i]]-c.groupIDOffset+1)
		}
	}
	if c.selection != nil {
		c.groupBuckets = make([][]*bucketState, len(c.ctx.Levels))
		for i := range c.groupBuckets {
			c.groupBuckets[i] = make([]*bucketState, groupCount)
		}
		defer func() {
			c.groupBuckets = nil
			c.groupIDOffset = 0
		}()
	}
	return c.computeLevel(ctx, 0, rows)
}

func (c *SearchAggregationComputer) computeLevel(ctx context.Context, levelIdx int, rows []int64) ([]*AggBucketResult, error) {
	if levelIdx < 0 || levelIdx >= len(c.ctx.Levels) {
		return nil, merr.WrapErrServiceInternalMsg("invalid level index %d", levelIdx)
	}
	level := c.ctx.Levels[levelIdx]
	isLeaf := levelIdx == len(c.ctx.Levels)-1

	// Hash-based lookup with collision chain: matches the pattern used by
	// internal/agg/aggregate_reducer.go. No string canonicalization per row.
	buckets := make(map[uint64][]*bucketState)
	keyOrder := make([]*bucketState, 0)

	var cached []*bucketState
	var touched []int
	if c.selection != nil {
		cached = c.groupBuckets[levelIdx]
		defer func() {
			// A complete group belongs to one subtree; later siblings cannot
			// reuse these states, including buckets discarded by size pruning.
			for _, index := range touched {
				cached[index] = nil
			}
		}()
	}

	for _, ref := range rows {
		var bucket *bucketState
		groupID := -1
		if c.selection != nil {
			groupID = c.selection.GroupIDs[ref]
			bucket = cached[groupID-c.groupIDOffset]
		}
		if bucket == nil {
			var values []any
			if groupID >= 0 {
				key := c.selection.GroupKeys[groupID]
				if slices.Equal(level.OwnFieldIDs, c.selection.GroupFieldIDs) {
					// A single full-key level can reuse the canonical key itself.
					values = key
				} else {
					values = make([]any, len(level.OwnFieldIDs))
					for i, pos := range c.groupFieldPositions[levelIdx] {
						values[i] = key[pos]
					}
				}
			} else {
				var err error
				values, err = c.extractOwnValues(ref, level.OwnFieldIDs)
				if err != nil {
					return nil, err
				}
			}
			h := reduce.HashGroupValues(values)
			for _, cand := range buckets[h] {
				if reduce.EqualGroupValues(cand.key, values) {
					bucket = cand
					break
				}
			}
			if bucket == nil {
				bucket = newBucketState(values, level.metricPlans)
				buckets[h] = append(buckets[h], bucket)
				keyOrder = append(keyOrder, bucket)
			}
			if groupID >= 0 {
				index := groupID - c.groupIDOffset
				cached[index] = bucket
				touched = append(touched, index)
			}
		}

		bucket.count++
		bucket.rows = append(bucket.rows, ref)

		if err := c.updateMetrics(bucket, ref, c.metricSources[levelIdx]); err != nil {
			return nil, err
		}
	}

	// Two-pass build: order/size applies only to local-level fields (_count,
	// _key, or a metric alias of THIS level), never to Hits or sub-agg
	// output. So emit skeleton (Key/Count/Metrics) first, trim by
	// applyOrderAndSize, then populate Hits + sub-agg only for survivors —
	// avoids wasted buildTopHits and sub-level recursion on dropped buckets.
	output := make([]*AggBucketResult, 0, len(keyOrder))
	bucketForResult := make(map[*AggBucketResult]*bucketState, len(keyOrder))
	for _, bucket := range keyOrder {
		result := &AggBucketResult{
			Key:   keyValuesToMap(bucket.key, level.OwnFieldIDs),
			Count: bucket.count,
		}
		metrics, err := finalizeMetrics(level.metricPlans, bucket.metricStates)
		if err != nil {
			return nil, err
		}
		if len(metrics) > 0 {
			result.Metrics = metrics
		}
		output = append(output, result)
		bucketForResult[result] = bucket
	}

	output, err := applyOrderAndSize(output, level)
	if err != nil {
		return nil, err
	}

	for _, result := range output {
		bucket := bucketForResult[result]
		if level.TopHits != nil {
			hits, err := c.buildTopHits(bucket.rows, level.TopHits)
			if err != nil {
				return nil, err
			}
			result.Hits = hits
		}
		if !isLeaf {
			subBuckets, err := c.computeLevel(ctx, levelIdx+1, bucket.rows)
			if err != nil {
				return nil, err
			}
			result.SubAggBuckets = subBuckets
		}
	}

	return output, nil
}

func (c *SearchAggregationComputer) buildTopHits(rows []int64, cfg *TopHitsConfig) ([]*HitResult, error) {
	if cfg == nil {
		return nil, nil
	}

	sorted := append([]int64(nil), rows...)
	var sortErr error
	sort.SliceStable(sorted, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		cmp, err := c.compareRowsForTopHits(sorted[i], sorted[j], cfg.Sort)
		if err != nil {
			sortErr = err
			return false
		}
		return cmp < 0
	})
	if sortErr != nil {
		return nil, sortErr
	}

	limit := min(int(normalizeAggregationSize(cfg.Size)), len(sorted))
	hits := make([]*HitResult, 0, limit)
	for i := 0; i < limit; i++ {
		hit, err := c.buildHitResult(sorted[i])
		if err != nil {
			return nil, err
		}
		hits = append(hits, hit)
	}
	return hits, nil
}

func (c *SearchAggregationComputer) compareRowsForTopHits(a, b int64, sortCriteria []SortCriterion) (int, error) {
	for _, criterion := range sortCriteria {
		av, _, err := c.readValueByFieldID(a, criterion.FieldID)
		if err != nil {
			return 0, err
		}
		bv, _, err := c.readValueByFieldID(b, criterion.FieldID)
		if err != nil {
			return 0, err
		}

		if cmp, decided := compareNulls(av, bv, criterion.NullFirst); decided {
			if cmp == 0 {
				continue
			}
			return cmp, nil
		}

		cmp, err := compareValues(av, bv)
		if err != nil {
			return 0, err
		}
		if cmp == 0 {
			continue
		}

		direction := criterion.Dir
		if criterion.FieldID == ScoreFieldID && direction == "" {
			direction = c.defaultScoreDirection
		}
		if direction == "desc" {
			cmp = -cmp
		}
		return cmp, nil
	}

	scoreA := c.score(a)
	scoreB := c.score(b)
	if cmp := compareFloat64(float64(scoreA), float64(scoreB)); cmp != 0 {
		if c.defaultScoreDirection == "desc" {
			cmp = -cmp
		}
		return cmp, nil
	}

	pkA := c.primaryKey(a)
	pkB := c.primaryKey(b)
	if pkA != nil && pkB != nil && pkA != pkB {
		if typeutil.ComparePK(pkA, pkB) {
			return -1, nil
		}
		return 1, nil
	}

	if a < b {
		return -1, nil
	}
	if a > b {
		return 1, nil
	}
	return 0, nil
}

func (c *SearchAggregationComputer) buildHitResult(ref int64) (*HitResult, error) {
	hit := &HitResult{
		PK:     c.primaryKey(ref),
		Score:  c.score(ref),
		Fields: make(map[int64]any, len(c.ctx.UserOutputFieldIDs)),
	}

	for fieldID := range c.ctx.UserOutputFieldIDs {
		val, _, err := c.readValueByFieldID(ref, fieldID)
		if err != nil {
			return nil, err
		}
		hit.Fields[fieldID] = val
	}
	return hit, nil
}

// extractOwnValues reads group-by values in OwnFieldIDs order and normalizes
// scalar types via reduce.NormalizeScalar so hashing and equality behave
// consistently regardless of the raw Go type the iterator surface returns.
// Null values pass through as nil so grouping treats null == null.
func (c *SearchAggregationComputer) extractOwnValues(ref int64, ownFieldIDs []int64) ([]any, error) {
	values := make([]any, len(ownFieldIDs))
	for i, fieldID := range ownFieldIDs {
		raw, isNull, err := c.readValueByFieldID(ref, fieldID)
		if err != nil {
			return nil, err
		}
		if isNull {
			values[i] = nil
			continue
		}
		values[i] = reduce.NormalizeScalar(raw)
	}
	return values, nil
}

// keyValuesToMap materializes the public Key map from a level's OwnFieldIDs
// and the internal []any key slice. Called once per bucket at emission — not
// on the per-row hot path.
func keyValuesToMap(values []any, ownFieldIDs []int64) map[int64]any {
	if len(ownFieldIDs) == 0 {
		return nil
	}
	key := make(map[int64]any, len(ownFieldIDs))
	for i, fid := range ownFieldIDs {
		key[fid] = values[i]
	}
	return key
}

// updateMetrics reads each metric source once and delegates state updates to internal/agg.
func (c *SearchAggregationComputer) updateMetrics(bucket *bucketState, ref int64, sources []metricSource) error {
	for _, source := range sources {
		var raw any = int64(1) // count(*) is always present.
		if source.fieldID != CountAllFieldID {
			value, isNull, err := c.readValueByFieldID(ref, source.fieldID)
			if err != nil {
				return err
			}
			if isNull {
				continue
			}
			raw = value
		}
		input := agg.NewFieldValue(raw)
		for _, plan := range source.plans {
			targets := bucket.metricStates[plan.alias]
			if targets == nil {
				return merr.WrapErrServiceInternalMsg("metric %q: missing bucket state", plan.alias)
			}
			if err := plan.aggregate.UpdateState(targets, input); err != nil {
				return merr.WrapErrServiceInternalMsg("metric %q update failed: %v", plan.alias, err)
			}
		}
	}
	return nil
}

func (c *SearchAggregationComputer) readValueByFieldID(ref int64, fieldID int64) (any, bool, error) {
	source, index := c.sourceRow(ref)
	data := c.sources[source]
	if data == nil {
		return nil, true, merr.WrapErrServiceInternalMsg("nil SearchResultData")
	}
	if fieldID == ScoreFieldID {
		if index < 0 || index >= int64(len(data.GetScores())) {
			return nil, true, merr.WrapErrServiceInternalMsg("score index %d out of range", index)
		}
		return c.score(ref), false, nil
	}
	accessor := c.accessors[source][fieldID]
	if accessor == nil {
		if c.ctx.IsGroupByField(fieldID) {
			return nil, true, merr.WrapErrServiceInternalMsg("group-by field %d missing from group_by_field_values", fieldID)
		}
		return nil, true, merr.WrapErrServiceInternalMsg("field %d missing from fields_data", fieldID)
	}
	if accessor.iter == nil {
		accessor.iter = typeutil.GetDataIterator(accessor.data)
	}
	value := accessor.iter(int(index))
	return value, value == nil, nil
}

type bucketState struct {
	key          []any
	count        int64
	metricStates map[string][]*agg.FieldValue
	rows         []int64
}

func newBucketState(key []any, plans []metricPlan) *bucketState {
	state := &bucketState{
		key:          key,
		metricStates: make(map[string][]*agg.FieldValue, len(plans)),
	}
	for _, plan := range plans {
		state.metricStates[plan.alias] = plan.aggregate.NewState()
	}
	return state
}

func finalizeMetrics(plans []metricPlan, states map[string][]*agg.FieldValue) (map[string]any, error) {
	if len(plans) == 0 {
		return nil, nil
	}
	metrics := make(map[string]any, len(plans))
	for _, plan := range plans {
		slots := states[plan.alias]
		if plan.aggregate == nil {
			return nil, merr.WrapErrServiceInternalMsg("metric %q: semantic aggregate is nil", plan.alias)
		}
		value, err := plan.aggregate.Terminate(slots)
		if err != nil {
			return nil, merr.WrapErrServiceInternalMsg("metric %q: %v", plan.alias, err)
		}
		metrics[plan.alias] = value
	}
	return metrics, nil
}

func compareNulls(a, b any, nullFirst bool) (int, bool) {
	if a == nil && b == nil {
		return 0, true
	}
	if a == nil {
		if nullFirst {
			return -1, true
		}
		return 1, true
	}
	if b == nil {
		if nullFirst {
			return 1, true
		}
		return -1, true
	}
	return 0, false
}

// compareValues keeps the legacy nil-first behavior for bucket ordering;
// top_hits sort applies SortCriterion.NullFirst via compareNulls before calling this.
func compareValues(a, b any) (int, error) {
	if a == nil && b == nil {
		return 0, nil
	}
	if a == nil {
		return -1, nil
	}
	if b == nil {
		return 1, nil
	}

	switch av := a.(type) {
	case int:
		bv, ok := b.(int)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case int8:
		bv, ok := b.(int8)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case int16:
		bv, ok := b.(int16)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case int32:
		bv, ok := b.(int32)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint:
		bv, ok := b.(uint)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint8:
		bv, ok := b.(uint8)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint16:
		bv, ok := b.(uint16)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint32:
		bv, ok := b.(uint32)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint64:
		bv, ok := b.(uint64)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case float32:
		bv, ok := b.(float32)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareFloat64(float64(av), float64(bv)), nil
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareFloat64(av, bv), nil
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareBool(av, bv), nil
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	}

	return 0, merr.WrapErrServiceInternalMsg("unsupported comparable types: %T and %T", a, b)
}

func compareOrdered[T ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareFloat64(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareBool(a, b bool) int {
	switch {
	case !a && b:
		return -1
	case a && !b:
		return 1
	default:
		return 0
	}
}

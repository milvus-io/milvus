package search_agg

import (
	"context"
	"fmt"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// SearchAggregationComputer runs hierarchical aggregation over a single
// SearchResultData that has already been cross-shard-reduced upstream by
// searchReduceOperator. Composite-key group reduce is NOT done here — the
// pipeline is SearchReduce → SearchAgg so this stage is pure hierarchy walk
// (grouping per level, metric accumulation, top_hits, sub-aggregation).
type SearchAggregationComputer struct {
	ctx  *SearchAggregationContext
	data *schemapb.SearchResultData

	// fieldsByID maps FieldID → FieldData, unioning fields_data (metric
	// sources, top_hits sort, user output) with group_by_field_values
	// (composite group-by key columns). Field IDs never overlap between the
	// two channels, so a single map is unambiguous.
	fieldsByID map[int64]*schemapb.FieldData
}

// NewSearchAggregationComputer wraps an already-reduced SearchResultData.
// Upstream searchReduceOperator owns cross-shard merge + group-size /
// topK enforcement; this computer only does per-NQ hierarchical aggregation.
func NewSearchAggregationComputer(
	data *schemapb.SearchResultData,
	ctx *SearchAggregationContext,
) *SearchAggregationComputer {
	m := make(map[int64]*schemapb.FieldData, len(data.GetFieldsData())+len(data.GetGroupByFieldValues()))
	for _, fd := range data.GetFieldsData() {
		if fd != nil {
			m[fd.GetFieldId()] = fd
		}
	}
	for _, fd := range data.GetGroupByFieldValues() {
		if fd != nil {
			m[fd.GetFieldId()] = fd
		}
	}
	return &SearchAggregationComputer{
		ctx:        ctx,
		data:       data,
		fieldsByID: m,
	}
}

func (c *SearchAggregationComputer) Compute(ctx context.Context) ([][]*AggBucketResult, error) {
	if c.ctx == nil {
		return nil, fmt.Errorf("search aggregation context is nil")
	}
	if len(c.ctx.Levels) == 0 {
		return nil, fmt.Errorf("search aggregation context has no levels")
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
	topks := c.data.GetTopks()
	if qi < 0 || qi >= int64(len(topks)) {
		return nil, fmt.Errorf("invalid qi %d, topks length=%d", qi, len(topks))
	}
	var start int64
	for i := int64(0); i < qi; i++ {
		start += topks[i]
	}
	count := topks[qi]
	rows := make([]reduce.RowRef, count)
	for i := int64(0); i < count; i++ {
		rows[i] = reduce.RowRef{ResultIdx: 0, RowIdx: start + i}
	}
	return c.computeLevel(ctx, qi, 0, rows)
}

func (c *SearchAggregationComputer) computeLevel(ctx context.Context, qi int64, levelIdx int, rows []reduce.RowRef) ([]*AggBucketResult, error) {
	if levelIdx < 0 || levelIdx >= len(c.ctx.Levels) {
		return nil, fmt.Errorf("invalid level index %d", levelIdx)
	}
	level := c.ctx.Levels[levelIdx]
	isLeaf := levelIdx == len(c.ctx.Levels)-1

	// Hash-based lookup with collision chain: matches the pattern used by
	// internal/agg/aggregate_reducer.go. No string canonicalization per row.
	buckets := make(map[uint64][]*bucketState)
	keyOrder := make([]*bucketState, 0)

	for _, ref := range rows {
		values, err := c.extractOwnValues(ref, level.OwnFieldIDs)
		if err != nil {
			return nil, err
		}
		h := reduce.HashGroupValues(values)

		var bucket *bucketState
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

		bucket.count++
		bucket.rows = append(bucket.rows, ref)

		if err := c.updateMetrics(bucket, ref, level.metricPlans); err != nil {
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
			subBuckets, err := c.computeLevel(ctx, qi, levelIdx+1, bucket.rows)
			if err != nil {
				return nil, err
			}
			result.SubAggBuckets = subBuckets
		}
	}

	return output, nil
}

func (c *SearchAggregationComputer) buildTopHits(rows []reduce.RowRef, cfg *TopHitsConfig) ([]*HitResult, error) {
	if cfg == nil {
		return nil, nil
	}

	sorted := make([]reduce.RowRef, len(rows))
	copy(sorted, rows)
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

	limit := int(normalizeAggregationSize(cfg.Size))
	if limit > len(sorted) {
		limit = len(sorted)
	}

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

func (c *SearchAggregationComputer) compareRowsForTopHits(a, b reduce.RowRef, sortCriteria []SortCriterion) (int, error) {
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

		if criterion.Dir == "desc" {
			cmp = -cmp
		}
		return cmp, nil
	}

	scoreA := c.data.GetScores()[a.RowIdx]
	scoreB := c.data.GetScores()[b.RowIdx]
	if scoreA > scoreB {
		return -1, nil
	}
	if scoreA < scoreB {
		return 1, nil
	}

	pkA := typeutil.GetPK(c.data.GetIds(), a.RowIdx)
	pkB := typeutil.GetPK(c.data.GetIds(), b.RowIdx)
	if pkA != nil && pkB != nil && pkA != pkB {
		if typeutil.ComparePK(pkA, pkB) {
			return -1, nil
		}
		return 1, nil
	}

	if a.ResultIdx < b.ResultIdx {
		return -1, nil
	}
	if a.ResultIdx > b.ResultIdx {
		return 1, nil
	}
	if a.RowIdx < b.RowIdx {
		return -1, nil
	}
	if a.RowIdx > b.RowIdx {
		return 1, nil
	}
	return 0, nil
}

func (c *SearchAggregationComputer) buildHitResult(ref reduce.RowRef) (*HitResult, error) {
	hit := &HitResult{
		PK:     typeutil.GetPK(c.data.GetIds(), ref.RowIdx),
		Score:  c.data.GetScores()[ref.RowIdx],
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
func (c *SearchAggregationComputer) extractOwnValues(ref reduce.RowRef, ownFieldIDs []int64) ([]any, error) {
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
func (c *SearchAggregationComputer) updateMetrics(bucket *bucketState, ref reduce.RowRef, plans []metricPlan) error {
	if len(plans) == 0 {
		return nil
	}
	for _, plan := range plans {
		targets := bucket.metricStates[plan.alias]
		if targets == nil {
			return fmt.Errorf("metric %q: missing bucket state", plan.alias)
		}

		var raw any
		isNull := false
		if plan.spec.FieldID == CountAllFieldID {
			// count(*) uses a synthetic always-present int64(1) source.
			raw = int64(1)
		} else {
			v, null, err := c.readValueByFieldID(ref, plan.spec.FieldID)
			if err != nil {
				return err
			}
			raw = v
			isNull = null
		}
		if isNull {
			// Skip null inputs: matches internal/agg semantics.
			continue
		}

		if err := plan.aggregate.UpdateState(targets, agg.NewFieldValue(raw)); err != nil {
			return fmt.Errorf("metric %q update failed: %w", plan.alias, err)
		}
	}
	return nil
}

func (c *SearchAggregationComputer) readValueByFieldID(ref reduce.RowRef, fieldID int64) (any, bool, error) {
	if c.data == nil {
		return nil, true, fmt.Errorf("nil SearchResultData")
	}

	if fieldID == ScoreFieldID {
		scores := c.data.GetScores()
		if ref.RowIdx < 0 || ref.RowIdx >= int64(len(scores)) {
			return nil, true, fmt.Errorf("score index %d out of range", ref.RowIdx)
		}
		return scores[ref.RowIdx], false, nil
	}

	fd := c.fieldsByID[fieldID]
	if fd == nil {
		if c.ctx.IsGroupByField(fieldID) {
			return nil, true, fmt.Errorf("group-by field %d missing from group_by_field_values", fieldID)
		}
		return nil, true, fmt.Errorf("field %d missing from fields_data", fieldID)
	}

	iter := typeutil.GetDataIterator(fd)
	value := iter(int(ref.RowIdx))
	if value == nil {
		return nil, true, nil
	}
	return value, false, nil
}

type bucketState struct {
	key          []any
	count        int64
	metricStates map[string][]*agg.FieldValue
	rows         []reduce.RowRef
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
			return nil, fmt.Errorf("metric %q: semantic aggregate is nil", plan.alias)
		}
		value, err := plan.aggregate.Terminate(slots)
		if err != nil {
			return nil, fmt.Errorf("metric %q: %w", plan.alias, err)
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
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case int8:
		bv, ok := b.(int8)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case int16:
		bv, ok := b.(int16)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case int32:
		bv, ok := b.(int32)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint:
		bv, ok := b.(uint)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint8:
		bv, ok := b.(uint8)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint16:
		bv, ok := b.(uint16)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint32:
		bv, ok := b.(uint32)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case uint64:
		bv, ok := b.(uint64)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	case float32:
		bv, ok := b.(float32)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareFloat64(float64(av), float64(bv)), nil
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareFloat64(av, bv), nil
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareBool(av, bv), nil
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0, fmt.Errorf("type mismatch: %T vs %T", a, b)
		}
		return compareOrdered(av, bv), nil
	}

	return 0, fmt.Errorf("unsupported comparable types: %T and %T", a, b)
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

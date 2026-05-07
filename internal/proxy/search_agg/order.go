package search_agg

import (
	"fmt"
	"sort"
)

func applyOrderAndSize(buckets []*AggBucketResult, level LevelContext) ([]*AggBucketResult, error) {
	if len(buckets) == 0 {
		return buckets, nil
	}

	if len(level.Order) > 0 {
		var cmpErr error
		sort.SliceStable(buckets, func(i, j int) bool {
			if cmpErr != nil {
				return false
			}
			cmp, err := compareBucketsByOrder(buckets[i], buckets[j], level)
			if err != nil {
				cmpErr = err
				return false
			}
			return cmp < 0
		})
		if cmpErr != nil {
			return nil, cmpErr
		}
	}

	size := normalizeAggregationSize(level.Size)
	if int64(len(buckets)) > size {
		return buckets[:size], nil
	}
	return buckets, nil
}

func compareBucketsByOrder(a, b *AggBucketResult, level LevelContext) (int, error) {
	for _, criterion := range level.Order {
		cmp, err := compareBucketByCriterion(a, b, level.OwnFieldIDs, criterion)
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
	return 0, nil
}

func compareBucketByCriterion(a, b *AggBucketResult, ownFieldIDs []int64, criterion OrderCriterion) (int, error) {
	switch criterion.Key {
	case "_count":
		return compareInt64(a.Count, b.Count), nil
	case "_key":
		return compareBucketKeys(a.Key, b.Key, ownFieldIDs)
	default:
		// Metric values are typed `any` (since P2) so compareValues handles
		// numeric and string orderings uniformly. Missing aliases compare as
		// nil; compareValues sorts nil before any other value.
		av := a.Metrics[criterion.Key]
		bv := b.Metrics[criterion.Key]
		return compareValues(av, bv)
	}
}

func compareBucketKeys(a, b map[int64]any, ownFieldIDs []int64) (int, error) {
	for _, fieldID := range ownFieldIDs {
		cmp, err := compareValues(a[fieldID], b[fieldID])
		if err != nil {
			return 0, fmt.Errorf("compare _key field %d failed: %w", fieldID, err)
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

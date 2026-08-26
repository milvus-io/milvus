package typeutil

// Segment size metric constants. The active metric is configured by
// dataCoord.segment.sizeMetric and interpreted at segment creation time
// (invariant I1 of the better-segmentation design).
const (
	// SizeMetricWholeRow constrains a segment's whole-row binary size — the
	// default and historically the only behavior.
	SizeMetricWholeRow = "wholeRow"
	// SizeMetricMainIndex constrains the main index column (the vector field
	// with the largest index memory footprint) instead of the whole row.
	SizeMetricMainIndex = "mainIndex"
)

// IsMainIndexSizeMetric reports whether the given size metric string enables
// main-index-column semantics.
func IsMainIndexSizeMetric(metric string) bool {
	return metric == SizeMetricMainIndex
}

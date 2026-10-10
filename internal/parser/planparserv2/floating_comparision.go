package planparserv2

import "github.com/milvus-io/milvus/pkg/v3/util/typeutil"

const float64EqualityThreshold = 1e-9

// SQL scalar comparisons use the same order as numeric indexes: all NaNs
// compare equal and sort after every other number, including +Inf.
func floatingEqual(a, b float64) bool {
	return typeutil.Float64ToSortableUint64(a) == typeutil.Float64ToSortableUint64(b)
}

func floatingLess(a, b float64) bool {
	return typeutil.Float64ToSortableUint64(a) < typeutil.Float64ToSortableUint64(b)
}

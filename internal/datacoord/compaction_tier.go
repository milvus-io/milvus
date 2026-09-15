package datacoord

const (
	compactionFillRate = 0.85
)

func compactionMiddleSize(idealSize int64) int64 {
	return idealSize / 4
}

func compactionFullThreshold(idealSize int64) int64 {
	return int64(float64(idealSize) * compactionFillRate)
}

func compactionFragmentThreshold(idealSize int64) int64 {
	return int64(float64(compactionMiddleSize(idealSize)) * compactionFillRate)
}

func isFullSegment(idealSize, residualSize int64) bool {
	return residualSize >= compactionFullThreshold(idealSize)
}

func isFragmentSegment(idealSize, residualSize int64) bool {
	return residualSize < compactionFragmentThreshold(idealSize)
}

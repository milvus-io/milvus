package compaction

import "github.com/milvus-io/milvus/internal/datacoord/compaction/view"

func compactionMiddleSize(idealSize int64) int64 {
	return idealSize / 4
}

func compactionFullThreshold(idealSize int64) int64 {
	return view.TwoTierFullThreshold(idealSize)
}

func compactionFragmentThreshold(idealSize int64) int64 {
	return view.TwoTierFragmentThreshold(idealSize)
}

func isFullSegment(idealSize, residualSize int64) bool {
	return residualSize >= compactionFullThreshold(idealSize)
}

func isFragmentSegment(idealSize, residualSize int64) bool {
	return residualSize < compactionFragmentThreshold(idealSize)
}

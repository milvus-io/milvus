package qnview

import "github.com/milvus-io/milvus/pkg/v3/proto/viewpb"

func segmentPartitionMap(view *viewpb.QueryViewOfQueryNode) map[int64]int64 {
	segments := make(map[int64]int64)
	for _, partition := range view.GetPartitions() {
		for _, segmentID := range partition.GetSegmentIds() {
			segments[segmentID] = partition.GetPartitionId()
		}
	}
	return segments
}

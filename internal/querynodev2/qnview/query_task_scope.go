package qnview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func filterQueryNodeViewByPartitions(view *viewpb.QueryViewOfQueryNode, partitionIDs []int64) *viewpb.QueryViewOfQueryNode {
	if len(partitionIDs) == 0 {
		return proto.Clone(view).(*viewpb.QueryViewOfQueryNode)
	}
	keep := make(map[int64]struct{}, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		keep[partitionID] = struct{}{}
	}
	filtered := &viewpb.QueryViewOfQueryNode{NodeId: view.GetNodeId()}
	for _, partition := range view.GetPartitions() {
		if _, ok := keep[partition.GetPartitionId()]; !ok {
			continue
		}
		filtered.Partitions = append(filtered.Partitions, proto.Clone(partition).(*viewpb.QueryViewOfPartition))
	}
	return filtered
}

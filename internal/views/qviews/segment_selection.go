package qviews

import "github.com/milvus-io/milvus/pkg/v3/proto/viewpb"

// SelectSegments encodes all/none without IDs and partial sets using the smaller
// side. The caller supplies the existing selection scope.
func SelectSegments(candidates []int64, excluded map[int64]struct{}) *viewpb.SegmentSelection {
	kept, removed := make([]int64, 0), make([]int64, 0)
	for _, id := range candidates {
		if _, ok := excluded[id]; ok {
			removed = append(removed, id)
		} else {
			kept = append(kept, id)
		}
	}
	if len(kept) == 0 {
		return &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_None}
	}
	if len(removed) == 0 {
		return &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_All}
	}
	if len(removed) < len(kept) {
		return &viewpb.SegmentSelection{SegmentIds: removed, Mode: viewpb.SegmentSelection_Exclude}
	}
	return &viewpb.SegmentSelection{SegmentIds: kept, Mode: viewpb.SegmentSelection_Include}
}

func SegmentSelector(selection *viewpb.SegmentSelection) func(int64) bool {
	switch selection.GetMode() {
	case viewpb.SegmentSelection_All:
		return func(int64) bool { return true }
	case viewpb.SegmentSelection_Include, viewpb.SegmentSelection_Exclude:
		// Only partial selections need an ID set.
	default:
		return func(int64) bool { return false }
	}
	ids := make(map[int64]struct{}, len(selection.GetSegmentIds()))
	for _, id := range selection.GetSegmentIds() {
		ids[id] = struct{}{}
	}
	return func(id int64) bool {
		_, exists := ids[id]
		return exists != (selection.GetMode() == viewpb.SegmentSelection_Exclude)
	}
}

// FilterSelectedSegments borrows immutable topology and preserves parallel
// manifest/ready arrays. It never alters the retained QueryView.
func FilterSelectedSegments(view *viewpb.QueryViewOfQueryNode, selection *viewpb.SegmentSelection) *viewpb.QueryViewOfQueryNode {
	if selection.GetMode() == viewpb.SegmentSelection_All {
		return view
	}
	if selection.GetMode() == viewpb.SegmentSelection_None {
		return &viewpb.QueryViewOfQueryNode{NodeId: view.GetNodeId()}
	}
	accepts := SegmentSelector(selection)
	result := &viewpb.QueryViewOfQueryNode{NodeId: view.GetNodeId()}
	for _, part := range view.GetPartitions() {
		selected := &viewpb.QueryViewOfPartition{PartitionId: part.GetPartitionId()}
		for i, id := range part.GetSegmentIds() {
			if !accepts(id) {
				continue
			}
			selected.SegmentIds = append(selected.SegmentIds, id)
			if i < len(part.GetSegmentManifestVersions()) {
				selected.SegmentManifestVersions = append(selected.SegmentManifestVersions, part.GetSegmentManifestVersions()[i])
			}
		}
		for _, id := range part.GetReadySegmentIds() {
			if accepts(id) {
				selected.ReadySegmentIds = append(selected.ReadySegmentIds, id)
			}
		}
		if len(selected.SegmentIds) > 0 {
			result.Partitions = append(result.Partitions, selected)
		}
	}
	return result
}

func WorkNodeSelection(plan *viewpb.QueryPlan, node WorkNode) *viewpb.QueryPlanWorkNode {
	for _, worker := range plan.GetWorkNodes() {
		if qn, ok := node.(QueryNode); ok && worker.GetQueryNode() != nil && worker.GetQueryNode().GetNodeId() == qn.ID {
			return worker
		}
		if sn, ok := node.(StreamingNode); ok && worker.GetStreamingNode() != nil && worker.GetStreamingNode().GetPchannel() == sn.PChannel {
			return worker
		}
	}
	return nil
}

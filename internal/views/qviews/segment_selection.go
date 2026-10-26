package qviews

import "github.com/milvus-io/milvus/pkg/v3/proto/viewpb"

// SelectSegments encodes the smaller side of a selection. A nil selection means
// all members; an explicit empty inclusion means none.
func SelectSegments(candidates []int64, excluded map[int64]struct{}) *viewpb.SegmentSelection {
	kept, removed := make([]int64, 0), make([]int64, 0)
	for _, id := range candidates {
		if _, ok := excluded[id]; ok {
			removed = append(removed, id)
		} else {
			kept = append(kept, id)
		}
	}
	if len(removed) == 0 {
		return nil
	}
	if len(removed) < len(kept) {
		return &viewpb.SegmentSelection{SegmentIds: removed, Exclude: true}
	}
	return &viewpb.SegmentSelection{SegmentIds: kept}
}

func SegmentSelector(selection *viewpb.SegmentSelection) func(int64) bool {
	if selection == nil {
		return func(int64) bool { return true }
	}
	ids := make(map[int64]struct{}, len(selection.GetSegmentIds()))
	for _, id := range selection.GetSegmentIds() {
		ids[id] = struct{}{}
	}
	return func(id int64) bool { _, exists := ids[id]; return exists != selection.GetExclude() }
}

// FilterSelectedSegments borrows immutable topology and preserves parallel
// manifest/ready arrays. It never alters the retained QueryView.
func FilterSelectedSegments(view *viewpb.QueryViewOfQueryNode, selection *viewpb.SegmentSelection) *viewpb.QueryViewOfQueryNode {
	if selection == nil {
		return view
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

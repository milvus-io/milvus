package queryresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/optimizer/pruning"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// OptimizePlan makes one global decision per subrequest before grouping work by
// QN. Statistics and topology belong to the leased View, never a latest lookup.
func (o globalOptimizer) OptimizePlan(ctx context.Context, plan *viewpb.QueryPlan, view *viewpb.QueryViewOfShard) (*viewpb.GlobalOptimizerExecutionSummary, error) {
	pruneByStats := paramtable.Get().QueryNodeCfg.EnableSegmentPrune.GetAsBool() && len(o.partitionStats) != 0
	if !pruneByStats {
		return nil, nil
	}
	flags := pruning.NoPruningFlags
	searches := []*internalpb.SearchRequest{plan.GetLegacySearchRequest()}
	advanced := plan.GetLegacySearchRequest().GetIsAdvanced()
	if advanced {
		var err error
		searches, err = viewquery.BuildSubSearchRequests(plan.GetLegacySearchRequest())
		if err != nil {
			return nil, err
		}
	}
	selectedBySubquery := make([]map[int64]bool, len(searches))
	partitionScopes := make([]map[int64]bool, len(searches))
	for i, search := range searches {
		selectedBySubquery[i] = make(map[int64]bool)
		if advanced && plan.GetLegacySearchRequest().GetSubReqs()[i].GetSkip() {
			continue
		}
		partitions := plan.GetLegacyRetrieveRequest().GetPartitionIDs()
		if search != nil {
			partitions = search.GetPartitionIDs()
		}
		wanted := make(map[int64]bool, len(partitions))
		for _, id := range partitions {
			wanted[id] = true
		}
		partitionScopes[i] = wanted
		candidates := make(map[int64][]int64)
		for _, node := range view.GetQueryNode() {
			for _, part := range node.GetPartitions() {
				if len(wanted) == 0 || wanted[part.GetPartitionId()] {
					candidates[part.GetPartitionId()] = append(candidates[part.GetPartitionId()], part.GetSegmentIds()...)
				}
			}
		}
		excluded, pruningFlags := pruning.Exclude(ctx, o.schema, o.partitionStats, candidates, search, plan.GetLegacyRetrieveRequest(), paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat())
		for _, ids := range candidates {
			for _, id := range ids {
				if _, removed := excluded[id]; !removed {
					selectedBySubquery[i][id] = true
				}
			}
		}
		flags |= pruningFlags
	}
	assignments := make(map[int64]*viewpb.QueryViewOfQueryNode)
	for _, node := range view.GetQueryNode() {
		assignments[node.GetNodeId()] = node
	}
	workers := plan.WorkNodes[:0]
	for _, worker := range plan.WorkNodes {
		if worker.GetQueryNode() == nil {
			workers = append(workers, worker)
			continue
		}
		node := assignments[worker.GetQueryNode().GetNodeId()]
		all := make([]int64, 0)
		union := make(map[int64]bool)
		for _, part := range node.GetPartitions() {
			all = append(all, part.GetSegmentIds()...)
		}
		for i, selected := range selectedBySubquery {
			removed := make(map[int64]struct{})
			for _, id := range all {
				if selected[id] {
					union[id] = true
				} else {
					removed[id] = struct{}{}
				}
			}
			if advanced {
				// Each subrequest executes within its own partition scope. Keep the
				// worker union below relative to full membership for retained requery.
				scoped := all
				if wanted := partitionScopes[i]; len(wanted) != 0 {
					scoped = nil
					for _, part := range node.GetPartitions() {
						if wanted[part.GetPartitionId()] {
							scoped = append(scoped, part.GetSegmentIds()...)
						}
					}
				}
				selection := &viewpb.SegmentSelection{} // No candidates means none, not all.
				if len(scoped) != 0 {
					selection = qviews.SelectSegments(scoped, removed)
					if selection == nil {
						selection = &viewpb.SegmentSelection{Exclude: true}
					}
				}
				worker.SubSelections = append(worker.SubSelections, selection)
			}
		}
		if len(union) == 0 {
			continue
		}
		removed := make(map[int64]struct{})
		for _, id := range all {
			if !union[id] {
				removed[id] = struct{}{}
			}
		}
		worker.SegmentSelection = qviews.SelectSegments(all, removed)
		workers = append(workers, worker)
	}
	plan.WorkNodes = workers
	if flags == pruning.NoPruningFlags {
		return nil, nil
	}
	return &viewpb.GlobalOptimizerExecutionSummary{Flags: flags}, nil
}

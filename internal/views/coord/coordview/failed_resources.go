package coordview

import (
	"maps"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func cloneFailedResources(failures map[int64]map[ResourceKey]qviews.QueryViewVersion) map[int64]map[ResourceKey]qviews.QueryViewVersion {
	if len(failures) == 0 {
		return nil
	}
	copy := make(map[int64]map[ResourceKey]qviews.QueryViewVersion, len(failures))
	for node, resources := range failures {
		copy[node] = maps.Clone(resources)
	}
	return copy
}

// Failure history survives replacement and teardown, but cannot poison another
// materialization identity. It is ephemeral and owns no physical references.
func retainFailedResources(failures map[int64]map[ResourceKey]qviews.QueryViewVersion, sm *CoordQueryViewStateMachine) {
	for node, resources := range failures {
		for key := range resources {
			if key.DataVersion != sm.Version().DataVersion || key.LoadInfoVersion != sm.View().GetMeta().GetLoadInfoVersion() {
				delete(resources, key)
			}
		}
		if len(resources) == 0 {
			delete(failures, node)
		}
	}
}

func fillFailedResources(failed map[int64]map[ResourceKey]qviews.QueryViewVersion, sm *CoordQueryViewStateMachine) map[int64]map[ResourceKey]qviews.QueryViewVersion {
	if sm.State() != qviews.QueryViewStateUnrecoverable {
		return failed
	}
	for _, node := range sm.View().GetQueryNode() {
		id := node.GetNodeId()
		// A whole-view failure does not blame other nodes or unfinished siblings.
		if sm.qnStates[id] != qviews.QueryViewStateUnrecoverable || len(sm.qnPreparationFailures[id]) == 0 {
			continue
		}
		ready := segmentSet(sm.QNReadySegments()[id])
		for _, partition := range node.GetPartitions() {
			for _, segment := range partition.GetSegmentIds() {
				if ready[segment] {
					continue
				}
				switch sm.qnPreparationFailures[id][segment] {
				case viewpb.SegmentPerparingFailure_MemoryLimitExceeded,
					viewpb.SegmentPerparingFailure_DiskLimitExceeded,
					viewpb.SegmentPerparingFailure_ResourceReservationFailed:
				default:
					// Old reports and unknown future reasons carry no capacity fact.
					continue
				}
				if failed == nil {
					failed = make(map[int64]map[ResourceKey]qviews.QueryViewVersion)
				}
				if failed[id] == nil {
					failed[id] = make(map[ResourceKey]qviews.QueryViewVersion)
				}
				key := ResourceKey{
					PartitionID: partition.GetPartitionId(), SegmentID: segment,
					DataVersion: sm.Version().DataVersion, LoadInfoVersion: sm.View().GetMeta().GetLoadInfoVersion(),
				}
				if previous, ok := failed[id][key]; !ok || sm.Version().GT(previous) {
					failed[id][key] = sm.Version()
				}
			}
		}
	}
	return failed
}

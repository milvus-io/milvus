package coordview

import (
	"maps"
	"time"

	"github.com/milvus-io/milvus/internal/views/coord/resourcewatermark"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

const CapacityFailureHistoryTTL = 10 * time.Minute

// FailedResourceKey identifies the materialization, not the collection's latest
// flush. Legacy manifests have no immutable identity, so retain their DataVersion.
type FailedResourceKey struct {
	PartitionID, SegmentID, ManifestVersion int64
	SchemaVersion                           int32
	LoadInfoVersion                         uint64
	DataVersion                             qviews.DataVersion
}

type ResourceFailure struct {
	Version    qviews.QueryViewVersion
	Reason     viewpb.SegmentPerparingFailure_Reason
	ObservedAt time.Time
	Watermarks resourcewatermark.Sample
}

func FailureKey(partition, segment, manifest int64, schema int32, loadInfo uint64, data qviews.DataVersion) FailedResourceKey {
	key := FailedResourceKey{PartitionID: partition, SegmentID: segment, ManifestVersion: manifest, SchemaVersion: schema, LoadInfoVersion: loadInfo}
	if manifest == 0 {
		key.DataVersion = data
	}
	return key
}

func cloneFailedResources(failures map[int64]map[FailedResourceKey]ResourceFailure) map[int64]map[FailedResourceKey]ResourceFailure {
	if len(failures) == 0 {
		return nil
	}
	copy := make(map[int64]map[FailedResourceKey]ResourceFailure, len(failures))
	for node, resources := range failures {
		copy[node] = maps.Clone(resources)
	}
	return copy
}

func viewFailureKeys(sm *CoordQueryViewStateMachine) map[FailedResourceKey]struct{} {
	keys := make(map[FailedResourceKey]struct{})
	meta := sm.View().GetMeta()
	for _, node := range sm.View().GetQueryNode() {
		for _, partition := range node.GetPartitions() {
			for i, segment := range partition.GetSegmentIds() {
				manifest := int64(0)
				if i < len(partition.GetSegmentManifestVersions()) {
					manifest = partition.GetSegmentManifestVersions()[i]
				}
				keys[FailureKey(partition.GetPartitionId(), segment, manifest, meta.GetSchemaVersion(), meta.GetLoadInfoVersion(), sm.Version().DataVersion)] = struct{}{}
			}
		}
	}
	return keys
}

// History survives other shards' flushes, replacement and teardown. Bound its
// lifetime when materializations disappear, and age out old probe evidence.
func retainFailedResources(failures map[int64]map[FailedResourceKey]ResourceFailure, sm *CoordQueryViewStateMachine) {
	if len(failures) == 0 {
		return
	}
	keys := viewFailureKeys(sm)
	for node, resources := range failures {
		for key, failure := range resources {
			_, exists := keys[key]
			if !exists || (!failure.ObservedAt.IsZero() && time.Since(failure.ObservedAt) > CapacityFailureHistoryTTL) {
				delete(resources, key)
			}
		}
		if len(resources) == 0 {
			delete(failures, node)
		}
	}
}

// A successful node/segment clears only its own evidence; it says nothing about
// whether another node has recovered capacity.
func clearSuccessfulFailures(failures map[int64]map[FailedResourceKey]ResourceFailure, sm *CoordQueryViewStateMachine) {
	if len(failures) == 0 {
		return
	}
	meta := sm.View().GetMeta()
	for _, node := range sm.View().GetQueryNode() {
		resources := failures[node.GetNodeId()]
		if len(resources) == 0 {
			continue
		}
		for _, partition := range node.GetPartitions() {
			for i, segment := range partition.GetSegmentIds() {
				manifest := int64(0)
				if i < len(partition.GetSegmentManifestVersions()) {
					manifest = partition.GetSegmentManifestVersions()[i]
				}
				key := FailureKey(partition.GetPartitionId(), segment, manifest, meta.GetSchemaVersion(), meta.GetLoadInfoVersion(), sm.Version().DataVersion)
				if failure, ok := resources[key]; ok && !failure.Version.GT(sm.Version()) {
					delete(resources, key)
				}
			}
		}
		if len(resources) == 0 {
			delete(failures, node.GetNodeId())
		}
	}
}

func fillFailedResources(failed map[int64]map[FailedResourceKey]ResourceFailure, sm *CoordQueryViewStateMachine, readers ...resourcewatermark.Reader) map[int64]map[FailedResourceKey]ResourceFailure {
	if sm.State() != qviews.QueryViewStateUnrecoverable {
		return failed
	}
	meta := sm.View().GetMeta()
	for _, node := range sm.View().GetQueryNode() {
		id := node.GetNodeId()
		if sm.qnStates[id] != qviews.QueryViewStateUnrecoverable || len(sm.qnPreparationFailures[id]) == 0 {
			continue
		}
		ready := segmentSet(sm.QNReadySegments()[id])
		for _, partition := range node.GetPartitions() {
			for i, segment := range partition.GetSegmentIds() {
				if ready[segment] {
					continue
				}
				reason := sm.qnPreparationFailures[id][segment]
				switch reason {
				case viewpb.SegmentPerparingFailure_MemoryLimitExceeded, viewpb.SegmentPerparingFailure_DiskLimitExceeded, viewpb.SegmentPerparingFailure_ResourceReservationFailed:
				default:
					continue
				}
				if failed == nil {
					failed = make(map[int64]map[FailedResourceKey]ResourceFailure)
				}
				if failed[id] == nil {
					failed[id] = make(map[FailedResourceKey]ResourceFailure)
				}
				manifest := int64(0)
				if i < len(partition.GetSegmentManifestVersions()) {
					manifest = partition.GetSegmentManifestVersions()[i]
				}
				key := FailureKey(partition.GetPartitionId(), segment, manifest, meta.GetSchemaVersion(), meta.GetLoadInfoVersion(), sm.Version().DataVersion)
				if previous, ok := failed[id][key]; !ok || sm.Version().GT(previous.Version) {
					fact := ResourceFailure{Version: sm.Version(), Reason: reason, ObservedAt: time.Now()}
					if len(readers) > 0 && readers[0] != nil {
						if sample, ok := readers[0].Read(id, fact.ObservedAt); ok {
							fact.Watermarks = sample
						}
					}
					failed[id][key] = fact
				}
			}
		}
	}
	return failed
}

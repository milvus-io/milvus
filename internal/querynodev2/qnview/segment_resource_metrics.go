package qnview

import (
	"sort"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// SegmentMetrics snapshots actual physical bases, once per base rather than per
// View. The existing operation references prevent teardown during native reads;
// no Schema is needed and native calls run outside the manager lock.
func (m *SegmentResourceManager) SegmentMetrics(collectionID, nodeID int64) []*metricsinfo.Segment {
	type pinnedBase struct {
		resource *segmentResource
		base     *segmentBase
	}
	var bases []pinnedBase
	m.mu.Lock()
	for _, r := range m.resources {
		if r.closing || (collectionID > 0 && r.collectionID != collectionID) {
			continue
		}
		for _, base := range r.bases {
			r.operations.Add(1)
			base.operationRefs++
			bases = append(bases, pinnedBase{r, base})
		}
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, pinned := range bases {
			pinned.base.operationRefs--
			pinned.resource.wakeLocked()
			pinned.resource.operations.Done()
		}
	}()
	byID := make(map[int64]*metricsinfo.Segment)
	for _, pinned := range bases {
		// The physical adapter exposes metadata without borrowing a View schema.
		readable, ok := UnwrapTransformSegment(pinned.base.segment).(interface{ QuerySegment() segments.Segment })
		if !ok {
			continue
		}
		s := readable.QuerySegment()
		if s == nil {
			continue
		}
		item := byID[s.ID()]
		if item == nil {
			item = &metricsinfo.Segment{SegmentID: s.ID(), CollectionID: s.Collection(), PartitionID: s.Partition(), State: s.Type().String(), ResourceGroup: s.ResourceGroup(), NodeID: nodeID}
			byID[s.ID()] = item
		}
		// Distinct bases own distinct physical storage. Compatible View ReadStates
		// share a base and must not multiply its memory or logical row count.
		item.MemSize += s.MemSize()
		item.LoadedInsertRowCount = max(item.LoadedInsertRowCount, s.InsertCount())
		for _, index := range s.Indexes() {
			info := index.IndexInfo
			found := false
			for _, existing := range item.IndexedFields {
				if existing.IndexFieldID == info.FieldID && existing.BuildID == info.BuildID {
					existing.IsLoaded = existing.IsLoaded || index.IsLoaded
					existing.HasRawData = existing.HasRawData || s.HasRawData(info.FieldID)
					found = true
					break
				}
			}
			if !found {
				item.IndexedFields = append(item.IndexedFields, &metricsinfo.IndexedField{IndexFieldID: info.FieldID, IndexID: info.IndexID, IndexSize: info.IndexSize, BuildID: info.BuildID, IsLoaded: index.IsLoaded, HasRawData: s.HasRawData(info.FieldID)})
			}
		}
	}
	result := make([]*metricsinfo.Segment, 0, len(byID))
	for _, item := range byID {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SegmentID < result[j].SegmentID })
	return result
}

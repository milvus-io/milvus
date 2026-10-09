// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataview

import (
	"context"
	"maps"
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (m *dataViewManager) captureFrontiers(ctx context.Context, collectionID int64) (map[string]uint64, error) {
	if m.frontierProjector == nil {
		return nil, nil
	}
	return m.frontierProjector(ctx, collectionID)
}

// advanceTransformFrontiers derives A from the selected bases and F from W/A.
// Bounds are WAL certificates, not recovery checkpoint observations. A may fall
// after a late Flush; the resulting shared-buffer frontier must never regress.
func advanceTransformFrontiers(view *viewpb.DataViewOfCollection, bounds map[string]uint64) error {
	for _, shard := range view.GetShards() {
		old := transformStart(shard)
		w := max(shard.GetGrowingTransformStartAfterTimetick(), bounds[shard.GetVchannel()])
		if w == 0 && bounds == nil {
			// Legacy, unwired managers are also used without Transform metadata.
			continue
		}
		if w == 0 {
			return merr.WrapErrServiceNotReadyMsg("WAL publication proof for %s is not available", shard.GetVchannel())
		}
		a := w
		hasMembers := false
		for _, partition := range shard.GetPartitions() {
			if len(partition.GetSegmentIds()) != len(partition.GetSegmentTransformStartAfterTimeticks()) {
				return merr.WrapErrDataIntegrityMsg("Segment Transform cursors are misaligned in shard %s", shard.GetVchannel())
			}
			for _, start := range partition.GetSegmentTransformStartAfterTimeticks() {
				if start == 0 {
					return merr.WrapErrServiceNotReadyMsg("Segment Transform coverage is not available")
				}
				if !hasMembers || start < a {
					a = start
				}
				hasMembers = true
			}
		}
		candidate := min(w, a)
		if candidate < old {
			return merr.WrapErrDataIntegrityMsg("Transform frontier cannot regress: collection=%d vchannel=%s old=%d candidate=%d", view.GetCollectionId(), shard.GetVchannel(), old, candidate)
		}
		shard.GrowingTransformStartAfterTimetick = w
		shard.CompactedTransformStartAfterTimetick = a
	}
	return nil
}

func (m *dataViewManager) advanceTransformFrontiers(ctx context.Context, view *viewpb.DataViewOfCollection, certificates map[string]uint64) error {
	var constraints map[string]uint64
	if m.pendingTransformProjector != nil {
		var err error
		constraints, err = m.pendingTransformProjector(ctx, view)
		if err != nil {
			return err
		}
	}
	certificates = maps.Clone(certificates)
	for channel, certificate := range certificates {
		if certificate == 0 {
			delete(certificates, channel)
		}
	}
	if len(certificates) == 0 {
		certificates = nil
	}
	for channel, bound := range constraints {
		if certificate, ok := certificates[channel]; ok {
			certificates[channel] = min(certificate, bound)
		}
	}
	if err := advanceTransformFrontiers(view, certificates); err != nil {
		return err
	}
	for _, shard := range view.GetShards() {
		if bound, ok := constraints[shard.GetVchannel()]; ok && transformStart(shard) > bound {
			return merr.WrapErrServiceNotReadyMsg("pending data for %s requires Transform history before %d", shard.GetVchannel(), bound)
		}
		if m.frontierProjector != nil && shard.GetGrowingTransformStartAfterTimetick() == 0 {
			return merr.WrapErrServiceNotReadyMsg("WAL publication proof for %s is not available", shard.GetVchannel())
		}
	}
	return nil
}

// restoreTransformFrontiers validates each version's persisted W/A before it is
// exposed. Only pre-W/A snapshots use the isolated legacy migration projection;
// their reconstructed frontier is bounded by all retained successors.
func (m *dataViewManager) restoreTransformFrontiers(ctx context.Context, state *collectionState) error {
	var bounds map[string]uint64
	needsMigration := false
	entries := make([]*versionEntry, 0, len(state.versions))
	for _, entry := range state.versions {
		entries = append(entries, entry)
		for _, shard := range entry.view.GetShards() {
			needsMigration = needsMigration || shard.GetGrowingTransformStartAfterTimetick() == 0
		}
	}
	if needsMigration {
		var err error
		bounds, err = m.captureFrontiers(ctx, state.id)
		if err != nil {
			return err
		}
		bounds = maps.Clone(bounds)
	}
	sort.Slice(entries, func(i, j int) bool {
		return compareDataVersion(entries[i].view.GetDataVersion(), entries[j].view.GetDataVersion()) > 0
	})
	successors := make(map[string]*viewpb.DataViewOfShard)
	for _, entry := range entries {
		{
			restored := canonicalDataViewClone(entry.view)
			for _, shard := range restored.GetShards() {
				one := &viewpb.DataViewOfCollection{CollectionId: state.id, Shards: []*viewpb.DataViewOfShard{shard}}
				if shard.GetGrowingTransformStartAfterTimetick() == 0 {
					if err := advanceTransformFrontiers(one, bounds); err != nil {
						return err
					}
				} else {
					storedA := shard.GetCompactedTransformStartAfterTimetick()
					if err := advanceTransformFrontiers(one, nil); err != nil {
						return err
					}
					if storedA != shard.GetCompactedTransformStartAfterTimetick() {
						return merr.WrapErrDataIntegrityMsg("persisted Transform coverage does not match selected bases in %s", shard.GetVchannel())
					}
				}
			}
			for _, shard := range restored.GetShards() {
				if next := successors[shard.GetVchannel()]; next != nil &&
					(shard.GetGrowingTransformStartAfterTimetick() > next.GetGrowingTransformStartAfterTimetick() || transformStart(shard) > transformStart(next)) {
					return merr.WrapErrDataIntegrityMsg("persisted Transform frontiers regress across DataVersions in %s", shard.GetVchannel())
				}
				successors[shard.GetVchannel()] = shard
			}
			entry.view = restored
		}
		for _, shard := range entry.view.GetShards() {
			if bounds != nil {
				bounds[shard.GetVchannel()] = min(bounds[shard.GetVchannel()], transformStart(shard))
			}
		}
	}
	return nil
}

// A recovered legacy snapshot may not yet have sufficient inputs. Do not hand
// a zero cursor to the QueryView builder while reconciliation is pending.
func (m *dataViewManager) acquireReadyRefLocked(ctx context.Context, state *collectionState, entry *versionEntry) (DataViewRef, error) {
	if m.frontierProjector != nil && entry != nil && !entry.isTombstone && !transformFrontiersReady(entry.view) {
		err := m.restoreTransformFrontiers(ctx, state)
		// An older retained entry may still lack legacy coverage even after
		// the requested version was successfully reconstructed.
		if !transformFrontiersReady(entry.view) {
			if err != nil {
				return nil, err
			}
			return nil, merr.WrapErrServiceNotReadyMsg("Transform frontier for collection %d is not ready", state.id)
		}
		m.publishDataViewLocked(state)
	}
	return acquireRefLocked(state, entry), nil
}

func transformFrontiersReady(view *viewpb.DataViewOfCollection) bool {
	for _, shard := range view.GetShards() {
		if transformStart(shard) == 0 {
			return false
		}
	}
	return true
}

// transformStart is the exclusive shared-buffer cursor. DataView persists only
// the two inputs; QueryView receives their minimum at publication time.
func transformStart(shard *viewpb.DataViewOfShard) uint64 {
	return min(shard.GetGrowingTransformStartAfterTimetick(), shard.GetCompactedTransformStartAfterTimetick())
}

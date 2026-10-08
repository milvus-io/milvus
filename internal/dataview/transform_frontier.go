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

// advanceTransformFrontiers combines the checkpoint/unpublished bounds with
// the exact Segment revisions selected into this snapshot. Missing checkpoint
// reports cannot authorize progress. A regression is never hidden by clamping.
func advanceTransformFrontiers(view *viewpb.DataViewOfCollection, bounds map[string]uint64) error {
	if bounds == nil {
		return nil
	}
	for _, shard := range view.GetShards() {
		old := shard.GetTransformStartAfterTimetick()
		candidate, exists := bounds[shard.GetVchannel()]
		if !exists {
			candidate = old
		}
		for _, partition := range shard.GetPartitions() {
			for _, start := range partition.GetSegmentTransformStartAfterTimeticks() {
				if start == 0 {
					return merr.WrapErrServiceNotReadyMsg("Segment Transform coverage is not available")
				}
				candidate = min(candidate, start)
			}
		}
		if candidate == 0 {
			return merr.WrapErrServiceNotReadyMsg("Transform checkpoint for %s is not available", shard.GetVchannel())
		}
		if candidate < old {
			return merr.WrapErrDataIntegrityMsg("Transform frontier cannot regress: collection=%d vchannel=%s old=%d candidate=%d", view.GetCollectionId(), shard.GetVchannel(), old, candidate)
		}
		shard.TransformStartAfterTimetick = candidate
	}
	return nil
}

// restoreTransformFrontiers derives runtime-only cursors before recovered
// snapshots are exposed. Historical empty snapshots must also be bounded by
// their successor: an old empty membership alone must not pick today's K and
// overtake a later snapshot that still needs an earlier Segment cursor.
func (m *dataViewManager) restoreTransformFrontiers(ctx context.Context, state *collectionState) error {
	bounds, err := m.captureFrontiers(ctx, state.id)
	if err != nil || bounds == nil {
		return err
	}
	bounds = maps.Clone(bounds)
	entries := make([]*versionEntry, 0, len(state.versions))
	for _, entry := range state.versions {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return compareDataVersion(entries[i].view.GetDataVersion(), entries[j].view.GetDataVersion()) > 0
	})
	for _, entry := range entries {
		// Entries already handed to consumers stay immutable. An entry with
		// unknown F has never been acquirable, so it can be initialized here.
		if !transformFrontiersReady(entry.view) {
			restored := canonicalDataViewClone(entry.view)
			if err := advanceTransformFrontiers(restored, bounds); err != nil {
				return err
			}
			entry.view = restored
		}
		for _, shard := range entry.view.GetShards() {
			bounds[shard.GetVchannel()] = min(bounds[shard.GetVchannel()], shard.GetTransformStartAfterTimetick())
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
	}
	return acquireRefLocked(state, entry), nil
}

func transformFrontiersReady(view *viewpb.DataViewOfCollection) bool {
	for _, shard := range view.GetShards() {
		if shard.GetTransformStartAfterTimetick() == 0 {
			return false
		}
	}
	return true
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A pending segment has catalog metadata but may still have completed indexes
// whose only durable inventory is in its manifest. Recovery clears the entry
// after installing all records from the current revision.
type manifestIndexRecovery struct {
	mu          sync.RWMutex
	segments    map[int64]int64
	collections map[int64]int
}

func (m *Meta) prepareManifestIndexRecovery(ctx context.Context) {
	segments := m.SelectSegments(ctx, SegmentFilterFunc(needsManifestIndexRecovery))
	m.manifestIndexRecovery.segments = make(map[int64]int64, len(segments))
	m.manifestIndexRecovery.collections = make(map[int64]int)
	for _, segment := range segments {
		m.manifestIndexRecovery.segments[segment.GetID()] = segment.GetCollectionID()
		m.manifestIndexRecovery.collections[segment.GetCollectionID()]++
	}
}

func (m *Meta) HasPendingManifestIndexes() bool {
	if m == nil {
		return false
	}
	recovery := &m.manifestIndexRecovery
	recovery.mu.RLock()
	defer recovery.mu.RUnlock()
	return len(recovery.segments) != 0
}

func (m *Meta) ManifestIndexesPending(segmentID int64) bool {
	if m == nil {
		return false
	}
	recovery := &m.manifestIndexRecovery
	recovery.mu.RLock()
	defer recovery.mu.RUnlock()
	_, pending := recovery.segments[segmentID]
	return pending
}

func (m *Meta) CollectionManifestIndexesPending(collectionID int64) bool {
	if m == nil {
		return false
	}
	recovery := &m.manifestIndexRecovery
	recovery.mu.RLock()
	defer recovery.mu.RUnlock()
	return recovery.collections[collectionID] != 0
}

func (m *Meta) CheckCollectionManifestIndexes(collectionID int64) error {
	if m.CollectionManifestIndexesPending(collectionID) {
		return merr.WrapErrServiceNotReadyMsg("recovering manifest indexes for collection %d", collectionID)
	}
	return nil
}

func (m *Meta) CheckSegmentManifestIndexes(segmentID int64) error {
	if m.ManifestIndexesPending(segmentID) {
		return merr.WrapErrServiceNotReadyMsg("recovering manifest indexes for segment %d", segmentID)
	}
	return nil
}

func (m *Meta) finishManifestIndexRecovery(segmentID int64) int64 {
	recovery := &m.manifestIndexRecovery
	recovery.mu.Lock()
	defer recovery.mu.Unlock()
	collectionID, pending := recovery.segments[segmentID]
	if !pending {
		return 0
	}
	delete(recovery.segments, segmentID)
	if len(recovery.segments) == 0 {
		recovery.segments = nil
		recovery.collections = nil
		return collectionID
	}
	recovery.collections[collectionID]--
	if recovery.collections[collectionID] == 0 {
		delete(recovery.collections, collectionID)
		return collectionID
	}
	return 0
}

// RecoverPendingManifestIndexes runs after DataCoord becomes healthy. A failure
// keeps just that segment pending while other segments continue to recover.
func (m *Meta) RecoverPendingManifestIndexes(ctx context.Context, collectionRecovered func(int64) error) {
	recoveredCollections := make(map[int64]struct{})
	for ctx.Err() == nil {
		m.recoverPendingManifestIndexBatch(ctx, func(collectionID int64) {
			recoveredCollections[collectionID] = struct{}{}
		})
		for collectionID := range recoveredCollections {
			if err := collectionRecovered(collectionID); err != nil {
				mlog.RatedWarn(ctx, 1, "failed to resume DataView after index recovery",
					mlog.FieldCollectionID(collectionID), mlog.Err(err))
				continue
			}
			delete(recoveredCollections, collectionID)
		}
		if !m.HasPendingManifestIndexes() && len(recoveredCollections) == 0 {
			return
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (m *Meta) recoverPendingManifestIndexBatch(ctx context.Context, collectionRecovered func(int64)) {
	recovery := &m.manifestIndexRecovery
	recovery.mu.RLock()
	ids := make([]int64, 0, len(recovery.segments))
	for id := range recovery.segments {
		ids = append(ids, id)
	}
	recovery.mu.RUnlock()

	report := func(segmentID int64) {
		collectionID := m.finishManifestIndexRecovery(segmentID)
		NotifySegmentIndexBuild(segmentID)
		if collectionID != 0 {
			collectionRecovered(collectionID)
		}
	}
	segments := make([]*SegmentInfo, 0, len(ids))
	for _, id := range ids {
		segment := m.GetSegment(ctx, id)
		if !needsManifestIndexRecovery(segment) {
			report(id)
		} else if segment.GetManifestPath() != "" {
			segments = append(segments, segment)
		} else {
			mlog.RatedWarn(ctx, 1, "marked segment has no manifest for index recovery", mlog.FieldSegmentID(id))
		}
	}
	_ = m.readManifestIndexInventory(ctx, segments, func(result manifestIndexReadResult, rootPath string) error {
		segmentID := result.request.segment.GetID()
		if err := m.installRecoveredManifestIndexes(ctx, result, rootPath); err != nil {
			mlog.RatedWarn(ctx, 1, "manifest indexes still pending", mlog.FieldSegmentID(segmentID), mlog.Err(err))
			return nil
		}
		report(segmentID)
		return nil
	})
}

func (m *Meta) installRecoveredManifestIndexes(ctx context.Context, result manifestIndexReadResult, rootPath string) error {
	observed := result.request.segment
	indexes, removed, err := m.recoverManifestIndexes(ctx, observed, result.entries, result.err, rootPath)
	if err != nil {
		return err
	}

	segmentID := observed.GetID()
	locks := m.GetSegmentManifestLocks()
	locks.Lock(segmentID)
	defer locks.Unlock(segmentID)
	current := m.GetSegment(ctx, segmentID)
	if !needsManifestIndexRecovery(current) {
		return nil
	}
	if !packed.SameManifestPath(current.GetManifestPath(), observed.GetManifestPath()) ||
		removed && IsSegmentHealthy(current) {
		return StaleSegmentManifestError(segmentID, observed.GetManifestPath(), current.GetManifestPath())
	}
	if len(indexes) == 0 && !removed {
		return m.UpdateSegmentsInfo(ctx, ClearEmptyManifestIndexMarker(segmentID, observed.GetManifestPath()))
	}
	for _, record := range indexes {
		ids := []int64{record.BuildID}
		m.IndexCatalog.LockBuildIDs(ids)
		if !m.IndexCatalog.HasBuildInfo(record.BuildID) {
			m.IndexCatalog.UpdateSegmentIndex(record)
		}
		m.IndexCatalog.UnlockBuildIDs(ids)
	}
	return nil
}

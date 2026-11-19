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

package meta

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ProtectSplitAdoptionInputs pauses source replacement only while the lifecycle-
// guarded prepare/commit attempt runs. A failed attempt can be re-prepared with
// new baseline identities after a legitimate schema or manifest mutation.
func (m *Meta) ProtectSplitAdoptionInputs(moves []split.AdoptionMove) (func(), error) {
	ids := make([]int64, 0, len(moves))
	for _, move := range moves {
		ids = append(ids, move.SegmentID)
	}
	defer m.LockSegments(ids...)()
	m.groupMu.Lock()
	defer m.groupMu.Unlock()
	excluded := m.segmentChangeExclusionsLocked(0, true)
	for _, move := range moves {
		segment := m.Segments.GetSegment(move.SegmentID)
		if !IsSegmentHealthy(segment) || segment.IsCompacting || excluded.Contain(move.SegmentID) {
			return nil, merr.WrapErrServiceNotReadyMsg("split input %d is occupied", move.SegmentID)
		}
	}
	for _, move := range moves {
		m.splitAdoptionInputs[move.SegmentID] = struct{}{}
	}
	return func() {
		m.groupMu.Lock()
		defer m.groupMu.Unlock()
		for _, move := range moves {
			delete(m.splitAdoptionInputs, move.SegmentID)
		}
	}, nil
}

func (m *Meta) splitAdoptionInputProtected(id int64) bool {
	_, ok := m.splitAdoptionInputs[id]
	return ok
}

// PrepareSplitAdoption writes only invisible, immutable move pages. Target
// flushes remain free to publish while this potentially long operation runs.
func (m *Meta) PrepareSplitAdoption(ctx context.Context, task *datapb.SplitShardTask, moves []split.AdoptionMove) (*model.SplitAdoption, error) {
	catalog, ok := m.Catalog.(model.SplitAdoptionCatalog)
	if !ok {
		return nil, merr.WrapErrServiceNotReadyMsg("catalog does not support staged split adoption")
	}
	existing, err := catalog.LoadSplitAdoption(ctx, task.GetCollectionId(), task.GetTaskId())
	if err != nil {
		return nil, err
	}
	if existing != nil && (existing.State == "Committed" || existing.State == "Cleaning" || existing.State == "Done") {
		return existing, nil
	}
	record := &model.SplitAdoption{TaskID: task.GetTaskId(), CollectionID: task.GetCollectionId(), Source: task.GetSourceVchannel()}
	ids := make([]int64, 0, len(moves))
	for _, move := range moves {
		ids = append(ids, move.SegmentID)
	}
	defer m.LockSegments(ids...)()
	for _, move := range moves {
		current := m.Segments.GetSegment(move.SegmentID)
		if !IsSegmentHealthy(current) || current.IsCompacting || current.GetInsertChannel() != record.Source || current.GetSplitTarget() != move.Target {
			return nil, merr.WrapErrServiceNotReadyMsg("adoption input %d changed", move.SegmentID)
		}
		record.Moves = append(record.Moves, model.SplitAdoptionMove{SegmentID: move.SegmentID, PartitionID: move.PartitionID, Target: move.Target, TransformStart: move.TransformStart, ManifestPath: current.GetManifestPath(), SourceTransformStart: current.GetTransformStartAfterTimetick()})
	}
	return catalog.PrepareSplitAdoption(ctx, record)
}

// AdoptSplit publishes the sole ownership authority with the current DataView
// root, then installs every effective owner in one cache publication. Canonical
// segment rows may subsequently be written in bounded batches; restart applies
// the committed overlay in Catalog.ListSegments before MetaStore sees any row.
func (m *Meta) AdoptSplit(ctx context.Context, record *model.SplitAdoption, view *viewpb.DataViewOfCollection) error {
	catalog, ok := m.Catalog.(model.SplitAdoptionCatalog)
	if !ok {
		return merr.WrapErrServiceNotReadyMsg("catalog does not support staged split adoption")
	}
	ids := make([]int64, 0, len(record.Moves))
	for _, move := range record.Moves {
		ids = append(ids, move.SegmentID)
	}
	defer m.LockSegments(ids...)()
	m.groupMu.RLock()
	defer m.groupMu.RUnlock()
	excluded := m.segmentChangeExclusionsLocked(record.CollectionID, true)
	if view == nil {
		return merr.WrapErrServiceNotReadyMsg("split collection DataView has been dropped")
	}
	committed := record.State == "Committed" || record.State == "Cleaning" || record.State == "Done"
	updates := make([]*SegmentInfo, 0, len(record.Moves))
	for _, move := range record.Moves {
		current := m.Segments.GetSegment(move.SegmentID)
		if committed && (current == nil || current.GetInsertChannel() == move.Target) {
			continue
		}
		if !IsSegmentHealthy(current) || current.IsCompacting || excluded.Contain(move.SegmentID) || current.GetCollectionID() != record.CollectionID || current.GetPartitionID() != move.PartitionID || current.GetInsertChannel() != record.Source || current.GetSplitTarget() != move.Target || current.GetManifestPath() != move.ManifestPath || current.GetTransformStartAfterTimetick() != move.SourceTransformStart {
			return merr.WrapErrServiceNotReadyMsg("adoption segment %d changed while preparing", move.SegmentID)
		}
		next := current.Clone()
		next.InsertChannel = move.Target
		next.SplitTarget = ""
		next.TransformStartAfterTimetick = move.TransformStart
		next.StartPosition = &msgpb.MsgPosition{ChannelName: move.Target, Timestamp: move.TransformStart}
		next.DmlPosition = &msgpb.MsgPosition{ChannelName: move.Target, Timestamp: move.TransformStart}
		if proto.Size(next.SegmentInfo) > (1<<20)-1024 {
			return merr.Wrapf(merr.ErrServiceResourceInsufficient, "adopted segment %d exceeds metadata single-value budget", move.SegmentID)
		}
		updates = append(updates, next)
	}
	if !committed {
		if err := catalog.CommitSplitAdoption(ctx, record, view); err != nil {
			return err
		}
	}
	m.Segments.PublishSegments(updates)
	return nil
}

// FinishSplitAdoption canonicalizes current effective rows, never the stale
// prepare image. Holding each segment writer lock during its canonical write prevents a newer
// target flush/manifest update from being overwritten by cleanup.
func (m *Meta) FinishSplitAdoption(ctx context.Context, record *model.SplitAdoption) error {
	catalog := m.Catalog.(model.SplitAdoptionCatalog)
	if record.State == "Done" {
		return nil
	}
	if err := catalog.SaveSplitAdoptionState(ctx, record, "Cleaning"); err != nil {
		return err
	}
	for _, move := range record.Moves {
		unlock := m.LockSegments(move.SegmentID)
		current := m.Segments.GetSegment(move.SegmentID)
		var err error
		if current != nil {
			err = catalog.CanonicalizeSplitAdoptionSegment(ctx, current.SegmentInfo)
		}
		unlock()
		if err != nil {
			return err
		}
	}
	return catalog.SaveSplitAdoptionState(ctx, record, "Done")
}

// AdvanceSplitFoldedCursors consumes persisted M >= through captured before
// reading SegmentMeta. A live L0 starting at/before through is a hole for its
// partition (including straddling L0s). Strictly newer L0s do not obstruct the
// finite history certificate. This also certifies empty/no-hit histories.
func (m *Meta) AdvanceSplitFoldedCursors(ctx context.Context, collectionID int64, channel string, through uint64) ([]int64, error) {
	locks := newSegmentLockSet(m.GetSegmentLocks())
	defer locks.release()
	ids := lockSelection(locks, func() []int64 {
		var ids []int64
		for _, segment := range m.Segments.GetSegmentsBySelector(WithChannel(channel), WithCollection(collectionID)) {
			ids = append(ids, segment.GetID())
		}
		return ids
	})
	segments := m.GetSegmentInfos(ids)
	holes := make(map[int64][]int64)
	for _, segment := range segments {
		if !IsSegmentHealthy(segment) || segment.GetCollectionID() != collectionID || segment.GetLevel() != datapb.SegmentLevel_L0 {
			continue
		}
		// L0 CommitTimestamp is an import visibility timestamp, not a
		// certificate of its earliest Delete. Keep imported L0 conservative
		// even after IsImporting is cleared by commit.
		start := segment.GetStartPosition().GetTimestamp()
		if segment.GetIsImporting() || segment.GetCommitTimestamp() != 0 || start <= through {
			holes[segment.GetPartitionID()] = append(holes[segment.GetPartitionID()], segment.GetID())
		}
	}
	blocked := make(map[int64]bool)
	var updates []*SegmentInfo
	var actions []metastore.UpdateAction
	for _, segment := range segments {
		if !IsSegmentHealthy(segment) || segment.GetCollectionID() != collectionID || segment.GetLevel() == datapb.SegmentLevel_L0 || segment.GetTransformStartAfterTimetick() >= through {
			continue
		}
		if len(holes[segment.GetPartitionID()]) > 0 || len(holes[common.AllPartitionsID]) > 0 {
			blocked[segment.GetPartitionID()] = true
			blocked[common.AllPartitionsID] = true
			continue
		}
		next := segment.Clone()
		next.TransformStartAfterTimetick = through
		updates = append(updates, next)
		actions = append(actions, metastore.UpdateSegment(next.SegmentInfo))
	}
	if len(actions) > 0 {
		if err := m.Catalog.Update(ctx, actions...); err != nil {
			return nil, err
		}
		m.Segments.PublishSegments(updates)
	}
	var blockers []int64
	for partition := range blocked {
		blockers = append(blockers, holes[partition]...)
	}
	return blockers, nil
}

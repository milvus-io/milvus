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

package datacoord

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

var errUnsupportedCompactionTarget = errors.New("unsupported compaction target")

type compactionTargetMeta struct {
	sync.RWMutex
	ctx     context.Context
	catalog metastore.DataCoordCatalog
	targets map[int64]*compactionTarget
}

func newCompactionTargetMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*compactionTargetMeta, error) {
	meta := &compactionTargetMeta{
		ctx:     ctx,
		catalog: catalog,
		targets: make(map[int64]*compactionTarget),
	}
	if err := meta.reloadFromKV(); err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *compactionTargetMeta) reloadFromKV() error {
	tr := timerecord.NewTimeRecorder("compactionTargetMeta-reloadFromKV")
	targets, err := m.catalog.ListCompactionTargets(m.ctx)
	if err != nil {
		return err
	}
	loadedTargets := make(map[int64]*compactionTarget, len(targets))
	for _, target := range targets {
		runtimeTarget, err := newCompactionTarget(target)
		if err != nil {
			log.Warn("materialize inert compaction target",
				zap.Int64("targetID", target.GetTargetID()),
				zap.Int64("collectionID", target.GetCollectionID()),
				zap.String("intent", target.GetIntent().String()),
				zap.Error(err))
		}
		loadedTargets[target.GetTargetID()] = runtimeTarget
	}
	m.targets = loadedTargets
	log.Info("DataCoord compactionTargetMeta reloadFromKV done", zap.Duration("duration", tr.ElapseSpan()))
	return nil
}

// GetCompactionTarget returns a cloned target by ID, or nil when the target is
// not loaded in DataCoord memory.
func (m *compactionTargetMeta) GetCompactionTarget(targetID int64) *datapb.CompactionTarget {
	m.RLock()
	defer m.RUnlock()

	target, ok := m.targets[targetID]
	if !ok {
		return nil
	}
	return target.Clone()
}

// GetCompactionTargets returns cloned targets keyed by target ID.
func (m *compactionTargetMeta) GetCompactionTargets() map[int64]*datapb.CompactionTarget {
	m.RLock()
	defer m.RUnlock()

	targets := make(map[int64]*datapb.CompactionTarget, len(m.targets))
	for targetID, target := range m.targets {
		targets[targetID] = target.Clone()
	}
	return targets
}

func (m *compactionTargetMeta) GetActiveCompactionTargets() map[int64]compactionTargetEvaluator {
	m.RLock()
	defer m.RUnlock()

	active := make(map[int64]compactionTargetEvaluator)
	for targetID, target := range m.targets {
		if target.active() {
			active[targetID] = target
		}
	}
	return active
}

// SaveCompactionTarget persists a target before updating the in-memory cache.
func (m *compactionTargetMeta) SaveCompactionTarget(ctx context.Context, target *datapb.CompactionTarget) error {
	m.Lock()
	defer m.Unlock()

	if err := m.catalog.SaveCompactionTarget(ctx, target); err != nil {
		log.Error("meta update: save compaction target failed",
			zap.Int64("targetID", target.GetTargetID()),
			zap.Int64("collectionID", target.GetCollectionID()),
			zap.Error(err))
		return err
	}
	m.upsertCompactionTargetLocked(target)
	log.Info("meta update: save compaction target done",
		zap.Int64("targetID", target.GetTargetID()),
		zap.Int64("collectionID", target.GetCollectionID()),
		zap.String("intent", target.GetIntent().String()),
		zap.String("state", target.GetState().String()))
	return nil
}

// UpdateCompactionTargetState persists a state transition and mirrors it
// into the in-memory cache when the target is loaded.
func (m *compactionTargetMeta) UpdateCompactionTargetState(ctx context.Context, targetID int64, state datapb.TargetState) error {
	m.Lock()
	defer m.Unlock()

	var inactivatedAtTS uint64
	target, loaded := m.targets[targetID]
	if loaded {
		var changed bool
		inactivatedAtTS, changed = compactionTargetStateUpdate(target, state)
		if !changed {
			log.Info("meta update: skip unchanged compaction target state",
				zap.Int64("targetID", targetID),
				zap.String("state", state.String()))
			return nil
		}
	} else {
		inactivatedAtTS = compactionTargetInactivatedAtTS(state)
	}
	if err := m.catalog.UpdateCompactionTargetState(ctx, targetID, state, inactivatedAtTS); err != nil {
		log.Error("meta update: update compaction target state failed",
			zap.Int64("targetID", targetID),
			zap.String("state", state.String()),
			zap.Error(err))
		return err
	}
	if loaded {
		updated := target.Clone()
		updated.State = state
		updated.InactivatedAtTS = inactivatedAtTS
		m.upsertCompactionTargetLocked(updated)
	}
	log.Info("meta update: update compaction target state done",
		zap.Int64("targetID", targetID),
		zap.String("state", state.String()),
		zap.Uint64("inactivatedAtTS", inactivatedAtTS))
	return nil
}

// DropCompactionTarget removes a target from KV and memory.
func (m *compactionTargetMeta) DropCompactionTarget(ctx context.Context, targetID int64) error {
	m.Lock()
	defer m.Unlock()

	target, ok := m.targets[targetID]
	var record *datapb.CompactionTarget
	if !ok {
		record = &datapb.CompactionTarget{TargetID: targetID}
	} else {
		record = target.Clone()
	}
	if err := m.catalog.DropCompactionTarget(ctx, record); err != nil {
		log.Error("meta update: drop compaction target failed",
			zap.Int64("targetID", targetID),
			zap.Int64("collectionID", record.GetCollectionID()),
			zap.Error(err))
		return err
	}
	delete(m.targets, targetID)
	log.Info("meta update: drop compaction target done",
		zap.Int64("targetID", targetID),
		zap.Int64("collectionID", record.GetCollectionID()))
	return nil
}

func (m *compactionTargetMeta) upsertCompactionTargetLocked(target *datapb.CompactionTarget) {
	if target == nil {
		return
	}
	runtimeTarget, err := newCompactionTarget(target)
	if err != nil {
		log.Warn("materialize inert compaction target",
			zap.Int64("targetID", target.GetTargetID()),
			zap.Int64("collectionID", target.GetCollectionID()),
			zap.String("intent", target.GetIntent().String()),
			zap.Error(err))
	}
	m.targets[target.GetTargetID()] = runtimeTarget
}

type compactionTargetFactory interface {
	Create(ctx context.Context, alloc allocator.Allocator) (*datapb.CompactionTarget, error)
}

type manualRewriteCompactionTarget struct {
	collectionID int64
	segmentIDs   []int64
}

var _ compactionTargetFactory = (*manualRewriteCompactionTarget)(nil)

func newManualRewriteCompactionTarget(collectionID int64, segmentIDs []int64) *manualRewriteCompactionTarget {
	return &manualRewriteCompactionTarget{
		collectionID: collectionID,
		segmentIDs:   sortedCompactionTargetSegmentIDs(segmentIDs),
	}
}

func (target *manualRewriteCompactionTarget) Create(ctx context.Context, alloc allocator.Allocator) (*datapb.CompactionTarget, error) {
	targetID, activatedAtTS, err := allocCompactionTargetIdentity(ctx, alloc)
	if err != nil {
		return nil, err
	}
	return &datapb.CompactionTarget{
		TargetID:      targetID,
		CollectionID:  target.collectionID,
		Intent:        datapb.TargetIntent_INTENT_REWRITE,
		Properties:    target.Properties(),
		ExpectedTS:    activatedAtTS,
		TailLimit:     0,
		State:         datapb.TargetState_TARGET_STATE_ACTIVE,
		ActivatedAtTS: activatedAtTS,
	}, nil
}

func (target *manualRewriteCompactionTarget) Properties() map[string]string {
	return compactionTargetSegmentIDProperties(target.segmentIDs)
}

type matchRule interface {
	Match(segment *SegmentInfo) bool
}

type compactionTargetEvaluator interface {
	MatchFilters() []SegmentFilter
	Satisfied(matches []*SegmentInfo) bool
}

type compactionTarget struct {
	*datapb.CompactionTarget
	segmentIDs []int64
	rule       matchRule
}

var _ compactionTargetEvaluator = (*compactionTarget)(nil)

func newCompactionTarget(target *datapb.CompactionTarget) (*compactionTarget, error) {
	if target == nil {
		return nil, merr.WrapErrParameterInvalidMsg("nil compaction target record")
	}
	runtimeTarget := &compactionTarget{
		CompactionTarget: proto.Clone(target).(*datapb.CompactionTarget),
	}
	switch target.GetIntent() {
	case datapb.TargetIntent_INTENT_REWRITE:
		segmentIDs, err := parseCompactionTargetSegmentIDs(runtimeTarget.CompactionTarget)
		if err != nil {
			return runtimeTarget, err
		}
		runtimeTarget.segmentIDs = segmentIDs
		runtimeTarget.rule = newRewriteRule(runtimeTarget.CompactionTarget)
		return runtimeTarget, nil
	default:
		return runtimeTarget, errUnsupportedCompactionTarget
	}
}

func (target *compactionTarget) Clone() *datapb.CompactionTarget {
	if target == nil || target.CompactionTarget == nil {
		return nil
	}
	return proto.Clone(target.CompactionTarget).(*datapb.CompactionTarget)
}

func (target *compactionTarget) active() bool {
	return target != nil &&
		target.CompactionTarget != nil &&
		target.rule != nil &&
		target.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE
}

func (target *compactionTarget) finite() bool {
	return target.GetTailLimit() >= 0
}

func (target *compactionTarget) scopeIn(segment *SegmentInfo) bool {
	if target == nil || target.CompactionTarget == nil || target.rule == nil || segment == nil {
		return false
	}
	if target.GetCollectionID() != 0 && segment.GetCollectionID() != target.GetCollectionID() {
		return false
	}
	if len(target.segmentIDs) > 0 {
		inSegmentScope := false
		for _, segmentID := range target.segmentIDs {
			if segment.GetID() == segmentID {
				inSegmentScope = true
				break
			}
		}
		if !inSegmentScope {
			return false
		}
	}
	if target.finite() && segment.GetDmlPosition().GetTimestamp() > target.GetExpectedTS() {
		return false
	}
	return true
}

func (target *compactionTarget) match(segment *SegmentInfo) bool {
	return target.scopeIn(segment) && target.rule.Match(segment)
}

// MatchFilters returns the complete authoritative semantic match filters for the target.
func (target *compactionTarget) MatchFilters() []SegmentFilter {
	filters := make([]SegmentFilter, 0, 2)
	if target != nil && target.CompactionTarget != nil && target.GetCollectionID() != 0 {
		filters = append(filters, WithCollection(target.GetCollectionID()))
	}
	return append(filters, SegmentFilterFunc(target.match))
}

func (target *compactionTarget) Satisfied(matches []*SegmentInfo) bool {
	tail := target.GetTailLimit()
	if tail < 0 {
		return false
	}
	matchedByLabel := make(map[CompactionGroupLabel]int64)
	for _, segment := range matches {
		label := CompactionGroupLabel{
			CollectionID: segment.GetCollectionID(),
			PartitionID:  segment.GetPartitionID(),
			Channel:      segment.GetInsertChannel(),
		}
		matchedByLabel[label]++
		if matchedByLabel[label] > int64(tail) {
			return false
		}
	}
	return true
}

func allocCompactionTargetIdentity(ctx context.Context, alloc allocator.Allocator) (int64, uint64, error) {
	if alloc == nil {
		return 0, 0, merr.WrapErrParameterInvalidMsg("compaction target allocator is nil")
	}
	targetID, err := alloc.AllocID(ctx)
	if err != nil {
		return 0, 0, err
	}
	activatedAtTS, err := alloc.AllocTimestamp(ctx)
	if err != nil {
		return 0, 0, err
	}
	return targetID, activatedAtTS, nil
}

func compactionTargetStateUpdate(target *compactionTarget, state datapb.TargetState) (uint64, bool) {
	if state == datapb.TargetState_TARGET_STATE_INACTIVE {
		if target.GetState() == state && target.GetInactivatedAtTS() != 0 {
			return target.GetInactivatedAtTS(), false
		}
		return tsoutil.GetCurrentTime(), true
	}
	if target.GetState() == state && target.GetInactivatedAtTS() == 0 {
		return 0, false
	}
	return 0, true
}

func compactionTargetInactivatedAtTS(state datapb.TargetState) uint64 {
	if state == datapb.TargetState_TARGET_STATE_INACTIVE {
		return tsoutil.GetCurrentTime()
	}
	return 0
}

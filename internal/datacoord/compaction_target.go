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
	"encoding/json"
	"sort"
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

const compactionTargetSegmentIDsProperty = "segment_ids"

var errUnsupportedCompactionTarget = errors.New("unsupported compaction target")

type compactionTargetMeta struct {
	sync.RWMutex
	ctx     context.Context
	catalog metastore.DataCoordCatalog
	records map[int64]*datapb.CompactionTarget
	targets *compactionTargets
}

func newCompactionTargetMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*compactionTargetMeta, error) {
	meta := &compactionTargetMeta{
		ctx:     ctx,
		catalog: catalog,
		records: make(map[int64]*datapb.CompactionTarget),
		targets: newCompactionTargets(nil),
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
	loadedTargets := make(map[int64]*datapb.CompactionTarget, len(targets))
	for _, target := range targets {
		loadedTargets[target.GetTargetID()] = proto.Clone(target).(*datapb.CompactionTarget)
	}
	m.records = loadedTargets
	m.targets = newCompactionTargets(loadedTargets)
	log.Info("DataCoord compactionTargetMeta reloadFromKV done", zap.Duration("duration", tr.ElapseSpan()))
	return nil
}

// GetCompactionTarget returns a cloned target by ID, or nil when the target is
// not loaded in DataCoord memory.
func (m *compactionTargetMeta) GetCompactionTarget(targetID int64) *datapb.CompactionTarget {
	m.RLock()
	defer m.RUnlock()

	target, ok := m.records[targetID]
	if !ok {
		return nil
	}
	return proto.Clone(target).(*datapb.CompactionTarget)
}

// GetCompactionTargets returns cloned targets keyed by target ID.
func (m *compactionTargetMeta) GetCompactionTargets() map[int64]*datapb.CompactionTarget {
	m.RLock()
	defer m.RUnlock()

	targets := make(map[int64]*datapb.CompactionTarget, len(m.records))
	for targetID, target := range m.records {
		targets[targetID] = proto.Clone(target).(*datapb.CompactionTarget)
	}
	return targets
}

func (m *compactionTargetMeta) GetActiveCompactionTargets() []*compactionTarget {
	m.RLock()
	defer m.RUnlock()

	return m.targets.Active()
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
	cloned := proto.Clone(target).(*datapb.CompactionTarget)
	m.records[target.GetTargetID()] = cloned
	m.targets.Upsert(cloned)
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

	inactivatedAtTS := uint64(0)
	if state == datapb.TargetState_TARGET_STATE_INACTIVE {
		inactivatedAtTS = tsoutil.GetCurrentTime()
	}
	if err := m.catalog.UpdateCompactionTargetState(ctx, targetID, state, inactivatedAtTS); err != nil {
		log.Error("meta update: update compaction target state failed",
			zap.Int64("targetID", targetID),
			zap.String("state", state.String()),
			zap.Error(err))
		return err
	}
	if target, ok := m.records[targetID]; ok {
		updated := proto.Clone(target).(*datapb.CompactionTarget)
		updated.State = state
		updated.InactivatedAtTS = inactivatedAtTS
		m.records[targetID] = updated
		m.targets.Upsert(updated)
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

	target, ok := m.records[targetID]
	if !ok {
		target = &datapb.CompactionTarget{TargetID: targetID}
	}
	if err := m.catalog.DropCompactionTarget(ctx, target); err != nil {
		log.Error("meta update: drop compaction target failed",
			zap.Int64("targetID", targetID),
			zap.Int64("collectionID", target.GetCollectionID()),
			zap.Error(err))
		return err
	}
	delete(m.records, targetID)
	m.targets.Delete(targetID)
	log.Info("meta update: drop compaction target done",
		zap.Int64("targetID", targetID),
		zap.Int64("collectionID", target.GetCollectionID()))
	return nil
}

type compactionTargets struct {
	targets map[int64]*compactionTarget
}

func newCompactionTargets(targetsByID map[int64]*datapb.CompactionTarget) *compactionTargets {
	runtimeTargets := &compactionTargets{
		targets: make(map[int64]*compactionTarget, len(targetsByID)),
	}
	for _, target := range targetsByID {
		runtimeTargets.Upsert(target)
	}
	return runtimeTargets
}

func (t *compactionTargets) Upsert(target *datapb.CompactionTarget) {
	if t == nil || target == nil {
		return
	}
	runtimeTarget, err := newCompactionTarget(target)
	if err != nil {
		delete(t.targets, target.GetTargetID())
		log.Warn("skip materializing compaction target",
			zap.Int64("targetID", target.GetTargetID()),
			zap.Int64("collectionID", target.GetCollectionID()),
			zap.String("intent", target.GetIntent().String()),
			zap.Error(err))
		return
	}
	t.targets[target.GetTargetID()] = runtimeTarget
}

func (t *compactionTargets) Delete(targetID int64) {
	if t == nil {
		return
	}
	delete(t.targets, targetID)
}

func (t *compactionTargets) Active() []*compactionTarget {
	if t == nil {
		return nil
	}
	active := make([]*compactionTarget, 0, len(t.targets))
	for _, target := range t.targets {
		if target.target.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE {
			active = append(active, target)
		}
	}
	sort.Slice(active, func(i, j int) bool {
		return active[i].target.GetTargetID() < active[j].target.GetTargetID()
	})
	return active
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

type compactionTarget struct {
	target     *datapb.CompactionTarget
	segmentIDs []int64
	rule       matchRule
}

func newCompactionTarget(target *datapb.CompactionTarget) (*compactionTarget, error) {
	if target == nil {
		return nil, merr.WrapErrParameterInvalidMsg("nil compaction target record")
	}
	switch target.GetIntent() {
	case datapb.TargetIntent_INTENT_REWRITE:
		return newRewriteCompactionTarget(target)
	default:
		return nil, errUnsupportedCompactionTarget
	}
}

func newRewriteCompactionTarget(target *datapb.CompactionTarget) (*compactionTarget, error) {
	cloned := proto.Clone(target).(*datapb.CompactionTarget)
	segmentIDs, err := parseCompactionTargetSegmentIDs(cloned)
	if err != nil {
		return nil, err
	}
	return &compactionTarget{
		target:     cloned,
		segmentIDs: segmentIDs,
		rule:       rewriteRule{expectedTS: cloned.GetExpectedTS()},
	}, nil
}

func (target *compactionTarget) Record() *datapb.CompactionTarget {
	if target == nil || target.target == nil {
		return nil
	}
	return proto.Clone(target.target).(*datapb.CompactionTarget)
}

func (target *compactionTarget) finite() bool {
	return target.target.GetTailLimit() >= 0
}

func (target *compactionTarget) ScopeIn(segment *SegmentInfo) bool {
	if target == nil || target.target == nil || segment == nil {
		return false
	}
	if target.target.GetCollectionID() != 0 && segment.GetCollectionID() != target.target.GetCollectionID() {
		return false
	}
	if len(target.segmentIDs) > 0 && !matchCompactionTargetSegmentIDScope(target.segmentIDs, segment) {
		return false
	}
	if target.finite() && segment.GetDmlPosition().GetTimestamp() > target.target.GetExpectedTS() {
		return false
	}
	return true
}

func (target *compactionTarget) Match(segment *SegmentInfo) bool {
	return target.ScopeIn(segment) && target.rule.Match(segment)
}

func (target *compactionTarget) Satisfied(inScopeByLabel map[CompactionGroupLabel][]*SegmentInfo) bool {
	tail := target.target.GetTailLimit()
	if tail < 0 {
		return false
	}
	for _, segments := range inScopeByLabel {
		matched := int64(0)
		for _, segment := range segments {
			if target.Match(segment) {
				matched++
			}
		}
		if matched > int64(tail) {
			return false
		}
	}
	return true
}

func (target *compactionTarget) SegmentsInScope(candidates []*SegmentInfo) []*SegmentInfo {
	segments := make([]*SegmentInfo, 0, len(candidates))
	for _, segment := range candidates {
		if target.ScopeIn(segment) {
			segments = append(segments, segment)
		}
	}
	return segments
}

func matchCompactionTargetSegmentIDScope(segmentIDs []int64, segment *SegmentInfo) bool {
	for _, segmentID := range segmentIDs {
		if segment.GetID() == segmentID {
			return true
		}
	}
	return false
}

type rewriteRule struct {
	expectedTS uint64
}

func (rule rewriteRule) Match(segment *SegmentInfo) bool {
	return segment != nil && segment.GetCreateTs() < rule.expectedTS
}

func sortedCompactionTargetSegmentIDs(segmentIDs []int64) []int64 {
	sorted := append([]int64(nil), segmentIDs...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	return sorted
}

func parseCompactionTargetSegmentIDs(target *datapb.CompactionTarget) ([]int64, error) {
	if target.GetIntent() != datapb.TargetIntent_INTENT_REWRITE {
		return nil, nil
	}
	raw := target.GetProperties()[compactionTargetSegmentIDsProperty]
	if raw == "" {
		return nil, nil
	}
	var segmentIDs []int64
	if err := json.Unmarshal([]byte(raw), &segmentIDs); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid compaction target segment_ids property: %v", err)
	}
	return sortedCompactionTargetSegmentIDs(segmentIDs), nil
}

func compactionTargetSegmentIDs(target *datapb.CompactionTarget) ([]int64, bool) {
	segmentIDs, err := parseCompactionTargetSegmentIDs(target)
	if err != nil {
		return nil, false
	}
	return segmentIDs, true
}

func compactionTargetSegmentIDProperties(segmentIDs []int64) map[string]string {
	if len(segmentIDs) == 0 {
		return nil
	}
	value, err := json.Marshal(sortedCompactionTargetSegmentIDs(segmentIDs))
	if err != nil {
		return nil
	}
	return map[string]string{compactionTargetSegmentIDsProperty: string(value)}
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

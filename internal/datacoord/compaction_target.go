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

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

const compactionTargetSegmentIDsProperty = "segment_ids"

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
	record := timerecord.NewTimeRecorder("compactionTargetMeta-reloadFromKV")
	records, err := m.catalog.ListCompactionTargets(m.ctx)
	if err != nil {
		return err
	}
	loadedRecords := make(map[int64]*datapb.CompactionTarget, len(records))
	for _, target := range records {
		loadedRecords[target.GetTargetID()] = proto.Clone(target).(*datapb.CompactionTarget)
	}
	m.records = loadedRecords
	m.targets = newCompactionTargets(loadedRecords)
	log.Info("DataCoord compactionTargetMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

// GetCompactionTarget returns a cloned target by ID, or nil when the target is
// not loaded in DataCoord memory.
func (m *compactionTargetMeta) GetCompactionTarget(targetID int64) *datapb.CompactionTarget {
	m.RLock()
	defer m.RUnlock()

	record, ok := m.records[targetID]
	if !ok {
		return nil
	}
	return proto.Clone(record).(*datapb.CompactionTarget)
}

// GetCompactionTargets returns cloned targets keyed by target ID.
func (m *compactionTargetMeta) GetCompactionTargets() map[int64]*datapb.CompactionTarget {
	m.RLock()
	defer m.RUnlock()

	records := make(map[int64]*datapb.CompactionTarget, len(m.records))
	for targetID, record := range m.records {
		records[targetID] = proto.Clone(record).(*datapb.CompactionTarget)
	}
	return records
}

func (m *compactionTargetMeta) GetActiveCompactionTargets() []compactionTarget {
	m.RLock()
	defer m.RUnlock()

	return m.targets.Active()
}

// SaveCompactionTarget persists a target before updating the in-memory cache.
func (m *compactionTargetMeta) SaveCompactionTarget(ctx context.Context, record *datapb.CompactionTarget) error {
	m.Lock()
	defer m.Unlock()

	if err := m.catalog.SaveCompactionTarget(ctx, record); err != nil {
		log.Error("meta update: save compaction target failed",
			zap.Int64("targetID", record.GetTargetID()),
			zap.Int64("collectionID", record.GetCollectionID()),
			zap.Error(err))
		return err
	}
	cloned := proto.Clone(record).(*datapb.CompactionTarget)
	m.records[record.GetTargetID()] = cloned
	m.targets.Upsert(cloned)
	log.Info("meta update: save compaction target done",
		zap.Int64("targetID", record.GetTargetID()),
		zap.Int64("collectionID", record.GetCollectionID()),
		zap.String("intent", record.GetIntent().String()),
		zap.String("state", record.GetState().String()))
	return nil
}

// UpdateCompactionTargetState persists a state transition and mirrors it
// into the in-memory cache when the record is loaded.
func (m *compactionTargetMeta) UpdateCompactionTargetState(ctx context.Context, targetID int64, state datapb.TargetState, inactivatedAtTS uint64) error {
	m.Lock()
	defer m.Unlock()

	if state != datapb.TargetState_TARGET_STATE_INACTIVE {
		inactivatedAtTS = 0
	}
	if err := m.catalog.UpdateCompactionTargetState(ctx, targetID, state, inactivatedAtTS); err != nil {
		log.Error("meta update: update compaction target state failed",
			zap.Int64("targetID", targetID),
			zap.String("state", state.String()),
			zap.Error(err))
		return err
	}
	if record, ok := m.records[targetID]; ok {
		updated := proto.Clone(record).(*datapb.CompactionTarget)
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
func (m *compactionTargetMeta) DropCompactionTarget(ctx context.Context, record *datapb.CompactionTarget) error {
	m.Lock()
	defer m.Unlock()

	if err := m.catalog.DropCompactionTarget(ctx, record); err != nil {
		log.Error("meta update: drop compaction target failed",
			zap.Int64("targetID", record.GetTargetID()),
			zap.Int64("collectionID", record.GetCollectionID()),
			zap.Error(err))
		return err
	}
	delete(m.records, record.GetTargetID())
	m.targets.Delete(record.GetTargetID())
	return nil
}

type compactionTargets struct {
	targets map[int64]compactionTarget
}

func newCompactionTargets(records map[int64]*datapb.CompactionTarget) *compactionTargets {
	targets := &compactionTargets{
		targets: make(map[int64]compactionTarget, len(records)),
	}
	for _, record := range records {
		targets.Upsert(record)
	}
	return targets
}

func (t *compactionTargets) Upsert(record *datapb.CompactionTarget) {
	if t == nil || record == nil {
		return
	}
	if !isSupportedCompactionTarget(record) {
		delete(t.targets, record.GetTargetID())
		return
	}
	t.targets[record.GetTargetID()] = newCompactionTarget(record)
}

func (t *compactionTargets) Delete(targetID int64) {
	if t == nil {
		return
	}
	delete(t.targets, targetID)
}

func (t *compactionTargets) Active() []compactionTarget {
	if t == nil {
		return nil
	}
	active := make([]compactionTarget, 0, len(t.targets))
	for _, target := range t.targets {
		record := target.Record()
		if record.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE {
			active = append(active, target)
		}
	}
	sort.Slice(active, func(i, j int) bool {
		return active[i].Record().GetTargetID() < active[j].Record().GetTargetID()
	})
	return active
}

type compactionTarget interface {
	Record() *datapb.CompactionTarget
	ScopeIn(segment *SegmentInfo) bool
	Match(segment *SegmentInfo) bool
	Satisfied(segmentsInScope []*SegmentInfo) bool
	SegmentsInScope(candidates []*SegmentInfo) []*SegmentInfo
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
		segmentIDs:   normalizedCompactionTargetSegmentIDs(segmentIDs),
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

type compactionTargetBase struct {
	record *datapb.CompactionTarget
}

func newCompactionTarget(record *datapb.CompactionTarget) compactionTarget {
	if record != nil {
		record = proto.Clone(record).(*datapb.CompactionTarget)
	}
	return finiteCompactionTarget{compactionTargetBase: compactionTargetBase{record: record}}
}

func (r compactionTargetBase) Record() *datapb.CompactionTarget {
	if r.record == nil {
		return nil
	}
	return proto.Clone(r.record).(*datapb.CompactionTarget)
}

func (r compactionTargetBase) ScopeIn(segment *SegmentInfo) bool {
	if r.record == nil || segment == nil {
		return false
	}
	if r.record.GetCollectionID() != 0 && segment.GetCollectionID() != r.record.GetCollectionID() {
		return false
	}
	segmentIDs, ok := compactionTargetSegmentIDs(r.record)
	if !ok {
		return false
	}
	if len(segmentIDs) > 0 && !matchCompactionTargetSegmentIDScope(segmentIDs, segment) {
		return false
	}
	return true
}

func (r compactionTargetBase) SegmentsInScope(candidates []*SegmentInfo) []*SegmentInfo {
	segments := make([]*SegmentInfo, 0, len(candidates))
	for _, segment := range candidates {
		if r.ScopeIn(segment) {
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

type finiteCompactionTarget struct {
	compactionTargetBase
}

func (r finiteCompactionTarget) Match(segment *SegmentInfo) bool {
	return r.match(segment)
}

func (r finiteCompactionTarget) Satisfied(segmentsInScope []*SegmentInfo) bool {
	if !isBaseRewriteCompactionTarget(r.record) {
		return false
	}
	for _, segment := range segmentsInScope {
		if r.match(segment) {
			return false
		}
	}
	return true
}

func (r finiteCompactionTarget) match(segment *SegmentInfo) bool {
	if !isBaseRewriteCompactionTarget(r.record) || !r.ScopeIn(segment) {
		return false
	}
	return segment.GetCreateTs() < r.record.GetExpectedTS() &&
		segment.GetDmlPosition().GetTimestamp() < r.record.GetExpectedTS()
}

func normalizedCompactionTargetSegmentIDs(segmentIDs []int64) []int64 {
	normalized := append([]int64(nil), segmentIDs...)
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i] < normalized[j]
	})
	return normalized
}

func compactionTargetSegmentIDs(record *datapb.CompactionTarget) ([]int64, bool) {
	if record.GetIntent() != datapb.TargetIntent_INTENT_REWRITE {
		return nil, true
	}
	raw := record.GetProperties()[compactionTargetSegmentIDsProperty]
	if raw == "" {
		return nil, true
	}
	var segmentIDs []int64
	if err := json.Unmarshal([]byte(raw), &segmentIDs); err != nil {
		return nil, false
	}
	return normalizedCompactionTargetSegmentIDs(segmentIDs), true
}

func compactionTargetSegmentIDProperties(segmentIDs []int64) map[string]string {
	if len(segmentIDs) == 0 {
		return nil
	}
	value, err := json.Marshal(normalizedCompactionTargetSegmentIDs(segmentIDs))
	if err != nil {
		return nil
	}
	return map[string]string{compactionTargetSegmentIDsProperty: string(value)}
}

func isBaseRewriteCompactionTarget(record *datapb.CompactionTarget) bool {
	return record != nil &&
		record.GetIntent() == datapb.TargetIntent_INTENT_REWRITE &&
		record.GetTailLimit() == 0
}

func isSupportedCompactionTarget(record *datapb.CompactionTarget) bool {
	return isBaseRewriteCompactionTarget(record)
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

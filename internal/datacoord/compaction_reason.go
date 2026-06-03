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
	"sort"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type compactionReasonMeta struct {
	sync.RWMutex
	ctx     context.Context
	catalog metastore.DataCoordCatalog
	records map[int64]*datapb.CompactionReasonRecord
}

func newCompactionReasonMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*compactionReasonMeta, error) {
	meta := &compactionReasonMeta{
		ctx:     ctx,
		catalog: catalog,
		records: make(map[int64]*datapb.CompactionReasonRecord),
	}
	if err := meta.reloadFromKV(); err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *compactionReasonMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("compactionReasonMeta-reloadFromKV")
	records, err := m.catalog.ListCompactionReasonRecords(m.ctx)
	if err != nil {
		return err
	}
	for _, reasonRecord := range records {
		m.records[reasonRecord.GetReasonID()] = proto.Clone(reasonRecord).(*datapb.CompactionReasonRecord)
	}
	log.Info("DataCoord compactionReasonMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

// GetCompactionReasonRecord returns a cloned reason record by ID, or nil when
// the record is not loaded in DataCoord memory.
func (m *compactionReasonMeta) GetCompactionReasonRecord(reasonID int64) *datapb.CompactionReasonRecord {
	m.RLock()
	defer m.RUnlock()

	record, ok := m.records[reasonID]
	if !ok {
		return nil
	}
	return proto.Clone(record).(*datapb.CompactionReasonRecord)
}

// GetCompactionReasonRecords returns cloned reason records keyed by reason ID.
func (m *compactionReasonMeta) GetCompactionReasonRecords() map[int64]*datapb.CompactionReasonRecord {
	m.RLock()
	defer m.RUnlock()

	records := make(map[int64]*datapb.CompactionReasonRecord, len(m.records))
	for reasonID, record := range m.records {
		records[reasonID] = proto.Clone(record).(*datapb.CompactionReasonRecord)
	}
	return records
}

// SaveCompactionReasonRecord persists a reason record before updating the
// in-memory cache.
func (m *compactionReasonMeta) SaveCompactionReasonRecord(ctx context.Context, record *datapb.CompactionReasonRecord) error {
	m.Lock()
	defer m.Unlock()

	if err := m.catalog.SaveCompactionReasonRecord(ctx, record); err != nil {
		log.Error("meta update: save compaction reason record failed",
			zap.Int64("reasonID", record.GetReasonID()),
			zap.Int64("collectionID", record.GetScope().GetCollectionID()),
			zap.Error(err))
		return err
	}
	m.records[record.GetReasonID()] = proto.Clone(record).(*datapb.CompactionReasonRecord)
	log.Info("meta update: save compaction reason record done",
		zap.Int64("reasonID", record.GetReasonID()),
		zap.Int64("collectionID", record.GetScope().GetCollectionID()),
		zap.String("reasonType", record.GetReasonType().String()),
		zap.String("state", record.GetState().String()))
	return nil
}

// UpdateCompactionReasonRecordState persists a state transition and mirrors it
// into the in-memory cache when the record is loaded.
func (m *compactionReasonMeta) UpdateCompactionReasonRecordState(ctx context.Context, reasonID int64, state datapb.CompactionReasonState, droppedAtTS uint64) error {
	m.Lock()
	defer m.Unlock()

	if state != datapb.CompactionReasonState_REASON_STATE_DROPPED {
		droppedAtTS = 0
	}
	if err := m.catalog.UpdateCompactionReasonRecordState(ctx, reasonID, state, droppedAtTS); err != nil {
		log.Error("meta update: update compaction reason record state failed",
			zap.Int64("reasonID", reasonID),
			zap.String("state", state.String()),
			zap.Error(err))
		return err
	}
	if record, ok := m.records[reasonID]; ok {
		updated := proto.Clone(record).(*datapb.CompactionReasonRecord)
		updated.State = state
		updated.DroppedAtTS = droppedAtTS
		m.records[reasonID] = updated
	}
	log.Info("meta update: update compaction reason record state done",
		zap.Int64("reasonID", reasonID),
		zap.String("state", state.String()),
		zap.Uint64("droppedAtTS", droppedAtTS))
	return nil
}

// DropCompactionReasonRecord removes a reason record from KV and memory.
func (m *compactionReasonMeta) DropCompactionReasonRecord(ctx context.Context, record *datapb.CompactionReasonRecord) error {
	m.Lock()
	defer m.Unlock()

	if err := m.catalog.DropCompactionReasonRecord(ctx, record); err != nil {
		log.Error("meta update: drop compaction reason record failed",
			zap.Int64("reasonID", record.GetReasonID()),
			zap.Int64("collectionID", record.GetScope().GetCollectionID()),
			zap.Error(err))
		return err
	}
	delete(m.records, record.GetReasonID())
	return nil
}

type compactionReason interface {
	Record() *datapb.CompactionReasonRecord
	ScopeIn(segment *SegmentInfo) bool
	Match(segment *SegmentInfo) bool
	ReasonSatisfied(segmentsInScope []*SegmentInfo) bool
	SegmentsInScope(candidates []*SegmentInfo) []*SegmentInfo
}

type compactionReasonBase struct {
	record *datapb.CompactionReasonRecord
}

func newCompactionReason(record *datapb.CompactionReasonRecord) compactionReason {
	return finiteCompactionReason{compactionReasonBase: compactionReasonBase{record: record}}
}

func (r compactionReasonBase) Record() *datapb.CompactionReasonRecord {
	if r.record == nil {
		return nil
	}
	return proto.Clone(r.record).(*datapb.CompactionReasonRecord)
}

func (r compactionReasonBase) ScopeIn(segment *SegmentInfo) bool {
	if r.record == nil || segment == nil {
		return false
	}
	scope := r.record.GetScope()
	if scope.GetCollectionID() != 0 && segment.GetCollectionID() != scope.GetCollectionID() {
		return false
	}
	if scope.GetPartitionID() != 0 &&
		scope.GetPartitionID() != common.AllPartitionsID &&
		segment.GetPartitionID() != scope.GetPartitionID() {
		return false
	}
	if scope.GetChannel() != "" && segment.GetInsertChannel() != scope.GetChannel() {
		return false
	}
	segmentIDs := compactionReasonScopedSegmentIDs(r.record)
	if len(segmentIDs) > 0 && !matchCompactionReasonSegmentIDScope(segmentIDs, segment) {
		return false
	}
	return true
}

func (r compactionReasonBase) SegmentsInScope(candidates []*SegmentInfo) []*SegmentInfo {
	segments := make([]*SegmentInfo, 0, len(candidates))
	for _, segment := range candidates {
		if r.ScopeIn(segment) {
			segments = append(segments, segment)
		}
	}
	return segments
}

func matchCompactionReasonSegmentIDScope(segmentIDs []int64, segment *SegmentInfo) bool {
	for _, segmentID := range segmentIDs {
		if segment.GetID() == segmentID {
			return true
		}
	}
	return false
}

type finiteCompactionReason struct {
	compactionReasonBase
}

func (r finiteCompactionReason) Match(segment *SegmentInfo) bool {
	return r.match(segment)
}

func (r finiteCompactionReason) ReasonSatisfied(segmentsInScope []*SegmentInfo) bool {
	if !isBaseRewriteCompactionReason(r.record) {
		return false
	}
	for _, segment := range segmentsInScope {
		if r.match(segment) {
			return false
		}
	}
	return true
}

func (r finiteCompactionReason) match(segment *SegmentInfo) bool {
	if !isBaseRewriteCompactionReason(r.record) || !r.ScopeIn(segment) {
		return false
	}
	return segment.GetCreateTs() < r.record.GetExpectedTS() &&
		segment.GetDmlPosition().GetTimestamp() < r.record.GetExpectedTS()
}

func normalizedCompactionReasonSegmentIDs(segmentIDs []int64) []int64 {
	normalized := append([]int64(nil), segmentIDs...)
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i] < normalized[j]
	})
	return normalized
}

func compactionReasonScopedSegmentIDs(record *datapb.CompactionReasonRecord) []int64 {
	if record.GetReasonType() != datapb.CompactionReasonType_REASON_INTENT_REWRITE {
		return nil
	}
	return normalizedCompactionReasonSegmentIDs(record.GetScope().GetSegmentIDs())
}

func isBaseRewriteCompactionReason(record *datapb.CompactionReasonRecord) bool {
	return record != nil &&
		record.GetReasonType() == datapb.CompactionReasonType_REASON_INTENT_REWRITE &&
		record.GetTailLimit() == 0
}

func allocCompactionReasonIdentity(ctx context.Context, alloc allocator.Allocator) (int64, uint64, error) {
	if alloc == nil {
		return 0, 0, merr.WrapErrParameterInvalidMsg("compaction reason allocator is nil")
	}
	reasonID, err := alloc.AllocID(ctx)
	if err != nil {
		return 0, 0, err
	}
	createdAtTS, err := alloc.AllocTimestamp(ctx)
	if err != nil {
		return 0, 0, err
	}
	return reasonID, createdAtTS, nil
}

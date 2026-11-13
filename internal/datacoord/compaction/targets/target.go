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

package targets

import (
	"context"
	"slices"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const (
	StopSatisfied       = "satisfied"
	StopOperatorDropped = "operator_dropped"
	StopScopeDropped    = "scope_dropped"
)

type CompactionTargetMeta struct {
	mu      sync.RWMutex
	catalog metastore.DataCoordCatalog
	targets map[int64]*datapb.CompactionTarget
	// A failed stop may already have reached storage. Refresh the durable
	// record before making another decision about this Target.
	unconfirmedStops   map[int64]struct{}
	unconfirmedCreates map[int64]*datapb.CompactionTarget
	unconfirmedDeletes map[int64]struct{}
	writes             *lock.KeyLock[int64]
	admission          *lock.KeyLock[int64]
	collectionWrites   *lock.KeyLock[int64]
	tryLockScope       func(context.Context, int64) (func(), error)

	standingMu            sync.Mutex
	pendingSort           *datapb.CompactionTarget
	unconfirmedSortUpdate bool
}

func NewCompactionTargetMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*CompactionTargetMeta, error) {
	records, err := catalog.ListCompactionTargets(ctx)
	if err != nil {
		return nil, err
	}
	m := &CompactionTargetMeta{
		catalog:            catalog,
		targets:            make(map[int64]*datapb.CompactionTarget, len(records)),
		unconfirmedStops:   make(map[int64]struct{}),
		unconfirmedCreates: make(map[int64]*datapb.CompactionTarget),
		unconfirmedDeletes: make(map[int64]struct{}),
		writes:             lock.NewKeyLock[int64](),
		admission:          lock.NewKeyLock[int64](),
		collectionWrites:   lock.NewKeyLock[int64](),
	}
	for _, record := range records {
		m.targets[record.GetTargetID()] = record
	}
	return m, nil
}

// LockCollectionAdmission orders a final observation against new compaction
// admission. SegmentMeta itself supplies an atomic collection snapshot for
// observing publication, including when the collection has no segments.
func (m *CompactionTargetMeta) LockCollectionAdmission(collectionID int64) func() {
	m.admission.Lock(collectionID)
	return func() { m.admission.Unlock(collectionID) }
}

// SetCollectionScopeGuard installs the DDL guard before starting reconciliation.
func (m *CompactionTargetMeta) SetCollectionScopeGuard(guard func(context.Context, int64) (func(), error)) {
	m.tryLockScope = guard
}

// ActiveCollectionIDs lists the stable collection IDs whose scope needs to
// be checked during recovery. Cluster-wide Targets have no collection scope.
func (m *CompactionTargetMeta) ActiveCollectionIDs() []int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make(map[int64]struct{})
	for _, record := range m.targets {
		if record.GetCollectionID() > 0 && record.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE {
			ids[record.GetCollectionID()] = struct{}{}
		}
	}
	result := make([]int64, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	slices.Sort(result)
	return result
}

// CloseCollectionScope runs after the authoritative collection tombstone.
// A writer that passed validation before the tombstone either published its
// Target, recorded an ambiguous candidate, or lost the durable scope guard.
func (m *CompactionTargetMeta) CloseCollectionScope(ctx context.Context, collectionID int64) error {
	m.collectionWrites.Lock(collectionID)
	defer m.collectionWrites.Unlock(collectionID)

	m.mu.RLock()
	var ids []int64
	for id, record := range m.targets {
		if record.GetCollectionID() == collectionID && record.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE {
			ids = append(ids, id)
		}
	}
	for id, candidate := range m.unconfirmedCreates {
		if candidate.GetCollectionID() == collectionID {
			ids = append(ids, id)
		}
	}
	m.mu.RUnlock()
	slices.Sort(ids)

	var firstErr error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := m.stop(ctx, id, StopScopeDropped, func(record *datapb.CompactionTarget) (bool, error) {
			return record.GetCollectionID() == collectionID, nil
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *CompactionTargetMeta) GetCompactionTarget(targetID int64) *datapb.CompactionTarget {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return proto.CloneOf(m.targets[targetID])
}

func (m *CompactionTargetMeta) GetCompactionTargets() map[int64]*datapb.CompactionTarget {
	m.mu.RLock()
	defer m.mu.RUnlock()
	records := make(map[int64]*datapb.CompactionTarget, len(m.targets))
	for id, record := range m.targets {
		records[id] = proto.CloneOf(record)
	}
	return records
}

// List takes one immutable snapshot of the Target collection. Resolve prior
// ambiguous writes before taking it so no item carries an obsolete outcome.
func (m *CompactionTargetMeta) List(ctx context.Context) (map[int64]*datapb.CompactionTarget, error) {
	m.mu.RLock()
	ids := make([]int64, 0, len(m.unconfirmedStops)+len(m.unconfirmedCreates)+len(m.unconfirmedDeletes))
	for id := range m.unconfirmedStops {
		ids = append(ids, id)
	}
	for id := range m.unconfirmedCreates {
		ids = append(ids, id)
	}
	for id := range m.unconfirmedDeletes {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	for _, id := range ids {
		if _, err := m.Read(ctx, id); err != nil {
			return nil, err
		}
	}
	return m.GetCompactionTargets(), ctx.Err()
}

func (m *CompactionTargetMeta) GetActiveCompactionTargets() []*datapb.CompactionTarget {
	m.mu.RLock()
	defer m.mu.RUnlock()
	records := make([]*datapb.CompactionTarget, 0, len(m.targets))
	for _, record := range m.targets {
		if record.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE && IsRewrite(record) {
			records = append(records, proto.CloneOf(record))
		}
	}
	return records
}

func IsRewrite(record *datapb.CompactionTarget) bool {
	return record.GetIntent() == datapb.TargetIntent_INTENT_REWRITE && record.GetRewrite() != nil
}

func (m *CompactionTargetMeta) store(record *datapb.CompactionTarget, id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if record == nil {
		delete(m.targets, id)
	} else {
		m.targets[id] = proto.CloneOf(record)
	}
	delete(m.unconfirmedStops, id)
	delete(m.unconfirmedCreates, id)
	delete(m.unconfirmedDeletes, id)
}

func (m *CompactionTargetMeta) Create(ctx context.Context, candidate *datapb.CompactionTarget, scope *metastore.CompactionTargetCollectionVersion, validate func() error) (*datapb.CompactionTarget, error) {
	if collectionID := candidate.GetCollectionID(); collectionID != 0 {
		m.collectionWrites.Lock(collectionID)
		defer m.collectionWrites.Unlock(collectionID)
	}
	id := candidate.GetTargetID()
	m.writes.Lock(id)
	defer m.writes.Unlock(id)
	if err := validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mayHaveCommitted, err := m.catalog.CreateCompactionTarget(ctx, candidate, scope)
	if err == nil {
		m.store(candidate, id)
		return candidate, nil
	}
	if !mayHaveCommitted {
		return nil, merr.Wrapf(err, "create compaction target %d", id)
	}
	confirmation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	stored, readErr := m.catalog.LoadCompactionTarget(confirmation, id)
	if readErr == nil && sameCreation(stored, candidate) {
		m.store(stored, id)
		return stored, nil
	}
	m.mu.Lock()
	m.unconfirmedCreates[id] = proto.CloneOf(candidate)
	m.mu.Unlock()
	mlog.Warn(ctx, "could not confirm compaction target creation",
		mlog.Int64("targetID", id), mlog.FieldCollectionID(candidate.GetCollectionID()),
		mlog.Err(err), mlog.NamedError("confirmationError", readErr))
	return nil, merr.Wrapf(merr.ErrCompactionTargetCreateOutcomeUnknown, "candidate target ID %d", id)
}

func sameCreation(stored, candidate *datapb.CompactionTarget) bool {
	if stored == nil {
		return false
	}
	creation := proto.CloneOf(stored)
	creation.State = datapb.TargetState_TARGET_STATE_ACTIVE
	creation.StopReason = ""
	creation.InactivatedAtTS = 0
	return proto.Equal(creation, candidate)
}

// Read refreshes a Target whose previous write returned an ambiguous result.
func (m *CompactionTargetMeta) Read(ctx context.Context, id int64) (*datapb.CompactionTarget, error) {
	m.mu.RLock()
	_, stopping := m.unconfirmedStops[id]
	_, deleting := m.unconfirmedDeletes[id]
	if !stopping && !deleting && m.unconfirmedCreates[id] == nil {
		record := proto.CloneOf(m.targets[id])
		m.mu.RUnlock()
		return record, nil
	}
	m.mu.RUnlock()
	m.writes.Lock(id)
	defer m.writes.Unlock(id)
	return m.readLocked(ctx, id)
}

func (m *CompactionTargetMeta) readLocked(ctx context.Context, id int64) (*datapb.CompactionTarget, error) {
	m.mu.RLock()
	_, stopping := m.unconfirmedStops[id]
	_, deleting := m.unconfirmedDeletes[id]
	candidate := m.unconfirmedCreates[id]
	record := proto.CloneOf(m.targets[id])
	m.mu.RUnlock()
	if !stopping && !deleting && candidate == nil {
		return record, nil
	}
	stored, err := m.catalog.LoadCompactionTarget(ctx, id)
	if err != nil {
		return nil, merr.Wrapf(err, "read compaction target %d after ambiguous write", id)
	}
	if deleting {
		// A surviving value cannot rule out a delayed delete. Only a
		// confirmed absence stops read-through for this ID.
		if stored == nil {
			m.store(nil, id)
		}
		return stored, nil
	}
	if candidate != nil {
		if stored == nil {
			return nil, nil
		}
		if !sameCreation(stored, candidate) {
			return nil, merr.WrapErrDataIntegrityMsg("persisted compaction target %d differs from its candidate", id)
		}
		m.store(stored, id)
		return stored, nil
	}
	// A read of an active value cannot exclude a late commit of a previous
	// stop. Keep checking until a terminal value is durable.
	if stored == nil || stored.GetState() == datapb.TargetState_TARGET_STATE_INACTIVE {
		m.store(stored, id)
		if stored != nil {
			observeSatisfaction(stored)
		}
	}
	return stored, nil
}

func (m *CompactionTargetMeta) refreshUnconfirmedCreates(ctx context.Context) {
	m.mu.RLock()
	ids := make([]int64, 0, len(m.unconfirmedCreates))
	for id := range m.unconfirmedCreates {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		if _, err := m.Read(ctx, id); err != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "could not refresh compaction target creation",
				mlog.Int64("targetID", id), mlog.Err(err))
		}
	}
}

// Stop revalidates the current record before the first transition. A satisfied
// transition runs its full observation while the current Target is serialized.
func (m *CompactionTargetMeta) Stop(ctx context.Context, id int64, reason string, validate func(*datapb.CompactionTarget) (bool, error)) error {
	found, err := m.stop(ctx, id, reason, validate)
	if !found && err == nil {
		return merr.Wrapf(merr.ErrCompactionTargetNotFound, "target ID %d", id)
	}
	return err
}

func (m *CompactionTargetMeta) stop(ctx context.Context, id int64, reason string, validate func(*datapb.CompactionTarget) (bool, error)) (bool, error) {
	m.writes.Lock(id)
	defer m.writes.Unlock(id)
	record, err := m.readLocked(ctx, id)
	if err != nil {
		return false, err
	}
	if record == nil {
		return false, nil
	}
	if record.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE && record.GetState() != datapb.TargetState_TARGET_STATE_INACTIVE {
		return true, merr.WrapErrDataIntegrityMsg("compaction target %d has invalid state %d", id, record.GetState())
	}
	if reason == StopSatisfied && record.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE && m.tryLockScope != nil {
		release, err := m.tryLockScope(ctx, record.GetCollectionID())
		if err != nil || release == nil {
			return true, err
		}
		defer release()
	}
	if ok, err := validate(record); err != nil || !ok {
		return true, err
	}
	if record.GetState() == datapb.TargetState_TARGET_STATE_INACTIVE {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	record.State = datapb.TargetState_TARGET_STATE_INACTIVE
	record.StopReason = reason
	record.InactivatedAtTS = tsoutil.ComposeTSByTime(time.Now())
	stored, changed, err := m.catalog.StopCompactionTarget(ctx, record)
	if err != nil {
		m.mu.Lock()
		m.unconfirmedStops[id] = struct{}{}
		m.mu.Unlock()
		return true, merr.Wrapf(err, "stop compaction target %d", id)
	}
	m.store(stored, id)
	if stored == nil {
		return false, nil
	}
	if changed || stored.GetStopReason() == StopSatisfied {
		// A previously ambiguous write can win between the read and this
		// conditional update. It becomes visible only once in this process.
		observeSatisfaction(stored)
	}
	return true, nil
}

func observeSatisfaction(record *datapb.CompactionTarget) {
	if record.GetStopReason() != StopSatisfied || record.GetTailLimit() < 0 || record.GetActivatedAtTS() == 0 {
		return
	}
	family := "other"
	if IsRewrite(record) {
		family = "rewrite"
	}
	elapsed := tsoutil.PhysicalTime(record.GetInactivatedAtTS()).Sub(tsoutil.PhysicalTime(record.GetActivatedAtTS()))
	metrics.DataCoordCompactionTargetDuration.WithLabelValues(family).Observe(max(0, elapsed.Seconds()))
}

func NewRewriteRecord(ctx context.Context, alloc allocator.Allocator, collectionID int64, roots []int64) (*datapb.CompactionTarget, error) {
	id, err := alloc.AllocID(ctx)
	if err != nil {
		return nil, err
	}
	ts, err := alloc.AllocTimestamp(ctx)
	if err != nil {
		return nil, err
	}
	if id <= 0 || ts == 0 {
		return nil, merr.WrapErrServiceInternalMsg("allocator returned invalid compaction target identity")
	}
	return &datapb.CompactionTarget{
		TargetID:      id,
		CollectionID:  collectionID,
		Intent:        datapb.TargetIntent_INTENT_REWRITE,
		Rewrite:       &datapb.CompactionTargetRewrite{RootSegmentIds: roots},
		ExpectedTS:    ts,
		ActivatedAtTS: ts,
	}, nil
}

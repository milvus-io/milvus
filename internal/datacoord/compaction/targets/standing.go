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
	"maps"
	"math"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type standingState struct {
	id                int64
	pending           *datapb.CompactionTarget
	unconfirmedUpdate bool
}

func IsStandingSort(record *datapb.CompactionTarget) bool {
	return record.GetIntent() == datapb.TargetIntent_INTENT_SORT && HasStandingCondition(record)
}

func HasStandingCondition(record *datapb.CompactionTarget) bool {
	return isConditionIntent(record.GetIntent()) && isStanding(record) &&
		record.Enabled != nil && record.GetConditionRevision() != 0
}

func isConditionIntent(intent datapb.TargetIntent) bool {
	switch intent {
	case datapb.TargetIntent_INTENT_SORT, datapb.TargetIntent_INTENT_INDEX_VERSION,
		datapb.TargetIntent_INTENT_SCHEMA_VERSION, datapb.TargetIntent_INTENT_STORAGE_VERSION,
		datapb.TargetIntent_INTENT_STORAGE_FORMAT, datapb.TargetIntent_INTENT_DELETE_CLEANUP,
		datapb.TargetIntent_INTENT_TTL:
		return true
	default:
		return false
	}
}

func isStanding(record *datapb.CompactionTarget) bool {
	return record.GetCollectionID() == 0 && record.GetTailLimit() == -1 && record.GetActivatedAtTS() != 0
}

// SyncSort publishes the current SORT condition for both periodic and reactive planning.
func (m *CompactionTargetMeta) SyncSort(ctx context.Context, alloc allocator.Allocator, enabled bool) (*datapb.CompactionTarget, error) {
	return m.SyncStanding(ctx, alloc, datapb.TargetIntent_INTENT_SORT, enabled, nil)
}

// SyncStanding makes the normalized family condition durable before a round
// uses it. A lost create response retains the same candidate for confirmation.
func (m *CompactionTargetMeta) SyncStanding(
	ctx context.Context, alloc allocator.Allocator, intent datapb.TargetIntent, enabled bool, properties map[string]string,
) (*datapb.CompactionTarget, error) {
	if !isConditionIntent(intent) {
		return nil, merr.WrapErrServiceInternalMsg("invalid Standing condition family %s", intent)
	}
	m.standingMu.Lock()
	defer m.standingMu.Unlock()

	state, current, err := m.loadStanding(ctx, alloc, intent, proto.Bool(enabled), properties)
	if err != nil {
		return nil, err
	}
	matches := func(record *datapb.CompactionTarget) bool {
		return record.GetEnabled() == enabled && maps.Equal(record.GetProperties(), properties)
	}
	// An update with an unknown outcome could still commit against its
	// original revision. Confirm or fence it before reusing the old value.
	if matches(current) && !state.unconfirmedUpdate {
		return current, nil
	}

	id := current.GetTargetID()
	m.writes.Lock(id)
	defer m.writes.Unlock(id)

	previous, err := m.catalog.LoadCompactionTarget(ctx, id)
	if err != nil {
		return nil, err
	}
	if !HasStandingCondition(previous) || previous.GetIntent() != intent ||
		previous.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE ||
		previous.GetActivatedAtTS() != current.GetActivatedAtTS() ||
		previous.GetConditionRevision() < current.GetConditionRevision() || previous.GetConditionRevision() == math.MaxUint64 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid persisted %s condition for target %d", intent, id)
	}
	if previous.GetConditionRevision() > current.GetConditionRevision() {
		m.store(previous, id)
		state.unconfirmedUpdate = false
	}
	if !state.unconfirmedUpdate && matches(previous) {
		return previous, nil
	}
	next := proto.CloneOf(previous)
	next.Enabled = proto.Bool(enabled)
	next.Properties = maps.Clone(properties)
	next.ConditionRevision++
	if err := m.catalog.UpdateStandingCompactionTarget(ctx, previous, next, m.writeEpoch); err != nil {
		confirmation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		stored, readErr := m.catalog.LoadCompactionTarget(confirmation, id)
		if readErr != nil || !proto.Equal(stored, next) {
			state.unconfirmedUpdate = true
			return nil, merr.Wrapf(err, "synchronize %s target %d", intent, id)
		}
		next = stored
	}
	m.store(next, id)
	state.unconfirmedUpdate = false
	return next, nil
}

// loadStanding is called with standingMu held. Recover the fixed identity
// once per family and use an ID lookup for subsequent notifications or rounds.
func (m *CompactionTargetMeta) loadStanding(
	ctx context.Context, alloc allocator.Allocator, intent datapb.TargetIntent, enabled *bool, properties map[string]string,
) (*standingState, *datapb.CompactionTarget, error) {
	state := m.standing[intent]
	if state == nil {
		state = &standingState{}
		m.standing[intent] = state
	}
	if state.id != 0 {
		if record := m.GetCompactionTarget(state.id); record != nil {
			return state, record, nil
		}
		return nil, nil, merr.WrapErrDataIntegrityMsg("missing %s target %d", intent, state.id)
	}

	current, err := m.recoverStanding(intent)
	if err != nil {
		return nil, nil, err
	}
	if current == nil {
		if state.pending == nil {
			if alloc == nil {
				return nil, nil, merr.WrapErrServiceInternalMsg("allocator is unavailable for %s target", intent)
			}
			id, err := alloc.AllocID(ctx)
			if err != nil {
				return nil, nil, err
			}
			created, err := alloc.AllocTimestamp(ctx)
			if err != nil {
				return nil, nil, err
			}
			if id <= 0 || created == 0 {
				return nil, nil, merr.WrapErrServiceInternalMsg("allocator returned invalid %s identity", intent)
			}
			state.pending = &datapb.CompactionTarget{
				TargetID: id, Intent: intent, TailLimit: -1, ActivatedAtTS: created,
				Enabled: enabled, Properties: maps.Clone(properties),
			}
			if enabled != nil {
				state.pending.ConditionRevision = 1
			}
		}
		candidate := state.pending
		current, err = m.Create(ctx, candidate, nil, func() error { return nil })
		if err != nil {
			confirmation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			// A late commit can return a conditional conflict on retry.
			stored, readErr := m.catalog.LoadCompactionTarget(confirmation, candidate.GetTargetID())
			if readErr != nil || !sameCreation(stored, candidate) {
				return nil, nil, err
			}
			m.store(stored, candidate.GetTargetID())
			current = stored
		}
	}
	state.pending = nil
	state.id = current.GetTargetID()
	return state, current, nil
}

func (m *CompactionTargetMeta) recoverStanding(intent datapb.TargetIntent) (*datapb.CompactionTarget, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result *datapb.CompactionTarget
	for _, record := range m.targets {
		if record.GetIntent() != intent || record.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE {
			continue
		}
		valid := HasStandingCondition(record)
		if intent == datapb.TargetIntent_INTENT_SIZE {
			valid = IsStandingSize(record)
		}
		if !valid {
			continue
		}
		if result != nil {
			return nil, merr.WrapErrDataIntegrityMsg("multiple active %s targets", intent)
		}
		result = record
	}
	return proto.CloneOf(result), nil
}

// IsStandingSize recognizes the durable SIZE identity. Its enabled flag and
// parameters come from the current configuration.
func IsStandingSize(record *datapb.CompactionTarget) bool {
	return record.GetIntent() == datapb.TargetIntent_INTENT_SIZE && isStanding(record)
}

func (m *CompactionTargetMeta) SyncSize(ctx context.Context, alloc allocator.Allocator) (*datapb.CompactionTarget, error) {
	m.standingMu.Lock()
	defer m.standingMu.Unlock()
	_, record, err := m.loadStanding(ctx, alloc, datapb.TargetIntent_INTENT_SIZE, nil, nil)
	return record, err
}

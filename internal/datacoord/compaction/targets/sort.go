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
	"math"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func IsStandingSort(record *datapb.CompactionTarget) bool {
	return record.GetIntent() == datapb.TargetIntent_INTENT_SORT &&
		record.GetCollectionID() == 0 && record.GetTailLimit() == -1 && record.Enabled != nil &&
		record.GetActivatedAtTS() != 0 && record.GetConditionRevision() != 0
}

func (m *CompactionTargetMeta) sortRecord() (*datapb.CompactionTarget, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var sort *datapb.CompactionTarget
	for _, record := range m.targets {
		if !IsStandingSort(record) || record.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE {
			continue
		}
		if sort != nil {
			return nil, merr.WrapErrDataIntegrityMsg("multiple active Standing SORT compaction targets")
		}
		sort = record
	}
	return proto.CloneOf(sort), nil
}

// SyncSort publishes a confirmed definition before a round uses it. An
// ambiguous initial create is always retried using the same candidate ID.
func (m *CompactionTargetMeta) SyncSort(ctx context.Context, alloc allocator.Allocator, enabled bool) (*datapb.CompactionTarget, error) {
	m.standingMu.Lock()
	defer m.standingMu.Unlock()

	current, err := m.sortRecord()
	if err != nil {
		return nil, err
	}
	if current == nil {
		if m.pendingSort == nil {
			id, err := alloc.AllocID(ctx)
			if err != nil {
				return nil, err
			}
			created, err := alloc.AllocTimestamp(ctx)
			if err != nil {
				return nil, err
			}
			if id <= 0 || created == 0 {
				return nil, merr.WrapErrServiceInternalMsg("allocator returned invalid Standing SORT identity")
			}
			m.pendingSort = &datapb.CompactionTarget{
				TargetID: id, Intent: datapb.TargetIntent_INTENT_SORT, TailLimit: -1,
				ActivatedAtTS: created, Enabled: proto.Bool(enabled), ConditionRevision: 1,
			}
		}
		candidate := m.pendingSort
		current, err = m.Create(ctx, candidate, nil, func() error { return nil })
		if err != nil {
			confirmation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			// A create that committed after its readback returns a conditional
			// conflict on retry. Confirm that record before adopting the ID.
			stored, readErr := m.catalog.LoadCompactionTarget(confirmation, candidate.GetTargetID())
			if readErr != nil || !sameCreation(stored, candidate) {
				return nil, err
			}
			m.store(stored, candidate.GetTargetID())
			current = stored
		}
	}
	m.pendingSort = nil
	// A previously timed-out update could still commit against this revision.
	// A successful read alone cannot fence it.
	if current.GetEnabled() == enabled && !m.unconfirmedSortUpdate {
		return current, nil
	}

	id := current.GetTargetID()
	m.writes.Lock(id)
	defer m.writes.Unlock(id)

	previous, err := m.catalog.LoadCompactionTarget(ctx, id)
	if err != nil {
		return nil, err
	}
	if !IsStandingSort(previous) || previous.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE ||
		previous.GetActivatedAtTS() != current.GetActivatedAtTS() ||
		previous.GetConditionRevision() < current.GetConditionRevision() || previous.GetConditionRevision() == math.MaxUint64 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid persisted Standing SORT condition for target %d", id)
	}
	if previous.GetConditionRevision() > current.GetConditionRevision() {
		m.store(previous, id)
		m.unconfirmedSortUpdate = false
	}
	if !m.unconfirmedSortUpdate && previous.GetEnabled() == enabled {
		return previous, nil
	}
	next := proto.CloneOf(previous)
	next.Enabled = proto.Bool(enabled)
	next.ConditionRevision++
	if err := m.catalog.UpdateStandingCompactionTarget(ctx, previous, next); err != nil {
		confirmation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		stored, readErr := m.catalog.LoadCompactionTarget(confirmation, id)
		if readErr != nil || !proto.Equal(stored, next) {
			m.unconfirmedSortUpdate = true
			return nil, merr.Wrapf(err, "synchronize Standing SORT target %d", id)
		}
		next = stored
	}
	m.store(next, id)
	m.unconfirmedSortUpdate = false
	return next, nil
}

type SortObservation struct {
	Match []*meta.SegmentInfo
	Rows  int64
}

// ObserveSort evaluates cluster scope independently of ownership, visibility,
// protection, and available admission. Import owns its own sort stage.
func ObserveSort(ctx context.Context, mt *meta.Meta) (SortObservation, error) {
	var observation SortObservation
	internal := make(map[int64]bool)
	var rowErr error
	for _, segment := range mt.SelectSegments(ctx, meta.SegmentFilterFunc(policy.MatchesSortCompaction)) {
		if err := ctx.Err(); err != nil {
			return SortObservation{}, err
		}
		collectionID := segment.GetCollectionID()
		inScope, checked := internal[collectionID]
		if !checked {
			collection := mt.GetCollection(collectionID)
			inScope = collection.IsCreated() && !collection.IsExternal()
			internal[collectionID] = inScope
		}
		if !inScope {
			continue
		}
		rows := segment.GetNumOfRows()
		if rows < 0 || rows > math.MaxInt64-observation.Rows {
			if rowErr == nil {
				rowErr = merr.WrapErrDataIntegrityMsg("invalid SORT observation row count at segment %d", segment.GetID())
			}
			if rows < 0 {
				continue
			}
		} else {
			observation.Rows += rows
		}
		observation.Match = append(observation.Match, segment)
	}
	if err := ctx.Err(); err != nil {
		return SortObservation{}, err
	}
	return observation, rowErr
}

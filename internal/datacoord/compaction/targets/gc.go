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
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// CollectExpired deletes only persisted, expired Target records. The current
// retention is read under each record's write lock so config changes affect
// records that are still eligible for a new delete.
func (m *CompactionTargetMeta) CollectExpired(ctx context.Context, now time.Time) {
	m.mu.RLock()
	var candidates []int64
	for id, record := range m.targets {
		_, stopping := m.unconfirmedStops[id]
		if record.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE || stopping {
			candidates = append(candidates, id)
		}
	}
	m.mu.RUnlock()
	for _, id := range candidates {
		if ctx.Err() != nil {
			return
		}
		if err := m.collectExpiredTarget(ctx, id, now); err != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "could not collect compaction target",
				mlog.Int64("targetID", id), mlog.Err(err))
		}
	}
}

func (m *CompactionTargetMeta) collectExpiredTarget(ctx context.Context, id int64, now time.Time) error {
	m.writes.Lock(id)
	defer m.writes.Unlock(id)

	record, err := m.readLocked(ctx, id, false)
	if err != nil || record == nil {
		return err
	}
	if record.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE {
		return nil
	}
	stoppedAt := tsoutil.PhysicalTime(record.GetInactivatedAtTS())
	if record.GetState() != datapb.TargetState_TARGET_STATE_INACTIVE || !knownTargetRecord(record) ||
		stoppedAt.UnixMilli() <= 0 || stoppedAt.After(now) || record.GetInactivatedAtTS() < record.GetActivatedAtTS() {
		mlog.RatedWarn(ctx, rate.Limit(1), "retaining compaction target with unverified terminal metadata",
			mlog.Int64("targetID", id), mlog.FieldCollectionID(record.GetCollectionID()),
			mlog.String("intent", record.GetIntent().String()), mlog.String("state", record.GetState().String()),
			mlog.Uint64("stoppedAt", record.GetInactivatedAtTS()))
		return nil
	}
	if now.Sub(stoppedAt) <= Params.DataCoordCfg.CompactionTargetRetentionSeconds.GetAsDuration(time.Second) {
		return nil
	}
	if err := m.catalog.DropCompactionTarget(ctx, record); err != nil {
		m.mu.Lock()
		m.unconfirmedDeletes[id] = struct{}{}
		m.mu.Unlock()

		// The delete may have succeeded despite its RPC result. Use a
		// fresh deadline to resolve it even if the caller already timed out.
		confirmation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		stored, readErr := m.readLocked(confirmation, id, false)
		if readErr == nil && stored == nil {
			return nil
		}
		if readErr != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "could not confirm compaction target deletion",
				mlog.Int64("targetID", id), mlog.Err(readErr))
		}
		return merr.Wrapf(err, "delete expired compaction target %d", id)
	}
	m.store(nil, id)
	return nil
}

func knownTargetRecord(record *datapb.CompactionTarget) bool {
	if len(record.ProtoReflect().GetUnknown()) != 0 {
		return false
	}
	switch record.GetIntent() {
	case datapb.TargetIntent_INTENT_REWRITE:
		return IsRewrite(record) && record.GetCollectionID() > 0
	case datapb.TargetIntent_INTENT_OPTIMIZE:
		return IsOptimize(record) && record.GetCollectionID() > 0
	case datapb.TargetIntent_INTENT_SIZE:
		return IsStandingSize(record)
	default:
		return HasStandingCondition(record)
	}
}

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

package coordinator

import (
	"context"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Readiness witness: revoke ⟺ write side clear && currentTargetVersion > T_c.
//
// The read-side proof is DELEGATED to the target promote barrier instead of being
// re-derived from the dist report. currentTarget is only ever REPLACED by promoting the
// next target (never mutated in place), and promotion requires, for EVERY segment of the
// next target: presence of every dist copy across all nodes with manifest/DataVersion >=
// the target's snapshot (utils.CheckSegmentDataReady, default-on) plus per-replica
// delegator serviceability. A next target is built from a live datacoord snapshot
// (GetRecoveryInfoV2) and stamped with its build time (CollectionTarget.version =
// UnixNano at construction). Therefore:
//
//	currentTargetVersion > T_c
//	  ⟹ the serving contract now in force was built from a datacoord snapshot taken
//	    AFTER the backfill finished (its manifest/DataVersion content subsumes the
//	    round's thresholds — the backfill bumps them together with the column data)
//	  ⟹ its promotion certified that every replica's every loaded copy had caught up
//	    (loaded AND reopened) to that snapshot.
//
// T_c is uniformly the FIRST SWEEP at which the round's write side was observed complete
// (transient on the round; monotone, so the first observation is well-defined; re-observed
// after a restart — revoke only delayed, fail-closed). For a watermark round (internal
// add_function_field) "complete" means the DataView check passes; for an external round
// (Spark) it holds from the first sweep, because the round registers in the manifest-apply
// ack callback, AFTER datacoord meta holds the result.
//
// Multi-replica correctness is inherent: the single collection-level roll is gated on
// every replica's delegators. An unloaded/released collection has no current target
// (version 0) and simply keeps the round held until load+promote — nothing is searchable
// meanwhile. Known residual (the theory floor of pure observation): an extra copy whose
// load info was resolved BEFORE the apply and which lands AFTER the promote (e.g. an
// intra-replica balance move) — the segment checker reopens it within a cycle.

// DataViewReader exposes the datacoord segment meta (the write-side truth) to the
// witness. datacoord's Server satisfies it structurally; mixCoord wires it.
type DataViewReader interface {
	// StaleBackfillSegments returns the IDs of the collection's backfill-eligible
	// segments (healthy, flushed-side, non-L0) whose schema_version is strictly below
	// the watermark, i.e. the write side has not finished backfilling them.
	StaleBackfillSegments(ctx context.Context, collectionID int64, watermark int32) []int64
	// ChannelCheckpointsAtLeast reports whether EVERY vchannel checkpoint of the
	// collection has reached ts. Checkpoints advance only after all prior DML is
	// durably persisted, and the schema-change DDL fences+flushes all pre-V growing
	// segments -- so checkpoints past the DDL timetick prove no pre-V data still hides
	// in growing segments (which are invisible to StaleBackfillSegments).
	ChannelCheckpointsAtLeast(ctx context.Context, collectionID int64, ts uint64) bool
}

// TargetVersionReader exposes the querycoord currentTarget build-time version. The
// concrete adapter over querycoord TargetManager is wired in mixCoord.
type TargetVersionReader interface {
	// CurrentTargetVersion returns the build timestamp (UnixNano) of the collection's
	// current target, or 0 when the collection has no current target (not loaded).
	CurrentTargetVersion(ctx context.Context, collectionID int64) int64
}

// ReadinessProvider answers "can ALL in-scope segments correctly serve this round?",
// encapsulating the per-scope witness so the observer never sees internal-vs-Spark
// distinctions. It must only be driven from the observer's single sweep goroutine
// (it records T_c on the round without locking).
type ReadinessProvider struct {
	dataView DataViewReader
	target   TargetVersionReader
}

// NewReadinessProvider constructs a provider. Both readers are required.
func NewReadinessProvider(dataView DataViewReader, target TargetVersionReader) *ReadinessProvider {
	if dataView == nil || target == nil {
		panic("bump_defence readiness provider requires both a data-view reader and a target-version reader")
	}
	return &ReadinessProvider{dataView: dataView, target: target}
}

// IsRoundReady reports whether the round's backfill result is fully servable on the read
// side. A round reveals atomically — it returns false until a target built after the
// write side finished has been promoted to currentTarget.
func (p *ReadinessProvider) IsRoundReady(ctx context.Context, round *BackfillRound) bool {
	if round == nil || len(round.Fields) == 0 {
		return true
	}
	// Defence: with the promote barrier disabled nothing can ever certify this round --
	// holding it would block its fields FOREVER (no promotion is meaningful). Stand down:
	// release the round with a loud warning instead of keeping an un-revokable gate.
	// New registrations are likewise skipped while disabled (Register).
	// OPERATIONAL HAZARD: this is one-way -- revoke is irreversible by design, so even a
	// brief accidental flip of the flag permanently releases every in-flight round, and
	// fields not yet fully backfilled are then served without the gate until their
	// backfill completes.
	if !paramtable.Get().QueryCoordCfg.UpdateTargetNeedSegmentDataReady.GetAsBool() {
		mlog.RatedWarn(ctx, rate.Limit(0.1),
			"bump_defence standing down: queryCoord.updateTargetNeedSegmentDataReady is disabled, "+
				"revoking round without read-side certification",
			mlog.FieldCollectionID(round.CollectionID),
			mlog.Int64("roundID", round.RoundID))
		return true
	}

	switch round.Scope.Kind {
	case ScopeWatermark:
		// Write side, condition 1: every vchannel checkpoint must have passed the DDL
		// timetick -- the DDL fences+flushes all pre-V growing segments, so this proves
		// no pre-V data is still hiding in growing state (invisible to the sealed check
		// below). Without it, a collection whose pre-V data is still growing (e.g. all
		// data in growing segments at DDL time) would read trivially clear.
		if round.Scope.SchemaChangeTimeTick > 0 &&
			!p.dataView.ChannelCheckpointsAtLeast(ctx, round.CollectionID, round.Scope.SchemaChangeTimeTick) {
			round.writeDoneObservedAtNanos = 0
			return false
		}
		// Write side, condition 2: the bump-schema backfill must have covered every
		// sealed eligible segment.
		if len(p.dataView.StaleBackfillSegments(ctx, round.CollectionID, round.Scope.Watermark)) > 0 {
			// Re-stamp defence: the sealed population is not monotone -- a stale segment
			// can join it AFTER the first clear (an import/snapshot-restore started
			// before the DDL and finishing late genuinely lacks the field and is not
			// fenced by the DDL; #48865 zero-stamped seals re-dirty it at the metadata
			// level). T_c must track the LAST transition to clear: otherwise a target
			// built before the late segment existed -- never barrier-certified for it --
			// would satisfy the version check the moment the write side clears again.
			round.writeDoneObservedAtNanos = 0
			return false
		}
	case ScopeExternal:
		// Write side holds by construction: the round registers only AFTER the manifest
		// apply landed in datacoord meta.
	default:
		return false
	}
	// T_c = the first sweep observing the write side complete since it last went dirty
	// (dirty observations above reset it). Not persisted; a restart re-observes a later
	// T_c — revoke delayed by at most one promote cycle, fail-closed. Known residual: a
	// dirty window falling entirely between two sweeps is not observed and does not
	// reset T_c (sampling limit); the checkpoint condition above removes the main source
	// of late re-dirtying (pre-V growing seals).
	if round.writeDoneObservedAtNanos == 0 {
		round.writeDoneObservedAtNanos = time.Now().UnixNano()
	}
	return p.target.CurrentTargetVersion(ctx, round.CollectionID) > round.writeDoneObservedAtNanos
}

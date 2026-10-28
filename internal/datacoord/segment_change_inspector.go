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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// This file implements the segment change groupInspector: the periodic driver
// of the group state machine (design §4.1):
//
//   - STAGED: re-evaluate readiness per member via the shared segmentReadiness
//     predicate; when every member is ready, transition to READY (persisted,
//     independent catalog txn). Unconvergeable groups (member externally
//     dropped, C23 schema fence behind) FAIL immediately instead of burning the
//     staging timeout; a group past SegmentChangeStagingTimeout ABORTS.
//   - READY: trigger publishGroup. On failure, only an unconvergeable condition
//     (member gone) FAILs the group; every transient failure (manifest-lock
//     contention, monotonicity conflict, catalog retry exhaustion) leaves the
//     group READY for the next round.
//
// FAILED/ABORTED members are reclaimed here and during startup recovery.
//
// The inspector is nil-safe by construction: with no persisted groups the loop
// is a no-op, and every meta access guards nil (a dataview-less DataCoord runs
// the same code — publishGroup already branches on dataViewManager == nil).
type segmentChangeInspector struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mt    *meta
	alloc allocator.Allocator
}

func newSegmentChangeInspector(ctx context.Context, mt *meta, alloc allocator.Allocator) *segmentChangeInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &segmentChangeInspector{
		ctx:    ctx,
		cancel: cancel,
		mt:     mt,
		alloc:  alloc,
	}
}

// Start launches the periodic group evaluation loop. It is safe to call with a
// nil meta (no groups can exist, the loop no-ops) and on a dataview-less
// DataCoord (publishGroup branches internally).
func (i *segmentChangeInspector) Start() {
	if i.mt == nil {
		mlog.Warn(context.TODO(), "segment change inspector started without meta; disabling", mlog.Int("goroutines", 0))
		return
	}
	i.wg.Add(1)
	go i.loop()
}

func (i *segmentChangeInspector) Stop() {
	i.cancel()
	i.wg.Wait()
}

func (i *segmentChangeInspector) loop() {
	defer i.wg.Done()
	mlog.Info(i.ctx, "start segment change inspector loop",
		mlog.Int64("intervalSeconds", Params.DataCoordCfg.TaskCheckInterval.GetAsInt64()))
	ticker := time.NewTicker(Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-i.ctx.Done():
			return
		case <-ticker.C:
			i.checkAllGroups(i.ctx)
		}
	}
}

// checkAllGroups walks every persisted group once per tick. Groups are cloned
// by GetSegmentChangeGroupsByCollection, so the state transitions applied here
// never mutate the table under a reader.
func (i *segmentChangeInspector) checkAllGroups(ctx context.Context) {
	for _, g := range i.mt.GetSegmentChangeGroupsByCollection(ctx, 0) {
		switch g.State {
		case model.SegmentChangeStateStaged:
			i.evaluateStaged(ctx, g)
		case model.SegmentChangeStateReady:
			i.tryPublish(ctx, g)
		case model.SegmentChangeStateFailed, model.SegmentChangeStateAborted:
			i.reclaimGroup(ctx, g)
		}
	}
}

// evaluateStaged re-evaluates a STAGED group's members with the shared
// readiness predicate. All members ready -> READY; any unconvergeable member
// (missing, Dropped, schema version behind while staged — C23) -> FAILED; a
// group older than SegmentChangeStagingTimeout -> ABORTED; otherwise wait for
// the next round.
//
// The unconvergeable scan runs BEFORE the wait scan: if the first not-ready
// member is merely converging (index/stats in flight) while a later member is
// schema-behind, the C23 fail-fast must still fire this round instead of
// waiting out the batch until the first member converges (design C23).
func (i *segmentChangeInspector) evaluateStaged(ctx context.Context, g *model.SegmentChangeGroup) {
	coll := i.mt.GetCollection(g.CollectionID)
	if coll == nil || coll.Schema == nil {
		i.failGroup(ctx, g, "collection dropped or schema-less while staged")
		return
	}
	readiness := i.mt.prepareReadiness(coll)
	if readiness == nil {
		i.failGroup(ctx, g, "cannot prepare readiness for collection")
		return
	}
	// First pass: unconvergeable conditions (fail-fast, never a wait).
	for _, id := range g.NewSegmentIDs {
		seg := i.mt.GetSegment(ctx, id)
		if seg == nil || (seg.GetState() == commonpb.SegmentState_Dropped && !isEmptySegmentChangeOutput(seg)) {
			i.failGroup(ctx, g, fmt.Sprintf("member %d externally dropped", id))
			return
		}
		if isEmptySegmentChangeOutput(seg) {
			continue
		}
		// C23: a schema bump landing while a group is staged is UNCONVERGEABLE —
		// the bump materialization only targets visible segments, so nothing can
		// advance a staged member's SchemaVersion. Fail immediately instead of
		// waiting out the staging timeout.
		if !readiness.schemaReady(seg) {
			i.failGroup(ctx, g, fmt.Sprintf("member %d schema version behind collection while staged (C23)", id))
			return
		}
	}
	// Second pass: wait on the still-converging members, bounded by the staging
	// timeout.
	for _, id := range g.NewSegmentIDs {
		seg := i.mt.GetSegment(ctx, id)
		if seg == nil {
			// Member vanished between the two scans: treat as not ready and let
			// the next round's unconvergeable scan fail-fast (FAILED).
			return
		}
		if !isEmptySegmentChangeOutput(seg) && !readiness.Ready(seg) {
			if i.stagingTimedOut(g) {
				i.abortGroup(ctx, g, "staging timeout")
				return
			}
			return
		}
	}

	ready := g.Clone()
	ready.State = model.SegmentChangeStateReady
	ready.ReadyTS = time.Now().Unix()
	if err := i.mt.UpdateSegmentChangeGroup(ctx, ready); err != nil {
		mlog.Warn(ctx, "segment change inspector failed to mark group ready, will retry",
			mlog.Int64("groupID", g.GroupID),
			mlog.Int64("collectionID", g.CollectionID),
			mlog.Err(err))
		return
	}
	mlog.Info(ctx, "segment change group marked ready",
		mlog.Int64("groupID", g.GroupID),
		mlog.Int64("collectionID", g.CollectionID),
		mlog.Int64("readyTS", ready.ReadyTS))
}

// tryPublish drives a READY group through publishGroup. Success (or an
// idempotent replay) converges the group to COMMITTED inside publishGroup's
// composite write. Failures:
//   - errSegmentChangeGroupCollectionDropped -> ABORTED;
//   - a member gone from meta or Dropped -> FAILED (unconvergeable);
//   - every other error is transient -> keep READY, retry next round.
func (i *segmentChangeInspector) tryPublish(ctx context.Context, g *model.SegmentChangeGroup) {
	if g.AwaitCommitFence && g.CommitTS == 0 && i.stagingTimedOut(g) {
		i.abortGroup(ctx, g, "import commit fence timeout")
		return
	}
	err := i.mt.publishGroup(ctx, g, i.alloc)
	if err == nil {
		return
	}
	if errors.Is(err, errSegmentChangeGroupCollectionDropped) {
		i.abortGroup(ctx, g, "collection data views dropped at publish")
		return
	}
	if i.groupUnconvergeable(ctx, g) {
		i.failGroup(ctx, g, fmt.Sprintf("publish failed: %v", err))
		return
	}
	// R5: a publish rejected because a member's readiness regressed after READY
	// (index rebuild retry / index DDL / schema fence) must go back to STAGED
	// for re-evaluation, not retry READY forever.
	if i.groupNoLongerReady(ctx, g) {
		i.regressToStaged(ctx, g)
		return
	}
	mlog.Warn(ctx, "segment change inspector publish failed, will retry",
		mlog.Int64("groupID", g.GroupID),
		mlog.Int64("collectionID", g.CollectionID),
		mlog.Err(err))
}

// groupNoLongerReady reports whether any member's readiness regressed after the
// group was marked READY. This is the R5 signal: the publish was rejected by
// the Phase-2 index / schema re-check, so the group must be re-evaluated from
// STAGED instead of retrying an impossible publish.
func (i *segmentChangeInspector) groupNoLongerReady(ctx context.Context, g *model.SegmentChangeGroup) bool {
	coll := i.mt.GetCollection(g.CollectionID)
	if coll == nil || coll.Schema == nil {
		return true
	}
	readiness := i.mt.prepareReadiness(coll)
	if readiness == nil {
		return true
	}
	for _, id := range g.NewSegmentIDs {
		seg := i.mt.GetSegment(ctx, id)
		if seg != nil && !isEmptySegmentChangeOutput(seg) && !readiness.Ready(seg) {
			return true
		}
	}
	return false
}

// regressToStaged moves a READY group back to STAGED (design R5 "abort back to
// STAGED"): a readiness regression between READY and publish means the READY
// decision no longer holds and the members must be re-evaluated from scratch.
func (i *segmentChangeInspector) regressToStaged(ctx context.Context, g *model.SegmentChangeGroup) {
	clone := g.Clone()
	clone.State = model.SegmentChangeStateStaged
	clone.ReadyTS = 0
	if err := i.mt.UpdateSegmentChangeGroup(ctx, clone); err != nil {
		mlog.Warn(ctx, "segment change inspector failed to regress ready group to staged",
			mlog.Int64("groupID", g.GroupID),
			mlog.Int64("collectionID", g.CollectionID),
			mlog.Err(err))
		return
	}
	mlog.Info(ctx, "segment change group regressed to staged for re-evaluation",
		mlog.Int64("groupID", g.GroupID),
		mlog.Int64("collectionID", g.CollectionID))
}

// groupUnconvergeable reports whether the group can no longer be published,
// aligned with the design's publish failure matrix: a member externally dropped
// (Dropped / removed from meta), a member with no data footprint, or a member
// no longer staged by this group (ownership mismatch). Such a group can never
// reach COMMITTED, so retrying is pointless.
func (i *segmentChangeInspector) groupUnconvergeable(ctx context.Context, g *model.SegmentChangeGroup) bool {
	for _, id := range g.NewSegmentIDs {
		seg := i.mt.GetSegment(ctx, id)
		if isEmptySegmentChangeOutput(seg) {
			continue
		}
		if seg == nil || seg.GetState() == commonpb.SegmentState_Dropped {
			return true
		}
		if len(seg.GetBinlogs()) == 0 && seg.GetManifestPath() == "" {
			return true
		}
		// Ownership mismatch: the member must still be staged by THIS group
		// (e.g. a DDL re-staged or dropped it while the group was alive).
		if owner := i.mt.GetSegmentChangeGroupBySegmentID(ctx, id); owner == nil || owner.GroupID != g.GroupID {
			return true
		}
	}
	return false
}

// stagingTimedOut reports whether the group exceeded SegmentChangeStagingTimeout
// since CreateTS (Unix seconds). A zero CreateTS (legacy record) never times out.
func (i *segmentChangeInspector) stagingTimedOut(g *model.SegmentChangeGroup) bool {
	if groupHasCommittedImportFence(g) {
		return false
	}
	if g.CreateTS <= 0 {
		return false
	}
	timeout := Params.DataCoordCfg.SegmentChangeStagingTimeout.GetAsDuration(time.Second)
	return time.Now().Unix()-g.CreateTS > int64(timeout.Seconds())
}

func (i *segmentChangeInspector) failGroup(ctx context.Context, g *model.SegmentChangeGroup, reason string) {
	i.applyTerminalTransition(ctx, g, model.SegmentChangeStateFailed, reason)
}

func (i *segmentChangeInspector) abortGroup(ctx context.Context, g *model.SegmentChangeGroup, reason string) {
	i.applyTerminalTransition(ctx, g, model.SegmentChangeStateAborted, reason)
}

func (i *segmentChangeInspector) applyTerminalTransition(ctx context.Context, g *model.SegmentChangeGroup, state model.SegmentChangeState, reason string) {
	if g.IsTerminal() {
		return
	}
	terminal := g.Clone()
	terminal.State = state
	terminal.FailReason = reason
	if err := i.mt.UpdateSegmentChangeGroup(ctx, terminal); err != nil {
		mlog.Warn(ctx, "segment change inspector failed to apply terminal state",
			mlog.Int64("groupID", g.GroupID),
			mlog.String("state", state.String()),
			mlog.String("reason", reason),
			mlog.Err(err))
		return
	}
	i.reclaimGroup(ctx, terminal)
	mlog.Info(ctx, "segment change group reached terminal state",
		mlog.Int64("groupID", g.GroupID),
		mlog.Int64("collectionID", g.CollectionID),
		mlog.String("state", state.String()),
		mlog.String("reason", reason))
}

func (i *segmentChangeInspector) reclaimGroup(ctx context.Context, g *model.SegmentChangeGroup) {
	if err := i.mt.reclaimSegmentChangeGroup(ctx, g); err != nil {
		mlog.Warn(ctx, "segment change member reclamation failed, will retry",
			mlog.Int64("groupID", g.GroupID), mlog.Err(err))
	}
}

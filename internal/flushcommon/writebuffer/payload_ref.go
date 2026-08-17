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

package writebuffer

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var errGrowingSourceUnavailable = errors.New("growing source is unavailable")

// growingFlushInput is the growing side of one flush attempt, fixed by
// refPayload.Snapshot. The coordinator itself never inspects it beyond a nil
// check — it is handed whole to the growing task builder.
// The source lease Snapshot acquired is NOT carried here: it stays on the
// snapshot record (refSnapshot) until takeSource transfers it to the task.
type growingFlushInput struct {
	// startPos is the segment's first uncommitted batch start at snapshot time
	// (nil for a metadata-only attempt).
	startPos *msgpb.MsgPosition
	// checkpoint is the frozen flush fence AND the position the attempt will
	// publish; for a committed-manifest replay it is the frozen position that
	// manifest covers, never a newer one.
	checkpoint *msgpb.MsgPosition
	// flushFromTs is lastFlushedTs at snapshot time — the lower fence of the
	// flush range (flushFromTs, checkpoint.Timestamp].
	flushFromTs uint64
	// pendingCommitted replays a flush whose data reached storage but whose
	// metadata commit did not; the attempt must re-publish exactly what was
	// written. See syncmgr.CommittedFlushRecord for why everything in it is
	// frozen rather than re-derived at retry time.
	pendingCommitted *syncmgr.CommittedFlushRecord
}

// refSnapshot is one in-flight ref attempt's settlement record: the source
// lease Snapshot acquired (until the task takes it over) and the frozen fence
// CommitFlush advances the ledger to.
type refSnapshot struct {
	source     syncmgr.GrowingFlushSource
	checkpoint *msgpb.MsgPosition
}

// growingSourceProgressBatch is one WAL message pack's contribution to a
// segment, recorded when the pack was consumed.
//
// endPosition is the flush fence AND the checkpoint: a pack's rows all carry a
// timestamp <= its end position's, and the next pack's rows carry a strictly
// greater one, so a fence set here can never split a pack. rowNum is this
// side's own tally, used only to cross-check what the source reports it wrote.
type growingSourceProgressBatch struct {
	startPosition *msgpb.MsgPosition
	endPosition   *msgpb.MsgPosition
	rowNum        int64
}

// refPayload is the segmentPayload whose insert bytes stay pinned in a segcore
// growing segment: the write buffer records only batch positions and row
// counts, and flushes by fencing the source over a timestamp range. It IS the
// per-segment growing ledger — the old growingSourceProgress struct dissolved
// into it, so all growing state is reached through wb.buffers[segID].payload.
//
// A growing-source flush does not take the rows — they stay pinned in segcore
// until CommitGrowingFlush — so progress is expressed as offsets and batches
// rather than as a queue of tasks holding payload.
//
// Like every segmentPayload, all methods run under wb.mut.
type refPayload struct {
	wb        *writeBufferBase
	segmentID int64

	// lastFlushedPosition is the complete checkpoint already known durable for
	// this segment. Metadata-only flush/drop debt uses it to keep the WAL behind
	// the control message that created the debt even when no data batch remains.
	lastFlushedPosition *msgpb.MsgPosition
	// lastFlushedTs is the timestamp of the position this segment was last
	// flushed through — the lower fence of the next flush. It is the only
	// "how far along am I" state this side keeps, and it is a POSITION, not a
	// row count: row counts live in the growing segment's own coordinate
	// system, which a WAL replay resets.
	lastFlushedTs uint64
	syncing       bool
	// claimedRows is how many of rowsTotal the in-flight task took as its
	// batch. Owned by the payload and updated under the same lock as batches, so
	// "buffered = rowsTotal - claimedRows" is a single-clock read — the
	// metacache SyncingRows counter tracks the same quantity but is advanced by
	// the task at different moments, and subtracting across the two clocks
	// produced transient negatives. Zero whenever no task is in flight.
	claimedRows int64
	// rowsTotal is the running sum of batches[*].rowNum — this side's tally of
	// the rows recorded but not yet flushed, kept O(1) for the accounting reads
	// (UnflushedRows/IsFull) that run on every policy round. It moves ONLY with
	// the batches slice: incremented by Buffer, decremented by the batches ack
	// trims. Cross-checked against the source's report; never used to bound the
	// flush.
	rowsTotal int64
	// sourceFinalized records the proof separately from its fence: timestamp zero
	// is also the protobuf default and must not mean that a source was notified.
	// sourceFinalizedThroughTs is the highest terminal fence already delivered to
	// the growing source. A concurrent Drop may still need a metadata-only task,
	// but it must not require a source that the earlier finalization released.
	sourceFinalized          bool
	sourceFinalizedThroughTs uint64
	// pendingCommitted replays a flush whose data reached storage but whose
	// metadata commit did not; see syncmgr.CommittedFlushRecord.
	pendingCommitted    *syncmgr.CommittedFlushRecord
	nonRetryableFailure bool
	batches             []growingSourceProgressBatch
	failureCount        int64
	lastFailure         string

	// nextSnapshotID makes snapshot IDs unique per payload; 0 is reserved for
	// "no snapshot".
	nextSnapshotID int64
	snapshots      map[int64]*refSnapshot
}

var _ segmentPayload = (*refPayload)(nil)

func newRefPayload(wb *writeBufferBase, segmentID int64) *refPayload {
	return &refPayload{
		wb:        wb,
		segmentID: segmentID,
		snapshots: make(map[int64]*refSnapshot),
	}
}

// refPayloadLocked resolves segmentID's growing ledger through the buffers map
// — the ONE place growing state lives. Caller must hold wb.mut.
func (wb *writeBufferBase) refPayloadLocked(segmentID int64) (*refPayload, bool) {
	buffer, ok := wb.buffers[segmentID]
	if !ok || buffer.payload == nil {
		return nil, false
	}
	payload, ok := buffer.payload.(*refPayload)
	return payload, ok
}

// Buffer records one insert pack's positions and row count. The bytes stay in
// the segcore growing segment, so no resident memory is added.
func (p *refPayload) Buffer(data *InsertData, start, end *msgpb.MsgPosition) (int64, error) {
	p.batches = append(p.batches, growingSourceProgressBatch{
		startPosition: start,
		endPosition:   end,
		rowNum:        data.rowNum,
	})
	p.rowsTotal += data.rowNum
	return 0, nil
}

// terminalStateLocked reports the metacache state that IS this segment's
// terminal flush/drop debt — the old owesFlush/owesDrop bits derived instead of
// stored: seal set Sealed (claimed to Flushing in getSyncTask), drop set
// Dropped, and only the terminal task's commit removes the segment or its
// buffer. Caller must hold wb.mut.
func (p *refPayload) terminalStateLocked() (state commonpb.SegmentState, exists bool) {
	segment, ok := p.wb.metaCache.GetSegmentByID(p.segmentID)
	if !ok {
		return commonpb.SegmentState_SegmentStateNone, false
	}
	return segment.State(), true
}

func (p *refPayload) owesFlushLocked() bool {
	state, ok := p.terminalStateLocked()
	return ok && pendingFlushState(state)
}

func (p *refPayload) owesDropLocked() bool {
	state, ok := p.terminalStateLocked()
	return ok && state == commonpb.SegmentState_Dropped
}

// owesTerminalLocked reports whether this segment carries ANY terminal debt —
// a flush (Sealed/Flushing) or a drop (Dropped) still owed. Caller must hold
// wb.mut.
func (p *refPayload) owesTerminalLocked() bool {
	state, ok := p.terminalStateLocked()
	return ok && terminalSegmentState(state)
}

// owesFlushAfterLocked / owesDropAfterLocked report whether the terminal debt
// SURVIVES a just-completed task: the metacache state still owes it and the
// task did not carry the flag (or, for drop, a frozen replay left ledger
// batches its manifest does not cover). Caller must hold wb.mut.
func (p *refPayload) owesFlushAfterLocked(task syncmgr.Task) bool {
	return p.owesFlushLocked() && !task.IsFlush()
}

func (p *refPayload) owesDropAfterLocked(task syncmgr.Task) bool {
	return p.owesDropLocked() && (!task.IsDrop() || len(p.batches) > 0)
}

// Snapshot fixes the content of the next growing flush attempt: it freezes the
// fence over the ledger batches with end <= throughTs, resolves and pins the
// growing source, and reserves the attempt (syncing, claimedRows, syncing-rows
// counters, checkpoint pin). It MAY FAIL — an Unavailable/Pending source
// returns errGrowingSourceUnavailable, a retryable debt; nothing is reserved
// on the failure path.
func (p *refPayload) Snapshot(ctx context.Context, throughTs uint64) (*flushInput, error) {
	wb := p.wb
	segmentInfo, ok := wb.metaCache.GetSegmentByID(p.segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(p.segmentID)
	}
	if segmentInfo.GetStorageVersion() != storage.StorageV3 {
		return nil, merr.WrapErrServiceInternalMsg("growing-source sync requires StorageV3 segment, segmentID=%d storageVersion=%d",
			segmentInfo.SegmentID(), segmentInfo.GetStorageVersion())
	}
	pendingCommitted := p.pendingCommitted
	startPos := p.firstUncommittedPosition()

	// The flush target is a POSITION this side already holds — the newest pack
	// recorded at or before throughTs — not a count derived from anything. It
	// is published unchanged as the checkpoint, so the range written and the
	// position published cannot drift apart.
	checkpoint, batchRows := p.flushTargetThrough(throughTs)
	if pendingCommitted != nil {
		// Replay of a flush whose data is already in storage: publish the exact
		// position that manifest covers, never a newer one, and carry the row
		// count frozen with it.
		checkpoint = pendingCommitted.Checkpoint
		batchRows = pendingCommitted.BatchRows
	}
	if checkpoint == nil {
		// No pack recorded: this is a metadata-only flush (a sealed segment that
		// owes a flush but holds no new rows). The fence must NOT advance here —
		// falling back to the channel's latest consumed position would move
		// lastFlushedTs over ground this segment never verified. Re-publish the
		// position already reached.
		checkpoint = segmentInfo.LastFlushPosition()
	}
	if checkpoint == nil {
		checkpoint = startPos
	}
	// A terminal metadata-only attempt — Sealed/Flushing/Dropped IS the debt —
	// whose source was already told its terminal fence is durable may proceed
	// without reacquiring the source.
	sourceSettlementSatisfied := batchRows == 0 &&
		p.sourceSettledThrough(checkpoint.GetTimestamp()) &&
		terminalSegmentState(segmentInfo.State())
	source, state := wb.getGrowingSource(p.segmentID, checkpoint)
	if state != syncmgr.GrowingSourceUsable {
		if source != nil {
			source.Release()
			source = nil
		}
		// One three-way disposition of an unusable source: a committed-manifest
		// replay proceeds without it (SaveBinlogPaths needs no re-flush), a
		// settled metadata-only terminal sync proceeds without it, and anything
		// else must wait for the source to come back.
		switch {
		case pendingCommitted != nil:
			wb.logger.Warn(ctx, "growing source unavailable during committed flush ack retry; retrying SaveBinlogPaths without re-flush",
				mlog.Int64("segmentID", p.segmentID),
				mlog.Uint64("flushThroughTs", checkpoint.GetTimestamp()),
				mlog.Int("state", int(state)))
		case sourceSettlementSatisfied:
			wb.logger.Info(ctx, "growing source already finalized; continue metadata-only terminal sync without reacquiring it",
				mlog.FieldSegmentID(p.segmentID),
				mlog.Uint64("flushThroughTs", checkpoint.GetTimestamp()))
		default:
			return nil, errors.Wrapf(errGrowingSourceUnavailable, "segment %d state %d", p.segmentID, state)
		}
	}

	// Reserve the attempt. From here the segment is in flight until CommitFlush
	// or AbortFlush settles this snapshot.
	if queue := wb.writeBufferSyncQueues[p.segmentID]; queue != nil {
		queue.intent.clear()
	}
	p.syncing = true
	if batchRows > 0 {
		// No checkpoint registration: the ledger batches themselves ARE the pin —
		// they are trimmed only by CommitFlush's ack, so EarliestPosition keeps
		// reporting startPos for the whole in-flight window.
		p.claimedRows = batchRows
		wb.metaCache.UpdateSegments(metacache.StartSyncing(batchRows), metacache.WithSegmentIDs(p.segmentID))
	}
	p.nextSnapshotID++
	snapshotID := p.nextSnapshotID
	p.snapshots[snapshotID] = &refSnapshot{source: source, checkpoint: checkpoint}
	return &flushInput{
		snapshotID: snapshotID,
		rows:       batchRows,
		startPos:   startPos,
		growing: &growingFlushInput{
			startPos:         startPos,
			checkpoint:       checkpoint,
			flushFromTs:      p.lastFlushedTs,
			pendingCommitted: pendingCommitted,
		},
	}, nil
}

// takeSource transfers ownership of the snapshot's source lease to the caller
// (the task builder). After this, AbortFlush no longer releases it — the task
// does, via Commit's success path or ReleaseSource on failure.
func (p *refPayload) takeSource(snapshotID int64) syncmgr.GrowingFlushSource {
	record := p.snapshots[snapshotID]
	if record == nil {
		return nil
	}
	source := record.source
	record.source = nil
	return source
}

// CommitFlush settles the snapshot: the ledger advances to the frozen fence
// (batches with end <= it are trimmed, lastFlushedTs moves) and the in-flight
// state clears. The caller guarantees metadata is durable first — this is the
// growing counterpart of dropping an owned floor. CommitGrowingFlush on the
// source itself stays inside the syncmgr task (after metadata durability).
func (p *refPayload) CommitFlush(snapshotID int64) {
	record := p.snapshots[snapshotID]
	if record == nil {
		return
	}
	delete(p.snapshots, snapshotID)
	if record.source != nil {
		// Defensive: a snapshot whose source was never taken by a task.
		record.source.Release()
	}
	p.ack(record.checkpoint)
	p.wb.notifyGrowingSettledLocked()
}

// AbortFlush returns what Snapshot reserved WITHOUT settling: the ledger keeps
// its batches (the floor keeps pinning the checkpoint), the debt stays with
// the metacache state, and the source lease — if the task never took it — is
// released. Failure counters are coordinator bookkeeping, not settlement, and
// live in finishWriteBufferSync.
func (p *refPayload) AbortFlush(snapshotID int64) {
	record := p.snapshots[snapshotID]
	if record == nil {
		return
	}
	delete(p.snapshots, snapshotID)
	if record.source != nil {
		record.source.Release()
	}
	p.syncing = false
	p.claimedRows = 0
	p.wb.notifyGrowingSettledLocked()
}

// EarliestPosition is the replay origin of everything not yet CommitFlush'd:
// the first uncommitted batch start, or — once all data is durable but a
// terminal metadata task is still owed or in flight — the last durable
// position. The terminal debt is READ off the metacache state (the old
// owesFlush/owesDrop bits): a Sealed/Flushing/Dropped segment still owes its
// metadata-only task until the commit that removes the segment or this buffer.
func (p *refPayload) EarliestPosition() *msgpb.MsgPosition {
	if position := p.firstUncommittedPosition(); position != nil {
		return position
	}
	if p.syncing || p.pendingCommitted != nil || len(p.snapshots) > 0 {
		return p.lastFlushedPosition
	}
	if p.owesTerminalLocked() {
		return p.lastFlushedPosition
	}
	return nil
}

// sourceSettledThrough reports whether the growing source was already told
// that everything through ts is durable — the settlement proof a metadata-only
// terminal attempt (and the release handoff) may rely on instead of
// reacquiring a source the finalization released.
func (p *refPayload) sourceSettledThrough(ts uint64) bool {
	return p.sourceFinalized && p.sourceFinalizedThroughTs >= ts
}

// requiresHandoffLocked reports whether releasing this segment's growing
// source now would still lose something: packs are outstanding, or a terminal
// flush that must reach the source is still owed. Caller must hold wb.mut.
func (p *refPayload) requiresHandoffLocked() bool {
	if len(p.batches) > 0 {
		return true
	}
	// Nothing left to write. Whatever terminal task remains is metadata-only,
	// and refPayload.Snapshot builds that one WITHOUT reacquiring the source
	// once this same settlement proof holds (sourceSettlementSatisfied).
	// Blocking the release on a source this segment will never ask for again
	// only delays the release.
	//
	// The proof is trustworthy: sourceFinalized is set at exactly one site, on
	// the success path of a terminal task, immediately after the source was told
	// the flush through that fence is durable.
	if p.sourceSettledThrough(p.handoffFenceTs()) {
		return false
	}
	segment, ok := p.wb.metaCache.GetSegmentByID(p.segmentID)
	if !ok {
		return false
	}
	return segment.FlushSourceMode() == metacache.FlushSourceGrowing &&
		segment.State() != commonpb.SegmentState_Flushed
}

func (p *refPayload) UnflushedRows() int64 {
	return p.rowsTotal - p.claimedRows
}

// UnflushedBytes is 0: the rows live in segcore, not in resident Go memory,
// so nothing here is evictable and nothing enters the buffered-size gauge.
func (p *refPayload) UnflushedBytes() int64 {
	return 0
}

func (p *refPayload) MinTimestamp() uint64 {
	if position := p.firstUncommittedPosition(); position != nil {
		return position.GetTimestamp()
	}
	return math.MaxUint64
}

// IsFull carries the growing flush trigger into the standard policy input: the
// ledger holds at least one flush budget's worth of unclaimed rows.
func (p *refPayload) IsFull() bool {
	rows := p.rowsTotal - p.claimedRows
	if rows <= 0 {
		return false
	}
	if p.wb.estSizePerRecord <= 0 {
		return false
	}
	thresholdRows := int64(p.wb.getEstBatchSize())
	if thresholdRows <= 0 {
		return true
	}
	return rows >= thresholdRows
}

func (p *refPayload) firstUncommittedPosition() *msgpb.MsgPosition {
	if len(p.batches) == 0 {
		return nil
	}
	return p.batches[0].startPosition
}

// flushTarget is the position the next flush should run to: the newest pack
// recorded for this segment. Everything recorded is flushed in one go — there is
// no partial target to compute, because the fence is a position this side
// already holds rather than a count it has to derive.
func (p *refPayload) flushTarget() *msgpb.MsgPosition {
	if len(p.batches) == 0 {
		return nil
	}
	return p.batches[len(p.batches)-1].endPosition
}

// flushTargetThrough is the position the next flush should run to — the newest
// pack with end <= throughTs — together with the ledger's row tally over that
// range. In practice every recorded batch is at or before the channel
// checkpoint the coordinator snapshots at, so this covers the whole ledger.
func (p *refPayload) flushTargetThrough(throughTs uint64) (*msgpb.MsgPosition, int64) {
	var target *msgpb.MsgPosition
	var rows int64
	for _, batch := range p.batches {
		if batch.endPosition.GetTimestamp() > throughTs {
			break
		}
		target = batch.endPosition
		rows += batch.rowNum
	}
	return target, rows
}

// handoffFenceTs is how far this segment must be flushed before its growing
// source may be released, as a WAL timestamp.
//
// With packs outstanding it is the newest one's end position — releasing before
// that would drop rows still only in the segment. With none outstanding the
// segment still owes a metadata-only final flush, so the fence is where it was
// last flushed to; reporting zero there would skip retention entirely and let
// the source go away before that flush runs.
func (p *refPayload) handoffFenceTs() uint64 {
	if target := p.flushTarget(); target != nil {
		return target.GetTimestamp()
	}
	return p.lastFlushedTs
}

// ack records that everything through checkpoint is now persisted.
func (p *refPayload) ack(checkpoint *msgpb.MsgPosition) {
	// A nil checkpoint (fresh, never-flushed empty segment finishing a
	// metadata-only task) is deliberately safe here: GetTimestamp() on nil is
	// 0, which trims no batch, and the position advance below is nil-guarded.
	flushedThroughTs := checkpoint.GetTimestamp()
	keepIdx := 0
	for keepIdx < len(p.batches) && p.batches[keepIdx].endPosition.GetTimestamp() <= flushedThroughTs {
		p.rowsTotal -= p.batches[keepIdx].rowNum
		keepIdx++
	}
	p.batches = p.batches[keepIdx:]
	if checkpoint != nil && (flushedThroughTs > p.lastFlushedTs || p.lastFlushedPosition == nil) {
		p.lastFlushedTs = flushedThroughTs
		p.lastFlushedPosition = typeutil.Clone(checkpoint)
	}
	if p.pendingCommitted != nil && flushedThroughTs >= p.pendingCommitted.FlushThroughTs {
		p.pendingCommitted = nil
	}
	p.syncing = false
	p.claimedRows = 0
	p.failureCount = 0
	p.lastFailure = ""
}

func (p *refPayload) failSync(err error) {
	p.syncing = false
	p.claimedRows = 0
	p.recordFailure(err)
}

// recordFailure is the failure-streak bookkeeping alone: refPayload.AbortFlush
// already cleared the in-flight state, so completion callbacks record the
// streak without touching syncing/claimedRows a second time.
func (p *refPayload) recordFailure(err error) {
	p.failureCount++
	if err != nil {
		p.lastFailure = err.Error()
	}
}

func (p *refPayload) markNonRetryableFailure() {
	p.nonRetryableFailure = true
}

// failGrowingSyncLocked and refPayload.CommitFlush/AbortFlush are the ONLY ways
// a ref payload may leave the in-flight state. They all broadcast so the
// wake-up cannot be forgotten: waitSyncsSettled blocks on growingSettled rather
// than polling, so a caller that cleared `syncing` without broadcasting would
// hang every shutdown until the grace expired.
func (wb *writeBufferBase) failGrowingSyncLocked(p *refPayload, err error) {
	p.failSync(err)
	wb.notifyGrowingSettledLocked()
}

func (wb *writeBufferBase) notifyGrowingSettledLocked() {
	close(wb.growingSettled)
	wb.growingSettled = make(chan struct{})
}

// anyGrowingSyncingLocked reports whether any growing-source flush is in flight.
func (wb *writeBufferBase) anyGrowingSyncingLocked() bool {
	for _, buffer := range wb.buffers {
		if p, ok := buffer.payload.(*refPayload); ok && p.syncing {
			return true
		}
	}
	return false
}

// refPayloadSyncableLocked reports whether this ledger can produce a task now.
// Source resolution belongs to task construction, so every attempt takes
// exactly one source lease and has one owner responsible for releasing it.
//
// Read-only: the terminal debt is the metacache Sealed/Flushing/Dropped state
// itself, so there is no bit to record here. Claiming the flush belongs to
// getSyncTask, where the content is fixed.
func (wb *writeBufferBase) refPayloadSyncableLocked(segmentID int64, p *refPayload) bool {
	if p.nonRetryableFailure || p.syncing {
		return false
	}
	if p.pendingCommitted != nil {
		return true
	}
	if len(p.batches) == 0 && !p.owesTerminalLocked() {
		return false
	}
	checkpoint := wb.checkpoint
	if len(p.batches) > 0 {
		checkpoint = p.batches[len(p.batches)-1].endPosition
	}
	return checkpoint != nil
}

// armRefRetryLocked arms one segment's clock on the SAME per-segment debt the
// owned path uses: the sync queue's flushIntent. There is no timer — the merged
// driveRetries picks it up on the next timetick or manager ticker round.
func (wb *writeBufferBase) armRefRetryLocked(segmentID int64) {
	if wb.closed || wb.dropping || wb.flushRetryInterval < 0 {
		return
	}
	// The attempt that could not be made (or just failed) restarts the clock;
	// the debt itself only settles on a completed task.
	queue := wb.getOrCreateSyncQueueLocked(segmentID)
	queue.intent.want()
	queue.intent.attempted(time.Now())
}

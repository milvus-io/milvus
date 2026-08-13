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
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Growing-source PROGRESS reporting and drain: what the release handoff reads
// (GetGrowingFlushProgress / WaitGrowingFlushDrained) plus the failure-streak
// observability for growing flush attempts. All per-segment state lives on the
// segment's refPayload (payload_ref.go); everything here derives from it under
// wb.mut.

const growingSourceSyncFailureWarnThreshold = 600

// GetGrowingFlushProgress reports growing-source progress as of right now.
//
// It deliberately does NOT wait for the write buffer to consume the release
// fence first. That wait would change nothing: refPayload.requiresHandoffLocked
// reports a growing-source segment as owing a flush until it is Flushed,
// whether or not the seal has been consumed yet, so WaitGrowingFlushDrained
// waits the ManualFlush out regardless. Waiting here only delayed the release
// on channels that owed nothing at all.
func (wb *writeBufferBase) GetGrowingFlushProgress(ctx context.Context, segmentIDs []int64) ([]GrowingFlushSegmentProgress, error) {
	// The caller must already have fenced growing-source admission — see
	// FenceGrowingSourceAdmission for why it has to happen before the
	// ManualFlush is appended, not here.
	// Reporting only. Waiting for these flushes to finish is a separate,
	// explicit step (WaitGrowingFlushDrained) so that callers who just want to
	// read progress are never blocked by one that is stuck.
	return wb.growingFlushProgressSnapshot(segmentIDs), nil
}

// growingFlushProgressSnapshot reports per-segment progress.
func (wb *writeBufferBase) growingFlushProgressSnapshot(segmentIDs []int64) []GrowingFlushSegmentProgress {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	tracked := wb.refSegmentIDsLocked()
	if len(segmentIDs) == 0 {
		segmentIDs = tracked
	} else {
		segmentIDs = lo.Uniq(append(segmentIDs, tracked...))
	}

	progresses := make([]GrowingFlushSegmentProgress, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		progress := GrowingFlushSegmentProgress{
			SegmentID:  segmentID,
			SourceMode: metacache.FlushSourceUnknown,
		}
		if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok {
			progress.SourceMode = segment.FlushSourceMode()
		}
		if payload, ok := wb.refPayloadLocked(segmentID); ok {
			progress.FlushThroughTs = payload.handoffFenceTs()
			progress.NeedReleaseHandoff = payload.requiresHandoffLocked()
			progress.SourceMode = metacache.FlushSourceGrowing
		}
		progresses = append(progresses, progress)
	}
	return progresses
}

// refSegmentIDsLocked lists the segments whose buffer is growing-backed.
func (wb *writeBufferBase) refSegmentIDsLocked() []int64 {
	var ids []int64
	for segmentID, buffer := range wb.buffers {
		if _, ok := buffer.payload.(*refPayload); ok {
			ids = append(ids, segmentID)
		}
	}
	return ids
}

// WaitGrowingFlushDrained blocks until no segment on this channel still owes a
// growing-source flush. segmentIDs is advisory (logging/context); the scan
// always covers every growing-backed buffer so a segment admitted between the
// caller's snapshot and the admission fence is still waited out — the fence
// guarantees no NEW segment enters growing mode afterward, so this converges.
//
// Bounded only by ctx on purpose. Giving up early and releasing anyway would
// reintroduce exactly the unflushable state this wait exists to prevent, so a
// timeout is surfaced to the caller, which fails the release and lets the
// coordinator retry it.
func (wb *writeBufferBase) WaitGrowingFlushDrained(ctx context.Context, segmentIDs []int64) error {
	var pending []int64
	err := wb.waitFor(ctx, func(closed bool) (bool, error) {
		pending = pending[:0]
		for segmentID, buffer := range wb.buffers {
			payload, ok := buffer.payload.(*refPayload)
			if !ok {
				continue
			}
			if payload.requiresHandoffLocked() {
				pending = append(pending, segmentID)
			}
		}
		if len(pending) == 0 {
			return true, nil
		}
		if closed {
			// The buffer is going down with the channel, so no further flush
			// will run for it. Safe to stop waiting: a batch that never
			// committed also never advanced the checkpoint, so recovery replays
			// the same WAL range.
			mlog.Info(ctx, "write buffer closed while waiting for growing-source flush to drain",
				mlog.String("channel", wb.channelName),
				mlog.Int64s("pendingSegments", pending))
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return errors.Wrapf(err,
			"growing-source flush not drained for segments %v on channel %s", pending, wb.channelName)
	}
	return nil
}

// noteGrowingSourceCandidateFailed records the failed attempt of a candidate that
// could not produce its task: failure counters, metric, retry intent.
//
// Nothing is rolled back — refPayload.Snapshot reserves only on its success
// paths, so there is no bookkeeping to reverse. The segment's own state is left
// alone too: a claimed flush stays claimed, and GetSealedSegmentsPolicy
// re-selects Flushing segments, so the retry resumes the SAME flush instead of
// re-deciding what to flush.
func (wb *writeBufferBase) noteGrowingSourceCandidateFailed(segmentID int64) {
	if payload, ok := wb.refPayloadLocked(segmentID); ok {
		wb.failGrowingSyncLocked(payload, errGrowingSourceUnavailable)
		wb.observeGrowingSourceSyncFailureLocked(segmentID, payload)
		wb.armRefRetryLocked(segmentID)
	}
}

// settleFailedGrowingTaskLocked returns what a failed task RESERVED: the
// metacache syncing rows it claimed.
//
// Derived entirely from the task, and therefore run unconditionally — never
// under a buffer lookup. A concurrent teardown can remove the buffer while
// tasks are still in flight, and a callback landing afterwards must still give
// these back: otherwise the segment keeps inflated syncingRows forever. (The
// checkpoint needs no settlement here: the ledger batches are the pin, and
// they die with the buffer or stay until CommitFlush trims them.)
func (wb *writeBufferBase) settleFailedGrowingTaskLocked(task *syncmgr.GrowingSourceSyncTask) {
	if task.BatchRows() > 0 {
		wb.metaCache.UpdateSegments(metacache.AbortSyncing(task.BatchRows()), metacache.WithSegmentIDs(task.SegmentID()))
	}
}

func (wb *writeBufferBase) observeGrowingSourceSyncFailureLocked(segmentID int64, payload *refPayload) {
	wb.updateGrowingSourceSyncFailureMetricLocked()

	if payload.failureCount < growingSourceSyncFailureWarnThreshold ||
		payload.failureCount%growingSourceSyncFailureWarnThreshold != 0 {
		return
	}

	wb.logger.RatedWarn(wb.syncCtx, rate.Limit(1), "growing-source source sync keeps failing",
		mlog.Int64("segmentID", segmentID),
		mlog.Int64("failureCount", payload.failureCount),
		mlog.Uint64("lastFlushedTs", payload.lastFlushedTs),
		mlog.String("lastFailure", payload.lastFailure),
	)
}

func (wb *writeBufferBase) resetGrowingSourceSyncFailureMetricLocked(payload *refPayload) {
	payload.failureCount = 0
	payload.lastFailure = ""
	wb.updateGrowingSourceSyncFailureMetricLocked()
}

// updateGrowingSourceSyncFailureMetricLocked publishes the worst consecutive
// failure streak on this channel. A channel-scoped gauge cannot safely be set to
// one segment's value: a success on a different segment would hide the failure.
func (wb *writeBufferBase) updateGrowingSourceSyncFailureMetricLocked() {
	if wb.growingSourceFailureMetricSettled {
		return
	}
	var maxFailures int64
	for _, buffer := range wb.buffers {
		if p, ok := buffer.payload.(*refPayload); ok && p.failureCount > maxFailures {
			maxFailures = p.failureCount
		}
	}
	if maxFailures == 0 {
		metrics.DataNodeGrowingSourceSyncFailureCount.DeleteLabelValues(
			paramtable.GetStringNodeID(),
			fmt.Sprint(wb.collectionID),
			wb.channelName,
		)
		return
	}
	metrics.DataNodeGrowingSourceSyncFailureCount.WithLabelValues(
		paramtable.GetStringNodeID(),
		fmt.Sprint(wb.collectionID),
		wb.channelName,
	).Set(float64(maxFailures))
}

// settleGrowingSourceFailureMetricLocked ends this write buffer's ownership of
// the channel-scoped gauge. DataSyncService closes before the write-buffer
// manager on the streaming path, so flowgraph cleanup cannot safely delete it:
// an in-flight callback could recreate the series afterwards. The settled flag
// makes every such late callback a no-op.
func (wb *writeBufferBase) settleGrowingSourceFailureMetricLocked() {
	if wb.growingSourceFailureMetricSettled {
		return
	}
	wb.growingSourceFailureMetricSettled = true
	metrics.DataNodeGrowingSourceSyncFailureCount.DeleteLabelValues(
		paramtable.GetStringNodeID(),
		fmt.Sprint(wb.collectionID),
		wb.channelName,
	)
}

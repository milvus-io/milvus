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
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// Growing-source ADMISSION: whether a segment may put its flush debt on a
// segcore growing segment at all — the sticky per-segment source decision and
// the channel-level release fence that closes it. The flush machinery itself is
// mode-blind (segment_payload.go / sync_coordinator files); the growing ledger
// is payload_ref.go.

func (wb *writeBufferBase) AllowGrowingSourceFlush() bool {
	return wb.allowGrowingSourceFlush
}

// FenceGrowingSourceAdmission stops NEW segments on this channel from being
// admitted to growing-source mode.
//
// It must be called BEFORE the release ManualFlush is appended, and that
// ordering is the whole correctness argument:
//
//   - A segment admitted before the fence was created by an insert that is
//     already in the WAL. WAL timestamps are monotonic and the ManualFlush is
//     appended afterwards, so its fence timestamp is above every such insert
//     and it seals all of them. They get flushed, and the drain converges.
//   - A segment admitted after the fence is refused growing-source mode and
//     buffers its rows in the write buffer, where they survive the release
//     without needing the delegator at all.
//
// Fencing any later leaves a window — even one as small as the gap between the
// append returning and this call — in which a segment can be admitted to
// growing-source mode without being sealed by that ManualFlush. Such a segment
// owes a flush forever from the drain's point of view (still FlushSourceGrowing,
// never Flushed), so the release blocks until its deadline.
//
// The fence records the newest provider registration token;
// growingSourceAdmissionOpenLocked reopens admission once a NEWER registration
// appears (a fresh local subscription after the release). It never moves
// backward, so a retried release only re-asserts it. An abandoned release
// therefore leaves the channel in write-buffer mode until it is re-subscribed:
// safe, and the cost of not having a rollback to get wrong.
// The token comes from the SAME resolver that serves segment resolution
// (wb.growingSourceResolver), never from the global registry directly: an
// injected resolver that never registered globally would make the fence record
// token 0 = "never fenced" = admission never closes. The seam and the fence
// must agree on the authority.
func (wb *writeBufferBase) FenceGrowingSourceAdmission() {
	token := wb.growingSourceResolver.LatestRegistrationToken(wb.channelName)
	wb.mut.Lock()
	defer wb.mut.Unlock()
	if token > wb.growingSourceAdmissionFence {
		wb.growingSourceAdmissionFence = token
	}
}

// growingSourceAdmissionOpenLocked reports whether a NEW segment may still
// choose FlushSourceGrowing. Callers must hold mut.
func (wb *writeBufferBase) growingSourceAdmissionOpenLocked() bool {
	if wb.growingSourceAdmissionFence == 0 {
		return true
	}
	return wb.growingSourceResolver.LatestRegistrationToken(wb.channelName) > wb.growingSourceAdmissionFence
}

func (wb *writeBufferBase) decideGrowingFlushSource(segmentID int64, endPos *msgpb.MsgPosition) metacache.FlushSourceMode {
	// 1. Honor the sticky decision recorded in metacache. Once the first
	//    insert for a segment commits a source choice, every subsequent call
	//    must return the same kind so that ledger / payload tracking stays
	//    consistent for the segment's lifetime.
	if seg, ok := wb.metaCache.GetSegmentByID(segmentID); ok {
		if seg.GetStorageVersion() != storage.StorageV3 {
			return metacache.FlushSourceWriteBuffer
		}
		switch seg.FlushSourceMode() {
		case metacache.FlushSourceGrowing:
			return metacache.FlushSourceGrowing
		case metacache.FlushSourceWriteBuffer:
			return metacache.FlushSourceWriteBuffer
		}
	}

	// 2. Fallback for the brief window where the in-memory buffer has been
	//    populated but the metacache sticky bit hasn't been set yet (e.g. on
	//    re-entry after a partial state).
	if _, ok := wb.refPayloadLocked(segmentID); ok {
		return metacache.FlushSourceGrowing
	}

	if wb.hasWriteBufferInsertPayload(segmentID) {
		return metacache.FlushSourceWriteBuffer
	}

	// 3. Release fence. Once a release handoff has been prepared for this
	//    channel, a segment seen here for the first time was created after the
	//    release fence and its growing segment is about to be dropped with the
	//    channel unsubscribe. Admitting it to growing-source mode would leave
	//    its only data copy in a segment that will not survive the release, so
	//    buffer its rows in the write buffer instead. Segments admitted before
	//    the fence returned above via their sticky decision or ref buffer and
	//    are waited out by WaitGrowingFlushDrained.
	if !wb.growingSourceAdmissionOpenLocked() {
		wb.warnGrowingSourceFallback(segmentID, endPos)
		return metacache.FlushSourceWriteBuffer
	}

	if state := wb.getGrowingSourceState(segmentID, endPos); state == syncmgr.GrowingSourceUsable || state == syncmgr.GrowingSourcePending {
		return metacache.FlushSourceGrowing
	}
	wb.warnGrowingSourceFallback(segmentID, endPos)
	return metacache.FlushSourceWriteBuffer
}

func (wb *writeBufferBase) getGrowingSource(segmentID int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	if wb.growingSourceResolver == nil {
		return nil, syncmgr.GrowingSourceUnavailable
	}
	return wb.growingSourceResolver.GetGrowingFlushSource(segmentID, endPos)
}

func (wb *writeBufferBase) getGrowingSourceState(segmentID int64, endPos *msgpb.MsgPosition) syncmgr.GrowingSourceState {
	source, state := wb.getGrowingSource(segmentID, endPos)
	if source != nil {
		source.Release()
	}
	return state
}

func (wb *writeBufferBase) warnGrowingSourceFallback(segmentID int64, endPos *msgpb.MsgPosition) {
	if !wb.allowGrowingSourceFlush {
		return
	}
	wb.logger.RatedWarn(wb.syncCtx, rate.Limit(1), "growing-source source is unavailable, fallback to WriteBuffer",
		mlog.Int64("segmentID", segmentID),
		mlog.Any("endPosition", endPos),
	)
}

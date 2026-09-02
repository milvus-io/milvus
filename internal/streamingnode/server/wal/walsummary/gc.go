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

package walsummary

import (
	"context"
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// GCOnce releases retained chunks that are both (a) above the byte budget
// (soft bound, whole objects) and (b) entirely at or below every covered
// vchannel's GC position — see chunkReleasedLocked. A chunk that still holds
// a not-yet-releasable record is never released, whatever the budget
// pressure.
//
// TODO(term-orphan-gc): the manifest is the only index into the chunk set, so
// objects a superseded term wrote after this term's takeover probe are
// unreachable here — they sit in no manifest and no pending_gc. They are
// inert (this term never reads generations it did not inherit), but they leak
// storage until a future term-scoped orphan sweep lists chunks/* under the
// pchannel prefix and drops objects not referenced by any manifest.
//
// The manifest is the only index into the chunk set, so release is a manifest
// edit: the chunk moves from `chunks` to `pending_gc`, the manifest is
// published, and only then is the object deleted. `pending_gc` is both the
// work queue and the progress record: a crash between the manifest write and
// the delete leaves the entry in `pending_gc`, and the next GC run finishes
// the delete. The delete itself is best-effort — a leftover object is inert
// (nothing references it) and is reaped by a later run or by store removal.
//
// A manifest write that fails mid-GC is safe: the in-memory manifest still
// lists everything, and the next attempt redoes the same computation. All
// manifest edits go through publishManifest, which serializes concurrent
// publishers (this GC and the write task) with the single manager lock plus a
// compare-and-swap on the manifest version.
func (m *Manager) GCOnce(ctx context.Context) error {
	// Snapshot the pending queue: removePendingGC compacts the live array in
	// place, so ranging over the live slice while deleting would shift the
	// indexes and skip entries.
	m.mu.Lock()
	pendingGC := make([]*streamingpb.PChannelSummaryChunkRef, 0, len(m.manifest.GetPendingGc()))
	pendingGC = append(pendingGC, m.manifest.GetPendingGc()...)
	m.mu.Unlock()
	for _, ref := range pendingGC {
		if err := m.cfg.Store.DeleteChunk(ctx, ref.GetGeneration(), ref.GetTerm()); err != nil {
			return err
		}
		m.removePendingGC(ref)
	}

	released := m.computeRetention()
	if len(released) == 0 {
		return nil
	}
	// Move the released chunks into pending_gc and publish. The edit is made
	// on a clone by publishManifest and only installed after the write
	// succeeds, so a concurrent marshal never sees a half-edited manifest and
	// a failed write leaves the in-memory state untouched for the next run to
	// recompute.
	if err := m.publishManifest(ctx, func(next *streamingpb.PChannelSummaryManifest) {
		for _, ref := range released {
			next.Chunks = removeChunkEntry(next.Chunks, ref.GetGeneration())
			next.PendingGc = append(next.PendingGc, ref)
		}
	}); err != nil {
		return err
	}
	// The objects are deleted only after the manifest records the move, so a
	// crash in between leaves them referenced by pending_gc for the next run.
	for _, ref := range released {
		if err := m.cfg.Store.DeleteChunk(ctx, ref.GetGeneration(), ref.GetTerm()); err != nil {
			return err
		}
		m.removePendingGC(ref)
	}
	return nil
}

// computeRetention returns the chunk refs, oldest first, that may be released
// to bring the retained bytes back under the budget. A chunk is releasable
// only when every vchannel it covers has a GC position at or above the
// chunk's end timetick (chunkReleasedLocked); the scan stops at the first
// chunk that is not, because release is oldest-first and a younger chunk can
// never be releasable while an older one is not — release order follows
// timetick, which chunks are ordered by.
func (m *Manager) computeRetention() []*streamingpb.PChannelSummaryChunkRef {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cfg.RetentionMaxBytes == 0 {
		return nil
	}
	var retained uint64
	for _, chunk := range m.manifest.GetChunks() {
		retained += chunk.GetObjectSize()
	}
	if retained <= m.cfg.RetentionMaxBytes {
		return nil
	}
	released := make([]*streamingpb.PChannelSummaryChunkRef, 0)
	for _, chunk := range m.manifest.GetChunks() {
		if !m.chunkReleasedLocked(chunk) {
			// The oldest unreleasable chunk bounds the release: nothing newer
			// may be dropped either.
			break
		}
		released = append(released, &streamingpb.PChannelSummaryChunkRef{
			Generation: chunk.GetGeneration(),
			Term:       chunk.GetTerm(),
		})
		retained -= chunk.GetObjectSize()
		if retained <= m.cfg.RetentionMaxBytes {
			break
		}
	}
	return released
}

// chunkReleasedLocked reports whether a chunk is eligible for release: every
// vchannel it covers has a GC position at or above the chunk's end timetick.
// A dropped vchannel reports DroppedVChannelTimeTick, so its chunks are
// always releasable regardless of materialization. Caller holds m.mu.
func (m *Manager) chunkReleasedLocked(chunk *streamingpb.PChannelSummaryChunkIndexEntry) bool {
	for _, index := range chunk.GetVchannels() {
		floor := m.gcFrontiers[index.GetVchannel()]
		if floor == 0 {
			// No GC position yet: nothing of this vchannel may be released.
			return false
		}
		if index.GetEndTimetick() > floor {
			// The chunk still holds records past the GC position.
			return false
		}
	}
	return true
}

// removePendingGC drops a finished deletion from the pending queue and, when
// that empties the queue, publishes the manifest so recovery stops probing the
// deleted objects.
func (m *Manager) removePendingGC(ref *streamingpb.PChannelSummaryChunkRef) {
	m.mu.Lock()
	pending := m.manifest.GetPendingGc()[:0]
	for _, existing := range m.manifest.GetPendingGc() {
		if existing.GetGeneration() == ref.GetGeneration() {
			continue
		}
		pending = append(pending, existing)
	}
	m.manifest.PendingGc = pending
	needsPublish := len(pending) == 0
	m.mu.Unlock()
	if needsPublish {
		if err := m.publishManifest(context.TODO(), func(next *streamingpb.PChannelSummaryManifest) {
			next.PendingGc = nil
		}); err != nil {
			if logger := m.cfg.Logger; logger != nil {
				logger.Warn(context.TODO(), "summary gc failed to publish drained pending_gc", mlog.Err(err))
			}
		}
	}
}

// removeChunkEntry drops one chunk from the manifest by generation.
func removeChunkEntry(chunks []*streamingpb.PChannelSummaryChunkIndexEntry, generation uint64) []*streamingpb.PChannelSummaryChunkIndexEntry {
	out := chunks[:0]
	for _, chunk := range chunks {
		if chunk.GetGeneration() == generation {
			continue
		}
		out = append(out, chunk)
	}
	clear(out[len(out):])
	return out
}

// sortChunkEntries keeps the manifest index in generation order.
func sortChunkEntries(chunks []*streamingpb.PChannelSummaryChunkIndexEntry) {
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].GetGeneration() < chunks[j].GetGeneration()
	})
}

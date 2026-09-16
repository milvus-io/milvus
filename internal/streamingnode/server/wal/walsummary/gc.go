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
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// orphanSweepBudget limits physical deletion work per scheduler execution.
const orphanSweepBudget = 1000

// GCOnce requests retention and garbage collection. Manifest publication and
// physical deletion are asynchronous. Cross-owner exclusion remains the design
// TODO; readMu protects consumers of this manager only.
func (m *Manager) GCOnce(ctx context.Context) error {
	m.mu.Lock()
	terminal := m.terminalErr
	m.mu.Unlock()
	if terminal != nil {
		return terminal
	}
	released := m.computeRetention()
	m.mu.Lock()
	if len(released) > 0 {
		for _, ref := range released {
			for _, chunk := range m.manifest.Chunks {
				if chunk.GetGeneration() != ref.Generation {
					continue
				}
				for _, index := range chunk.GetVchannels() {
					if index.GetTransform() == nil {
						continue
					}
					end := index.GetTransformEndTimetick()
					if end == 0 {
						end = index.GetEndTimetick()
					}
					if m.manifest.TransformTruncatedThrough == nil {
						m.manifest.TransformTruncatedThrough = make(map[string]uint64)
					}
					vc := index.GetVchannel()
					m.manifest.TransformTruncatedThrough[vc] = max(m.manifest.TransformTruncatedThrough[vc], end)
					m.trimTransformStatsLocked(vc, end)
				}
			}
			m.manifest.Chunks = removeChunkEntry(m.manifest.Chunks, ref.Generation)
		}
		m.manifestVersion++
		m.notifyReadersLocked()
	}
	m.mu.Unlock()
	m.scheduleManifest()
	m.mu.Lock()
	if m.cfg.Runtime.Scheduler == nil || (m.gcTask != nil && !m.gcTask.Done()) {
		m.mu.Unlock()
		return nil
	}
	task := &summaryGCTask{manager: m}
	m.gcTask = task
	m.mu.Unlock()
	m.cfg.Runtime.Scheduler.Submit(task)
	return nil
}

type summaryGCTask struct {
	manager *Manager
	done    atomic.Bool
}

func (t *summaryGCTask) Done() bool { return t.done.Load() }
func (t *summaryGCTask) Execute(ctx context.Context) error {
	if t.Done() {
		return nil
	}
	m := t.manager
	m.readMu.Lock()
	defer m.readMu.Unlock()
	// Holding publication ordering across the sweep prevents deletion using a
	// snapshot whose successor has not yet been committed to storage.
	m.publishMu.Lock()
	defer m.publishMu.Unlock()
	m.mu.Lock()
	if m.terminalErr != nil {
		err := m.terminalErr
		t.done.Store(true)
		m.mu.Unlock()
		return err
	}
	if !m.manifestPublished || m.manifestVersion != m.publishedVersion {
		m.mu.Unlock()
		return nodescheduler.ErrDelay
	}
	refs := make(map[ChunkRef]struct{}, len(m.manifest.Chunks))
	for _, chunk := range m.manifest.Chunks {
		refs[ChunkRef{Generation: chunk.GetGeneration(), Term: chunk.GetTerm()}] = struct{}{}
	}
	version := m.manifestVersion
	last := m.manifest.GetLastChunk()
	m.mu.Unlock()
	_, finished, err := m.cfg.Store.sweepGarbage(ctx, m.cfg.Term, last, refs, orphanSweepBudget)
	if err != nil {
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	if !finished {
		return nodescheduler.ErrDelay
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.manifestVersion != version {
		return nodescheduler.ErrDelay
	}
	t.done.Store(true)
	return nil
}

// computeRetention returns the chunk refs, oldest first, that may be released
// to bring the retained bytes back under the budget.
//
// Idempotency records may expire at the byte or chunk-count budget. Transform
// records must remain until their materialization or cleanup frontier is durable,
// so the oldest chunk with an unconsumed transform stops retention release.
func (m *Manager) computeRetention() []*streamingpb.PChannelSummaryChunkRef {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cfg.RetentionMaxBytes == 0 && m.cfg.MaxRetainedChunks == 0 {
		return nil
	}
	chunks := m.manifest.GetChunks()
	var retained uint64
	for _, chunk := range chunks {
		retained += chunk.GetObjectSize()
	}
	count := len(chunks)
	if !m.overRetentionLocked(retained, count) {
		return nil
	}
	released := make([]*streamingpb.PChannelSummaryChunkRef, 0)
	for _, chunk := range chunks {
		if !m.chunkReleasedLocked(chunk) {
			break
		}
		released = append(released, &streamingpb.PChannelSummaryChunkRef{
			Generation: chunk.GetGeneration(),
			Term:       chunk.GetTerm(),
		})
		retained -= chunk.GetObjectSize()
		count--
		if !m.overRetentionLocked(retained, count) {
			break
		}
	}
	return released
}

// overRetentionLocked reports whether the retained set is over either bound.
//
// The count bound is not redundant with the byte bound: a chunk costs bytes in
// storage but it costs a whole object READ at recovery and a whole manifest
// entry on every publish, and those costs scale with the number of chunks, not
// with their size. A pchannel taking a slow trickle of keyed writes seals a
// small chunk per persist cycle, so the byte budget can stay orders of
// magnitude away from its bound while the chunk count climbs without limit --
// which shows up as a manifest PUT that grows with uptime and a WAL open that
// gets slower every day. Either bound alone leaves one of the two unbounded.
//
// A zero bound disables that half; both zero disables release entirely.
func (m *Manager) overRetentionLocked(retained uint64, count int) bool {
	if m.cfg.RetentionMaxBytes > 0 && retained > m.cfg.RetentionMaxBytes {
		return true
	}
	if m.cfg.MaxRetainedChunks > 0 && count > m.cfg.MaxRetainedChunks {
		return true
	}
	return false
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
	// Up to cap, not len: out[len(out):] is empty and clearing it does nothing,
	// leaving the released entries reachable from the shared backing array.
	clear(out[len(out):cap(out)])
	return out
}

func (m *Manager) chunkReleasedLocked(chunk *streamingpb.PChannelSummaryChunkIndexEntry) bool {
	for _, index := range chunk.GetVchannels() {
		if index.GetTransform() == nil {
			continue
		}
		floor := m.gcFrontiers[index.GetVchannel()]
		if floor == 0 {
			// No GC position yet: nothing of this vchannel may be released.
			return false
		}
		end := index.GetTransformEndTimetick()
		if end == 0 {
			// Legacy transform-only chunks use their whole vchannel span.
			end = index.GetEndTimetick()
		}
		if end > floor {
			// The chunk still holds records past the GC position.
			return false
		}
	}
	return true
}

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
	"slices"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const orphanSweepBudget = 1000

// RunGC schedules both policies independently of RecoveryStorage persistence.
// A nonpositive interval disables that policy. Configuration is fixed at WAL open.
func (m *Manager) RunGC(ctx context.Context) {
	var regular, orphan <-chan time.Time
	if m.cfg.GCInterval > 0 {
		ticker := time.NewTicker(m.cfg.GCInterval)
		defer ticker.Stop()
		regular = ticker.C
	}
	if m.cfg.OrphanGCInterval > 0 {
		ticker := time.NewTicker(m.cfg.OrphanGCInterval)
		defer ticker.Stop()
		orphan = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-regular:
			if err := m.GCOnce(ctx); err != nil {
				mlog.Warn(ctx, "failed to collect WALSummary prefix", mlog.Err(err))
			}
		case <-orphan:
			m.OrphanGCOnce()
		}
	}
}

// GCOnce retires one continuous prefix and schedules exactly that deletion list.
// It never discovers unrelated files; orphan collection has its own schedule.
func (m *Manager) GCOnce(ctx context.Context) error {
	now := time.Now()
	m.mu.Lock()
	if m.terminalErr != nil {
		err := m.terminalErr
		m.mu.Unlock()
		return err
	}
	ready := m.retentionTriggeredLocked(now) && (m.gcTask == nil || m.gcTask.Done())
	m.mu.Unlock()
	if !ready {
		return nil
	}
	var frontiers map[string]uint64
	if m.cfg.FetchDataViewRetention != nil {
		var err error
		frontiers, err = m.cfg.FetchDataViewRetention(ctx)
		if err != nil {
			return err
		}
	}
	m.mu.Lock()
	if m.terminalErr != nil {
		err := m.terminalErr
		m.mu.Unlock()
		return err
	}
	if m.gcTask != nil && !m.gcTask.Done() {
		m.mu.Unlock()
		return nil
	}
	// Only lifecycle registration can create an entry or release a Drop pin.
	for channel, frontier := range frontiers {
		if current, exists := m.dataViewRetention[channel]; exists && current != DroppedVChannelTimeTick {
			m.dataViewRetention[channel] = frontier
		}
	}
	released := m.computeRetentionLocked(now)
	if len(released) == 0 {
		m.mu.Unlock()
		return nil
	}
	for _, chunk := range m.manifest.Chunks[:len(released)] {
		for _, index := range chunk.GetVchannels() {
			if index.GetTransform() == nil {
				continue
			}
			if m.manifest.TransformFastForwardTimeTick == nil {
				m.manifest.TransformFastForwardTimeTick = make(map[string]uint64)
			}
			vc := index.GetVchannel()
			m.manifest.TransformFastForwardTimeTick[vc] = max(m.manifest.TransformFastForwardTimeTick[vc], index.GetTransform().GetEndTimeTick())
			m.notifyTransformLocked(vc)
		}
	}
	m.manifest.Chunks = slices.Delete(m.manifest.Chunks, 0, len(released))
	m.chunkIndex.removePrefix(len(released))
	m.manifestVersion++
	m.notifyReadersLocked()
	task := &summaryGCTask{manager: m, chunks: released}
	m.gcTask = task
	m.mu.Unlock()
	m.scheduleManifest()
	if m.cfg.Runtime.Scheduler != nil {
		m.cfg.Runtime.Scheduler.Submit(task, nodescheduler.WithPriority(moduleapi.PriorityData))
	}
	return nil
}

func (m *Manager) OrphanGCOnce() {
	m.mu.Lock()
	if m.terminalErr != nil || m.cfg.Runtime.Scheduler == nil || (m.orphanGCTask != nil && !m.orphanGCTask.Done()) {
		m.mu.Unlock()
		return
	}
	task := &summaryOrphanGCTask{manager: m, cutoff: time.Now().Add(-m.cfg.MaxRetentionDuration)}
	m.orphanGCTask = task
	m.mu.Unlock()
	m.cfg.Runtime.Scheduler.Submit(task, nodescheduler.WithPriority(moduleapi.PriorityData))
}

// gcSnapshot waits for durable retirement and drains older local read snapshots.
// New readers cannot acquire retired references. Uploads beyond the captured
// coverage are protected independently, so slow DELETEs need not hold these locks.
func (m *Manager) gcSnapshot() (*streamingpb.PChannelSummaryManifest, error) {
	if !m.readMu.TryLock() {
		return nil, nodescheduler.ErrDelay
	}
	defer m.readMu.Unlock()
	if !m.publishMu.TryLock() {
		return nil, nodescheduler.ErrDelay
	}
	defer m.publishMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.terminalErr != nil {
		return nil, m.terminalErr
	}
	if !m.manifestPublished || m.manifestVersion != m.publishedVersion {
		return nil, nodescheduler.ErrDelay
	}
	return proto.Clone(m.manifest).(*streamingpb.PChannelSummaryManifest), nil
}

type summaryGCTask struct {
	manager *Manager
	chunks  []*streamingpb.PChannelSummaryChunkRef
	done    atomic.Bool
}

func (t *summaryGCTask) Done() bool { return t.done.Load() }
func (t *summaryGCTask) Execute(ctx context.Context) error {
	if t.Done() {
		return nil
	}
	if _, err := t.manager.gcSnapshot(); err != nil {
		t.done.Store(!errors.Is(err, nodescheduler.ErrDelay))
		return err
	}
	// TODO(#55573): Retention and a unique RecoveryStorage owner must also cover
	// successor recovery and DELETEs already in flight from the previous owner.
	// Local locks and checkpoint term fencing alone do not provide that guarantee.
	count := min(len(t.chunks), orphanSweepBudget)
	for range count {
		ref := t.chunks[0]
		if err := t.manager.cfg.Store.DeleteChunk(ctx, ref.Generation, ref.Term); err != nil {
			return errors.Mark(err, nodescheduler.ErrDelay)
		}
		t.chunks[0] = nil
		t.chunks = t.chunks[1:]
	}
	if len(t.chunks) > 0 {
		return nodescheduler.ErrDelay
	}
	t.done.Store(true)
	return nil
}

type summaryOrphanGCTask struct {
	manager *Manager
	cutoff  time.Time
	done    atomic.Bool
}

func (t *summaryOrphanGCTask) Done() bool { return t.done.Load() }
func (t *summaryOrphanGCTask) Execute(ctx context.Context) error {
	if t.Done() {
		return nil
	}
	m := t.manager
	snapshot, err := m.gcSnapshot()
	if err != nil {
		t.done.Store(!errors.Is(err, nodescheduler.ErrDelay))
		return err
	}
	refs := make(map[ChunkRef]struct{}, len(snapshot.Chunks))
	for _, chunk := range snapshot.Chunks {
		refs[ChunkRef{Generation: chunk.Generation, Term: chunk.Term}] = struct{}{}
	}
	_, finished, err := m.cfg.Store.sweepGarbage(ctx, m.cfg.Term, snapshot.Coverage, refs, t.cutoff, orphanSweepBudget)
	if err != nil {
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	if !finished {
		return nodescheduler.ErrDelay
	}
	if err := m.cfg.Store.DeleteManifestsBelowTerm(ctx, m.cfg.Term, t.cutoff); err != nil {
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	t.done.Store(true)
	return nil
}

func (m *Manager) retainedBytesLocked() uint64 {
	var retained uint64
	for _, chunk := range m.manifest.GetChunks() {
		retained += chunk.GetObjectSize()
	}
	return retained
}

func (m *Manager) retentionTriggeredLocked(now time.Time) bool {
	chunks := m.manifest.GetChunks()
	return len(chunks) > 0 && m.retainedBytesLocked() > m.cfg.MaxRetentionBytes &&
		tsoutil.PhysicalTime(chunks[0].GetStartTimeTick()).Before(now.Add(-m.cfg.MaxRetentionDuration))
}

func (m *Manager) computeRetention() []*streamingpb.PChannelSummaryChunkRef {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.computeRetentionLocked(time.Now())
}

func (m *Manager) computeRetentionLocked(now time.Time) []*streamingpb.PChannelSummaryChunkRef {
	if !m.retentionTriggeredLocked(now) {
		return nil
	}
	// Use exactly the durable selection shared with idempotency recovery. Do not
	// credit pending/sealed records against history needed after a crash.
	protected := m.planDurableIdempotencyRecoveryLocked(idempotencyCutoff(now, m.cfg.IdempotencyRetention.TTL), uint64(max(0, m.cfg.IdempotencyRetention.MaxSize)))
	limit := len(m.manifest.Chunks)
	if len(protected) > 0 {
		first := protected[0].chunk.GetGeneration()
		for i, chunk := range m.manifest.Chunks {
			if chunk.Generation == first {
				limit = i
				break
			}
		}
	}
	retained := m.retainedBytesLocked()
	cutoff := now.Add(-m.cfg.MaxRetentionDuration)
	var released []*streamingpb.PChannelSummaryChunkRef
	for _, chunk := range m.manifest.Chunks[:limit] {
		if retained <= m.cfg.MaxRetentionBytes || retained-chunk.ObjectSize < m.cfg.MaxRetentionBytes ||
			!tsoutil.PhysicalTime(chunk.EndTimetick).Before(cutoff) || !m.chunkReleasedLocked(chunk) {
			break
		}
		released = append(released, &streamingpb.PChannelSummaryChunkRef{Generation: chunk.Generation, Term: chunk.Term})
		retained -= chunk.ObjectSize
	}
	return released
}

func (m *Manager) chunkReleasedLocked(chunk *streamingpb.PChannelSummaryChunkIndexEntry) bool {
	for _, index := range chunk.GetVchannels() {
		if index.GetTransform() == nil {
			continue
		}
		floor := m.dataViewRetention[index.GetVchannel()]
		if floor == 0 {
			// No GC position yet: nothing of this vchannel may be released.
			return false
		}
		end := index.GetTransform().GetEndTimeTick()
		if end > floor {
			// The chunk still holds records past the GC position.
			return false
		}
	}
	return true
}

// CanCleanupVChannel reports whether recovery no longer needs the VChannel's
// retained Delete history.
// The WAL cleanup fence independently prevents replay from recreating it.
func (m *Manager) CanCleanupVChannel(vchannel string, through uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.terminalErr != nil || m.lastAcked < through || m.manifestVersion != m.publishedVersion {
		return false
	}
	for _, chunk := range m.manifest.GetChunks() {
		for _, index := range chunk.GetVchannels() {
			if index.GetVchannel() == vchannel && index.GetTransform() != nil {
				return false
			}
		}
	}
	return true
}

// SetDataViewRetention initializes protection at recovery or collection creation,
// or releases it with DroppedVChannelTimeTick once durable Drop is cleanup-ready.
// The released entry stays until VChannelMeta deletion has been persisted.
// Segment lifecycle events must not reset a frontier already refreshed from Coord.
func (m *Manager) SetDataViewRetention(vchannel string, startAfter uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dataViewRetention == nil {
		m.dataViewRetention = make(map[string]uint64)
	}
	m.dataViewRetention[vchannel] = startAfter
}

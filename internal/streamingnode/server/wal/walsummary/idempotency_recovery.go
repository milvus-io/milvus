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
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type idempotencyCandidate struct {
	vchannel string
	key      *streamingpb.VChannelSummaryIdempotencyRecord
	insert   *streamingpb.VChannelSummaryInsertRecord
}

type idempotencyChunk struct {
	chunk   *indexedChunk
	indexes []*streamingpb.VChannelSummaryChunkIndex
}

// ReadIdempotencySnapshots plans the newest Summary suffix using manifest
// TimeTicks and encoded Insert/Idempotency section lengths before object I/O.
// One whole boundary chunk may exceed the budget. Loads are bounded and issued
// oldest first; ordered cache promotion keeps newer chunks hot despite completion
// reordering. Access refreshes and per-VChannel floors are not recovered.
func (m *Manager) ReadIdempotencySnapshots(ctx context.Context, config idempotencyview.RetentionConfig, now time.Time) (map[string]*idempotencyview.Snapshot, error) {
	recovered, err := m.ReadIdempotencyRecovery(ctx, config, now)
	if err != nil || recovered == nil {
		return nil, err
	}
	return recovered.VChannels, nil
}

// ReadIdempotencyRecovery preserves ordered chunk/section boundaries through
// the handoff to the live window, avoiding both full-history recovery sorts.
func (m *Manager) ReadIdempotencyRecovery(ctx context.Context, config idempotencyview.RetentionConfig, now time.Time) (*idempotencyview.RecoverySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.MaxSize <= 0 {
		return nil, nil
	}
	from := uint64(0)
	if config.TTL > 0 {
		cutoff := now.Add(-config.TTL)
		if cutoff.UnixMilli() >= 0 {
			// Expire every logical tick in the expired physical millisecond.
			from = tsoutil.ComposeTSByTime(cutoff.Truncate(time.Millisecond).Add(time.Millisecond)) - 1
		}
	}
	m.readMu.RLock()
	defer m.readMu.RUnlock()
	hot, chunks := m.planIdempotencyRecovery(from, uint64(config.MaxSize))

	out := &idempotencyview.RecoverySnapshot{VChannels: make(map[string]*idempotencyview.Snapshot)}
	type identity struct{ vchannel, key string }
	type location struct {
		section *idempotencyview.Snapshot
		offset  int
	}
	positions := make(map[identity]location)
	var sections []*idempotencyview.Snapshot
	var byVChannel map[string]*idempotencyview.Snapshot
	beginChunk := func() {
		sections = nil
		byVChannel = make(map[string]*idempotencyview.Snapshot)
	}
	add := func(vc string, key *streamingpb.VChannelSummaryIdempotencyRecord, insert *streamingpb.VChannelSummaryInsertRecord) {
		if key.GetKey() == "" || insert.GetSourceTimetick() <= from {
			return
		}
		id := identity{vc, key.GetKey()}
		if old, ok := positions[id]; ok {
			if old.section.Records[old.offset].SourceTimeTick >= insert.GetSourceTimetick() {
				return
			}
			// Remove the old slot, then append the new fact at its chronological
			// position. Never mutate the shared protobuf result or replace in place.
			old.section.Records[old.offset] = nil
		}
		section := byVChannel[vc]
		if section == nil {
			section = &idempotencyview.Snapshot{PChannel: m.cfg.PChannel, VChannel: vc}
			byVChannel[vc] = section
			sections = append(sections, section)
		}
		positions[id] = location{section, len(section.Records)}
		section.Records = append(section.Records, idempotencyview.RecordFromSections(key, insert))
	}

	// At most one bounded batch of encoded objects is held outside the cache.
	// Join every load (also on errors) before releasing the snapshot's GC pin.
	for start := 0; start < len(chunks); start += chunkReadConcurrency {
		batch := chunks[start:min(start+chunkReadConcurrency, len(chunks))]
		payloads := make([]*chunkPayload, len(batch))
		group, readCtx := errgroup.WithContext(ctx)
		for i, selected := range batch {
			group.Go(func() error {
				payload, err := m.chunkIndex.cache.read(readCtx, m.cfg.Store, selected.chunk)
				payloads[i] = payload
				return err
			})
		}
		if err := group.Wait(); err != nil {
			return nil, err
		}
		for i, selected := range batch {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			payload := payloads[i]
			// Loads may finish newest first. Re-admit/promote in generation
			// order using the acquired bytes, never a second storage read.
			m.chunkIndex.cache.retain(selected.chunk, payload)
			beginChunk()
			for _, index := range selected.indexes {
				sections, err := unmarshalIdempotencySections(payload.bytes, payload.footerStart, index)
				if err != nil {
					return nil, err
				}
				if len(sections.Idempotency) == 0 {
					continue
				}
				for j, insert := range sections.Inserts {
					add(index.GetVchannel(), sections.Idempotency[j], insert)
				}
			}
			out.Chunks = append(out.Chunks, sections)
			payloads[i] = nil
		}
	}
	for _, candidates := range hot {
		beginChunk()
		for _, candidate := range candidates {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			add(candidate.vchannel, candidate.key, candidate.insert)
		}
		out.Chunks = append(out.Chunks, sections)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Stable compaction removes obsolete versions without sorting. Compact
	// sparse arrays to avoid retaining an entire old section for one live key.
	for _, chunk := range out.Chunks {
		for _, section := range chunk {
			section.Records = slices.DeleteFunc(section.Records, func(record *idempotencyview.Record) bool { return record == nil })
			if len(section.Records) == 0 {
				section.Records = nil
				continue
			}
			if len(section.Records)*2 < cap(section.Records) {
				section.Records = slices.Clone(section.Records)
			}
			legacy := out.VChannels[section.VChannel]
			if legacy == nil {
				legacy = &idempotencyview.Snapshot{PChannel: m.cfg.PChannel, VChannel: section.VChannel}
				out.VChannels[section.VChannel] = legacy
			}
			legacy.Records = append(legacy.Records, section.Records...)
		}
	}
	return out, nil
}

// planIdempotencyRecovery captures the atomic hot/sealed/durable boundary.
// Encoded bytes are charged before deduplication. Only the selected hot facts
// and chunk refs are copied; there is no full-range chunk/candidate snapshot.
func (m *Manager) planIdempotencyRecovery(from, remaining uint64) ([][]idempotencyCandidate, []idempotencyChunk) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var hot [][]idempotencyCandidate
	var pending []idempotencyCandidate
	for i := len(m.pending) - 1; i >= 0 && remaining > 0; i-- {
		record := &m.pending[i]
		if record.timeTick <= from {
			break
		}
		if record.idempotency.GetKey() == "" {
			continue
		}
		pending = append(pending, idempotencyCandidate{record.vchannel, record.idempotency, record.insert})
		remaining -= min(remaining, idempotencyEncodedSize(record))
	}
	if len(pending) > 0 {
		slices.Reverse(pending)
		hot = append(hot, pending)
	}
	for i := len(m.pendingSealed) - 1; i >= 0 && remaining > 0; i-- {
		sealed := m.pendingSealed[i]
		if sealed.MaxTimeTick <= from {
			break
		}
		var candidates []idempotencyCandidate
		// A sealed chunk has no encoded refs yet. Compute the equivalent
		// repeated-record lengths without serializing or copying its payload.
		for vc, records := range sealed.RecordsByVChannel {
			if records[len(records)-1].timeTick <= from {
				continue
			}
			hasKey := false
			for _, record := range records {
				hasKey = hasKey || record.idempotency.GetKey() != ""
			}
			if !hasKey {
				continue
			}
			for _, record := range records {
				if record.insert == nil {
					continue
				}
				remaining -= min(remaining, idempotencyEncodedSize(record))
				if record.timeTick > from && record.idempotency.GetKey() != "" {
					candidates = append(candidates, idempotencyCandidate{vc, record.idempotency, record.insert})
				}
			}
		}
		if len(candidates) > 0 {
			hot = append(hot, candidates)
		}
	}
	slices.Reverse(hot)
	var chunks []idempotencyChunk
	for i := len(m.chunkIndex.chunks) - 1; i >= 0 && remaining > 0; i-- {
		chunk := m.chunkIndex.chunks[i]
		if chunk.GetEndTimetick() <= from {
			break
		}
		selected := idempotencyChunk{chunk: chunk}
		for _, index := range chunk.GetVchannels() {
			if index.GetIdempotency() == nil || index.GetEndTimetick() <= from {
				continue
			}
			selected.indexes = append(selected.indexes, index)
			remaining -= min(remaining, index.GetIdempotency().GetLength())
			remaining -= min(remaining, index.GetInserts().GetLength())
		}
		if len(selected.indexes) > 0 {
			chunks = append(chunks, selected)
		}
	}
	slices.Reverse(chunks)
	return hot, chunks
}

// Both sections use repeated record field #1. Include each protobuf envelope,
// matching section lengths rather than the live cache's Record.Size estimate.
func idempotencyEncodedSize(record *stagedRecord) uint64 {
	return uint64(2*protowire.SizeTag(1) + protowire.SizeBytes(proto.Size(record.idempotency)) + protowire.SizeBytes(proto.Size(record.insert)))
}

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
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// TransformStats bounds logical bytes in (after,through]. Fully included
// sections contribute to both bounds; intersected sections only to UpperBytes.
// Boundary sections need an asynchronous read to establish exact capacity.
type TransformStats struct {
	Bytes, UpperBytes           uint64
	FirstTimeTick, LastTimeTick uint64
}

func transformEntrySize(entry *streamingpb.TransformLogEntry) (uint64, uint64) {
	var rows uint64
	for _, block := range entry.GetDelete().GetBlocks() {
		rows += uint64(len(block.GetPrimaryKeys().GetIntId().GetData()) + len(block.GetPrimaryKeys().GetStrId().GetData()))
	}
	return rows, uint64(proto.Size(entry))
}

func addTransformSize(indexes map[string]*streamingpb.VChannelSummaryTransformIndex, vc string, entry *streamingpb.TransformLogEntry) {
	index := indexes[vc]
	if index == nil {
		index = &streamingpb.VChannelSummaryTransformIndex{StartTimeTick: entry.GetTimeTick()}
		indexes[vc] = index
	}
	index.EndTimeTick = entry.GetTimeTick()
	_, size := transformEntrySize(entry)
	index.TotalSize += size
}

func (stats *TransformStats) add(index *streamingpb.VChannelSummaryTransformIndex, after, through uint64) {
	if index == nil || through <= after || index.GetEndTimeTick() <= after || index.GetStartTimeTick() > through {
		return
	}
	if index.GetStartTimeTick() > after && index.GetEndTimeTick() <= through {
		stats.Bytes += index.GetTotalSize()
	}
	stats.UpperBytes += index.GetTotalSize()
	start := max(index.GetStartTimeTick(), after+1)
	if stats.FirstTimeTick == 0 || start < stats.FirstTimeTick {
		stats.FirstTimeTick = start
	}
	stats.LastTimeTick = max(stats.LastTimeTick, min(index.GetEndTimeTick(), through))
}

// visitTransformsLocked visits each immutable or pending section exactly once.
func (m *Manager) visitTransformsLocked(visit func(string, *streamingpb.VChannelSummaryTransformIndex)) {
	for _, chunk := range m.manifest.GetChunks() {
		for _, index := range chunk.GetVchannels() {
			if index.GetTransform() != nil {
				visit(index.GetVchannel(), index.GetTransform())
			}
		}
	}
	for _, chunk := range m.pendingSealed {
		for vc, index := range chunk.Transforms {
			visit(vc, index)
		}
	}
	for vc, index := range m.pendingTransforms {
		visit(vc, index)
	}
}

// TransformStats does no object I/O and retains no per-entry index.
func (m *Manager) TransformStats(vc string, after, through uint64) TransformStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	var stats TransformStats
	m.visitTransformsLocked(func(channel string, index *streamingpb.VChannelSummaryTransformIndex) {
		if channel == vc {
			stats.add(index, after, min(through, m.readableThrough))
		}
	})
	return stats
}

// ReportMaterialized suppresses redundant backlog work but never authorizes GC.
func (m *Manager) ReportMaterialized(vc string, through uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.materializedFrontiers[vc] = max(m.materializedFrontiers[vc], through)
}

// requestMaterializationBacklog runs in Summary's existing background worker.
// A partial section's oldest remaining Delete is resolved by a bounded read,
// never by an object read from ObserveMessage or a per-materializer timer.
func (m *Manager) requestMaterializationBacklog(ctx context.Context, now time.Time, maxAge time.Duration) {
	if m.cfg.RequestMaterialization == nil {
		return
	}
	type candidate struct {
		after uint64
		stats TransformStats
	}
	candidates := make(map[string]candidate)
	requests := make(map[string]uint64)
	m.mu.Lock()
	if m.terminalErr != nil {
		m.mu.Unlock()
		return
	}
	m.visitTransformsLocked(func(vc string, index *streamingpb.VChannelSummaryTransformIndex) {
		c := candidates[vc]
		c.after = max(m.gcFrontiers[vc], m.materializedFrontiers[vc])
		c.stats.add(index, c.after, m.readableThrough)
		candidates[vc] = c
	})
	var retained uint64
	for _, chunk := range m.manifest.Chunks {
		retained += chunk.GetObjectSize()
	}
	if len(m.manifest.Chunks) > 0 && m.overRetentionLocked(retained, len(m.manifest.Chunks)) {
		for _, index := range m.manifest.Chunks[0].GetVchannels() {
			vc, through := index.GetVchannel(), index.GetTransform().GetEndTimeTick()
			if through > max(m.gcFrontiers[vc], m.materializedFrontiers[vc]) {
				requests[vc] = through
			}
		}
	}
	m.mu.Unlock()
	for vc, c := range candidates {
		if c.stats.LastTimeTick == 0 || maxAge <= 0 || now.Sub(tsoutil.PhysicalTime(c.stats.FirstTimeTick)) < maxAge {
			continue
		}
		first := c.stats.FirstTimeTick
		if c.stats.Bytes != c.stats.UpperBytes {
			batch, err := m.ReadTransform(ctx, vc, c.after, c.stats.LastTimeTick, ReadLimits{MaxRows: 1, MaxBytes: 1})
			if err != nil || len(batch.Entries) == 0 {
				continue
			}
			first = batch.Entries[0].GetTimeTick()
		}
		if now.Sub(tsoutil.PhysicalTime(first)) >= maxAge {
			requests[vc] = max(requests[vc], c.stats.LastTimeTick)
		}
	}
	// VChannel callbacks must never execute while holding the Summary lock.
	for vc, target := range requests {
		m.cfg.RequestMaterialization(vc, target)
	}
}

func validateTransformIndex(index *streamingpb.VChannelSummaryChunkIndex) error {
	transform := index.GetTransform()
	if transform == nil {
		return nil
	}
	if transform.GetRef() == nil || transform.GetRef().GetRecordCount() == 0 || transform.GetTotalSize() == 0 ||
		transform.GetStartTimeTick() < index.GetStartTimetick() || transform.GetEndTimeTick() > index.GetEndTimetick() ||
		transform.GetStartTimeTick() > transform.GetEndTimeTick() {
		return storeCorruptedf("invalid transform index for %s", index.GetVchannel())
	}
	return nil
}

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
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// TransformStats describes retained Delete entries within a readable range.
// Rows are primary keys, not entry count. Bytes use the reader's logical size.
type TransformStats struct {
	Rows, Bytes                 uint64
	FirstTimeTick, LastTimeTick uint64
}

type (
	transformPoint struct{ tick, rows, bytes uint64 }
	transformIndex struct {
		points              []transformPoint
		baseRows, baseBytes uint64
	}
)

func transformEntrySize(entry *streamingpb.TransformLogEntry) (uint64, uint64) {
	var rows uint64
	for _, block := range entry.GetDelete().GetBlocks() {
		rows += uint64(len(block.GetPrimaryKeys().GetIntId().GetData()) + len(block.GetPrimaryKeys().GetStrId().GetData()))
	}
	return rows, uint64(proto.Size(entry))
}

func (m *Manager) appendTransformStatLocked(vc string, tick, rows, bytes uint64) {
	index := m.transformIndexes[vc]
	if index == nil {
		index = &transformIndex{}
		m.transformIndexes[vc] = index
	}
	if n := len(index.points); n > 0 {
		rows += index.points[n-1].rows
		bytes += index.points[n-1].bytes
	} else {
		rows += index.baseRows
		bytes += index.baseBytes
	}
	index.points = append(index.points, transformPoint{tick: tick, rows: rows, bytes: bytes})
}

func (index *transformIndex) stats(after, through uint64) TransformStats {
	if index == nil || through <= after {
		return TransformStats{}
	}
	start := sort.Search(len(index.points), func(i int) bool { return index.points[i].tick > after })
	end := sort.Search(len(index.points), func(i int) bool { return index.points[i].tick > through })
	if start == end {
		return TransformStats{}
	}
	rows, bytes := index.baseRows, index.baseBytes
	if start > 0 {
		rows = index.points[start-1].rows
		bytes = index.points[start-1].bytes
	}
	last := index.points[end-1]
	return TransformStats{Rows: last.rows - rows, Bytes: last.bytes - bytes, FirstTimeTick: index.points[start].tick, LastTimeTick: last.tick}
}

// TransformStats does no payload reads. The prefix index spans hot and durable
// storage; moving a record between them neither adds nor removes its statistics.
func (m *Manager) TransformStats(vc string, after, through uint64) TransformStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.transformIndexes[vc].stats(after, min(through, m.readableThrough))
}

func (m *Manager) trimTransformStatsLocked(vc string, through uint64) {
	index := m.transformIndexes[vc]
	if index == nil {
		return
	}
	n := sort.Search(len(index.points), func(i int) bool { return index.points[i].tick > through })
	if n == 0 {
		return
	}
	if n == len(index.points) {
		delete(m.transformIndexes, vc)
		return
	}
	index.baseRows, index.baseBytes = index.points[n-1].rows, index.points[n-1].bytes
	index.points = append([]transformPoint(nil), index.points[n:]...)
}

// ReportMaterialized records successful output before metadata publication.
// This suppresses redundant consumption requests but never authorizes GC.
func (m *Manager) ReportMaterialized(vc string, through uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.materializedFrontiers[vc] = max(m.materializedFrontiers[vc], through)
}

// requestMaterializationBacklog extends the existing Summary backlog worker to
// retained Deletes. No materializer timer exists. The existing Summary age
// budget applies to the source WAL time, including after upload or restart.
func (m *Manager) requestMaterializationBacklog(now time.Time, maxAge time.Duration) {
	if m.cfg.RequestMaterialization == nil {
		return
	}
	requests := make(map[string]uint64)
	m.mu.Lock()
	if m.terminalErr != nil {
		m.mu.Unlock()
		return
	}
	for vc, index := range m.transformIndexes {
		after := max(m.gcFrontiers[vc], m.materializedFrontiers[vc])
		stats := index.stats(after, m.readableThrough)
		if stats.LastTimeTick > 0 && maxAge > 0 && now.Sub(tsoutil.PhysicalTime(stats.FirstTimeTick)) >= maxAge {
			requests[vc] = stats.LastTimeTick
		}
	}
	// Retention pressure requests only the oldest blocking chunk. Subsequent
	// checks reassess after durable metadata and GC, rather than draining all VCs.
	var retained uint64
	for _, chunk := range m.manifest.Chunks {
		retained += chunk.GetObjectSize()
	}
	if len(m.manifest.Chunks) > 0 && m.overRetentionLocked(retained, len(m.manifest.Chunks)) {
		for _, index := range m.manifest.Chunks[0].GetVchannels() {
			vc, through := index.GetVchannel(), index.GetTransformEndTimetick()
			if through > max(m.gcFrontiers[vc], m.materializedFrontiers[vc]) {
				requests[vc] = max(requests[vc], through)
			}
		}
	}
	m.mu.Unlock()
	// Never call into a VChannel while holding the Summary lock: observations
	// acquire their component locks before consulting range statistics.
	for vc, target := range requests {
		m.cfg.RequestMaterialization(vc, target)
	}
}

// Statistics are part of the recovery index, not an optional estimate. Missing
// or inconsistent entries cannot silently turn a retained backlog into zero.
func validateTransformStats(index *streamingpb.VChannelSummaryChunkIndex) error {
	stats := index.GetTransformStats()
	if uint64(len(stats)) != index.GetTransform().GetRecordCount() {
		return storeCorruptedf("incomplete transform statistics for %s", index.GetVchannel())
	}
	var tick, rows, bytes uint64
	for _, stat := range stats {
		if stat.GetTimeTick() <= tick || stat.GetTimeTick() < index.GetStartTimetick() || stat.GetTimeTick() > index.GetEndTimetick() || stat.GetRows() < rows || stat.GetBytes() <= bytes {
			return storeCorruptedf("invalid transform statistics for %s", index.GetVchannel())
		}
		tick, rows, bytes = stat.GetTimeTick(), stat.GetRows(), stat.GetBytes()
	}
	if len(stats) > 0 && tick != index.GetTransformEndTimetick() {
		return storeCorruptedf("transform statistics boundary differs for %s", index.GetVchannel())
	}
	return nil
}

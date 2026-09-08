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

// statsSegmentChanged compares only eligibility/dependency fields. In
// particular, row-count/checkpoint updates do not enqueue discovery work.
// Stats filenames need not be compared: discovery cares about field presence
// and JSON format version, not about individual generated file paths.
func statsSegmentChanged(old, current *SegmentInfo) bool {
	if old == nil || current == nil {
		return old != current
	}
	if old.GetState() != current.GetState() || old.GetLevel() != current.GetLevel() ||
		old.GetIsSorted() != current.GetIsSorted() || old.GetIsSortedByNamespace() != current.GetIsSortedByNamespace() ||
		old.GetStorageVersion() != current.GetStorageVersion() || old.GetManifestPath() != current.GetManifestPath() ||
		old.GetSchemaVersion() != current.GetSchemaVersion() || old.GetDataVersion() != current.GetDataVersion() ||
		old.GetIsImporting() != current.GetIsImporting() || old.GetIsInvisible() != current.GetIsInvisible() ||
		len(old.GetBinlogs()) != len(current.GetBinlogs()) {
		return true
	}
	if len(old.GetTextStatsLogs()) != len(current.GetTextStatsLogs()) ||
		len(old.GetJsonKeyStats()) != len(current.GetJsonKeyStats()) {
		return true
	}
	for id, stats := range old.GetTextStatsLogs() {
		other, ok := current.GetTextStatsLogs()[id]
		if !ok || (stats == nil) != (other == nil) {
			return true
		}
	}
	for id, stats := range old.GetJsonKeyStats() {
		other, ok := current.GetJsonKeyStats()[id]
		if !ok || (stats == nil) != (other == nil) ||
			stats.GetJsonKeyStatsDataFormat() != other.GetJsonKeyStatsDataFormat() {
			return true
		}
	}
	return false
}

// Call only after successful persistence AND cache publication, never inside a
// transaction's retryable mutation callback. Consumers always reread meta;
// duplicate or stale publication notifications cannot resurrect old values.
func (m *meta) notifyStatsChange(old, current *SegmentInfo) {
	q := m.statsDiscovery.Load()
	if q == nil || !statsSegmentChanged(old, current) {
		return
	}
	segment := current
	if segment == nil {
		segment = old
	}
	q.notifySegment(segment.GetCollectionID(), segment.GetID())
}

func (m *meta) notifyStatsSegments(collectionID int64, segmentIDs ...int64) {
	if q := m.statsDiscovery.Load(); q != nil {
		for _, id := range segmentIDs {
			q.notifySegment(collectionID, id)
		}
	}
}

// Publication helpers run under segMu, after the corresponding catalog commit.
func (m *meta) setSegmentAndNotifyStats(id int64, segment *SegmentInfo) {
	old := m.segments.GetSegment(id)
	m.segments.SetSegment(id, segment)
	m.notifyStatsChange(old, segment)
}

func (m *meta) dropSegmentAndNotifyStats(id int64) {
	old := m.segments.GetSegment(id)
	m.segments.DropSegment(id)
	m.notifyStatsChange(old, nil)
}

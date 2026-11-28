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

package meta

import (
	"maps"

	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// fullQuotaSnapshot reads a full snapshot of the statistics cache for a
// diagnostic reader. It returns false when the cache is disabled and callers
// use the full-scan reference.
func (m *Meta) fullQuotaSnapshot() (*metricsinfo.QuotaSnapshot, bool) {
	if !Params.QuotaConfig.CollectionStatsCacheEnabled.GetAsBool() {
		m.quotaMetricsMu.Lock()
		m.Segments.resetQuotaCache()
		m.quotaDisplay = nil
		m.quotaMetricsMu.Unlock()
		return nil, false
	}
	snapshot := m.Segments.ReadQuotaSnapshot(true)
	m.Segments.discardQuotaPendingWithoutConsumer()
	return snapshot, true
}

func (m *Meta) GetQuotaInfo() *metricsinfo.DataCoordQuotaMetrics {
	snapshot, ok := m.fullQuotaSnapshot()
	if !ok {
		return m.getQuotaInfoByScan()
	}
	result := &metricsinfo.DataCoordQuotaMetrics{
		TotalBinlogSize:      snapshot.TotalBytes,
		CollectionBinlogSize: make(map[int64]int64),
		PartitionsBinlogSize: make(map[int64]map[int64]int64),
		CollectionL0RowCount: make(map[int64]int64),
	}
	for id, value := range snapshot.Collections {
		if value.HasQuota {
			result.CollectionBinlogSize[id] = value.Bytes
			result.PartitionsBinlogSize[id] = maps.Clone(value.PartitionBytes)
		}
		if value.HasL0 {
			result.CollectionL0RowCount[id] = value.L0Rows
		}
	}
	m.recordQuotaChanges(snapshot)
	return result
}

func (m *Meta) GetAllCollectionNumRows() map[int64]int64 {
	snapshot, ok := m.fullQuotaSnapshot()
	if !ok {
		return m.getAllCollectionNumRowsByScan()
	}
	result := make(map[int64]int64)
	for id, value := range snapshot.Collections {
		if value.HasRows {
			result[id] = value.Rows
		}
	}
	return result
}

func (m *Meta) ReadQuotaSnapshot(full bool) *metricsinfo.QuotaSnapshot {
	snapshot := m.Segments.ReadQuotaSnapshot(full)
	m.recordQuotaChanges(snapshot)
	return snapshot
}

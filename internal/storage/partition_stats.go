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

package storage

import (
	"encoding/json"
	"path"
	"strconv"
)

type SegmentStats struct {
	FieldStats []FieldStats `json:"fieldStats"`
	NumRows    int
}

func NewSegmentStats(fieldStats []FieldStats, rows int) *SegmentStats {
	return &SegmentStats{
		FieldStats: fieldStats,
		NumRows:    rows,
	}
}

type PartitionStatsSnapshot struct {
	SegmentStats map[UniqueID]SegmentStats `json:"segmentStats"`
	Version      int64
}

func NewPartitionStatsSnapshot() *PartitionStatsSnapshot {
	return &PartitionStatsSnapshot{
		SegmentStats: make(map[UniqueID]SegmentStats, 0),
	}
}

func (ps *PartitionStatsSnapshot) GetVersion() int64 {
	return ps.Version
}

func (ps *PartitionStatsSnapshot) SetVersion(v int64) {
	ps.Version = v
}

func (ps *PartitionStatsSnapshot) UpdateSegmentStats(segmentID UniqueID, segmentStats SegmentStats) {
	ps.SegmentStats[segmentID] = segmentStats
}

func DeserializePartitionsStatsSnapshot(data []byte) (*PartitionStatsSnapshot, error) {
	var messageMap map[string]*json.RawMessage
	err := json.Unmarshal(data, &messageMap)
	if err != nil {
		return nil, err
	}

	partitionStats := &PartitionStatsSnapshot{
		SegmentStats: make(map[UniqueID]SegmentStats),
	}
	err = json.Unmarshal(*messageMap["segmentStats"], &partitionStats.SegmentStats)
	if err != nil {
		return nil, err
	}
	return partitionStats, nil
}

func SerializePartitionStatsSnapshot(partStats *PartitionStatsSnapshot) ([]byte, error) {
	partData, err := json.Marshal(partStats)
	if err != nil {
		return nil, err
	}
	return partData, nil
}

func FindPartitionStatsMaxVersion(filePaths []string) (int64, string) {
	maxVersion := int64(-1)
	maxVersionFilePath := ""
	for _, filePath := range filePaths {
		versionStr := path.Base(filePath)
		version, err := strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			continue
		}
		if version > maxVersion {
			maxVersion = version
			maxVersionFilePath = filePath
		}
	}
	return maxVersion, maxVersionFilePath
}

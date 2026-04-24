// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// FieldBinlogStatEntry builds a StatEntry from a FieldBinlog with the given key prefix.
// For example: FieldBinlogStatEntry("bloom_filter", stats.GetFieldID(), stats)
func FieldBinlogStatEntry(prefix string, fieldID int64, fb *datapb.FieldBinlog) StatEntry {
	var memorySize int64
	paths := make([]string, 0, len(fb.GetBinlogs()))
	for _, b := range fb.GetBinlogs() {
		paths = append(paths, b.GetLogPath())
		memorySize += b.GetMemorySize()
	}
	entry := StatEntry{
		Key:   fmt.Sprintf("%s.%d", prefix, fieldID),
		Files: paths,
	}
	if memorySize > 0 {
		entry.Metadata = map[string]string{"memory_size": fmt.Sprintf("%d", memorySize)}
	}
	return entry
}

// TextIndexStatEntries builds StatEntry slice from text index stats.
func TextIndexStatEntries(textStats map[int64]*datapb.TextIndexStats, scalarIndexVersion int32) []StatEntry {
	entries := make([]StatEntry, 0, len(textStats))
	for fieldID, ts := range textStats {
		entries = append(entries, StatEntry{
			Key:   fmt.Sprintf("text_index.%d", fieldID),
			Files: ts.GetFiles(),
			Metadata: map[string]string{
				"version":                      fmt.Sprintf("%d", ts.GetVersion()),
				"build_id":                     fmt.Sprintf("%d", ts.GetBuildID()),
				"log_size":                     fmt.Sprintf("%d", ts.GetLogSize()),
				"memory_size":                  fmt.Sprintf("%d", ts.GetMemorySize()),
				"current_scalar_index_version": fmt.Sprintf("%d", scalarIndexVersion),
			},
		})
	}
	return entries
}

// JSONKeyStatEntries builds StatEntry slice from JSON key stats.
func JSONKeyStatEntries(jsonStats map[int64]*datapb.JsonKeyStats) []StatEntry {
	entries := make([]StatEntry, 0, len(jsonStats))
	for fieldID, js := range jsonStats {
		entries = append(entries, StatEntry{
			Key:   fmt.Sprintf("json_stats.%d", fieldID),
			Files: js.GetFiles(),
			Metadata: map[string]string{
				"version":                    fmt.Sprintf("%d", js.GetVersion()),
				"build_id":                   fmt.Sprintf("%d", js.GetBuildID()),
				"log_size":                   fmt.Sprintf("%d", js.GetLogSize()),
				"memory_size":                fmt.Sprintf("%d", js.GetMemorySize()),
				"json_key_stats_data_format": fmt.Sprintf("%d", js.GetJsonKeyStatsDataFormat()),
			},
		})
	}
	return entries
}

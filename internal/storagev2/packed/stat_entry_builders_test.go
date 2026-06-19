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

package packed

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestFieldBinlogStatEntry(t *testing.T) {
	t.Run("key format and paths", func(t *testing.T) {
		fb := &datapb.FieldBinlog{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{LogPath: "path/a", MemorySize: 1024},
				{LogPath: "path/b", MemorySize: 2048},
			},
		}
		entry := FieldBinlogStatEntry("bloom_filter", 100, fb)
		assert.Equal(t, "bloom_filter.100", entry.Key)
		assert.Equal(t, []string{"path/a", "path/b"}, entry.Files)
		assert.Equal(t, "3072", entry.Metadata["memory_size"])
	})

	t.Run("bm25 prefix", func(t *testing.T) {
		fb := &datapb.FieldBinlog{
			FieldID: 200,
			Binlogs: []*datapb.Binlog{
				{LogPath: "path/c", MemorySize: 500},
			},
		}
		entry := FieldBinlogStatEntry("bm25", 200, fb)
		assert.Equal(t, "bm25.200", entry.Key)
		assert.Equal(t, []string{"path/c"}, entry.Files)
		assert.Equal(t, "500", entry.Metadata["memory_size"])
	})

	t.Run("zero memory size omits metadata", func(t *testing.T) {
		fb := &datapb.FieldBinlog{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{LogPath: "path/a", MemorySize: 0},
			},
		}
		entry := FieldBinlogStatEntry("bloom_filter", 100, fb)
		assert.Nil(t, entry.Metadata)
	})

	t.Run("empty binlogs", func(t *testing.T) {
		fb := &datapb.FieldBinlog{FieldID: 100}
		entry := FieldBinlogStatEntry("bloom_filter", 100, fb)
		assert.Equal(t, "bloom_filter.100", entry.Key)
		assert.Empty(t, entry.Files)
		assert.Nil(t, entry.Metadata)
	})
}

func TestTextIndexStatEntries(t *testing.T) {
	textStats := map[int64]*datapb.TextIndexStats{
		10: {
			FieldID:    10,
			Version:    5,
			BuildID:    42,
			Files:      []string{"file1", "file2"},
			LogSize:    1000,
			MemorySize: 2000,
		},
	}
	entries := TextIndexStatEntries(textStats, 3)
	assert.Len(t, entries, 1)
	e := entries[0]
	assert.Equal(t, "text_index.10", e.Key)
	assert.Equal(t, []string{"file1", "file2"}, e.Files)
	assert.Equal(t, "5", e.Metadata["version"])
	assert.Equal(t, "42", e.Metadata["build_id"])
	assert.Equal(t, "1000", e.Metadata["log_size"])
	assert.Equal(t, "2000", e.Metadata["memory_size"])
	assert.Equal(t, "3", e.Metadata["current_scalar_index_version"])
}

func TestJSONKeyStatEntries(t *testing.T) {
	jsonStats := map[int64]*datapb.JsonKeyStats{
		20: {
			FieldID:                20,
			Version:                1,
			BuildID:                99,
			Files:                  []string{"json1"},
			LogSize:                500,
			MemorySize:             800,
			JsonKeyStatsDataFormat: 2,
		},
	}
	entries := JSONKeyStatEntries(jsonStats)
	assert.Len(t, entries, 1)
	e := entries[0]
	assert.Equal(t, "json_stats.20", e.Key)
	assert.Equal(t, []string{"json1"}, e.Files)
	assert.Equal(t, "1", e.Metadata["version"])
	assert.Equal(t, "99", e.Metadata["build_id"])
	assert.Equal(t, "500", e.Metadata["log_size"])
	assert.Equal(t, "800", e.Metadata["memory_size"])
	assert.Equal(t, "2", e.Metadata["json_key_stats_data_format"])
}

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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToRelativePath(t *testing.T) {
	tests := []struct {
		name     string
		fullPath string
		basePath string
		rootPath string
		expected string
	}{
		{
			name:     "deltalog relative to insert_log",
			fullPath: "/data/delta_log/123/456/789/1",
			basePath: "/data/insert_log/123/456/789",
			rootPath: "/data",
			expected: "../../../../delta_log/123/456/789/1",
		},
		{
			name:     "same collection different segment",
			fullPath: "/milvus/delta_log/100/200/300/5",
			basePath: "/milvus/insert_log/100/200/300",
			rootPath: "/milvus",
			expected: "../../../../delta_log/100/200/300/5",
		},
		{
			name:     "path without rootPath prefix",
			fullPath: "/other/path/file",
			basePath: "/data/insert_log/123/456/789",
			rootPath: "/data",
			expected: "/other/path/file",
		},
		{
			name:     "empty rootPath",
			fullPath: "delta_log/123/456/789/1",
			basePath: "insert_log/123/456/789",
			rootPath: "",
			expected: "../../../../delta_log/123/456/789/1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toRelativePath(tt.fullPath, tt.basePath, tt.rootPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeltaLogEntry(t *testing.T) {
	entry := DeltaLogEntry{
		Path:       "/data/delta_log/123/456/789/1",
		NumEntries: 100,
	}

	assert.Equal(t, "/data/delta_log/123/456/789/1", entry.Path)
	assert.Equal(t, int64(100), entry.NumEntries)
}

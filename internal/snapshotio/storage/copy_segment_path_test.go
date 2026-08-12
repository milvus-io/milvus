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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
)

func TestRemapCopySegmentRootPath(t *testing.T) {
	tests := []struct {
		name       string
		sourcePath string
		sourceRoot string
		targetRoot string
		want       string
		wantErr    string
	}{
		{
			name:       "empty source root is identity (same-cluster restore)",
			sourcePath: "files/insert_log/1/2/3/4/5.log",
			sourceRoot: "",
			targetRoot: "target",
			want:       "files/insert_log/1/2/3/4/5.log",
		},
		{
			name:       "object-key root re-roots under target",
			sourcePath: "source-root/files/insert_log/1/2/3/4/5.log",
			sourceRoot: "source-root",
			targetRoot: "target-root",
			want:       "target-root/files/insert_log/1/2/3/4/5.log",
		},
		{
			name:       "s3 root with object-key path re-roots under target",
			sourcePath: "bundle/files/insert_log/1/2/3/4/5.log",
			sourceRoot: "s3://bucket/bundle/files",
			targetRoot: "files",
			want:       "files/insert_log/1/2/3/4/5.log",
		},
		{
			name:       "s3 root with full URI path re-roots under target",
			sourcePath: "s3://bucket/bundle/files/insert_log/1/2/3/4/5.log",
			sourceRoot: "s3://bucket/bundle/files",
			targetRoot: "files",
			want:       "files/insert_log/1/2/3/4/5.log",
		},
		{
			name:       "endpoint style root",
			sourcePath: "https://storage.example.com/bucket/src/insert_log/1/2/3",
			sourceRoot: "https://storage.example.com/bucket/src",
			targetRoot: "files",
			want:       "files/insert_log/1/2/3",
		},
		{
			name:       "path equal to root maps to target root",
			sourcePath: "s3://bucket/src",
			sourceRoot: "s3://bucket/src",
			targetRoot: "files",
			want:       "files",
		},
		{
			name:       "empty target root keeps the relative path",
			sourcePath: "s3://bucket/src/insert_log/1",
			sourceRoot: "s3://bucket/src",
			targetRoot: "",
			want:       "insert_log/1",
		},
		{
			name:       "path outside root is rejected",
			sourcePath: "s3://bucket/other/insert_log/1/2/3",
			sourceRoot: "s3://bucket/src",
			targetRoot: "files",
			wantErr:    "outside source root",
		},
		{
			name:       "different bucket is rejected",
			sourcePath: "s3://otherbucket/src/insert_log/1/2/3",
			sourceRoot: "s3://bucket/src",
			targetRoot: "files",
			wantErr:    "does not match source root",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RemapCopySegmentRootPath(tt.sourcePath, tt.sourceRoot, tt.targetRoot)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTransformCopySegmentPath(t *testing.T) {
	t.Run("local restore replaces IDs only", func(t *testing.T) {
		got, err := TransformCopySegmentPath("files/insert_log/111/222/333/100/1.log", "", "files", 444, 555, 666)
		require.NoError(t, err)
		assert.Equal(t, "files/insert_log/444/555/666/100/1.log", got)
	})
	t.Run("external restore remaps root then replaces IDs", func(t *testing.T) {
		got, err := TransformCopySegmentPath("s3://bucket/src/files/insert_log/111/222/333/100/1.log",
			"s3://bucket/src/files", "files", 444, 555, 666)
		require.NoError(t, err)
		assert.Equal(t, "files/insert_log/444/555/666/100/1.log", got)
	})
	t.Run("unrecognized path is rejected", func(t *testing.T) {
		_, err := TransformCopySegmentPath("external/100/segments/11", "", "files", 444, 555, 666)
		require.Error(t, err)
	})
	t.Run("root remap error propagates", func(t *testing.T) {
		_, err := TransformCopySegmentPath("s3://other/src/insert_log/1/2/3", "s3://bucket/src", "files", 4, 5, 6)
		require.Error(t, err)
	})
}

func TestTransformCopySegmentManifestPath(t *testing.T) {
	t.Run("external root", func(t *testing.T) {
		src := packed.MarshalManifestPath("s3://bucket/src/files/insert_log/111/222/333", 7)
		got, err := TransformCopySegmentManifestPath(src, "s3://bucket/src/files", "files", 444, 555, 666)
		require.NoError(t, err)
		assert.Equal(t, packed.MarshalManifestPath("files/insert_log/444/555/666", 7), got)
	})
	t.Run("local root keeps version", func(t *testing.T) {
		src := packed.MarshalManifestPath("files/insert_log/111/222/333", 3)
		got, err := TransformCopySegmentManifestPath(src, "", "files", 444, 555, 666)
		require.NoError(t, err)
		assert.Equal(t, packed.MarshalManifestPath("files/insert_log/444/555/666", 3), got)
	})
	t.Run("malformed manifest is rejected", func(t *testing.T) {
		_, err := TransformCopySegmentManifestPath("not-json", "", "files", 444, 555, 666)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal manifest path")
	})
	t.Run("unsupported base path is rejected", func(t *testing.T) {
		_, err := TransformCopySegmentManifestPath(packed.MarshalManifestPath("external/100/segments/11", 7), "", "files", 4, 5, 6)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to generate target base path")
	})
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// The shared acceptance rule accepts exactly the entries the previous
// load-metadata builders accepted, and those builders' output is unchanged.
func TestManifestIndexUsableMatchesLoadMetadata(t *testing.T) {
	const root = "/root"
	segment := &datapb.SegmentInfo{ID: 5, CollectionID: 1, PartitionID: 2}
	builder := func(layout indexpb.IndexStorePathVersion, buildID int64) string {
		return metautil.NewIndexPathBuilder(root, layout, 1, 2, 5, buildID, 3).BuildPrefix()
	}
	good := packed.ManifestIndexInfo{
		IndexID: 10, BuildID: 20, FieldID: 101, IndexName: "index", IndexType: "HNSW", IndexVersion: 3,
		NumRows: 1, SerializedSize: 2, MemSize: 3, Path: builder(0, 20), IndexFileKeys: []string{"0", "1"},
		Properties: map[string]string{"metric_type": "L2", "dim": "8"},
	}
	for name, mutate := range map[string]func(*packed.ManifestIndexInfo){
		"valid":                  func(*packed.ManifestIndexInfo) {},
		"collection_rooted":      func(e *packed.ManifestIndexInfo) { e.IndexStorePathVersion, e.Path = 1, builder(1, 20) },
		"segment_rooted":         func(e *packed.ManifestIndexInfo) { e.IndexStorePathVersion, e.Path = 2, builder(2, 20) },
		"unclean_segment_rooted": func(e *packed.ManifestIndexInfo) { e.IndexStorePathVersion, e.Path = 2, builder(2, 20)+"/" },
		"unclean_owned_path":     func(e *packed.ManifestIndexInfo) { e.Path += "/" },
		"foreign_build_path":     func(e *packed.ManifestIndexInfo) { e.Path = builder(0, 21) },
		"foreign_layout_path":    func(e *packed.ManifestIndexInfo) { e.Path = builder(1, 20) },
		"relative_path":          func(e *packed.ManifestIndexInfo) { e.Path = "index_files/20/3/2/5" },
		"empty_path":             func(e *packed.ManifestIndexInfo) { e.Path = "" },
		"no_name":                func(e *packed.ManifestIndexInfo) { e.IndexName = "" },
		"no_type":                func(e *packed.ManifestIndexInfo) { e.IndexType = "" },
		"unknown_layout":         func(e *packed.ManifestIndexInfo) { e.IndexStorePathVersion = -1 },
		"negative_rows":          func(e *packed.ManifestIndexInfo) { e.NumRows = -1 },
		"negative_size":          func(e *packed.ManifestIndexInfo) { e.SerializedSize = -1 },
		"negative_memory":        func(e *packed.ManifestIndexInfo) { e.MemSize = -1 },
		"no_files":               func(e *packed.ManifestIndexInfo) { e.IndexFileKeys = nil },
		"empty_file_key":         func(e *packed.ManifestIndexInfo) { e.IndexFileKeys = []string{"0", ""} },
		"absolute_file_key":      func(e *packed.ManifestIndexInfo) { e.IndexFileKeys = []string{"/0"} },
		"nested_file_key":        func(e *packed.ManifestIndexInfo) { e.IndexFileKeys = []string{"a/0"} },
		"dot_file_key":           func(e *packed.ManifestIndexInfo) { e.IndexFileKeys = []string{"."} },
		"dot_dot_file_key":       func(e *packed.ManifestIndexInfo) { e.IndexFileKeys = []string{".."} },
		"traversal_file_key":     func(e *packed.ManifestIndexInfo) { e.IndexFileKeys = []string{"../../meta"} },
		"trailing_slash_key":     func(e *packed.ManifestIndexInfo) { e.IndexFileKeys = []string{"0/"} },
		"index_type_in_property": func(e *packed.ManifestIndexInfo) { e.Properties = map[string]string{"index_type": "FLAT"} },
	} {
		entry := good
		entry.IndexFileKeys = slices.Clone(good.IndexFileKeys)
		mutate(&entry)

		wantInfo, wantOK := LegacyManifestIndexFilePathInfo(5, entry)
		require.Equal(t, wantOK, manifestIndexUsable(entry), name)
		require.Equal(t, wantOK, ValidateManifestIndexPublishable(5, entry) == nil, name)
		gotInfo, gotOK := manifestIndexFilePathInfo(5, entry)
		require.Equal(t, wantOK, gotOK, name)
		require.True(t, !wantOK || proto.Equal(wantInfo, gotInfo), name)

		wantInfo, wantOK = LegacyManifestIndexFilePathInfoForSegment(root, segment, entry)
		require.Equal(t, wantOK, manifestIndexUsableForSegment(root, segment, entry), name)
		gotInfo, gotOK = ManifestIndexFilePathInfoForSegment(root, segment, entry)
		require.Equal(t, wantOK, gotOK, name)
		require.True(t, !wantOK || proto.Equal(wantInfo, gotInfo), name)
	}
}

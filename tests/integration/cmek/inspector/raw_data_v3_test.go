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

package inspector

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestLocateRawDataV3UsesManifestObjects(t *testing.T) {
	groups := []packed.ColumnGroupEntry{
		{Columns: []string{"100", "101"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: "manifest/raw/scalars-1"}, {Path: "manifest/raw/scalars-2"}}},
		{Columns: []string{"102"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: "manifest/raw/vector"}}},
	}
	patch := mockey.Mock(packed.ReadManifestColumnGroups).Return(groups, nil).Build()
	defer patch.UnPatch()
	segment := &datapb.SegmentInfo{
		ID: 31, CollectionID: 11, PartitionID: 21, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath("segment", 1),
		Binlogs:      []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "obsolete/binlog"}}}},
	}
	objects, err := LocateRawDataV3(nil, []*datapb.SegmentInfo{segment})
	require.NoError(t, err)
	require.Len(t, objects, 3)
	require.Equal(t, "manifest/raw/scalars-1", objects[0].Path)
	require.Equal(t, "manifest/raw/scalars-2", objects[1].Path)
	require.Equal(t, "manifest/raw/vector", objects[2].Path)
	for _, object := range objects {
		require.EqualValues(t, 11, object.CollectionID)
		require.EqualValues(t, 31, object.SegmentID)
		require.Equal(t, storage.StorageV3, object.StorageVersion)
	}
}

func TestLocateRawDataV3RejectsIncompleteManifest(t *testing.T) {
	for _, test := range []struct {
		name   string
		groups []packed.ColumnGroupEntry
		want   string
	}{
		{name: "empty", want: "no manifest column groups"},
		{name: "non parquet", groups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "vortex", Files: []packed.ColumnGroupFileEntry{{Path: "data"}}}}, want: "nonempty parquet"},
		{name: "no files", groups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet"}}, want: "nonempty parquet"},
		{name: "empty path", groups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{}}}}, want: "empty raw-data object path"},
		{name: "duplicate", groups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: "data"}, {Path: "data"}}}}, want: "reported more than once"},
	} {
		t.Run(test.name, func(t *testing.T) {
			patch := mockey.Mock(packed.ReadManifestColumnGroups).Return(test.groups, nil).Build()
			defer patch.UnPatch()
			objects, err := LocateRawDataV3(nil, []*datapb.SegmentInfo{{ID: 31, StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath("segment", 1)}})
			require.ErrorContains(t, err, test.want)
			require.Nil(t, objects)
		})
	}
	_, err := LocateRawDataV3(nil, []*datapb.SegmentInfo{{ID: 31, StorageVersion: storage.StorageV2}})
	require.ErrorContains(t, err, "use StorageV3")
	_, err = LocateRawDataV3(nil, []*datapb.SegmentInfo{{ID: 31, StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath("segment", 0)}})
	require.ErrorContains(t, err, "no committed manifest")
}

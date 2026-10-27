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

package snapshot

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotstorage "github.com/milvus-io/milvus/pkg/v3/snapshotio"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSummarySnapshotCompatibility(t *testing.T) {
	empty := false
	segment := &datapb.SegmentDescription{StorageVersion: storage.StorageV3, ManifestPath: "manifest", ManifestHasIndex: &empty}
	data := &snapshotstorage.SnapshotData{Segments: []*datapb.SegmentDescription{segment}}
	require.NoError(t, validateSummarySnapshot(t.Context(), nil, data, nil))
	segment.StorageVersion = storage.StorageV2
	require.ErrorIs(t, validateSummarySnapshot(t.Context(), nil, data, nil), merr.ErrServiceUnimplemented)
	segment.StorageVersion = storage.StorageV3
	segment.ManifestHasIndex = nil
	require.ErrorIs(t, validateSummarySnapshot(t.Context(), nil, data, nil), merr.ErrServiceUnimplemented)
	nonempty := true
	segment.ManifestHasIndex = &nonempty
	require.NoError(t, validateSummarySnapshot(t.Context(), nil, data, nil), "the manifest may retain physical entries after DropIndex")
	data.Indexes = []*indexpb.IndexInfo{{IndexID: 1}}
	segment.IndexFiles = []*indexpb.IndexFilePathInfo{{IndexID: 1, IndexFilePaths: []string{"index"}}}
	require.ErrorIs(t, validateSummarySnapshot(t.Context(), nil, data, nil), merr.ErrServiceUnimplemented)
	segment.IndexFiles[0].IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	require.NoError(t, validateSummarySnapshot(t.Context(), nil, data, nil))
}

func TestSummarySnapshotChecksPinnedManifestIndexIdentities(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	base := filepath.Join(root, "insert_log", "1", "2", "3")
	dropped := packed.ManifestIndexInfo{FieldID: 100, IndexID: 10, BuildID: 20, IndexName: "dropped", IndexType: "FLAT", Path: "20/1"}
	live := packed.ManifestIndexInfo{FieldID: 101, IndexID: 11, BuildID: 21, IndexName: "live", IndexType: "FLAT", Path: "21/1"}
	oldPath, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, packed.ManifestEarliest), cfg, &packed.ManifestUpdates{
		Indexes: []packed.ManifestIndexInfo{dropped},
	})
	require.NoError(t, err)
	newPath, err := packed.CommitManifestUpdates(oldPath, cfg, &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{live}})
	require.NoError(t, err)
	present := true
	segment := &datapb.SegmentDescription{SegmentId: 3, StorageVersion: storage.StorageV3, ManifestPath: oldPath, ManifestHasIndex: &present}
	data := &snapshotstorage.SnapshotData{Segments: []*datapb.SegmentDescription{segment}, Indexes: []*indexpb.IndexInfo{{IndexID: 11}}}
	mt := &meta.Meta{}
	require.NoError(t, validateSummarySnapshot(ctx, mt, data, cfg), "the live definition has no artifact in the pinned revision")
	segment.ManifestPath = newPath
	err = validateSummarySnapshot(ctx, mt, data, cfg)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.ErrorContains(t, err, "segment 3 index 11")
}

func TestSummarySnapshotAllowsL0Deletes(t *testing.T) {
	empty := false
	l0 := &datapb.SegmentDescription{
		SegmentLevel:   datapb.SegmentLevel_L0,
		StorageVersion: storage.StorageV2,
		Deltalogs:      []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "delete-log"}}}},
	}
	data := &snapshotstorage.SnapshotData{
		Segments: []*datapb.SegmentDescription{
			{StorageVersion: storage.StorageV3, ManifestPath: "manifest", ManifestHasIndex: &empty},
			l0,
		},
	}
	require.NoError(t, validateSummarySnapshot(t.Context(), nil, data, nil))
	l0.IndexFiles = []*indexpb.IndexFilePathInfo{{IndexFilePaths: []string{"index"}}}
	require.ErrorIs(t, validateSummarySnapshot(t.Context(), nil, data, nil), merr.ErrServiceUnimplemented)
}

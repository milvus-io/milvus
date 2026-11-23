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
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSummarySnapshotResolvesPinnedIndexesForRestoreAndExport(t *testing.T) {
	ctx := t.Context()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	config := &indexpb.StorageConfig{StorageType: "local", RootPath: cm.RootPath()}
	defer mockey.Mock(meta.CreateStorageConfig).Return(config).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestLobFiles).Return(nil, nil).Build().UnPatch()
	sm := CreateTestSnapshotMeta(t)
	t.Cleanup(sm.LoaderCancel)
	sm.catalog = kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	sm.chunkManager = cm
	sm.reader = snapshotstorage.NewSnapshotReader(cm)
	sm.writer = snapshotstorage.NewSnapshotWriter(cm)
	limit := &paramtable.Get().DataCoordCfg.SegmentIndexManifestLoadConcurrency
	previousLimit := limit.SwapTempValue("1")
	defer limit.SwapTempValue(previousLimit)
	mt := &meta.Meta{}
	sm.SetManifestIndexReader(mt.ReadManifestIndexesWithIO)

	base := filepath.Join(cm.RootPath(), "insert_log", "1", "2", "3")
	version := indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	selected := packed.ManifestIndexInfo{
		ColumnName: "100", FieldID: 100, IndexID: 10, BuildID: 20, IndexName: "vector",
		IndexType: "FLAT", IndexVersion: 1, Path: "20/1", IndexFileKeys: []string{"index.bin"}, IndexStorePathVersion: version,
	}
	unselected := selected
	unselected.IndexID, unselected.BuildID, unselected.Path = 12, 22, "22/1"
	pinned, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, packed.ManifestEarliest), config,
		&packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{selected, unselected}})
	require.NoError(t, err)
	buildPath := filepath.Join(base, "_index", selected.Path, "index.bin")
	unselectedPath := filepath.Join(base, "_index", unselected.Path, "index.bin")
	require.NoError(t, cm.Write(ctx, buildPath, []byte("selected index")))
	require.NoError(t, cm.Write(ctx, unselectedPath, []byte("unselected index")))

	data := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 1, Name: "pinned"},
		Collection:   &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "source"}},
		Indexes:      []*indexpb.IndexInfo{{IndexID: 10, FieldID: 100, IndexName: "vector"}, {IndexID: 11, FieldID: 101, IndexName: "empty"}},
		Segments: []*datapb.SegmentDescription{{
			SegmentId: 3, PartitionId: 2, StorageVersion: storage.StorageV3, ManifestPath: pinned, ManifestHasIndex: proto.Bool(true),
			IndexFiles: []*indexpb.IndexFilePathInfo{{SegmentID: 3, FieldID: 101, IndexID: 11, IndexName: "empty", BuildID: 21}},
		}},
		IndexFilesFromManifest: true,
		BuildIDs:               []int64{20},
	}
	require.NoError(t, sm.SaveSnapshot(ctx, data))
	require.True(t, sm.IsBuildIDGCBlocked(1, 20))
	require.False(t, sm.IsBuildIDGCBlocked(1, 22))
	metadataOnly, err := sm.ReadSnapshotData(ctx, 1, "pinned", false)
	require.NoError(t, err)
	require.Equal(t, []int64{20, 21}, metadataOnly.BuildIDs)
	require.Empty(t, metadataOnly.Segments)
	sm.SnapshotID2RefIndex.Insert(1, NewSnapshotRefIndex())
	require.True(t, sm.loadUnloadedRefIndexes())
	sm.RebuildAllSegmentProtection()
	require.True(t, sm.IsBuildIDGCBlocked(1, 20))
	require.False(t, sm.IsBuildIDGCBlocked(1, 22))
	stored, err := sm.reader.ReadSnapshot(ctx, data.SnapshotInfo.GetS3Location(), true)
	require.NoError(t, err)
	require.True(t, stored.IndexFilesFromManifest)
	require.Len(t, stored.Segments[0].GetIndexFiles(), 1)

	// The revision currently at the same base no longer contains the selected build.
	_, err = packed.CommitManifestUpdates(pinned, config, &packed.ManifestUpdates{
		DropIndexes: []packed.DropIndexEntry{{IndexID: selected.IndexID, ExpectedBuildID: selected.BuildID}},
	})
	require.NoError(t, err)

	t.Run("shared budget", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		otherRead := make(chan error, 1)
		reader := mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(_ context.Context, _ *packed.ManifestIOContext, path string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			if path != "other reader" {
				return nil, merr.ErrServiceUnavailable
			}
			close(entered)
			<-release
			return nil, nil
		}).Build()
		defer reader.UnPatch()
		go func() {
			_, err := mt.ReadManifestIndexesWithIO(t.Context(), nil, "other reader", config)
			otherRead <- err
		}()
		defer func() {
			close(release)
			require.NoError(t, <-otherRead)
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the other reader did not acquire the shared permit")
		}

		hydrating := make(chan struct{})
		sm.SetManifestIndexReader(func(ctx context.Context, io *packed.ManifestIOContext, path string, cfg *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			close(hydrating)
			return mt.ReadManifestIndexesWithIO(ctx, io, path, cfg)
		})
		defer sm.SetManifestIndexReader(mt.ReadManifestIndexesWithIO)
		waitCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := sm.ReadSnapshotData(waitCtx, 1, "pinned", true)
			done <- err
		}()
		select {
		case <-hydrating:
		case err := <-done:
			t.Fatalf("snapshot hydration bypassed the shared reader: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("snapshot hydration did not reach the shared reader")
		}
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("snapshot hydration did not cancel while waiting for the shared permit")
		}
		require.Equal(t, 1, reader.Times(), "snapshot hydration must wait for the same permit")
	})

	loaded, err := sm.ReadSnapshotData(ctx, 1, "pinned", true)
	require.NoError(t, err)
	require.Len(t, loaded.Segments[0].GetIndexFiles(), 2)
	require.Empty(t, loaded.Segments[0].GetIndexFiles()[0].GetIndexFilePaths())
	require.Equal(t, []string{buildPath}, loaded.Segments[0].GetIndexFiles()[1].GetIndexFilePaths())

	copier := newSnapshotExporterCopierMock(t, func(ctx context.Context, _, source, _, target string) error {
		return cm.Copy(ctx, source, target)
	})
	targetRoot := filepath.Join(cm.RootPath(), "exported")
	uri, err := exportSnapshot(ctx, cm, cm, copier, "", "", loaded, targetRoot)
	require.NoError(t, err)
	bundle, err := sm.reader.ReadSnapshot(ctx, uri, true)
	require.NoError(t, err)
	require.False(t, bundle.IndexFilesFromManifest)
	require.Len(t, bundle.Segments[0].GetIndexFiles(), 2)
	exportedPath := bundle.Segments[0].GetIndexFiles()[1].GetIndexFilePaths()[0]
	bytes, err := cm.Read(ctx, exportedPath)
	require.NoError(t, err)
	require.Equal(t, "selected index", string(bytes))
	exportedBase, _, err := packed.UnmarshalManifestPath(bundle.Segments[0].GetManifestPath())
	require.NoError(t, err)
	exists, err := cm.Exist(ctx, filepath.Join(exportedBase, "_index", unselected.Path, "index.bin"))
	require.NoError(t, err)
	require.False(t, exists)

	// A selected ID absent from the pinned revision fails before publishing any
	// resolved entries to the caller.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, snapshotstorage.ResolveSnapshotIndexFiles(canceled, stored, config, mt.ReadManifestIndexesWithIO), context.Canceled)
	require.Len(t, stored.Segments[0].GetIndexFiles(), 1)
	stored.BuildIDs = append(stored.BuildIDs, 23)
	err = snapshotstorage.ResolveSnapshotIndexFiles(ctx, stored, config, mt.ReadManifestIndexesWithIO)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "snapshot build 23 is missing")
	require.Len(t, stored.Segments[0].GetIndexFiles(), 1)
}

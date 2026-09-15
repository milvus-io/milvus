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

package datacoord

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newLocalManifestSnapshot(t *testing.T, layout datapb.SnapshotLayout, manifestPath string) (*snapshotMeta, *datapb.SnapshotInfo, map[string][]byte) {
	t.Helper()
	paramtable.Init()
	oldPrefix := Params.MinioCfg.RootPath.GetValue()
	require.NoError(t, Params.Save(Params.MinioCfg.RootPath.Key, "files"))
	t.Cleanup(func() { require.NoError(t, Params.Save(Params.MinioCfg.RootPath.Key, oldPrefix)) })

	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	sm := createTestSnapshotMeta(t)
	t.Cleanup(sm.loaderCancel)
	// A read must not persist to the metadata catalog.
	sm.catalog = nil
	sm.chunkManager = cm
	sm.reader = snapshotstorage.NewSnapshotReader(cm)
	sm.writer = snapshotstorage.NewSnapshotWriter(cm)
	info := &datapb.SnapshotInfo{
		Id:           10,
		CollectionId: 100,
		Name:         "legacy-local",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
	}
	manifestDir, metadataPath := snapshotstorage.GetSnapshotPaths(cm.RootPath(), info.GetCollectionId(), info.GetId())
	info.S3Location = metadataPath
	snapshot := &snapshotstorage.SnapshotData{
		SnapshotInfo: info,
		Collection:   &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "local"}},
		Segments: []*datapb.SegmentDescription{{
			SegmentId:      1001,
			PartitionId:    1,
			StorageVersion: storage.StorageV3,
			ManifestPath:   manifestPath,
		}},
		SegmentIDs: []int64{1001},
	}
	_, _, err := sm.writer.SaveToRootWithSize(ctx, snapshot, cm.RootPath(), layout)
	require.NoError(t, err)
	sm.snapshotID2Info.Insert(info.GetId(), info)
	sm.addToSecondaryIndexes(info)

	stored := make(map[string][]byte)
	for _, file := range []string{metadataPath, snapshotstorage.GetSegmentManifestPath(manifestDir, 1001)} {
		data, err := cm.Read(ctx, file)
		require.NoError(t, err)
		stored[file] = data
	}
	return sm, info, stored
}

func assertLocalSnapshotFilesUnchanged(t *testing.T, sm *snapshotMeta, stored map[string][]byte) {
	t.Helper()
	for file, original := range stored {
		data, err := sm.chunkManager.Read(context.Background(), file)
		require.NoError(t, err)
		assert.Equal(t, original, data, file)
	}
}

func TestReadSnapshotDataNormalizesLegacyLocalManifestWithoutWrites(t *testing.T) {
	ctx := context.Background()
	legacy := packed.MarshalManifestPath("files/insert_log/100/1/1001", 7)
	sm, info, stored := newLocalManifestSnapshot(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, legacy)
	want := packed.MarshalManifestPath(path.Join(sm.chunkManager.RootPath(), "files/insert_log/100/1/1001"), 7)

	var firstFingerprint string
	for read := range 2 {
		loaded, err := sm.ReadSnapshotData(ctx, info.GetCollectionId(), info.GetName(), true)
		require.NoError(t, err)
		require.Len(t, loaded.Segments, 1)
		assert.Equal(t, want, loaded.Segments[0].GetManifestPath())
		assertLocalSnapshotFilesUnchanged(t, sm, stored)
		fingerprint, err := snapshotstorage.SnapshotFingerprint(loaded)
		require.NoError(t, err)
		if read == 0 {
			firstFingerprint = fingerprint
		} else {
			assert.Equal(t, firstFingerprint, fingerprint)
		}
		// Neither a prior read nor an in-memory consumer can alter the source.
		loaded.Segments[0].ManifestPath = "modified-by-consumer"
	}
	storedSnapshot, err := sm.reader.ReadSnapshot(ctx, info.GetS3Location(), true)
	require.NoError(t, err)
	assert.Equal(t, legacy, storedSnapshot.Segments[0].GetManifestPath())
	metadataOnly, err := sm.ReadSnapshotData(ctx, info.GetCollectionId(), info.GetName(), false)
	require.NoError(t, err)
	assert.Empty(t, metadataOnly.Segments)
	assertLocalSnapshotFilesUnchanged(t, sm, stored)
}

func TestReadSnapshotDataRejectsMismatchedLegacyManifestIdentity(t *testing.T) {
	legacy := packed.MarshalManifestPath("files/insert_log/100/1/9999", 7)
	sm, info, stored := newLocalManifestSnapshot(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, legacy)
	loaded, err := sm.ReadSnapshotData(context.Background(), info.GetCollectionId(), info.GetName(), true)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.Nil(t, loaded)
	assertLocalSnapshotFilesUnchanged(t, sm, stored)
}

func TestReadSnapshotDataPreservesOtherManifestNamespaces(t *testing.T) {
	ctx := context.Background()
	legacy := packed.MarshalManifestPath("files/insert_log/100/1/1001", 7)
	t.Run("self-contained", func(t *testing.T) {
		sm, info, stored := newLocalManifestSnapshot(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, legacy)
		snapshot, err := sm.reader.ReadSnapshot(ctx, info.GetS3Location(), true)
		require.NoError(t, err)
		bundleManifest := packed.MarshalManifestPath(path.Join(sm.chunkManager.RootPath(),
			snapshotstorage.ExportedSnapshotFilesPath, "insert_log/100/1/1001"), 7)
		snapshot.Segments[0].ManifestPath = bundleManifest
		_, _, err = sm.writer.SaveToRootWithSize(ctx, snapshot, sm.chunkManager.RootPath(), datapb.SnapshotLayout_SnapshotLayoutSelfContained)
		require.NoError(t, err)
		for file := range stored {
			stored[file], err = sm.chunkManager.Read(ctx, file)
			require.NoError(t, err)
		}
		loaded, err := sm.ReadSnapshotData(ctx, info.GetCollectionId(), info.GetName(), true)
		require.NoError(t, err)
		assert.Equal(t, bundleManifest, loaded.Segments[0].GetManifestPath())
		assertLocalSnapshotFilesUnchanged(t, sm, stored)
	})
	t.Run("self-contained-rejects-outside-bundle", func(t *testing.T) {
		sm, info, stored := newLocalManifestSnapshot(t, datapb.SnapshotLayout_SnapshotLayoutSelfContained, legacy)
		// A self-contained bundle's paths are governed by the bundle root,
		// not by the instance's legacy prefix compatibility rule.
		_, directErr := sm.reader.ReadSnapshot(ctx, info.GetS3Location(), true)
		require.ErrorIs(t, directErr, merr.ErrDataIntegrity)
		loaded, err := sm.ReadSnapshotData(ctx, info.GetCollectionId(), info.GetName(), true)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		assert.Equal(t, directErr.Error(), err.Error())
		assert.Nil(t, loaded)
		assertLocalSnapshotFilesUnchanged(t, sm, stored)
	})
	t.Run("absolute", func(t *testing.T) {
		absolute := packed.MarshalManifestPath("/existing/insert_log/100/1/1001", 9)
		sm, info, stored := newLocalManifestSnapshot(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, absolute)
		loaded, err := sm.ReadSnapshotData(ctx, info.GetCollectionId(), info.GetName(), true)
		require.NoError(t, err)
		assert.Equal(t, absolute, loaded.Segments[0].GetManifestPath())
		assertLocalSnapshotFilesUnchanged(t, sm, stored)
	})
	t.Run("foreign", func(t *testing.T) {
		sm, info, stored := newLocalManifestSnapshot(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, legacy)
		loaded, err := sm.ReadExternalSnapshotDataWithChunkManager(ctx, sm.chunkManager, info.GetS3Location(), true)
		require.NoError(t, err)
		assert.Equal(t, legacy, loaded.Segments[0].GetManifestPath())
		assertLocalSnapshotFilesUnchanged(t, sm, stored)
	})
	t.Run("remote", func(t *testing.T) {
		sm, info, stored := newLocalManifestSnapshot(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, legacy)
		// Keep real snapshot reads on disk while exercising the non-local backend gate.
		sm.chunkManager = &snapshotManifestNonLocalChunkManager{ChunkManager: sm.chunkManager}
		loaded, err := sm.ReadSnapshotData(ctx, info.GetCollectionId(), info.GetName(), true)
		require.NoError(t, err)
		assert.Equal(t, legacy, loaded.Segments[0].GetManifestPath())
		assertLocalSnapshotFilesUnchanged(t, sm, stored)
	})
}

type snapshotManifestNonLocalChunkManager struct {
	storage.ChunkManager
}

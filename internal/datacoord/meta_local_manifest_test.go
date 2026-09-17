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

package datacoord

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestNormalizeLocalManifestPath(t *testing.T) {
	const root = "/var/lib/milvus/data"
	const suffix = "insert_log/1/2/3"
	const version = int64(17)
	for _, tc := range []struct {
		name   string
		base   string
		prefix string
	}{
		{"legacy prefix", "files/" + suffix, "files"},
		{"nested prefix", "legacy/files/" + suffix, "legacy/files"},
		{"empty prefix", suffix, ""},
		{"bucket root prefix", suffix, "."},
		{"empty component", "files//" + suffix, "files"},
		{"trailing separator", "files/" + suffix + "/", "files"},
		{"dot component", "files/./" + suffix, "files"},
		{"parent component within prefix", "files/tmp/../" + suffix, "files"},
		{"dot prefix component", "files/" + suffix, "./files"},
		{"empty prefix component", "legacy/files/" + suffix, "legacy//files"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeLocalManifestPath(packed.MarshalManifestPath(tc.base, version), root, tc.prefix, 1, 2, 3)
			require.NoError(t, err)
			assert.Equal(t, packed.MarshalManifestPath(filepath.Join(root, tc.base), version), got)
			// A persisted absolute path needs no further transformation.
			again, err := normalizeLocalManifestPath(got, root, tc.prefix, 1, 2, 3)
			require.NoError(t, err)
			assert.Equal(t, got, again)
		})
	}

	t.Run("absolute manifest preserved byte for byte", func(t *testing.T) {
		manifest := ` { "ver": 23, "base_path": "/var/lib/milvus/data/insert_log/1/2/3" } `
		got, err := normalizeLocalManifestPath(manifest, root, "files", 1, 2, 3)
		require.NoError(t, err)
		assert.Equal(t, manifest, got)
	})
	t.Run("no manifest yet", func(t *testing.T) {
		got, err := normalizeLocalManifestPath("", root, "files", 1, 2, 3)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
	t.Run("filesystem root", func(t *testing.T) {
		got, err := normalizeLocalManifestPath(packed.MarshalManifestPath("files/"+suffix, version), "/", "files", 1, 2, 3)
		require.NoError(t, err)
		assert.Equal(t, packed.MarshalManifestPath("/files/"+suffix, version), got)
	})
	t.Run("relative storage root rejected", func(t *testing.T) {
		got, err := normalizeLocalManifestPath(packed.MarshalManifestPath("files/"+suffix, version), "data", "files", 1, 2, 3)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Empty(t, got)
	})

	for _, tc := range []struct {
		name   string
		base   string
		prefix string
	}{
		{"empty base", "", "files"},
		{"dot base", ".", "files"},
		{"parent base", "..", "files"},
		{"parent component", "files/../" + suffix, "files"},
		{"URI base", "s3://files/" + suffix, "files"},
		{"missing configured prefix", suffix, "files"},
		{"wrong prefix", "other/" + suffix, "files"},
		{"wrong collection", "files/insert_log/9/2/3", "files"},
		{"wrong partition", "files/insert_log/1/9/3", "files"},
		{"wrong segment", "files/insert_log/1/2/9", "files"},
		{"noncanonical ID", "files/insert_log/1/2/03", "files"},
		{"unexpected suffix", "files/" + suffix + "/_manifest", "files"},
		{"absolute prefix", "files/" + suffix, "/files"},
		{"URI prefix", "files/" + suffix, "s3://files"},
		{"parent prefix", "files/" + suffix, "../files"},
		{"outside storage root", "../files/" + suffix, "../files"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeLocalManifestPath(packed.MarshalManifestPath(tc.base, version), root, tc.prefix, 1, 2, 3)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			assert.Empty(t, got)
		})
	}
	t.Run("malformed JSON", func(t *testing.T) {
		got, err := normalizeLocalManifestPath("not-json", root, "files", 1, 2, 3)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		assert.Empty(t, got)
	})
}

func setLocalManifestLegacyPrefix(t *testing.T, prefix string) {
	t.Helper()
	paramtable.Init()
	params := paramtable.Get()
	key := params.MinioCfg.RootPath.Key
	original := params.MinioCfg.RootPath.GetValue()
	require.NoError(t, params.Save(key, prefix))
	t.Cleanup(func() { require.NoError(t, params.Save(key, original)) })
}

func newLocalManifestTestMeta(catalog *catalogmocks.DataCoordCatalog, cm storage.ChunkManager) *meta {
	return &meta{
		ctx:          context.Background(),
		catalog:      catalog,
		chunkManager: cm,
		segments:     NewSegmentsInfo(),
		channelCPs:   newChannelCps(),
	}
}

func assertLocalManifestCatalogReadOnly(t *testing.T, catalog *catalogmocks.DataCoordCatalog) {
	t.Helper()
	catalog.AssertNotCalled(t, "AlterSegments", mock.Anything, mock.Anything)
	for _, call := range catalog.Calls {
		assert.Contains(t, []string{"ListSegments", "ListChannelCheckpoint"}, call.Method,
			"loading local manifests must not call a catalog mutation")
	}
}

func TestReloadLocalManifestPathsReadOnlyAndLazyPersistence(t *testing.T) {
	setLocalManifestLegacyPrefix(t, "files")
	ctx := context.Background()
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	legacyBase := "files/insert_log/1/2/3"
	legacyFile := filepath.Join(root, legacyBase, "_data", "test-marker.parquet")
	require.NoError(t, os.MkdirAll(filepath.Dir(legacyFile), 0o755))
	require.NoError(t, os.WriteFile(legacyFile, []byte("legacy-segment-data"), 0o600))
	// No layout migration: the marker exists only under the legacy prefix.
	require.NoDirExists(t, filepath.Join(root, "insert_log"))

	raw := &datapb.SegmentInfo{
		ID:             3,
		CollectionID:   1,
		PartitionID:    2,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(legacyBase, 17),
	}
	original := proto.Clone(raw).(*datapb.SegmentInfo)
	stored := raw
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegments(mock.Anything, int64(1)).RunAndReturn(func(context.Context, int64) ([]*datapb.SegmentInfo, error) {
		return []*datapb.SegmentInfo{stored}, nil
	}).Times(3)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil).Times(3)
	mt := newLocalManifestTestMeta(catalog, cm)
	expected := packed.MarshalManifestPath(filepath.ToSlash(filepath.Join(root, legacyBase)), 17)

	// The same legacy catalog value is resolved on every reload without writes.
	// Stats is deliberately nil: NewSegmentInfo fills it, so the compatibility
	// layer must clone before constructing the in-memory segment as well.
	for i := 0; i < 2; i++ {
		require.NoError(t, mt.reloadFromKV(ctx, []int64{1}))
		loaded := mt.segments.GetSegment(3)
		require.NotNil(t, loaded)
		assert.Equal(t, expected, loaded.GetManifestPath())
		assert.NotSame(t, raw, loaded.SegmentInfo)
		assert.True(t, proto.Equal(original, raw), "reload must not mutate the catalog-owned proto")
		assertLocalManifestCatalogReadOnly(t, catalog)

		base, version, err := packed.UnmarshalManifestPath(loaded.GetManifestPath())
		require.NoError(t, err)
		assert.Equal(t, int64(17), version)
		contents, err := cm.Read(ctx, filepath.Join(base, "_data", "test-marker.parquet"))
		require.NoError(t, err)
		assert.Equal(t, "legacy-segment-data", string(contents))
		assert.NoDirExists(t, filepath.Join(root, "insert_log"), "reload must not copy legacy files into the new layout")
	}

	// A failed ordinary write does not undo the resolved in-memory view or
	// commit the attempted state change.
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(merr.ErrIoTooManyRequests).Once()
	require.ErrorIs(t, mt.UpdateSegmentsInfo(ctx, UpdateStatusOperator(3, commonpb.SegmentState_Dropped)), merr.ErrIoTooManyRequests)
	assert.Equal(t, expected, mt.segments.GetSegment(3).GetManifestPath())
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(3).GetState())
	assert.True(t, proto.Equal(original, raw))
	assert.Same(t, raw, stored)

	// A successful normal segment update, not startup, persists the resolved path.
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, segments []*datapb.SegmentInfo, increments ...metastore.BinlogsIncrement) error {
		require.Len(t, segments, 1)
		assert.Empty(t, increments)
		assert.Equal(t, expected, segments[0].GetManifestPath())
		assert.Equal(t, commonpb.SegmentState_Dropped, segments[0].GetState())
		stored = proto.Clone(segments[0]).(*datapb.SegmentInfo)
		return nil
	}).Once()
	require.NoError(t, mt.UpdateSegmentsInfo(ctx, UpdateStatusOperator(3, commonpb.SegmentState_Dropped)))
	assert.True(t, proto.Equal(original, raw))
	assert.Equal(t, expected, stored.GetManifestPath())
	require.NoError(t, mt.reloadFromKV(ctx, []int64{1}))
	assert.Equal(t, expected, mt.segments.GetSegment(3).GetManifestPath())
	catalog.AssertNumberOfCalls(t, "AlterSegments", 2)
	legacyContents, err := os.ReadFile(legacyFile)
	require.NoError(t, err)
	assert.Equal(t, "legacy-segment-data", string(legacyContents))
	assert.NoDirExists(t, filepath.Join(root, "insert_log"))
}

func TestReloadLocalManifestPathsRejectInvalid(t *testing.T) {
	setLocalManifestLegacyPrefix(t, "files")
	for _, tc := range []struct {
		name     string
		manifest string
	}{
		{"malformed JSON", "not-json"},
		{"wrong segment", packed.MarshalManifestPath("files/insert_log/1/2/99", 1)},
		{"traversal", packed.MarshalManifestPath("files/../insert_log/1/2/3", 1)},
		{"missing configured prefix", packed.MarshalManifestPath("insert_log/1/2/3", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := &datapb.SegmentInfo{
				ID: 3, CollectionID: 1, PartitionID: 2,
				State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
				ManifestPath: tc.manifest,
			}
			original := proto.Clone(raw)
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ListSegments(mock.Anything, int64(1)).Return([]*datapb.SegmentInfo{raw}, nil).Once()
			catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil).Maybe()
			mt := newLocalManifestTestMeta(catalog, storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())))
			require.ErrorIs(t, mt.reloadFromKV(context.Background(), []int64{1}), merr.ErrDataIntegrity)
			assert.Nil(t, mt.segments.GetSegment(3), "invalid metadata must never enter the segment cache")
			assert.True(t, proto.Equal(original, raw))
			assertLocalManifestCatalogReadOnly(t, catalog)
		})
	}
}

func TestReloadLocalManifestPathsBypass(t *testing.T) {
	setLocalManifestLegacyPrefix(t, "files")
	for _, tc := range []struct {
		name     string
		remote   bool
		version  int64
		manifest string
	}{
		{"remote V3 remains relative", true, storage.StorageV3, packed.MarshalManifestPath("files/insert_log/1/2/3", 7)},
		{"local V2 does not parse manifest", false, storage.StorageV2, "not-a-v3-manifest"},
		{"local V3 absolute unchanged", false, storage.StorageV3, ` { "ver": 9, "base_path": "/var/lib/milvus/data/insert_log/1/2/3" } `},
		{"local V3 no manifest yet", false, storage.StorageV3, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := &datapb.SegmentInfo{
				ID: 3, CollectionID: 1, PartitionID: 2,
				State: commonpb.SegmentState_Flushed, StorageVersion: tc.version,
				ManifestPath: tc.manifest, Stats: &datapb.Statistics{},
			}
			original := proto.Clone(raw)
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ListSegments(mock.Anything, int64(1)).Return([]*datapb.SegmentInfo{raw}, nil).Once()
			catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil).Once()
			var cm storage.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			if tc.remote {
				cm = mocks.NewChunkManager(t)
			}
			mt := newLocalManifestTestMeta(catalog, cm)
			require.NoError(t, mt.reloadFromKV(context.Background(), []int64{1}))
			loaded := mt.segments.GetSegment(3)
			require.NotNil(t, loaded)
			assert.Equal(t, tc.manifest, loaded.GetManifestPath())
			assert.True(t, proto.Equal(original, raw))
			assertLocalManifestCatalogReadOnly(t, catalog)
		})
	}
}

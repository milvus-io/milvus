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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storage/localmigrate"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type legacyLocalPackedFixture struct {
	root             string
	legacyRoot       string
	physicalManifest string
	segment          *datapb.SegmentInfo
	cm               *storage.LocalChunkManager
	cfg              *indexpb.StorageConfig
	pks              []int64
	texts            []string
}

// Write a real manifest at the physical location used by the old rooted
// filesystem. Its writer-generated internal references are serialized relative
// to the segment/partition, while the catalog retains the old relative base.
// Compatibility must resolve that base to this original location without copying.
// This does not fabricate a parquet, manifest, stats blob, or LOB marker file.
func newLegacyLocalPackedFixture(t *testing.T) *legacyLocalPackedFixture {
	t.Helper()
	return newLocalPackedFixture(t, false)
}

// The displaced variant models complete keys passed to the old double-rooted
// filesystem. Both variants use real writers and unchanged relative manifest
// references; only the physical namespace and catalog base differ.
func newLocalPackedFixture(t *testing.T, displaced bool) *legacyLocalPackedFixture {
	t.Helper()
	setLocalManifestLegacyPrefix(t, "files")
	root := t.TempDir()
	params := paramtable.Get()
	for _, setting := range []struct{ key, value, original string }{
		{params.CommonCfg.StorageType.Key, "local", params.CommonCfg.StorageType.GetValue()},
		{params.LocalStorageCfg.Path.Key, root, params.LocalStorageCfg.Path.GetValue()},
	} {
		require.NoError(t, params.Save(setting.key, setting.value))
		t.Cleanup(func() { require.NoError(t, params.Save(setting.key, setting.original)) })
	}
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	legacyRoot := filepath.Join(root, "files")
	catalogBase := "files/insert_log/1/2/3"
	if displaced {
		legacyRoot = filepath.Join(root, strings.TrimPrefix(root, string(filepath.Separator)))
		catalogBase = filepath.Join(root, "insert_log/1/2/3")
	}
	base := filepath.Join(legacyRoot, "insert_log/1/2/3")
	pks := []int64{11, 22, 33}
	texts := []string{strings.Repeat("first lob ", 32), strings.Repeat("second lob ", 32), strings.Repeat("third lob ", 32)}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "100", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"100"})},
		{Name: "101", Type: arrow.BinaryTypes.String, Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"101"})},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues(pks, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(texts, nil)
	record := builder.NewRecord()
	defer record.Release()
	writer, err := packed.NewFFISegmentWriter(schema, &packed.SegmentWriterConfig{
		SegmentPath:  base,
		WriterFormat: "parquet",
		TextColumns: []packed.TextColumnConfig{{
			FieldID: 101, LobBasePath: filepath.Join(legacyRoot, "insert_log/1/2/lobs/101"),
			InlineThreshold: 1, MaxLobFileBytes: 1 << 20, FlushThresholdBytes: 1 << 20,
		}},
	}, cfg)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			out, closeErr := writer.Close()
			if out != nil {
				out.Destroy()
			}
			require.NoError(t, closeErr)
		}
	})
	require.NoError(t, writer.Write(record))
	output, err := writer.Close()
	closed = true
	require.NoError(t, err)
	defer output.Destroy()
	statsWriter := &storage.StatsWriter{}
	require.NoError(t, statsWriter.GenerateByData(100, schemapb.DataType_Int64, &storage.Int64FieldData{Data: pks}))
	statsPath := filepath.Join(base, "_stats/bloom_filter.100/42")
	require.NoError(t, packed.WriteFile(cfg, statsPath, statsWriter.GetBuffer()))
	physicalManifest, err := packed.CommitManifestUpdates(base, packed.ManifestEarliest, cfg, &packed.ManifestUpdates{
		NewFiles: output,
		Stats:    []packed.StatEntry{{Key: "bloom_filter.100", Files: []string{statsPath}, Metadata: map[string]string{"memory_size": "24"}}},
	})
	require.NoError(t, err)
	_, version, err := packed.UnmarshalManifestPath(physicalManifest)
	require.NoError(t, err)
	return &legacyLocalPackedFixture{
		root: root, legacyRoot: legacyRoot, physicalManifest: physicalManifest,
		segment: &datapb.SegmentInfo{
			ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3, NumOfRows: int64(len(pks)),
			ManifestPath: packed.MarshalManifestPath(catalogBase, version),
		},
		cm: storage.NewLocalChunkManager(objectstorage.RootPath(root)), cfg: cfg, pks: pks, texts: texts,
	}
}

func (f *legacyLocalPackedFixture) assertNoCanonicalLayout(t *testing.T) {
	t.Helper()
	_, err := os.Stat(filepath.Join(f.root, "insert_log"))
	require.ErrorIs(t, err, os.ErrNotExist, "legacy V3 compatibility must not create a canonical insert_log tree")
}

// Read the old segment's real data, stats and LOB payload in place. This does
// not exercise compaction between the legacy and canonical storage namespaces.
func (f *legacyLocalPackedFixture) assertReadable(t *testing.T, manifest, expectedRoot string) {
	t.Helper()
	base, _, err := packed.UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	fragments, err := packed.ReadFragmentsFromManifest(manifest, f.cfg, []string{"100"})
	require.NoError(t, err)
	require.NotEmpty(t, fragments)
	for _, fragment := range fragments {
		require.True(t, strings.HasPrefix(fragment.FilePath, filepath.Join(base, "_data")+"/"), fragment.FilePath)
	}
	pkSchema := arrow.NewSchema([]arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}}, nil)
	reader, err := packed.NewFFIPackedReader(manifest, pkSchema, []string{"100"}, 1<<20, f.cfg, nil, packed.ExternalReaderContext{})
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	var gotPKs []int64
	for {
		record, readErr := reader.ReadNext()
		if readErr == io.EOF {
			break
		}
		require.NoError(t, readErr)
		gotPKs = append(gotPKs, record.Column(0).(*array.Int64).Int64Values()...)
	}
	require.Equal(t, f.pks, gotPKs)

	statsPaths, err := packed.NewStatsResolver(manifest, f.cfg).BloomFilterPaths(100)
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(base, "_stats/bloom_filter.100/42")}, statsPaths)
	statsBytes, err := f.cm.Read(context.Background(), statsPaths[0])
	require.NoError(t, err)
	statsReader := &storage.StatsReader{}
	statsReader.SetBuffer(statsBytes)
	stats, err := statsReader.GetPrimaryKeyStats()
	require.NoError(t, err)
	assert.Equal(t, int64(100), stats.FieldID)
	assert.True(t, stats.MinPk.EQ(storage.NewInt64PrimaryKey(f.pks[0])))
	assert.True(t, stats.MaxPk.EQ(storage.NewInt64PrimaryKey(f.pks[len(f.pks)-1])))

	lobs, err := packed.GetManifestLobFiles(manifest, f.cfg)
	require.NoError(t, err)
	require.Len(t, lobs, 1, "the TEXT values must be stored out of line in a real Vortex file")
	lob := lobs[0]
	require.True(t, strings.HasPrefix(lob.Path, filepath.Join(expectedRoot, "insert_log/1/2/lobs/101/_data")+"/"), lob.Path)
	require.Equal(t, int64(101), lob.FieldID)
	require.Equal(t, int64(len(f.texts)), lob.TotalRows)
	lobSchema := arrow.NewSchema([]arrow.Field{{Name: "text_data", Type: arrow.BinaryTypes.String}}, nil)
	lobReader, err := packed.NewFFIPackedReaderWithFragments([]string{"text_data"}, "vortex", []packed.Fragment{{
		FilePath: lob.Path, StartRow: 0, EndRow: lob.TotalRows, RowCount: lob.TotalRows,
	}}, lobSchema, []string{"text_data"}, 1<<20, f.cfg, nil, packed.ExternalReaderContext{})
	require.NoError(t, err)
	defer func() { require.NoError(t, lobReader.Close()) }()
	var gotTexts []string
	for {
		record, readErr := lobReader.ReadNext()
		if readErr == io.EOF {
			break
		}
		require.NoError(t, readErr)
		column := record.Column(0).(*array.String)
		for i := 0; i < column.Len(); i++ {
			// ReadNext releases the preceding Arrow batch. Copy strings while
			// its backing buffer is still alive, including before the EOF read.
			gotTexts = append(gotTexts, strings.Clone(column.Value(i)))
		}
	}
	require.Equal(t, f.texts, gotTexts)
}

// Keep every fixture read beneath an opened root. The names returned by
// WalkDir are root-relative; they are never reopened as unchecked absolute
// filesystem paths.
func readLocalPackedFixtureFiles(t *testing.T, directory string) map[string][]byte {
	t.Helper()
	root, err := os.OpenRoot(directory)
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	rootFS := root.FS()
	files := make(map[string][]byte)
	require.NoError(t, fs.WalkDir(rootFS, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		contents, err := fs.ReadFile(rootFS, name)
		if err == nil {
			files[name] = contents
		}
		return err
	}))
	return files
}

func TestReloadLocalManifestPreservesPackedReferences(t *testing.T) {
	f := newLegacyLocalPackedFixture(t)
	t.Chdir(f.root)
	f.assertReadable(t, f.physicalManifest, f.legacyRoot)
	f.assertNoCanonicalLayout(t)
	originalFiles := readLocalPackedFixtureFiles(t, f.legacyRoot)
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegments(mock.Anything, int64(1)).Return([]*datapb.SegmentInfo{f.segment}, nil).Twice()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil).Twice()
	mt := newLocalManifestTestMeta(catalog, f.cm)
	originalSegment := proto.Clone(f.segment)
	for i := 0; i < 2; i++ {
		// The R/M namespace remains live compatibility data, not a rename source
		// candidate, even when startup also checks for the double-root layout.
		report, err := localmigrate.Migrate(t.Context(), f.root, localmigrate.Options{LegacyPrefix: "files"})
		require.NoError(t, err)
		require.Zero(t, report.Renamed)
		require.NoError(t, mt.reloadFromKV(context.Background(), []int64{1}))
		loaded := mt.segments.GetSegment(3)
		require.Equal(t, f.physicalManifest, loaded.GetManifestPath())
		f.assertReadable(t, loaded.GetManifestPath(), f.legacyRoot)
		require.True(t, proto.Equal(originalSegment, f.segment))
		assertLocalManifestCatalogReadOnly(t, catalog)
		f.assertNoCanonicalLayout(t)
	}
	// Neither metadata reload nor real reads rewrite the original manifest,
	// data, stats or partition-level LOB files under root/minio.rootPath.
	require.Equal(t, originalFiles, readLocalPackedFixtureFiles(t, f.legacyRoot))
	f.assertReadable(t, f.physicalManifest, f.legacyRoot)
}

func TestMigrateDoubleRootPackedReferencesAndGCRetry(t *testing.T) {
	f := newLocalPackedFixture(t, true)
	t.Chdir(f.root)
	f.assertNoCanonicalLayout(t)
	f.assertReadable(t, f.physicalManifest, f.legacyRoot)
	sources := readLocalPackedFixtureFiles(t, f.legacyRoot)
	// CWD is the canonical root, leaving only the double-root rename source.
	opts := localmigrate.Options{LegacyPrefix: "files"}
	report, err := localmigrate.Migrate(t.Context(), f.root, opts)
	require.NoError(t, err)
	// The displaced insert_log directory is moved atomically as one unit; no
	// file copy or backup tree is created.
	require.Equal(t, 1, report.Renamed)
	f.assertReadable(t, f.segment.GetManifestPath(), f.root)
	canonicalRoot, err := os.OpenRoot(f.root)
	require.NoError(t, err)
	defer func() { require.NoError(t, canonicalRoot.Close()) }()
	for name, original := range sources {
		require.NoFileExists(t, filepath.Join(f.legacyRoot, name))
		contents, err := fs.ReadFile(canonicalRoot.FS(), name)
		require.NoError(t, err)
		require.Equal(t, original, contents, name)
	}

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegments(mock.Anything, int64(1)).Return([]*datapb.SegmentInfo{f.segment}, nil).Twice()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil).Twice()
	mt := newLocalManifestTestMeta(catalog, f.cm)
	for range 2 {
		report, err = localmigrate.Migrate(t.Context(), f.root, opts)
		require.NoError(t, err)
		require.Zero(t, report.Renamed)
		require.NoError(t, mt.reloadFromKV(context.Background(), []int64{1}))
		require.Equal(t, f.segment.GetManifestPath(), mt.segments.GetSegment(3).GetManifestPath())
		assertLocalManifestCatalogReadOnly(t, catalog)
		f.assertReadable(t, mt.segments.GetSegment(3).GetManifestPath(), f.root)
	}

	// Segment GC and partition-level LOB GC may reclaim the canonical files.
	removePool := conc.NewPool[struct{}](1)
	t.Cleanup(removePool.Release)
	gc := &garbageCollector{option: GcOption{cli: f.cm, removeObjectPool: removePool}}
	dropped := proto.Clone(f.segment).(*datapb.SegmentInfo)
	dropped.State = commonpb.SegmentState_Dropped
	for range 2 {
		require.NoError(t, gc.removeDroppedSegmentFiles(context.Background(), &SegmentInfo{SegmentInfo: dropped}, nil))
	}
	canonicalBase, _, err := packed.UnmarshalManifestPath(f.segment.GetManifestPath())
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(canonicalBase, "_stats/bloom_filter.100/42"))
	params := paramtable.Get()
	safetyWindow := params.DataCoordCfg.GCLOBSafetyWindow.GetValue()
	require.NoError(t, params.Save(params.DataCoordCfg.GCLOBSafetyWindow.Key, "0"))
	t.Cleanup(func() { require.NoError(t, params.Save(params.DataCoordCfg.GCLOBSafetyWindow.Key, safetyWindow)) })
	lobGC := newLOBGCContext(gc)
	orphans := lobGC.scanOrphanLOBFiles(t.Context(), typeutil.NewSet[string]())
	require.Len(t, orphans, 1, "the canonical orphan LOB is offered to the deletion callback")
	require.True(t, strings.HasPrefix(orphans[0].FilePath, filepath.Join(f.root, "insert_log/1/2/lobs")+"/"))
	lobGC.removeOrphanLOBFiles(t.Context(), orphans)
	require.Empty(t, lobGC.scanOrphanLOBFiles(t.Context(), typeutil.NewSet[string]()))
	// A fresh startup never treats completed renames as sources. GC must not be
	// undone by restoring old immutable manifests, stats, data or LOB files.
	for range 2 {
		report, err = localmigrate.Migrate(t.Context(), f.root, opts)
		require.NoError(t, err)
		require.Zero(t, report.Renamed)
		for name := range sources {
			_, err := canonicalRoot.Stat(name)
			require.ErrorIs(t, err, fs.ErrNotExist, name)
			require.NoFileExists(t, filepath.Join(f.legacyRoot, name))
		}
	}
}

func TestReloadLocalManifestCommitSegmentManifestCAS(t *testing.T) {
	for _, mutationType := range []ManifestMutationType{ManifestMutationNoop, ManifestMutationCommitUpdates} {
		name := "noop CAS"
		if mutationType == ManifestMutationCommitUpdates {
			name = "structured mutation"
		}
		t.Run(name, func(t *testing.T) {
			f := newLegacyLocalPackedFixture(t)
			f.assertNoCanonicalLayout(t)
			stored := proto.Clone(f.segment).(*datapb.SegmentInfo)
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ListSegments(mock.Anything, int64(1)).RunAndReturn(func(context.Context, int64) ([]*datapb.SegmentInfo, error) {
				return []*datapb.SegmentInfo{stored}, nil
			}).Twice()
			catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil).Twice()
			mt := newLocalManifestTestMeta(catalog, f.cm)
			ctx := context.Background()
			require.NoError(t, mt.reloadFromKV(ctx, []int64{1}))
			assertLocalManifestCatalogReadOnly(t, catalog)
			base, version, err := packed.UnmarshalManifestPath(f.physicalManifest)
			require.NoError(t, err)
			prepared := packed.MarshalManifestPath(base, version+1)
			// CAS compares the authoritative normalized in-memory pointer, not the
			// still-relative catalog value. Old workers and old versions are stale.
			for _, expected := range []string{f.segment.GetManifestPath(), packed.MarshalManifestPath(base, version-1)} {
				err := mt.CommitSegmentManifest(ctx, SegmentManifestCommit{
					SegmentID: 3, ExpectedManifest: expected,
					Mutation: ManifestMutation{Type: ManifestMutationNoop, ManifestPath: prepared},
				})
				require.ErrorIs(t, err, errSegmentManifestStale)
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			}
			catalog.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
			require.Equal(t, f.segment.GetManifestPath(), stored.GetManifestPath())
			updates := &packed.ManifestUpdates{Stats: []packed.StatEntry{{
				Key: "bloom_filter.100", Files: []string{filepath.Join(base, "_stats/bloom_filter.100/42")},
				Metadata: map[string]string{"memory_size": "48"},
			}}}
			commit := SegmentManifestCommit{
				SegmentID: 3, StorageConfig: f.cfg,
				Mutation: ManifestMutation{Type: mutationType, Updates: updates},
			}
			if mutationType == ManifestMutationNoop {
				actualPrepared, err := packed.CommitManifestUpdates(base, version, f.cfg, updates)
				require.NoError(t, err)
				require.Equal(t, prepared, actualPrepared)
				commit.ExpectedManifest = f.physicalManifest
				commit.Mutation.ManifestPath = actualPrepared
			}
			catalog.EXPECT().Update(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, actions ...metastore.UpdateAction) error {
				require.Equal(t, f.segment.GetManifestPath(), stored.GetManifestPath(), "no startup write may precede the ordinary commit")
				require.Equal(t, f.physicalManifest, mt.segments.GetSegment(3).GetManifestPath(), "publish memory only after catalog success")
				require.Len(t, actions, 1)
				entry, ok := actions[0].Entry.(metastore.SegmentEntry)
				require.True(t, ok)
				require.Equal(t, prepared, entry.Segment.GetManifestPath())
				stored = proto.Clone(entry.Segment).(*datapb.SegmentInfo)
				return nil
			}).Once()
			require.NoError(t, mt.CommitSegmentManifest(ctx, commit))
			require.Equal(t, prepared, mt.segments.GetSegment(3).GetManifestPath())
			f.assertReadable(t, prepared, f.legacyRoot)
			f.assertNoCanonicalLayout(t)
			// A result prepared on the previously normalized pointer must not be
			// accepted after this publication, even with a higher prepared version.
			err = mt.CommitSegmentManifest(ctx, SegmentManifestCommit{
				SegmentID: 3, ExpectedManifest: f.physicalManifest,
				Mutation: ManifestMutation{Type: ManifestMutationNoop, ManifestPath: packed.MarshalManifestPath(base, version+2)},
			})
			require.ErrorIs(t, err, errSegmentManifestStale)
			require.Equal(t, prepared, stored.GetManifestPath())
			catalog.AssertNumberOfCalls(t, "Update", 1)
			require.NoError(t, mt.reloadFromKV(ctx, []int64{1}))
			require.Equal(t, prepared, mt.segments.GetSegment(3).GetManifestPath())
			f.assertNoCanonicalLayout(t)
		})
	}
}

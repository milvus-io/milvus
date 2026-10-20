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

	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"

	"github.com/milvus-io/milvus/internal/storage/localmigrate"
	"github.com/milvus-io/milvus/internal/storagev2/packed"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"

	"github.com/milvus-io/milvus/pkg/v3/util/conc"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

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
	mt := newLocalManifestTestMeta(t, catalog, f.cm)
	for range 2 {
		report, err = localmigrate.Migrate(t.Context(), f.root, opts)
		require.NoError(t, err)
		require.Zero(t, report.Renamed)
		require.NoError(t, loadAndNormalize(context.Background(), mt, []int64{1}))
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

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

package meta

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage/localmigrate"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Write a real manifest at the physical location used by the old rooted
// filesystem. Its writer-generated internal references are serialized relative
// to the segment/partition, while the catalog retains the old relative base.
// Compatibility must resolve that base to this original location without copying.
// This does not fabricate a parquet, manifest, stats blob, or LOB marker file.
func newLegacyLocalPackedFixture(t *testing.T) *legacyLocalPackedFixture {
	t.Helper()
	return NewLocalPackedFixture(t, false)
}

func TestReloadLocalManifestPreservesPackedReferences(t *testing.T) {
	f := newLegacyLocalPackedFixture(t)
	t.Chdir(f.Root)
	f.AssertReadable(t, f.PhysicalManifest, f.LegacyRoot)
	f.AssertNoCanonicalLayout(t)
	originalFiles := ReadLocalPackedFixtureFiles(t, f.LegacyRoot)
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegments(mock.Anything, int64(1)).Return([]*datapb.SegmentInfo{f.Segment}, nil).Twice()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil).Twice()
	mt := NewLocalManifestTestMeta(t, catalog, f.Cm)
	originalSegment := proto.Clone(f.Segment)
	for i := 0; i < 2; i++ {
		// The R/M namespace remains live compatibility data, not a rename source
		// candidate, even when startup also checks for the double-root layout.
		report, err := localmigrate.Migrate(t.Context(), f.Root, localmigrate.Options{LegacyPrefix: "files"})
		require.NoError(t, err)
		require.Zero(t, report.Renamed)
		require.NoError(t, LoadAndNormalize(context.Background(), mt, []int64{1}))
		loaded := mt.Segments.GetSegment(3)
		require.Equal(t, f.PhysicalManifest, loaded.GetManifestPath())
		f.AssertReadable(t, loaded.GetManifestPath(), f.LegacyRoot)
		require.True(t, proto.Equal(originalSegment, f.Segment))
		AssertLocalManifestCatalogReadOnly(t, catalog)
		f.AssertNoCanonicalLayout(t)
	}
	// Neither metadata reload nor real reads rewrite the original manifest,
	// data, stats or partition-level LOB files under root/minio.rootPath.
	require.Equal(t, originalFiles, ReadLocalPackedFixtureFiles(t, f.LegacyRoot))
	f.AssertReadable(t, f.PhysicalManifest, f.LegacyRoot)
}

func TestReloadLocalManifestCommitSegmentManifestCAS(t *testing.T) {
	for _, mutationType := range []ManifestMutationType{ManifestMutationNoop, ManifestMutationCommitUpdates} {
		name := "noop CAS"
		if mutationType == ManifestMutationCommitUpdates {
			name = "structured mutation"
		}
		t.Run(name, func(t *testing.T) {
			f := newLegacyLocalPackedFixture(t)
			f.AssertNoCanonicalLayout(t)
			stored := proto.Clone(f.Segment).(*datapb.SegmentInfo)
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ListSegments(mock.Anything, int64(1)).RunAndReturn(func(context.Context, int64) ([]*datapb.SegmentInfo, error) {
				return []*datapb.SegmentInfo{stored}, nil
			}).Twice()
			catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil).Twice()
			mt := NewLocalManifestTestMeta(t, catalog, f.Cm)
			ctx := context.Background()
			require.NoError(t, LoadAndNormalize(ctx, mt, []int64{1}))
			AssertLocalManifestCatalogReadOnly(t, catalog)
			base, version, err := packed.UnmarshalManifestPath(f.PhysicalManifest)
			require.NoError(t, err)
			prepared := packed.MarshalManifestPath(base, version+1)
			// CAS compares the authoritative normalized in-memory pointer, not the
			// still-relative catalog value. Old workers and old versions are stale.
			for _, expected := range []string{f.Segment.GetManifestPath(), packed.MarshalManifestPath(base, version-1)} {
				err := mt.CommitSegmentManifest(ctx, SegmentManifestCommit{
					SegmentID: 3, ExpectedManifest: expected,
					Mutation: ManifestMutation{Type: ManifestMutationNoop, ManifestPath: prepared},
				})
				require.ErrorIs(t, err, ErrSegmentManifestStale)
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			}
			catalog.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
			require.Equal(t, f.Segment.GetManifestPath(), stored.GetManifestPath())
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
				commit.ExpectedManifest = f.PhysicalManifest
				commit.Mutation.ManifestPath = actualPrepared
			}
			catalog.EXPECT().Update(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, actions ...metastore.UpdateAction) error {
				require.Equal(t, f.Segment.GetManifestPath(), stored.GetManifestPath(), "no startup write may precede the ordinary commit")
				require.Equal(t, f.PhysicalManifest, mt.Segments.GetSegment(3).GetManifestPath(), "publish memory only after catalog success")
				require.Len(t, actions, 1)
				entry, ok := actions[0].Entry.(metastore.SegmentEntry)
				require.True(t, ok)
				require.Equal(t, prepared, entry.Segment.GetManifestPath())
				stored = proto.Clone(entry.Segment).(*datapb.SegmentInfo)
				return nil
			}).Once()
			require.NoError(t, mt.CommitSegmentManifest(ctx, commit))
			require.Equal(t, prepared, mt.Segments.GetSegment(3).GetManifestPath())
			f.AssertReadable(t, prepared, f.LegacyRoot)
			f.AssertNoCanonicalLayout(t)
			// A result prepared on the previously normalized pointer must not be
			// accepted after this publication, even with a higher prepared version.
			err = mt.CommitSegmentManifest(ctx, SegmentManifestCommit{
				SegmentID: 3, ExpectedManifest: f.PhysicalManifest,
				Mutation: ManifestMutation{Type: ManifestMutationNoop, ManifestPath: packed.MarshalManifestPath(base, version+2)},
			})
			require.ErrorIs(t, err, ErrSegmentManifestStale)
			require.Equal(t, prepared, stored.GetManifestPath())
			catalog.AssertNumberOfCalls(t, "Update", 1)
			require.NoError(t, LoadAndNormalize(ctx, mt, []int64{1}))
			require.Equal(t, prepared, mt.Segments.GetSegment(3).GetManifestPath())
			f.AssertNoCanonicalLayout(t)
		})
	}
}

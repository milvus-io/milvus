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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage/localmigrate"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
	mt := NewLocalManifestTestMeta(catalog, f.Cm)
	originalSegment := proto.Clone(f.Segment)
	for i := 0; i < 2; i++ {
		// The R/M namespace remains live compatibility data, not a rename source
		// candidate, even when startup also checks for the double-root layout.
		report, err := localmigrate.Migrate(t.Context(), f.Root, localmigrate.Options{LegacyPrefix: "files"})
		require.NoError(t, err)
		require.Zero(t, report.Renamed)
		require.NoError(t, mt.ReloadFromKV(context.Background(), []int64{1}))
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

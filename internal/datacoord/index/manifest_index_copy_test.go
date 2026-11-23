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

package index

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestIndexMigrationCopyFailureAndRetry(t *testing.T) {
	t.Run("backfill", func(t *testing.T) {
		meta.WithSegmentIndexManifestWrites(t, false)
		meta.NewFakeManifestStore(t)
		catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
		m := BootMigrationMeta(t, catalog, meta.RestartCollID)
		SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
		ctx := context.Background()
		forward := NewMigrationBackfillInspector(ctx, m)
		before := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
		source, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
		sourceVersion := source.IndexStorePathVersion
		var fail atomic.Bool
		fail.Store(true)
		var copied atomic.Int32
		firstCopied := make(chan struct{})
		m.ChunkManager = migrationCopyManager{ChunkManager: m.ChunkManager, copy: func(ctx context.Context, src, dst string) error {
			if fail.Load() {
				if strings.HasSuffix(src, "/0") {
					copied.Add(1)
					close(firstCopied)
					return nil
				}
				<-firstCopied
				return merr.ErrIoTooManyRequests
			}
			assert.Contains(t, src, "/index_v1/")
			assert.Contains(t, dst, "/_index/")
			copied.Add(1)
			return nil
		}}
		migrate := func() error {
			return forward.backfillIndexes(ctx, meta.RestartSegID, meta.RestartBuildID)
		}
		require.ErrorIs(t, migrate(), merr.ErrIoTooManyRequests)
		assert.Equal(t, before, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
		current, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
		assert.Equal(t, source, current)
		rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
		require.NoError(t, err)
		assert.Len(t, rows, 1)
		assert.Equal(t, sourceVersion, rows[0].IndexStorePathVersion)
		assert.EqualValues(t, 1, copied.Load())
		fail.Store(false)
		require.NoError(t, migrate())
		assert.NotEqual(t, before, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
		assert.False(t, m.IndexCatalog.HasBuildInfo(meta.RestartBuildID))
		assert.Len(t, m.GetSegment(ctx, meta.RestartSegID).GetIndexSummaries(), 1)
	})
}

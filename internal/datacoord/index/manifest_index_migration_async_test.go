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
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestIndexMigrationAsyncProgress(t *testing.T) {
	t.Run("backfill", func(t *testing.T) {
		ctx := context.Background()
		meta.WithSegmentIndexManifestWrites(t, false)

		meta.NewFakeManifestStore(t)
		catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
		m := BootMigrationMeta(t, catalog, meta.RestartCollID)
		backfill := NewMigrationBackfillInspector(ctx, m)
		for offset := int64(0); offset < 3; offset++ {
			SeedLegacyBackfillRecord(t, m, meta.RestartSegID+offset, meta.RestartBuildID+offset)
		}
		meta.WithSegmentIndexManifestWrites(t, true)
		SetMigrationTestParam(t, &Params.DataCoordCfg.ManifestIndexBackfillConcurrency, "2")
		syncRead := mockey.Mock(packed.GetManifestIndexInfos).Return(nil, merr.ErrServiceInternal).Build()
		t.Cleanup(func() { syncRead.UnPatch() })
		syncCommit := mockey.Mock(packed.CommitManifestUpdates).Return("", merr.ErrServiceInternal).Build()
		t.Cleanup(func() { syncCommit.UnPatch() })
		reads := make(chan *packed.ManifestIOContext, 16)
		readPatch := mockey.Mock((*meta.Meta).ReadManifestIndexesWithIO).When(func(_ *meta.Meta, _ context.Context, io *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) bool {
			reads <- io
			return false
		}).Build()
		t.Cleanup(func() { readPatch.UnPatch() })
		entered := make(chan int64, 3)
		release := make(chan struct{})
		var once sync.Once
		patch := mockey.Mock(meta.CommitManifestMutationForTest).When(func(_ *meta.Meta, _ context.Context, _ *packed.ManifestIOContext, _ string, commit meta.SegmentManifestCommit) bool {
			entered <- commit.SegmentID
			if commit.SegmentID == meta.RestartSegID {
				<-release
			}
			return false
		}).Build()
		done := make(chan struct{})
		t.Cleanup(func() {
			once.Do(func() { close(release) })
			<-done
			patch.UnPatch()
		})
		go func() {
			defer close(done)
			backfill.runOnce(ctx)
		}()
		for range 3 {
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("a slow segment prevented replenishing the other migration slot")
			}
		}
		require.Eventually(t, func() bool {
			return len(m.GetSegment(ctx, meta.RestartSegID+2).GetIndexSummaries()) == 1
		}, 5*time.Second, time.Millisecond)
		assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID), "blocked segment must retain its source placement")
		once.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("migration failed to drain")
		}
		assert.Zero(t, syncRead.Times())
		assert.Zero(t, syncCommit.Times())
		rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
		require.NoError(t, err)
		assert.Empty(t, rows)
		assert.Empty(t, reads, "backfill needs no preliminary manifest read")
	})
}

func TestManifestIndexMigrationPreservesCommitOutcome(t *testing.T) {
	for _, outcome := range []packed.ManifestCommitOutcome{packed.ManifestNotCommitted, packed.ManifestCommitUnknown} {
		t.Run(map[packed.ManifestCommitOutcome]string{packed.ManifestNotCommitted: "not committed", packed.ManifestCommitUnknown: "unknown"}[outcome], func(t *testing.T) {
			ctx := context.Background()
			store := meta.NewFakeManifestStore(t)
			catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			m := BootMigrationMeta(t, catalog, meta.RestartCollID)
			SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
			before := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			commits := store.CommitCount
			failure := &packed.ManifestCommitError{Outcome: outcome, Err: merr.ErrIoTooManyRequests}
			patch := mockey.Mock(meta.CommitManifestMutationForTest).Return(packed.ManifestUpdateResult{}, failure).Build()
			t.Cleanup(func() { patch.UnPatch() })
			migrate := func() error {
				return NewMigrationBackfillInspector(ctx, m).backfillIndexes(ctx, meta.RestartSegID, meta.RestartBuildID)
			}
			err := migrate()
			var commitErr *packed.ManifestCommitError
			require.ErrorAs(t, err, &commitErr)
			assert.Equal(t, outcome, commitErr.Outcome)
			assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
			assert.Equal(t, before, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
			assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			assert.Len(t, rows, 1)
			assert.Equal(t, commits, store.CommitCount)
			patch.UnPatch()
			require.NoError(t, migrate(), "a later attempt must start from the still-published pointer")
			assert.Equal(t, commits+1, store.CommitCount)
		})
	}
}

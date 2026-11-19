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
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "backfill", true: "rollback"}[rollback], func(t *testing.T) {
			ctx := context.Background()
			meta.WithSegmentIndexManifestWrites(t, false)
			WithManifestIndexRollback(t, false)
			WithManifestIndexBackfillEnabled(t, true)
			meta.NewFakeManifestStore(t)
			catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			m := BootMigrationMeta(t, catalog, meta.RestartCollID)
			backfill := NewMigrationBackfillInspector(ctx, m)
			for offset := int64(0); offset < 3; offset++ {
				SeedLegacyBackfillRecord(t, m, meta.RestartSegID+offset, meta.RestartBuildID+offset)
				if rollback {
					require.NoError(t, backfill.backfillIndexes(ctx, meta.RestartSegID+offset, meta.RestartBuildID+offset))
				}
			}
			meta.WithSegmentIndexManifestWrites(t, true)
			WithManifestIndexRollback(t, rollback)
			SetRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexBackfillConcurrency, "2")
			SetRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexRollbackConcurrency, "2")
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
				if rollback {
					NewMigrationRollbackInspector(ctx, m).runOnce(ctx)
				} else {
					backfill.runOnce(ctx)
				}
			}()
			for range 3 {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("a slow segment prevented replenishing the other migration slot")
				}
			}
			require.Eventually(t, func() bool {
				record, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID + 2)
				return ok && record.ManifestPublished != rollback
			}, 5*time.Second, time.Millisecond)
			assert.Equal(t, rollback, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID), "blocked segment must retain its source placement")
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
			if rollback {
				require.Len(t, rows, 3)
				require.Len(t, reads, 3)
				io := <-reads
				assert.Same(t, io, <-reads)
				assert.Same(t, io, <-reads)
				assert.NotSame(t, m.ManifestCommitIOForTest(), io)
				// Submit on the actual context, bypassing the fixture's read mock.
				_, err = packed.GetManifestLobFilesAsync(ctx, io, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath(), nil)
				assert.ErrorIs(t, err, merr.ErrServiceUnavailable, "the scan must close its read executor")
			} else {
				assert.Empty(t, rows)
				assert.Empty(t, reads, "backfill needs no preliminary manifest read")
			}
		})
	}
}

func TestManifestIndexMigrationPreservesCommitOutcome(t *testing.T) {
	for _, outcome := range []packed.ManifestCommitOutcome{packed.ManifestNotCommitted, packed.ManifestCommitUnknown} {
		for _, rollback := range []bool{false, true} {
			t.Run(map[bool]string{false: "backfill", true: "rollback"}[rollback]+"/"+map[packed.ManifestCommitOutcome]string{packed.ManifestNotCommitted: "not committed", packed.ManifestCommitUnknown: "unknown"}[outcome], func(t *testing.T) {
				ctx := context.Background()
				m, catalog, store, _ := RollbackFixture(t)
				io := newMigrationManifestReadIO(t)
				if !rollback {
					_, err := m.RollbackSegmentIndexes(ctx, io, meta.RestartSegID, meta.RestartBuildID)
					require.NoError(t, err)
					WithManifestIndexRollback(t, false)
				}
				before := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
				commits := store.CommitCount
				failure := &packed.ManifestCommitError{Outcome: outcome, Err: merr.ErrIoTooManyRequests}
				patch := mockey.Mock(meta.CommitManifestMutationForTest).Return(packed.ManifestUpdateResult{}, failure).Build()
				t.Cleanup(func() { patch.UnPatch() })
				migrate := func() error {
					if rollback {
						_, err := m.RollbackSegmentIndexes(ctx, io, meta.RestartSegID, meta.RestartBuildID)
						return err
					}
					return NewMigrationBackfillInspector(ctx, m).backfillIndexes(ctx, meta.RestartSegID, meta.RestartBuildID)
				}
				err := migrate()
				var commitErr *packed.ManifestCommitError
				require.ErrorAs(t, err, &commitErr)
				assert.Equal(t, outcome, commitErr.Outcome)
				assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
				assert.Equal(t, before, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
				assert.Equal(t, rollback, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
				rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
				require.NoError(t, err)
				if rollback {
					assert.Empty(t, rows)
				} else {
					assert.Len(t, rows, 1)
				}
				assert.Equal(t, commits, store.CommitCount)
				patch.UnPatch()
				require.NoError(t, migrate(), "a later attempt must start from the still-published pointer")
				assert.Equal(t, commits+1, store.CommitCount)
			})
		}
	}
}

func TestManifestIndexRollbackNoopUsesSourceMarker(t *testing.T) {
	for _, unselected := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "unselected entry"}[unselected], func(t *testing.T) {
			m, catalog, store, _ := RollbackFixture(t)
			ctx := context.Background()
			before := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			entry := store.Revisions[before][0]
			store.Revisions[before] = nil
			if unselected {
				entry.BuildID++
				entry.IndexID++
				store.Revisions[before] = []packed.ManifestIndexInfo{entry}
			}
			reads, commits := store.ReadCount, store.CommitCount
			n, err := m.RollbackSegmentIndexes(ctx, newMigrationManifestReadIO(t), meta.RestartSegID, meta.RestartBuildID)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			assert.Equal(t, reads+1, store.ReadCount, "reuse the locked source read for the Noop marker")
			assert.Equal(t, commits, store.CommitCount, "no manifest write when the selected build is absent")
			assert.Equal(t, before, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
			assert.Equal(t, unselected, m.GetSegment(ctx, meta.RestartSegID).GetManifestHasIndex())
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, meta.RestartBuildID, rows[0].BuildID)
		})
	}
}

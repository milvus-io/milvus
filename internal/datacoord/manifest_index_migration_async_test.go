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
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A slow segment must not hold up subsequent segments when another slot is
// free. Also pin executor ownership: one read owner per rollback scan, separate
// from the long-lived commit executor, with no synchronous manifest fallback.
func TestManifestIndexMigrationAsyncProgress(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "backfill", true: "rollback"}[rollback], func(t *testing.T) {
			ctx := context.Background()
			withSegmentIndexManifestWrites(t, false)
			withManifestIndexRollback(t, false)
			withManifestIndexBackfillEnabled(t, true)
			newFakeManifestStore(t)
			catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
			m := bootMetaForRestart(t, catalog, restartCollID)
			backfill := newManifestIndexBackfillInspector(ctx, m)
			for offset := int64(0); offset < 3; offset++ {
				seedLegacyBackfillRecord(t, m, restartSegID+offset, restartBuildID+offset)
				if rollback {
					require.NoError(t, backfill.backfillIndexes(ctx, restartSegID+offset, restartBuildID+offset))
				}
			}
			withSegmentIndexManifestWrites(t, true)
			withManifestIndexRollback(t, rollback)
			setRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexBackfillConcurrency, "2")
			setRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexRollbackConcurrency, "2")
			syncRead := mockey.Mock(packed.GetManifestIndexInfos).Return(nil, merr.ErrServiceInternal).Build()
			t.Cleanup(func() { syncRead.UnPatch() })
			syncCommit := mockey.Mock(packed.CommitManifestUpdates).Return("", merr.ErrServiceInternal).Build()
			t.Cleanup(func() { syncCommit.UnPatch() })
			reads := make(chan *packed.ManifestIOContext, 16)
			readPatch := mockey.Mock((*meta).readManifestIndexesWithIO).When(func(_ *meta, _ context.Context, io *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) bool {
				reads <- io
				return false
			}).Build()
			t.Cleanup(func() { readPatch.UnPatch() })
			entered := make(chan int64, 3)
			release := make(chan struct{})
			var once sync.Once
			patch := mockey.Mock((*meta).commitManifestMutation).When(func(_ *meta, _ context.Context, _ *packed.ManifestIOContext, _ string, commit SegmentManifestCommit) bool {
				entered <- commit.SegmentID
				if commit.SegmentID == restartSegID {
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
					newManifestIndexRollbackInspector(ctx, m).runOnce(ctx)
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
				record, ok := m.indexMeta.GetIndexJob(restartBuildID + 2)
				return ok && record.ManifestPublished != rollback
			}, 5*time.Second, time.Millisecond)
			assert.Equal(t, rollback, indexManifestPublished(t, m.indexMeta, restartBuildID), "blocked segment must retain its source placement")
			once.Do(func() { close(release) })
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("migration failed to drain")
			}
			assert.Zero(t, syncRead.Times())
			assert.Zero(t, syncCommit.Times())
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			if rollback {
				require.Len(t, rows, 3)
				require.Len(t, reads, 3)
				io := <-reads
				assert.Same(t, io, <-reads)
				assert.Same(t, io, <-reads)
				assert.NotSame(t, m.manifestCommitExecutor.io, io)
				// Submit on the actual context, bypassing the fixture's read mock.
				_, err = packed.GetManifestLobFilesAsync(ctx, io, m.GetSegment(ctx, restartSegID).GetManifestPath(), nil)
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
				m, catalog, store, _ := rollbackFixture(t)
				io := newGCManifestReadIO(t)
				if !rollback {
					_, err := m.rollbackSegmentIndexes(ctx, io, restartSegID, restartBuildID)
					require.NoError(t, err)
					withManifestIndexRollback(t, false)
				}
				before := m.GetSegment(ctx, restartSegID).GetManifestPath()
				commits := store.commitCount
				failure := &packed.ManifestCommitError{Outcome: outcome, Err: merr.ErrIoTooManyRequests}
				patch := mockey.Mock((*meta).commitManifestMutation).Return(packed.ManifestUpdateResult{}, failure).Build()
				t.Cleanup(func() { patch.UnPatch() })
				migrate := func() error {
					if rollback {
						_, err := m.rollbackSegmentIndexes(ctx, io, restartSegID, restartBuildID)
						return err
					}
					return newManifestIndexBackfillInspector(ctx, m).backfillIndexes(ctx, restartSegID, restartBuildID)
				}
				err := migrate()
				var commitErr *packed.ManifestCommitError
				require.ErrorAs(t, err, &commitErr)
				assert.Equal(t, outcome, commitErr.Outcome)
				assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
				assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
				assert.Equal(t, rollback, indexManifestPublished(t, m.indexMeta, restartBuildID))
				rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
				require.NoError(t, err)
				if rollback {
					assert.Empty(t, rows)
				} else {
					assert.Len(t, rows, 1)
				}
				assert.Equal(t, commits, store.commitCount)
				patch.UnPatch()
				require.NoError(t, migrate(), "a later attempt must start from the still-published pointer")
				assert.Equal(t, commits+1, store.commitCount)
			})
		}
	}
}

func TestManifestIndexRollbackNoopUsesSourceMarker(t *testing.T) {
	for _, unselected := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "unselected entry"}[unselected], func(t *testing.T) {
			m, catalog, store, _ := rollbackFixture(t)
			ctx := context.Background()
			before := m.GetSegment(ctx, restartSegID).GetManifestPath()
			entry := store.revisions[before][0]
			store.revisions[before] = nil
			if unselected {
				entry.BuildID++
				entry.IndexID++
				store.revisions[before] = []packed.ManifestIndexInfo{entry}
			}
			reads, commits := store.readCount, store.commitCount
			n, err := m.rollbackSegmentIndexes(ctx, newGCManifestReadIO(t), restartSegID, restartBuildID)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			assert.Equal(t, reads+1, store.readCount, "reuse the locked source read for the Noop marker")
			assert.Equal(t, commits, store.commitCount, "no manifest write when the selected build is absent")
			assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
			assert.Equal(t, unselected, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, restartBuildID, rows[0].BuildID)
		})
	}
}

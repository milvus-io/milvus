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

package delegator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type idfCleanupCall struct {
	done chan struct{}
	err  error
}

func runIDFCleanupCall(fn func() error) *idfCleanupCall {
	c := &idfCleanupCall{done: make(chan struct{})}
	go func() {
		c.err = fn()
		close(c.done)
	}()
	return c
}

func (c *idfCleanupCall) result(t *testing.T) error {
	t.Helper()
	select {
	case <-c.done:
		return c.err
	case <-time.After(5 * time.Second):
		t.Fatal("IDF operation did not complete")
		return nil
	}
}

// Keep a statistics reader alive so physical removal cannot finish. Publication
// must still complete; cleanup and accounting are joined separately by the test.
func blockIDFCleanup(t *testing.T, o *idfOracle, id int64, target *snapshot) (*sealedBm25Stats, func(), *idfCleanupCall) {
	t.Helper()
	old, ok := o.sealed.Get(id)
	require.True(t, ok)
	old.RLock()
	var once sync.Once
	release := func() { once.Do(old.RUnlock) }
	o.next.SetSnapshot(target)
	call := runIDFCleanupCall(o.SyncDistribution)
	t.Cleanup(func() {
		release()
		require.NoError(t, call.result(t))
	})
	require.Eventually(t, func() bool { return o.TargetVersion() == target.targetVersion }, 5*time.Second, time.Millisecond)
	return old, release, call
}

func TestIDFCleanupDoesNotBlockReadersOrSchemaSync(t *testing.T) {
	o := newSyncTestOracle(t, 102)
	o.targetVersion.Store(1)
	dropped := newSyncTestSegment(t, 1, genBM25StatsForField(102, 1, 4))
	kept := newSyncTestSegment(t, 2, genBM25StatsForField(102, 10, 15))
	dropped.load(t, o, 102)
	kept.load(t, o, 102)
	// Keep the obsolete entry inactive: this isolates physical cleanup from
	// the separate activation-state reconciliation performed under the lock.
	syncWithHook(t, o, syncTestSnapshotUnreadable(2, []int64{2}, 1), func() {})
	diskBefore := o.diskSize()
	old, release, cleanup := blockIDFCleanup(t, o, 1, syncTestSnapshot(3, 2))

	assert.False(t, o.sealed.Contain(1))
	assert.True(t, o.retiredSealed.Contain(1))
	assert.DirExists(t, old.localDir)
	assert.Equal(t, diskBefore, o.diskSize(), "pending physical cleanup is still charged")

	reader := runIDFCleanupCall(func() error {
		_, _, err := o.BuildIDF(102, &schemapb.SparseFloatArray{})
		return err
	})
	t.Cleanup(func() { release(); require.NoError(t, reader.result(t)) })
	require.NoError(t, reader.result(t))
	assertCurrentField(t, o, 102, kept.stats[102])

	// Leaving the detached entry in sealed would make SyncFunctions wait for
	// its statistics lock while holding the global oracle lock again.
	schema := runIDFCleanupCall(func() error { return o.SyncFunctions(syncTestFunctions(102)) })
	t.Cleanup(func() { release(); require.NoError(t, schema.result(t)) })
	require.NoError(t, schema.result(t))
	assertCurrentField(t, o, 102, kept.stats[102])

	release()
	require.NoError(t, cleanup.result(t))
	assert.False(t, o.retiredSealed.Contain(1))
	assert.NoDirExists(t, old.localDir)
	keptEntry, ok := o.sealed.Get(2)
	require.True(t, ok)
	assert.Equal(t, keptEntry.diskSize, o.diskSize())
}

func TestIDFReloadWaitsForCleanup(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		name := "load"
		if reopen {
			name = "reopen"
		}
		t.Run(name, func(t *testing.T) {
			o := newSyncTestOracle(t, 102)
			o.targetVersion.Store(1)
			oldSegment := newSyncTestSegment(t, 1, genBM25StatsForField(102, 1, 4))
			oldSegment.load(t, o, 102)
			old, release, cleanup := blockIDFCleanup(t, o, 1, syncTestSnapshot(2))
			fresh := newSyncTestSegment(t, 1, genBM25StatsForField(102, 10, 15))
			load := runIDFCleanupCall(func() error {
				if reopen {
					return o.LoadSealedForReopen(context.Background(), 1, fresh.loadInfo(102), fresh.cm, false)
				}
				return o.LoadSealed(context.Background(), 1, fresh.loadInfo(102), fresh.cm)
			})
			t.Cleanup(func() { release(); require.NoError(t, load.result(t)) })
			select {
			case <-load.done:
				t.Fatal("reload completed before the old directory was removed")
			case <-time.After(20 * time.Millisecond):
			}
			release()
			require.NoError(t, cleanup.result(t))
			require.NoError(t, load.result(t))
			entry, ok := o.sealed.Get(1)
			require.True(t, ok)
			assert.NotSame(t, old, entry)
			stats, err := entry.FetchStats()
			require.NoError(t, err)
			assert.Equal(t, int64(5), stats[102].NumRow())
			assert.Equal(t, entry.diskSize, o.diskSize())
		})
	}
}

// A load/reopen can win the lifecycle lock before the sync starts its physical
// cleanup. It must finish the pending cleanup and a delayed old callback must
// leave both the replacement's files and its disk charge intact.
func TestIDFReloadFinishesPendingCleanup(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		name := "load"
		if reopen {
			name = "reopen"
		}
		t.Run(name, func(t *testing.T) {
			o := newSyncTestOracle(t, 102, 104)
			o.targetVersion.Store(1)
			oldSegment := newSyncTestSegment(t, 1, genBM25StatsForField(102, 1, 4))
			oldSegment.load(t, o, 102)
			old, _ := o.sealed.Get(1)
			o.retiredSealed.Insert(1, old)
			o.sealed.Remove(1)

			stats := genBM25StatsForField(102, 10, 15)
			stats[104] = genBM25StatsForField(104, 20, 25)[104]
			fresh := newSyncTestSegment(t, 1, stats)
			if reopen {
				require.NoError(t, o.LoadSealedForReopen(context.Background(), 1, fresh.loadInfo(102, 104), fresh.cm, true))
			} else {
				fresh.load(t, o, 102, 104)
			}
			entry, ok := o.sealed.Get(1)
			require.True(t, ok)
			assert.NotSame(t, old, entry)
			assert.False(t, o.retiredSealed.Contain(1))
			assert.True(t, old.removed)

			// Model the next retirement before the delayed old callback runs.
			o.retiredSealed.Insert(1, entry)
			o.sealed.Remove(1)
			o.removeRetiredSealedStats(1, old)
			pending, ok := o.retiredSealed.Get(1)
			require.True(t, ok)
			assert.Same(t, entry, pending)
			assert.DirExists(t, entry.localDir)
			assert.Equal(t, entry.diskSize, o.diskSize())
			o.sealed.Insert(1, entry)
			o.retiredSealed.Remove(1)

			syncWithHook(t, o, syncTestSnapshot(2, 1), func() {})
			assertCurrentField(t, o, 102, stats[102])
			assertCurrentField(t, o, 104, stats[104])
		})
	}
}

func TestIDFReopenRetriesAfterWaitingAcrossStateChange(t *testing.T) {
	for _, change := range []string{"target", "schema"} {
		t.Run(change, func(t *testing.T) {
			o := newSyncTestOracle(t, 102)
			o.targetVersion.Store(1)
			oldSegment := newSyncTestSegment(t, 1, genBM25StatsForField(102, 1, 4))
			oldSegment.load(t, o, 102)
			old, _ := o.sealed.Get(1)
			o.retiredSealed.Insert(1, old)
			o.sealed.Remove(1)
			old.RLock()
			var once sync.Once
			release := func() { once.Do(old.RUnlock) }
			defer release()
			fresh := newSyncTestSegment(t, 1, genBM25StatsForField(102, 10, 15))
			reopen := runIDFCleanupCall(func() error {
				return o.LoadSealedForReopen(context.Background(), 1, fresh.loadInfo(102), fresh.cm, true)
			})
			t.Cleanup(func() { release(); _ = reopen.result(t) })
			require.Eventually(t, func() bool {
				if o.sealedLifecycle.TryLock(1) {
					o.sealedLifecycle.Unlock(1)
					return false
				}
				return true
			}, 5*time.Second, time.Millisecond)
			// The lifecycle lock is held by the reopen, which is waiting for old's
			// statistics reader. Change the state before allowing it to continue.
			if change == "target" {
				o.targetVersion.Store(2)
			} else {
				require.NoError(t, o.SyncFunctions(syncTestFunctions(102)))
			}
			release()
			require.True(t, merr.IsRetryableErr(reopen.result(t)))
			fresh.cm.AssertNotCalled(t, "Reader", mock.Anything, fresh.remotePath(102))

			// The caller resamples readability on retry; the removed segment is not
			// readable, so loading its stats must not activate it from the stale flag.
			require.NoError(t, o.LoadSealedForReopen(context.Background(), 1, fresh.loadInfo(102), fresh.cm, false))
			assertCurrentField(t, o, 102, storage.NewBM25Stats())
		})
	}
}

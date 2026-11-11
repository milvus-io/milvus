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
	"bytes"
	"context"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
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
	assert.NoDirExists(t, old.localDir)
	keptEntry, ok := o.sealed.Get(2)
	require.True(t, ok)
	assert.Equal(t, keptEntry.diskSize, o.diskSize())
}

// The replacement owns different files and must not wait for an old local reader.
func TestIDFReloadDoesNotWaitForCleanup(t *testing.T) {
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
			t.Cleanup(func() { release(); _ = load.result(t) })
			require.NoError(t, load.result(t))
			entry, ok := o.sealed.Get(1)
			require.True(t, ok)
			require.NotEqual(t, old.localDir, entry.localDir)
			require.DirExists(t, old.localDir)
			assert.Equal(t, old.diskSize+entry.diskSize, o.diskSize())
			release()
			require.NoError(t, cleanup.result(t))
			assert.NoDirExists(t, old.localDir)
			assert.DirExists(t, entry.localDir)
			stats, err := entry.FetchStats()
			require.NoError(t, err)
			assert.Equal(t, fresh.stats[102].NumRow(), stats[102].NumRow())
			assert.Equal(t, entry.diskSize, o.diskSize())
			assertCurrentField(t, o, 102, storage.NewBM25Stats())
			syncWithHook(t, o, syncTestSnapshot(3, 1), func() {})
			assertCurrentField(t, o, 102, fresh.stats[102])
		})
	}
}

// A remote download must not hold up retirement, the next sync, or schema updates.
func TestIDFCleanupDoesNotWaitForReopenDownload(t *testing.T) {
	for _, firstTarget := range []bool{false, true} {
		name := "later-target"
		if firstTarget {
			name = "first-target"
		}
		t.Run(name, func(t *testing.T) {
			o := newSyncTestOracle(t, 102, 104)
			if !firstTarget {
				o.targetVersion.Store(1)
			}
			stats := genBM25StatsForField(102, 1, 4)
			stats[104] = genBM25StatsForField(104, 1, 4)[104]
			seg := newSyncTestSegment(t, 1, stats)
			seg.load(t, o, 102)
			old, ok := o.sealed.Get(1)
			require.True(t, ok)
			data, err := stats[104].Serialize()
			require.NoError(t, err)
			started, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			cm := mocks.NewChunkManager(t)
			cm.EXPECT().Reader(mock.Anything, seg.remotePath(104)).RunAndReturn(func(context.Context, string) (storage.FileReader, error) {
				close(started)
				<-resume
				return &bytesFileReader{bytes.NewReader(data)}, nil
			}).Once()
			reopen := runIDFCleanupCall(func() error { return o.LoadSealedForReopen(context.Background(), 1, seg.loadInfo(102, 104), cm, true) })
			t.Cleanup(func() { release(); _ = reopen.result(t) })
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("reopen download did not start")
			}
			sync := runIDFCleanupCall(func() error {
				o.SetNext(syncTestSnapshot(2))
				if err := o.SyncDistribution(); err != nil {
					return err
				}
				o.next.SetSnapshot(syncTestSnapshot(3))
				if err := o.SyncDistribution(); err != nil {
					return err
				}
				return o.SyncFunctions(syncTestFunctions(102, 104))
			})
			t.Cleanup(func() { release(); _ = sync.result(t) })
			require.NoError(t, sync.result(t))
			assert.Equal(t, int64(3), o.TargetVersion())
			assert.NoDirExists(t, old.localDir)
			assert.Zero(t, o.diskSize())
			release()
			require.True(t, merr.IsRetryableErr(reopen.result(t)))
			dirs, err := os.ReadDir(o.dirPath)
			require.NoError(t, err)
			assert.Empty(t, dirs, "failed reopen must remove its actual staging directory")
			require.NoError(t, o.LoadSealedForReopen(context.Background(), 1, seg.loadInfo(102, 104), seg.cm, false))
			entry, ok := o.sealed.Get(1)
			require.True(t, ok)
			assert.ElementsMatch(t, []int64{102, 104}, entry.FieldList())
			assert.Equal(t, entry.diskSize, o.diskSize())
			assertCurrentField(t, o, 102, storage.NewBM25Stats())
			syncWithHook(t, o, syncTestSnapshot(4, 1), func() {})
			assertCurrentField(t, o, 102, stats[102])
			assertCurrentField(t, o, 104, stats[104])
		})
	}
}

// A failed second rename must not publish or strand the first field. Retrying
// also replaces any unpublished field directory left by a previous failed cleanup.
func TestIDFReopenRenameFailureCanRetry(t *testing.T) {
	o := newSyncTestOracle(t, 102, 104, 106)
	stats := genBM25StatsForField(102, 1, 4)
	stats[104] = genBM25StatsForField(104, 1, 4)[104]
	stats[106] = genBM25StatsForField(106, 1, 4)[106]
	seg := newSyncTestSegment(t, 1, stats)
	seg.load(t, o, 102)
	entry, ok := o.sealed.Get(1)
	require.True(t, ok)
	diskBefore := o.diskSize()
	calls := 0
	patch := mockey.Mock(os.Rename).When(func(_, dst string) bool {
		if path.Dir(dst) != entry.localDir {
			return false
		}
		calls++
		return calls == 2
	}).Return(os.ErrPermission).Build()
	t.Cleanup(func() { patch.UnPatch() })
	err := o.LoadSealedForReopen(context.Background(), 1, seg.loadInfo(102, 104, 106), seg.cm, true)
	require.Error(t, err)
	require.Equal(t, 2, calls)
	patch.UnPatch()
	assert.ElementsMatch(t, []int64{102}, entry.FieldList())
	assert.Equal(t, diskBefore, o.diskSize())
	assertCurrentField(t, o, 102, stats[102])
	assertCurrentField(t, o, 104, storage.NewBM25Stats())
	assertCurrentField(t, o, 106, storage.NewBM25Stats())
	dirs, err := os.ReadDir(o.dirPath)
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	files, err := os.ReadDir(entry.localDir)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, "102", files[0].Name())
	// Deliberately leave garbage at an unpublished destination, including an
	// empty directory: rename behavior differs across supported platforms.
	require.NoError(t, os.MkdirAll(path.Join(entry.localDir, "104"), 0o700))
	require.NoError(t, os.WriteFile(path.Join(entry.localDir, "104", "stale.data"), []byte("stale"), 0o600))
	require.NoError(t, os.MkdirAll(path.Join(entry.localDir, "106"), 0o700))
	require.NoError(t, o.LoadSealedForReopen(context.Background(), 1, seg.loadInfo(102, 104, 106), seg.cm, true))
	assert.ElementsMatch(t, []int64{102, 104, 106}, entry.FieldList())
	fetched, err := entry.FetchStats()
	require.NoError(t, err)
	for _, id := range []int64{102, 104, 106} {
		assert.Equal(t, stats[id].NumRow(), fetched[id].NumRow())
		assertCurrentField(t, o, id, stats[id])
	}
	assert.Equal(t, entry.diskSize, o.diskSize())
	require.NoError(t, o.LoadSealedForReopen(context.Background(), 1, seg.loadInfo(102, 104, 106), seg.cm, true))
	for _, id := range []int64{102, 104, 106} {
		assertCurrentField(t, o, id, stats[id])
	}
	dirs, err = os.ReadDir(o.dirPath)
	require.NoError(t, err)
	assert.Len(t, dirs, 1)
}

func TestIDFObserveRemovedEntryIsRetryable(t *testing.T) {
	o := newSyncTestOracle(t, 102)
	seg := newSyncTestSegment(t, 1, genBM25StatsForField(102, 1, 4))
	seg.load(t, o, 102)
	old, ok := o.sealed.Get(1)
	require.True(t, ok)
	view, err := observeForReopen(old, true)
	require.NoError(t, err)
	syncWithHook(t, o, syncTestSnapshot(1), func() {})
	for _, activate := range []bool{false, true} {
		_, err := observeForReopen(old, activate)
		require.True(t, merr.IsRetryableErr(err))
	}
	_, err = o.activateExistingSealedStats(1, view)
	require.True(t, merr.IsRetryableErr(err))
}

// Cancellation and typed remote failures clean only the attempt's unique dir;
// an existing entry must remain usable and retain its disk charge.
func TestIDFDownloadFailureOwnsCleanup(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		name := "load"
		if reopen {
			name = "reopen"
		}
		t.Run(name, func(t *testing.T) {
			o := newSyncTestOracle(t, 102, 104)
			seg := newSyncTestSegment(t, 1, genBM25StatsForField(102, 1, 4))
			seg.load(t, o, 102)
			entry, ok := o.sealed.Get(1)
			require.True(t, ok)
			diskBefore := o.diskSize()
			for _, failure := range []error{context.Canceled, merr.WrapErrServiceUnavailableMsg("remote storage unavailable")} {
				cm := mocks.NewChunkManager(t)
				stagingDir := ""
				cm.EXPECT().Reader(mock.Anything, "failure").RunAndReturn(func(context.Context, string) (storage.FileReader, error) {
					dirs, err := os.ReadDir(o.dirPath)
					require.NoError(t, err)
					for _, dir := range dirs {
						candidate := path.Join(o.dirPath, dir.Name())
						if candidate != entry.localDir {
							stagingDir = candidate
						}
					}
					require.NotEmpty(t, stagingDir)
					return nil, failure
				}).Once()
				info := &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, "failure")}
				var err error
				if reopen {
					err = o.LoadSealedForReopen(context.Background(), 1, info, cm, true)
				} else {
					err = o.LoadSealed(context.Background(), 2, info, cm)
				}
				require.ErrorIs(t, err, failure)
				assert.Equal(t, merr.IsRetryableErr(failure), merr.IsRetryableErr(err))
				assert.NoDirExists(t, stagingDir)
				assert.DirExists(t, entry.localDir)
				assert.Equal(t, diskBefore, o.diskSize())
				fetched, err := entry.FetchStats()
				require.NoError(t, err)
				assert.Equal(t, seg.stats[102].NumRow(), fetched[102].NumRow())
			}
		})
	}
}

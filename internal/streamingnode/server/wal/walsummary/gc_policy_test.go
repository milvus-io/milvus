package walsummary

import (
	"context"
	"io/fs"
	"os"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestGCCapacityAndAgeBoundaries(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		floor uint64
		ages  []time.Duration
		want  int
	}{
		{"empty", 0, nil, 0},
		{"capacity equal", 300, []time.Duration{48 * time.Hour, 36 * time.Hour, 30 * time.Hour}, 0},
		{"capacity below", 400, []time.Duration{48 * time.Hour, 36 * time.Hour, 30 * time.Hour}, 0},
		{"exact capacity boundary", 200, []time.Duration{48 * time.Hour, 36 * time.Hour, 30 * time.Hour}, 1},
		{"whole capacity boundary", 201, []time.Duration{48 * time.Hour, 36 * time.Hour, 30 * time.Hour}, 0},
		{"time equal", 0, []time.Duration{24 * time.Hour}, 0},
		{"time recent", 0, []time.Duration{23 * time.Hour}, 0},
		{"time future", 0, []time.Duration{-time.Hour}, 0},
		{"prefix stops at time", 0, []time.Duration{48 * time.Hour, 24 * time.Hour, time.Hour}, 1},
		{"no capacity floor", 0, []time.Duration{48 * time.Hour, 30 * time.Hour}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := NewManager(ManagerConfig{MaxRetentionBytes: tc.floor, MaxRetentionDuration: 24 * time.Hour})
			for i, age := range tc.ages {
				tt := tsoutil.ComposeTSByTime(now.Add(-age))
				entry := &streamingpb.PChannelSummaryChunkIndexEntry{Generation: uint64(i), Term: 1, ObjectSize: 100, StartTimeTick: tt, EndTimetick: tt}
				m.manifest.Chunks = append(m.manifest.Chunks, entry)
				m.chunkIndex.chunks = append(m.chunkIndex.chunks, m.chunkIndex.newChunk(entry, nil))
			}
			require.Len(t, m.computeRetentionLocked(now), tc.want)
			if len(tc.ages) == 1 && tc.name == "time recent" {
				m.manifest.Chunks[0].StartTimeTick = tsoutil.ComposeTSByTime(now.Add(-48 * time.Hour))
				require.True(t, m.retentionTriggeredLocked(now))
				require.Empty(t, m.computeRetentionLocked(now), "old start must not authorize deletion of a recent end")
			}
		})
	}
}

func TestGCPreservesIdempotencyRecovery(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	for _, tc := range []struct {
		name     string
		ttl      time.Duration
		budget   int
		retained int
	}{
		{"whole budget boundary", 24 * time.Hour, 1, 1},
		{"all valid when budget is not filled", 24 * time.Hour, 1 << 20, 3},
		{"expired history does not pad capacity", 4 * time.Hour, 1 << 20, 1},
		{"TTL boundary excluded", 3 * time.Hour, 1 << 20, 0},
		{"unlimited TTL", 0, 1 << 20, 4},
		{"disabled cache", 24 * time.Hour, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, store := newTestManagerWithStore(t)
			ctx := t.Context()
			for _, age := range []time.Duration{30 * time.Hour, 12 * time.Hour, 6 * time.Hour, 3 * time.Hour} {
				require.NoError(t, stageChunk(t, m, tsoutil.ComposeTSByTime(now.Add(-age))).Execute(ctx))
			}
			require.NoError(t, drainSummary(ctx, m))
			m.cfg.MaxRetentionBytes = 0
			m.cfg.MaxRetentionDuration = time.Hour
			m.cfg.IdempotencyRetention = idempotencyview.RetentionConfig{TTL: tc.ttl, MaxSize: tc.budget}
			before, err := m.ReadIdempotencyRecovery(ctx, m.cfg.IdempotencyRetention, now)
			require.NoError(t, err)
			require.NoError(t, gcSummary(ctx, m))
			require.Len(t, m.Manifest().Chunks, tc.retained)
			successor := newTestManager(t, nextTermStore(store), 0)
			require.NoError(t, successor.Restore(ctx))
			after, err := successor.ReadIdempotencyRecovery(ctx, m.cfg.IdempotencyRetention, now)
			require.NoError(t, err)
			require.Equal(t, before, after, "GC must preserve the same durable recovery result across restart")
		})
	}
}

func TestGCHotRecordsDoNotReleaseDurableRecoveryHistory(t *testing.T) {
	m, _ := newTestManagerWithStore(t)
	ctx := t.Context()
	now := time.Now()
	for _, age := range []time.Duration{12 * time.Hour, 6 * time.Hour} {
		require.NoError(t, stageChunk(t, m, tsoutil.ComposeTSByTime(now.Add(-age))).Execute(ctx))
	}
	require.NoError(t, drainSummary(ctx, m))
	m.cfg.MaxRetentionBytes = 0
	m.cfg.MaxRetentionDuration = time.Hour
	m.cfg.IdempotencyRetention = idempotencyview.RetentionConfig{TTL: 24 * time.Hour, MaxSize: 1}
	stageChunk(t, m, tsoutil.ComposeTSByTime(now.Add(-30*time.Minute))) // sealed, not uploaded
	finalized := false
	observeKeyedInsert(t, m, "v1", tsoutil.ComposeTSByTime(now), &finalized)
	require.Len(t, m.computeRetentionLocked(now), 1, "only oldest durable chunk can be retired")
}

func TestGCTimersAndCancellation(t *testing.T) {
	var regular, orphan atomic.Int64
	regularPatch := mockey.Mock((*Manager).GCOnce).To(func(*Manager, context.Context) error {
		regular.Add(1)
		return merr.WrapErrServiceUnavailableMsg("retry at next tick")
	}).Build()
	defer regularPatch.UnPatch()
	orphanPatch := mockey.Mock((*Manager).OrphanGCOnce).To(func(*Manager) { orphan.Add(1) }).Build()
	defer orphanPatch.UnPatch()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		m := NewManager(ManagerConfig{GCInterval: 10 * time.Minute, OrphanGCInterval: 24 * time.Hour})
		done := make(chan struct{})
		go func() { m.RunGC(ctx); close(done) }()
		synctest.Wait()
		require.Zero(t, regular.Load())
		require.Zero(t, orphan.Load())
		time.Sleep(10 * time.Minute)
		synctest.Wait()
		require.EqualValues(t, 1, regular.Load())
		require.Zero(t, orphan.Load())
		time.Sleep(24*time.Hour - 10*time.Minute)
		synctest.Wait()
		require.EqualValues(t, 144, regular.Load())
		require.EqualValues(t, 1, orphan.Load())
		cancel()
		synctest.Wait()
		<-done
	})
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		m := NewManager(ManagerConfig{})
		done := make(chan struct{})
		go func() { m.RunGC(ctx); close(done) }()
		time.Sleep(48 * time.Hour)
		synctest.Wait()
		require.EqualValues(t, 144, regular.Load())
		require.EqualValues(t, 1, orphan.Load())
		cancel()
		synctest.Wait()
		<-done
	})
}

func TestOrphanGCFileAgeAndOwnership(t *testing.T) {
	ctx := t.Context()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.NoError(t, drainSummary(ctx, m))
	m.cfg.MaxRetentionDuration = 24 * time.Hour
	old := time.Now().Add(-48 * time.Hour)
	future := time.Now().Add(time.Hour)
	// A retained object stays, regardless of age. Recent files with ancient log
	// timestamps also stay, as do recoverable current-term tails and future terms.
	require.NoError(t, os.Chtimes(store.ChunkKey(0), old, old))
	for _, tc := range []struct {
		gen      uint64
		term     int64
		modified time.Time
		keep     bool
	}{
		{1, 0, old, false},
		{2, 0, time.Now(), true},
		{3, 0, future, true},
		{1, 1, old, true},
		{1, 2, old, true},
	} {
		s := NewStore(store.chunkManager, store.PChannel(), tc.term)
		key := s.ChunkKey(tc.gen)
		require.NoError(t, store.chunkManager.Write(ctx, key, []byte("orphan")))
		require.NoError(t, os.Chtimes(key, tc.modified, tc.modified))
	}
	for _, term := range []int64{0, 2} {
		s := NewStore(store.chunkManager, store.PChannel(), term)
		require.NoError(t, s.WriteManifest(ctx, &streamingpb.PChannelSummaryManifest{}))
		require.NoError(t, os.Chtimes(buildManifestKey(store.chunkManager, store.PChannel(), term), old, old))
	}
	m.OrphanGCOnce()
	task := m.orphanGCTask
	m.OrphanGCOnce()
	require.Same(t, task, m.orphanGCTask)
	require.NoError(t, task.Execute(ctx))
	require.True(t, task.Done())
	require.NoError(t, task.Execute(ctx))
	for _, tc := range []struct {
		gen  uint64
		term int64
		keep bool
	}{{0, 1, true}, {1, 0, false}, {2, 0, true}, {3, 0, true}, {1, 1, true}, {1, 2, true}} {
		exists, err := store.chunkManager.Exist(ctx, buildChunkKey(store.chunkManager, store.PChannel(), tc.gen, tc.term))
		require.NoError(t, err)
		require.Equal(t, tc.keep, exists)
	}
	for _, term := range []int64{0, 1, 2} {
		exists, err := store.chunkManager.Exist(ctx, buildManifestKey(store.chunkManager, store.PChannel(), term))
		require.NoError(t, err)
		require.Equal(t, term != 0, exists)
	}
}

func TestOrphanAgeBoundary(t *testing.T) {
	cutoff := time.Now().Add(-24 * time.Hour)
	require.False(t, orphanOldEnough(time.Time{}, cutoff))
	require.False(t, orphanOldEnough(cutoff, cutoff))
	require.False(t, orphanOldEnough(cutoff.Add(time.Nanosecond), cutoff))
	require.True(t, orphanOldEnough(cutoff.Add(-time.Nanosecond), cutoff))
	require.Zero(t, idempotencyCutoff(time.Unix(0, 0), time.Hour))
}

func TestGCBatchRetriesAndTerminalState(t *testing.T) {
	m, _ := newTestManagerWithStore(t)
	ctx := t.Context()
	require.NoError(t, m.Restore(ctx))
	require.NoError(t, drainSummary(ctx, m))
	task := &summaryGCTask{manager: m}
	for i := 0; i < orphanSweepBudget+1; i++ {
		task.chunks = append(task.chunks, &streamingpb.PChannelSummaryChunkRef{Generation: uint64(i), Term: 0})
	}
	var calls int
	patch := mockey.Mock((*Store).DeleteChunk).To(func(*Store, context.Context, uint64, int64) error { calls++; return nil }).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, task.Execute(ctx), nodescheduler.ErrDelay)
	require.Equal(t, orphanSweepBudget, calls)
	require.False(t, task.Done())
	require.NoError(t, task.Execute(ctx))
	require.True(t, task.Done())
	require.NoError(t, task.Execute(ctx))
	require.Equal(t, orphanSweepBudget+1, calls)
	m.terminalErr = merr.WrapErrServiceUnavailableMsg("closed")
	task = &summaryGCTask{manager: m}
	require.ErrorIs(t, task.Execute(ctx), m.terminalErr)
	require.True(t, task.Done())
	orphan := &summaryOrphanGCTask{manager: m}
	require.ErrorIs(t, orphan.Execute(ctx), m.terminalErr)
	require.True(t, orphan.Done())
	require.NoError(t, orphan.Execute(ctx))
	m.OrphanGCOnce()
	require.Nil(t, m.orphanGCTask)
	require.ErrorIs(t, m.GCOnce(ctx), m.terminalErr)
}

func TestOrphanGCRetriesSweepFailuresAndBatches(t *testing.T) {
	m, _ := newTestManagerWithStore(t)
	ctx := t.Context()
	require.NoError(t, m.Restore(ctx))
	require.NoError(t, drainSummary(ctx, m))
	m.OrphanGCOnce()
	failure := merr.WrapErrServiceUnavailableMsg("listing unavailable")
	patch := mockey.Mock((*Store).sweepGarbage).Return(0, false, failure).Build()
	defer patch.UnPatch()
	requireSummaryError(t, m.orphanGCTask.Execute(ctx), nodescheduler.ErrDelay)
	require.False(t, m.orphanGCTask.Done())
	patch.UnPatch()
	patch = mockey.Mock((*Store).sweepGarbage).Return(orphanSweepBudget, false, nil).Build()
	defer patch.UnPatch()
	requireSummaryError(t, m.orphanGCTask.Execute(ctx), nodescheduler.ErrDelay)
	require.False(t, m.orphanGCTask.Done())
	patch.UnPatch()
	require.NoError(t, m.orphanGCTask.Execute(ctx))
	require.True(t, m.orphanGCTask.Done())
}

func TestObsoleteManifestAgeAndStorageFailures(t *testing.T) {
	_, store := newTestManagerWithStore(t)
	ctx := t.Context()
	cutoff := time.Now().Add(-24 * time.Hour)
	old := cutoff.Add(-time.Hour)
	prefix := buildManifestPrefix(store.chunkManager, store.PChannel())
	for _, name := range []string{"bad", "-1", "0", "1", "2", "3"} {
		key := prefix + name
		require.NoError(t, store.chunkManager.Write(ctx, key, []byte("manifest")))
		modified := old
		if name == "1" {
			modified = time.Now()
		}
		require.NoError(t, os.Chtimes(key, modified, modified))
	}
	failure := merr.WrapErrServiceUnavailableMsg("object store failure")
	patch := mockey.Mock((*storage.LocalChunkManager).Remove).Return(failure).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, store.DeleteManifestsBelowTerm(ctx, 2, cutoff), failure)
	patch.UnPatch()
	require.NoError(t, store.DeleteManifestsBelowTerm(ctx, 2, cutoff))
	for _, name := range []string{"bad", "-1", "0", "1", "2", "3"} {
		exists, err := store.chunkManager.Exist(ctx, prefix+name)
		require.NoError(t, err)
		require.Equal(t, name != "0", exists, name)
	}
	require.NoError(t, store.chunkManager.Write(ctx, prefix+"0", []byte("manifest")))
	require.NoError(t, os.Chtimes(prefix+"0", old, old))
	patch = mockey.Mock((*storage.LocalChunkManager).Remove).Return(fs.ErrNotExist).Build()
	defer patch.UnPatch()
	require.NoError(t, store.DeleteManifestsBelowTerm(ctx, 2, cutoff))
	patch.UnPatch()
	for _, err := range []error{failure, fs.ErrNotExist} {
		patch = mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).Return(err).Build()
		defer patch.UnPatch()
		got := store.DeleteManifestsBelowTerm(ctx, 2, cutoff)
		if err == failure {
			require.ErrorIs(t, got, failure)
		} else {
			require.NoError(t, got)
		}
		patch.UnPatch()
	}
}

func TestGCRefreshRechecksRetirementState(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "concurrent retirement", true: "terminal during refresh"}[terminal], func(t *testing.T) {
			m, _ := newTestManagerWithStore(t)
			ctx := t.Context()
			require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
			require.NoError(t, drainSummary(ctx, m))
			calls := 0
			failure := merr.WrapErrServiceUnavailableMsg("WAL closed")
			m.cfg.FetchDataViewRetention = func(context.Context) (map[string]uint64, error) {
				calls++
				if terminal {
					m.mu.Lock()
					m.terminalErr = failure
					m.mu.Unlock()
				} else {
					m.cfg.FetchDataViewRetention = nil
					require.NoError(t, m.GCOnce(ctx))
				}
				return nil, nil
			}
			require.NoError(t, m.GCOnce(ctx))
			require.Zero(t, calls, "capacity below threshold must not request a Coord refresh")
			m.cfg.MaxRetentionBytes = 0
			version := m.manifestVersion
			err := m.GCOnce(ctx)
			if terminal {
				require.ErrorIs(t, err, failure)
				require.Len(t, m.Manifest().Chunks, 1)
			} else {
				require.NoError(t, err)
				require.Equal(t, version+1, m.manifestVersion, "concurrent refreshes must not retire twice")
				require.NoError(t, drainSummary(ctx, m))
			}
			require.Equal(t, 1, calls)
		})
	}
	m := NewManager(ManagerConfig{})
	m.OrphanGCOnce()
	require.Nil(t, m.orphanGCTask)
}

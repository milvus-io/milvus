package walsummary

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDataViewRetentionRefreshBeforeGC(t *testing.T) {
	m, _ := newTransformTestManagerWithStore(t)
	ctx := t.Context()
	m.SetDataViewRetention("v1", 1)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10, 1, 10))
	require.NoError(t, persistSummary(ctx, m))
	m.cfg.MaxRetentionBytes = 0
	m.AdvanceGCTimeTick("v1", 100)
	require.NoError(t, m.GCOnce(ctx))
	require.Len(t, m.manifest.Chunks, 1, "durable M must not override the origin pin")
	var fetchErr error
	frontiers := map[string]uint64{"v1": 9}
	m.cfg.FetchDataViewRetention = func(context.Context) (map[string]uint64, error) { return frontiers, fetchErr }
	require.NoError(t, m.GCOnce(ctx))
	require.Len(t, m.manifest.Chunks, 1)
	require.Equal(t, uint64(9), m.dataViewRetention["v1"])
	frontiers["v1"] = 10
	fetchErr = merr.WrapErrServiceUnavailableMsg("Coord unavailable")
	require.ErrorIs(t, m.GCOnce(ctx), fetchErr)
	require.Len(t, m.manifest.Chunks, 1)
	require.Equal(t, uint64(9), m.dataViewRetention["v1"])
	fetchErr = nil
	frontiers = nil
	require.NoError(t, m.GCOnce(ctx))
	require.Equal(t, uint64(9), m.dataViewRetention["v1"], "missing or dropped channels do not lose protection")
	require.Len(t, m.manifest.Chunks, 1)
	frontiers = map[string]uint64{"v1": 10}
	require.NoError(t, gcSummary(ctx, m))
	require.Empty(t, m.manifest.Chunks)
}

func TestDataViewRetentionReleasesOnlyContinuousPrefix(t *testing.T) {
	m, store := newTransformTestManagerWithStore(t)
	ctx := t.Context()
	m.SetDataViewRetention("idle", 1)
	m.SetDataViewRetention("a", 15)
	m.SetDataViewRetention("b", 2)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "b", 10, 1, 10))
	require.NoError(t, persistSummary(ctx, m))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "a", 20, 1, 20))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "b", 25, 1, 25))
	require.NoError(t, persistSummary(ctx, m))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "b", 30, 1, 30))
	require.NoError(t, persistSummary(ctx, m))
	original := m.Manifest().Chunks
	require.Len(t, original, 3)
	m.cfg.MaxRetentionBytes = 0
	frontiers := map[string]uint64{"b": 100}
	m.cfg.FetchDataViewRetention = func(context.Context) (map[string]uint64, error) { return frontiers, nil }
	require.NoError(t, gcSummary(ctx, m))
	require.Equal(t, original[1:], m.Manifest().Chunks, "idle/a cannot block a b-only head; mixed chunk blocks the entire suffix")
	require.Equal(t, uint64(15), m.dataViewRetention["a"], "missing frontier keeps its origin")
	require.Empty(t, m.gcFrontiers, "no materialization is needed for Summary retirement")
	_, _, err := store.ReadChunk(ctx, original[0].Generation, original[0].Term)
	require.Error(t, err)
	for _, chunk := range original[1:] {
		_, _, err = store.ReadChunk(ctx, chunk.Generation, chunk.Term)
		require.NoError(t, err)
	}
	frontiers["a"] = 20
	require.NoError(t, gcSummary(ctx, m))
	require.Empty(t, m.manifest.Chunks)
}

func TestDataViewRetentionConcurrentShardKeepsOrigin(t *testing.T) {
	m, _ := newTransformTestManagerWithStore(t)
	ctx := t.Context()
	m.SetDataViewRetention("old", 1)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "old", 10, 1, 10))
	require.NoError(t, persistSummary(ctx, m))
	m.cfg.MaxRetentionBytes = 0
	m.cfg.FetchDataViewRetention = func(ctx context.Context) (map[string]uint64, error) {
		m.SetDataViewRetention("new", 15)
		m.ObserveMessage(ctx, newTestDeleteMessage(t, "new", 20, 1, 20))
		require.NoError(t, persistSummary(ctx, m))
		return map[string]uint64{"old": 100}, nil
	}
	require.NoError(t, gcSummary(ctx, m))
	require.Len(t, m.manifest.Chunks, 1)
	require.Equal(t, "new", m.manifest.Chunks[0].Vchannels[0].Vchannel)
	require.Equal(t, uint64(15), m.dataViewRetention["new"])
}

func TestDataViewRetentionUnknownChannelBlocksOnlyItsPrefix(t *testing.T) {
	m, _ := newTransformTestManagerWithStore(t)
	ctx := t.Context()
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "unknown", 10, 1, 10))
	require.NoError(t, persistSummary(ctx, m))
	m.cfg.MaxRetentionBytes = 0
	m.cfg.FetchDataViewRetention = func(context.Context) (map[string]uint64, error) {
		return map[string]uint64{"other": 100}, nil
	}
	require.NoError(t, m.GCOnce(ctx))
	require.Len(t, m.manifest.Chunks, 1)
	require.Zero(t, m.dataViewRetention["unknown"])
	require.NotContains(t, m.dataViewRetention, "other", "Coord does not own VChannel lifetime registration")
}

func TestDataViewRetentionDropIgnoresInflightRefresh(t *testing.T) {
	m, _ := newTransformTestManagerWithStore(t)
	ctx := t.Context()
	m.SetDataViewRetention("v1", 1)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10, 1, 10))
	require.NoError(t, persistSummary(ctx, m))
	m.cfg.MaxRetentionBytes = 0
	require.NoError(t, gcSummary(ctx, m))
	require.Len(t, m.Manifest().Chunks, 1)
	require.False(t, m.CanCleanupVChannel("v1", 10))

	for _, phase := range []string{"drop ready", "durable deletion"} {
		t.Run(phase, func(t *testing.T) {
			if phase == "durable deletion" {
				// Another channel keeps this PChannel eligible for a refresh after
				// v1's history has already retired. Empty summaries skip the RPC.
				m.SetDataViewRetention("v2", 1)
				m.ObserveMessage(ctx, newTestDeleteMessage(t, "v2", 20, 1, 20))
				require.NoError(t, persistSummary(ctx, m))
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			m.cfg.FetchDataViewRetention = func(ctx context.Context) (map[string]uint64, error) {
				close(entered)
				select {
				case <-resume:
					return map[string]uint64{"v1": 1}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- m.GCOnce(fetchCtx) }()
			select {
			case <-entered:
			case <-fetchCtx.Done():
				t.Fatal("GC did not enter the retention RPC")
			}
			if phase == "drop ready" {
				m.SetDataViewRetention("v1", DroppedVChannelTimeTick)
			} else {
				require.True(t, m.CanCleanupVChannel("v1", 10))
				m.AdvanceGCTimeTick("v1", DroppedVChannelTimeTick)
			}
			close(resume)
			require.NoError(t, <-done)
			if phase == "drop ready" {
				require.Equal(t, uint64(DroppedVChannelTimeTick), m.dataViewRetention["v1"])
				require.Empty(t, m.Manifest().Chunks)
				require.False(t, m.CanCleanupVChannel("v1", 10), "retirement is not durable yet")
			} else {
				require.NotContains(t, m.dataViewRetention, "v1", "late RPC must not resurrect a removed lifetime")
			}
			require.NoError(t, drainSummary(ctx, m))
		})
	}
	m.cfg.FetchDataViewRetention = func(context.Context) (map[string]uint64, error) {
		return map[string]uint64{"v1": 1}, nil
	}
	require.NoError(t, gcSummary(ctx, m))
	require.NotContains(t, m.dataViewRetention, "v1", "later stale replies cannot register removed lifetimes either")
}

func TestDataViewRetentionDropRecovery(t *testing.T) {
	for _, retired := range []bool{false, true} {
		t.Run(map[bool]string{false: "before retirement", true: "after retirement"}[retired], func(t *testing.T) {
			m, store := newTransformTestManagerWithStore(t)
			ctx := t.Context()
			m.SetDataViewRetention("v1", 1)
			m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10, 1, 10))
			require.NoError(t, persistSummary(ctx, m))
			m.SetDataViewRetention("v1", DroppedVChannelTimeTick)
			if retired {
				m.cfg.MaxRetentionBytes = 0
				require.NoError(t, gcSummary(ctx, m))
			}
			recovered := newTestManager(t, NewStore(store.chunkManager, store.PChannel(), 2), 0)
			require.NoError(t, recovered.Restore(ctx))
			// Recovery reconstructs the origin from the still-durable VChannelMeta.
			recovered.SetDataViewRetention("v1", 1)
			require.NoError(t, gcSummary(ctx, recovered))
			if !retired {
				require.Len(t, recovered.Manifest().Chunks, 1)
				require.False(t, recovered.CanCleanupVChannel("v1", 10))
			}
			// Re-evaluating dropReady releases the pin again; no new WAL traffic is needed.
			recovered.SetDataViewRetention("v1", DroppedVChannelTimeTick)
			require.NoError(t, gcSummary(ctx, recovered))
			require.True(t, recovered.CanCleanupVChannel("v1", 10))
			recovered.AdvanceGCTimeTick("v1", DroppedVChannelTimeTick)
			require.Empty(t, recovered.dataViewRetention)
		})
	}
}

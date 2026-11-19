package walsummary

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptySummaryRecoveryPreservesOriginalReplayFloor(t *testing.T) {
	ctx := context.Background()
	m, store := newTransformTestManagerWithStore(t)
	require.NoError(t, m.Restore(ctx))
	m.InitLastAcked(10)
	// New vchannel genesis lies after Summary's initial checkpoint. Normal
	// Inserts and TimeTicks advance the checkpoint without creating records.
	m.SetTransformOrigin("target", 20)
	m.ObserveQueryBarrier("target", 40)
	m.ObserveMessage(ctx, newTestBarrierMessage(t, "target", 40))
	require.Equal(t, uint64(10), m.LastAcked(), "checkpoint waits for initial replay evidence")
	require.NoError(t, persistSummary(ctx, m))
	require.Equal(t, uint64(40), m.LastAcked())
	require.Empty(t, m.Manifest().GetChunks())

	recovered := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	recovered.InitLastAcked(40)
	recovered.SetTransformOrigin("target", 20)
	recovered.ObserveQueryBarrier("target", 50)
	batch, err := recovered.ReadTransform(ctx, "target", 19, 50, ReadLimits{})
	require.NoError(t, err)
	require.LessOrEqual(t, batch.FastForwardTimeTick, uint64(20), "the empty prefix is still available after restart")

	// The first later Delete chunk must cover the intervening empty prefix.
	recovered.ObserveMessage(ctx, newTestDeleteMessage(t, "target", 51, 1, 7))
	require.NoError(t, persistSummary(ctx, recovered))
	require.Equal(t, uint64(11), recovered.Manifest().GetCoverage().GetStartTimeTick())
	require.Len(t, recovered.Manifest().GetChunks(), 1)

	// A real GC watermark remains truncation; genesis must not hide it.
	recovered.mu.Lock()
	recovered.manifest.TransformFastForwardTimeTick = map[string]uint64{"target": 45}
	recovered.mu.Unlock()
	batch, err = recovered.ReadTransform(ctx, "target", 19, 51, ReadLimits{})
	require.NoError(t, err)
	require.Greater(t, batch.FastForwardTimeTick, batch.OriginTimeTick)
}

func TestZeroInitialReplayFloorSurvivesEmptyManifestRestart(t *testing.T) {
	ctx := context.Background()
	m, store := newTransformTestManagerWithStore(t)
	require.NoError(t, m.Restore(ctx))
	m.InitLastAcked(0)
	m.ObserveMessage(ctx, newTestBarrierMessage(t, "target", 40))
	require.Zero(t, m.LastAcked())
	require.NoError(t, persistSummary(ctx, m))
	require.NotNil(t, m.Manifest().InitialReplayTimeTick)
	recovered := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	recovered.InitLastAcked(40)
	recovered.SetTransformOrigin("target", 20)
	recovered.ObserveQueryBarrier("target", 50)
	batch, err := recovered.ReadTransform(ctx, "target", 0, 50, ReadLimits{})
	require.NoError(t, err)
	require.Zero(t, batch.FastForwardTimeTick)
	require.Equal(t, uint64(50), batch.CoveredThrough)
}

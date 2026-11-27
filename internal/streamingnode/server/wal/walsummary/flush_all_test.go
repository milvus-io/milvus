package walsummary

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestFlushAllSchedulesPendingSummary(t *testing.T) {
	const receipt = uint64(470702719669370897)
	marker := notificationMessage(message.MessageTypeFlushAll, "", receipt)
	t.Run("pending", func(t *testing.T) {
		ctx := t.Context()
		m, store := newTestManagerWithStore(t)
		m.cfg.FlushMaxBytes = 1 << 20
		m.InitLastAcked(receipt - 2)
		m.ObserveMessage(ctx, plainSummaryInsert(receipt-1))
		require.Positive(t, m.pendingBytes)
		require.Less(t, m.pendingBytes, m.cfg.FlushMaxBytes)

		m.ObserveMessage(ctx, marker)
		require.Equal(t, receipt-2, m.LastAcked())
		// Run only tasks already submitted by observing the marker.
		require.NoError(t, drainSummary(ctx, m))
		require.Equal(t, receipt, m.LastAcked())
		manifest, found, err := store.ReadManifest(ctx)
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, manifest.GetChunks(), 1)
		require.Equal(t, receipt, manifest.GetCoverage().GetEndTimeTick())
	})
	t.Run("empty", func(t *testing.T) {
		m, _ := newTestManagerWithStore(t)
		m.ObserveMessage(t.Context(), marker)
		require.Equal(t, receipt, m.LastAcked())
		require.False(t, m.HasPendingWork())
		require.Empty(t, m.cfg.Runtime.Scheduler.(*recordingScheduler).tasks)
	})
}

func TestFlushAllWaitsOnlyForPrecedingSummaryRecords(t *testing.T) {
	const receipt = uint64(470702719669370897)
	for _, phase := range []string{"chunk_upload", "first_manifest"} {
		t.Run(phase, func(t *testing.T) {
			ctx := t.Context()
			m, store := newTestManagerWithStore(t)
			m.cfg.FlushMaxBytes = 1 << 20
			require.NoError(t, m.Restore(ctx))
			m.InitLastAcked(receipt - 2)
			if phase == "chunk_upload" {
				require.NoError(t, drainSummary(ctx, m))
			}
			m.ObserveMessage(ctx, plainSummaryInsert(receipt-1))
			m.RequestFlushThrough(receipt - 1)
			if phase == "first_manifest" {
				require.NoError(t, m.pendingSealed[0].task.Execute(ctx))
				require.False(t, m.manifestPublished)
			}

			m.ObserveMessage(ctx, notificationMessage(message.MessageTypeFlushAll, "", receipt))
			m.ObserveMessage(ctx, plainSummaryInsert(receipt+1))
			require.Equal(t, receipt-2, m.LastAcked())
			require.NoError(t, drainSummary(ctx, m))
			require.Equal(t, receipt, m.LastAcked())
			m.ObserveMessage(ctx, notificationMessage(message.MessageTypeTimeTick, "", receipt+2))
			require.Equal(t, receipt, m.LastAcked())
			require.Len(t, m.pending, 1)

			manifest, found, err := store.ReadManifest(ctx)
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, manifest.GetChunks(), 1)
			require.Equal(t, receipt-1, manifest.GetCoverage().GetEndTimeTick())
		})
	}
}

func TestFlushAllWithOverlappingChunkWrites(t *testing.T) {
	for _, order := range []string{"in_order", "out_of_order"} {
		t.Run(order, func(t *testing.T) {
			ctx := t.Context()
			m, _ := newTestManagerWithStore(t)
			require.NoError(t, m.Restore(ctx))
			require.NoError(t, drainSummary(ctx, m))
			for _, tick := range []uint64{100, 300} {
				m.ObserveMessage(ctx, plainSummaryInsert(tick))
				m.RequestFlushThrough(tick)
				m.ObserveMessage(ctx, notificationMessage(message.MessageTypeFlushAll, "", tick+100))
			}
			m.ObserveMessage(ctx, plainSummaryInsert(500))
			require.Len(t, m.pendingSealed, 2)
			first, second := m.pendingSealed[0].task, m.pendingSealed[1].task
			if order == "out_of_order" {
				require.NoError(t, second.Execute(ctx))
				require.Less(t, m.LastAcked(), uint64(100))
				require.NoError(t, first.Execute(ctx))
			} else {
				require.NoError(t, first.Execute(ctx))
				require.Equal(t, uint64(200), m.LastAcked())
				require.NoError(t, second.Execute(ctx))
			}
			require.Equal(t, uint64(400), m.LastAcked())
		})
	}
}

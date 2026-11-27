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

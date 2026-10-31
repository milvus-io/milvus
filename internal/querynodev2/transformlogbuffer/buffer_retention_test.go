package transformlogbuffer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestBufferRetentionAfterReplayPinRemoval(t *testing.T) {
	for _, outcome := range []string{"caught up", "canceled", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			owner := New(nil, 1)
			buf := newVChannelBuffer(owner, "p", "v", 50)
			owner.channels["v"] = buf
			for _, start := range []uint64{50, 50, 80, 110} {
				require.NoError(t, buf.acquireLocked(start))
			}
			for _, tick := range []uint64{60, 70, 80, 90, 120} {
				buf.onEntry(&streamingpb.TransformLogEntry{TimeTick: tick})
			}
			buf.onSyncUp(120)
			segment := &fakeSegment{id: 10, vchannel: "v", startAfter: 50}
			if outcome == "failed" {
				segment.err = context.DeadlineExceeded
			}
			reg := newRegistration(buf, segment)
			pending := newRegistration(buf, &fakeSegment{id: 11, vchannel: "v", startAfter: 70})
			defer reg.Unregister()
			defer pending.Unregister()
			buf.pending[reg], buf.pending[pending] = reg, pending
			buf.releaseGuard(50)
			requireRetention(t, buf, 50, []uint64{60, 70, 80, 90, 120})
			buf.releaseGuard(50)
			requireRetention(t, buf, 50, []uint64{60, 70, 80, 90, 120})
			if outcome == "canceled" {
				reg.Unregister()
			} else {
				buf.mu.Lock()
				buf.scheduleDrainLocked(reg)
				buf.mu.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := reg.WaitCatchup(ctx)
				if outcome == "failed" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				} else {
					require.NoError(t, err)
				}
			}
			require.Eventually(t, func() bool { buf.mu.Lock(); defer buf.mu.Unlock(); return buf.retentionStart == 70 }, time.Second, time.Millisecond)
			requireRetention(t, buf, 70, []uint64{80, 90, 120})
			pending.Unregister()
			requireRetention(t, buf, 80, []uint64{90, 120})
			buf.releaseGuard(80)
			requireRetention(t, buf, 110, []uint64{120})
			buf.releaseGuard(110)
			require.Empty(t, owner.channels)
		})
	}
}

func requireRetention(t *testing.T, buf *vchannelBuffer, start uint64, ticks []uint64) {
	t.Helper()
	buf.mu.Lock()
	defer buf.mu.Unlock()
	require.Equal(t, start, buf.retentionStart)
	var retained []uint64
	for _, entry := range buf.entries {
		retained = append(retained, entry.GetTimeTick())
	}
	require.Equal(t, ticks, retained)
	for _, entry := range buf.entries[len(buf.entries):cap(buf.entries)] {
		require.Nil(t, entry, "trimmed backing-array payload must be released")
	}
}

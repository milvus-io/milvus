package transformlogbuffer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBufferRetentionFollowsMinimumStartAfter(t *testing.T) {
	for _, outcome := range []string{"caught up", "canceled", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			owner := New(nil, 1)
			buf := newVChannelBuffer(owner, "p1", "v1", 50)
			owner.channels["v1"] = buf
			for _, start := range []uint64{50, 50, 80, 110} {
				require.NoError(t, buf.acquireLocked(start))
			}
			for _, tick := range []uint64{60, 70, 80, 90, 120} {
				require.NoError(t, buf.onEntry(&streamingpb.TransformLogEntry{TimeTick: tick}))
			}
			buf.syncUp = true
			segment := &fakeSegment{id: 10, vchannel: "v1", startAfter: 50}
			registered, err := buf.registerSegment(context.Background(), segment)
			require.NoError(t, err)
			defer registered.Unregister()
			// A second queued segment must keep its own older replay range even
			// after the first segment stops pinning history.
			pending := newRegistration(buf, &fakeSegment{id: 11, vchannel: "v1", startAfter: 70})
			buf.pending[11] = pending
			defer pending.Unregister()

			buf.releaseGuard(50)
			require.Equal(t, 1, buf.guards[50])
			requireRetention(t, buf, 50, []uint64{60, 70, 80, 90, 120})
			buf.releaseGuard(50)
			requireRetention(t, buf, 50, []uint64{60, 70, 80, 90, 120})

			var applied []uint64
			failure := merr.WrapErrServiceUnavailableMsg("injected catch-up failure")
			patch := mockey.Mock((*fakeSegment).ApplyTransform).To(func(_ *fakeSegment, _ context.Context, entry *streamingpb.TransformLogEntry) error {
				applied = append(applied, entry.GetTimeTick())
				if outcome == "failed" {
					return failure
				}
				return nil
			}).Build()
			defer patch.UnPatch()
			if outcome == "canceled" {
				registered.Unregister()
			}
			caughtUp := startCatchup(registered)
			switch outcome {
			case "caught up":
				require.NoError(t, awaitCatchup(t, caughtUp))
				require.Equal(t, []uint64{60, 70, 80, 90, 120}, applied)
			case "canceled":
				require.ErrorIs(t, awaitCatchup(t, caughtUp), context.Canceled)
				require.Empty(t, applied)
			case "failed":
				require.ErrorIs(t, awaitCatchup(t, caughtUp), failure)
				require.Equal(t, []uint64{60}, applied)
			}
			requireRetention(t, buf, 70, []uint64{80, 90, 120})

			pending.Unregister()
			requireRetention(t, buf, 80, []uint64{90, 120})
			buf.releaseGuard(80)
			requireRetention(t, buf, 110, []uint64{120})
			if outcome == "caught up" {
				require.NoError(t, buf.onEntry(&streamingpb.TransformLogEntry{TimeTick: 130}))
				require.Equal(t, []uint64{60, 70, 80, 90, 120, 130}, applied)
			}
			buf.releaseGuard(110)
			require.Empty(t, owner.channels)
		})
	}
}

func requireRetention(t *testing.T, buf *vchannelBuffer, start uint64, ticks []uint64) {
	t.Helper()
	require.Equal(t, start, buf.retentionStart)
	var retained []uint64
	for _, entry := range buf.entries {
		retained = append(retained, entry.GetTimeTick())
	}
	require.Equal(t, ticks, retained)
	for _, entry := range buf.entries[len(buf.entries):cap(buf.entries)] {
		require.Nil(t, entry, "trimmed entries must not remain pinned by the backing array")
	}
}

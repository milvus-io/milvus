package replica

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func observed(ticks ...uint64) []message.ImmutableMessage {
	msgs := make([]message.ImmutableMessage, 0, len(ticks))
	for _, ts := range ticks {
		msgs = append(msgs, message.NewMutableMessageBeforeAppend(nil, map[string]string{}).WithTimeTick(ts).IntoImmutableMessage(nil))
	}
	return msgs
}

func TestBufferCaptureAcrossCheckpoint(t *testing.T) {
	b := NewBuffer(10)
	b.Observe(observed(9, 10, 11, 15), 15)
	low, high, suffix, r, err := b.Capture(2)
	require.NoError(t, err)
	require.EqualValues(t, 10, low)
	require.EqualValues(t, 15, high)
	require.Len(t, suffix, 2)
	b.Truncate(20)
	b.Truncate(10)
	b.Observe(observed(15), 15)
	b.Observe(observed(16, 19, 21), 21)
	next, err := r.Next(t.Context())
	require.NoError(t, err)
	require.Len(t, next, 3)
	require.EqualValues(t, 11, suffix[0].TimeTick())
	low, high, suffix, second, err := b.Capture(1)
	require.NoError(t, err)
	require.EqualValues(t, 20, low)
	require.EqualValues(t, 21, high)
	require.Len(t, suffix, 1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = second.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
	r.Close()
	r.Close()
	b.Close()
	b.Observe(observed(22), 22)
	_, err = second.Next(t.Context())
	require.ErrorIs(t, err, io.EOF)
	_, _, _, _, err = b.Capture(1)
	require.ErrorIs(t, err, io.EOF)
}

func TestBufferSlowReader(t *testing.T) {
	b := NewBuffer(0)
	_, _, _, slow, _ := b.Capture(1)
	_, _, _, fast, _ := b.Capture(2)
	b.Observe(observed(1), 1)
	b.Observe(observed(2), 2)
	_, err := slow.Next(t.Context())
	require.ErrorContains(t, err, "fell behind")
	for _, tick := range []int{1, 2} {
		next, err := fast.Next(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, tick, next[0].TimeTick())
	}
	fast.Close()
	b.Close()
}

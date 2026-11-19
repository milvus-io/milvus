package replica

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestPinnedRecoveryAndQueryCapture(t *testing.T) {
	b := NewBuffer(10)
	writer, floor, err := b.Pin(true)
	require.NoError(t, err)
	require.EqualValues(t, 10, floor)
	query, _, err := b.Pin(false)
	require.NoError(t, err)
	b.Observe(observed(11, 12, 13), 13)
	b.Truncate(20)
	batch, err := query.Window(12)
	require.NoError(t, err)
	require.Len(t, batch, 2)
	_, err = query.Window(14)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	batch, err = writer.TakeAvailable()
	require.NoError(t, err)
	require.Len(t, batch, 3)
	require.EqualValues(t, 10, b.floor)
	query.Close()
	require.EqualValues(t, 13, b.floor)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = writer.Next(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, b.WaitThrough(canceled, 14), context.Canceled)
	require.NoError(t, b.WaitThrough(t.Context(), 13))

	barrier := message.NewMutableMessageBeforeAppend(nil, map[string]string{}).WithTimeTick(15).IntoImmutableMessage(walimplstest.NewTestMessageID(8))
	txns := utility.NewTxnBuffer(nil, nil)
	_, ok := writer.TakeTransactions(barrier)
	require.False(t, ok)
	b.ObserveRecoveryBarrier(barrier, txns)
	b.Observe([]message.ImmutableMessage{barrier}, 15)
	snapshot, ok := writer.TakeTransactions(barrier)
	require.True(t, ok)
	require.NotSame(t, txns, snapshot)
	batch, err = writer.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []message.ImmutableMessage{barrier}, batch)
	b.Close()
	_, err = writer.TakeAvailable()
	require.ErrorIs(t, err, io.EOF)
	_, err = writer.Next(t.Context())
	require.ErrorIs(t, err, io.EOF)
	require.ErrorIs(t, b.WaitThrough(t.Context(), 20), io.EOF)
	_, _, err = b.Pin(false)
	require.ErrorIs(t, err, io.EOF)
	writer.Close()
}

func TestWriterProbeRequiresCurrentLink(t *testing.T) {
	b := NewBuffer(1)
	c, _, err := b.Pin(true)
	require.NoError(t, err)
	defer c.Close()
	probe := message.NewMutableMessageBeforeAppend(nil, map[string]string{}).WithTimeTick(8).IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	id := probe.MessageID()
	b.ObserveRecoveryBarrier(probe, utility.NewTxnBuffer(nil, nil))
	b.ObserveSwitch(10, id)
	b.ObserveSwitch(9, walimplstest.NewTestMessageID(2))
	b.Truncate(15)
	epoch, start, err := c.WaitWriterBarrier(t.Context(), 0, probe)
	require.NoError(t, err)
	require.EqualValues(t, 10, epoch)
	require.True(t, id.EQ(start))
	epoch, start = b.WriterPosition()
	require.EqualValues(t, 10, epoch)
	require.True(t, id.EQ(start))
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = c.WaitWriterBarrier(canceled, 10, observed(12)[0])
	require.ErrorIs(t, err, context.Canceled)
	b.Close()
	_, _, err = c.WaitWriterBarrier(t.Context(), 10, probe)
	require.ErrorIs(t, err, io.EOF)
}

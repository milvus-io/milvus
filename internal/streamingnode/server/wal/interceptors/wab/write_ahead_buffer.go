package wab

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var _ ROWriteAheadBuffer = (*WriteAheadBuffer)(nil)

// ROWriteAheadBuffer is the interface of the read-only write-ahead buffer.
type ROWriteAheadBuffer interface {
	// ReadFromExclusiveTimeTick reads messages from the buffer from the exclusive time tick.
	// Return a reader if the timetick can be consumed from the write-ahead buffer, otherwise return error.
	ReadFromExclusiveTimeTick(ctx context.Context, timetick uint64) (*WriteAheadBufferReader, error)
}

// NewWriteAheadBuffer creates a new WriteAheadBuffer.
func NewWirteAheadBuffer(
	pchannel string,
	logger *log.MLogger,
	capacity int,
	keepalive time.Duration,
	lastConfirmedTimeTickMessage message.ImmutableMessage,
) *WriteAheadBuffer {
	return &WriteAheadBuffer{
		logger:              logger,
		cond:                syncutil.NewContextCond(&sync.Mutex{}),
		pendingMessages:     newPendingQueue(capacity, keepalive, lastConfirmedTimeTickMessage),
		lastTimeTickMessage: lastConfirmedTimeTickMessage,
		metrics:             metricsutil.NewWriteAheadBufferMetrics(pchannel, capacity),
	}
}

// WriteAheadBuffer is a buffer that stores messages in order of time tick.
type WriteAheadBuffer struct {
	logger          *log.MLogger
	cond            *syncutil.ContextCond
	closed          bool
	pendingMessages *pendingQueue // The pending message is always sorted by timetick in monotonic ascending order.
	// Only keep the persisted messages in the buffer.
	lastTimeTickMessage message.ImmutableMessage
	metrics             *metricsutil.WriteAheadBufferMetrics
}

// Append appends a message to the buffer.
func (w *WriteAheadBuffer) Append(msgs []message.ImmutableMessage, tsMsg message.ImmutableMessage) {
	w.cond.LockAndBroadcast()
	defer w.cond.L.Unlock()
	if w.closed {
		return
	}

	if tsMsg.MessageType() != message.MessageTypeTimeTick {
		panic("the message is not a time tick message")
	}
	if tsMsg.TimeTick() <= w.lastTimeTickMessage.TimeTick() {
		panic("the time tick of the message is less or equal than the last time tick message")
	}
	if len(msgs) > 0 {
		if msgs[0].TimeTick() <= w.lastTimeTickMessage.TimeTick() {
			panic("the time tick of the message is less than or equal to the last time tick message")
		}
		if msgs[len(msgs)-1].TimeTick() > tsMsg.TimeTick() {
			panic("the time tick of the message is greater than the time tick message")
		}
		// if the len(msgs) > 0, the tsMsg is a persisted message.
		w.pendingMessages.Push(msgs)
		w.pendingMessages.Push([]message.ImmutableMessage{tsMsg})
	} else {
		w.pendingMessages.Evict()
	}
	w.lastTimeTickMessage = tsMsg

	w.metrics.Observe(
		w.pendingMessages.Len(),
		w.pendingMessages.Size(),
		w.pendingMessages.EarliestTimeTick(),
		w.lastTimeTickMessage.TimeTick(),
	)
}

// ReadFromExclusiveTimeTick reads messages from the buffer from the exclusive time tick.
func (w *WriteAheadBuffer) ReadFromExclusiveTimeTick(ctx context.Context, timetick uint64) (*WriteAheadBufferReader, error) {
	snapshot, nextOffset, err := w.createSnapshotFromTimeTick(ctx, timetick)
	if err != nil {
		return nil, err
	}
	return &WriteAheadBufferReader{
		nextOffset:    nextOffset,
		snapshot:      snapshot,
		underlyingBuf: w,
	}, nil
}

// createSnapshotFromOffset creates a snapshot of the buffer from the given offset.
func (w *WriteAheadBuffer) createSnapshotFromOffset(ctx context.Context, offset int, timeTick uint64) ([]messageWithOffset, error) {
	w.cond.L.Lock()
	if w.closed {
		w.cond.L.Unlock()
		return nil, ErrClosed
	}

	for {
		msgs, err := w.pendingMessages.CreateSnapshotFromOffset(offset)
		if err == nil {
			w.cond.L.Unlock()
			return msgs, nil
		}
		if !errors.Is(err, io.EOF) {
			w.cond.L.Unlock()
			return nil, err
		}

		// error is eof, which means that the time tick is behind the message buffer.
		// check if the last time tick is greater than the given time tick.
		// if so, return it to update the timetick.
		// lastTimeTickMessage will never be nil if call this api.
		if w.lastTimeTickMessage.TimeTick() > timeTick {
			msg := messageWithOffset{
				Message: w.lastTimeTickMessage,
				Offset:  w.pendingMessages.CurrentOffset(),
			}
			w.cond.L.Unlock()
			return []messageWithOffset{msg}, nil
		}
		// Block until the buffer updates.
		if err := w.cond.Wait(ctx); err != nil {
			return nil, err
		}
	}
}

// createSnapshotFromTimeTick creates a snapshot of the buffer from the given time tick.
func (w *WriteAheadBuffer) createSnapshotFromTimeTick(ctx context.Context, timeTick uint64) ([]messageWithOffset, int, error) {
	w.cond.L.Lock()
	if w.closed {
		w.cond.L.Unlock()
		return nil, 0, ErrClosed
	}

	for {
		msgs, err := w.pendingMessages.CreateSnapshotFromExclusiveTimeTick(timeTick)
		if err == nil {
			w.cond.L.Unlock()
			return msgs, msgs[0].Offset, nil
		}
		if !errors.Is(err, io.EOF) {
			w.cond.L.Unlock()
			return nil, 0, err
		}

		// error is eof, which means that the time tick is behind the message buffer.
		// The lastTimeTickMessage should always be greater or equal to the lastTimeTick in the pending queue.
		if w.pendingMessages.LastTimeTick() == timeTick {
			offset := w.pendingMessages.CurrentOffset() + 1
			w.cond.L.Unlock()
			return nil, offset, nil
		}

		if w.lastTimeTickMessage.TimeTick() > timeTick {
			// check if the last time tick is greater than the given time tick, return it to update the timetick.
			msg := messageWithOffset{
				Message: w.lastTimeTickMessage,
				Offset:  w.pendingMessages.CurrentOffset(), // We add a extra timetick message, so reuse the current offset.
			}
			w.cond.L.Unlock()
			return []messageWithOffset{msg}, msg.Offset, nil
		}

		if err := w.cond.Wait(ctx); err != nil {
			return nil, 0, err
		}
	}
}

func (w *WriteAheadBuffer) Close() {
	w.cond.L.Lock()
	w.metrics.Close()
	w.closed = true
	w.cond.L.Unlock()
}

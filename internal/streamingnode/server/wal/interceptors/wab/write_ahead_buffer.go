package wab

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

var _ ROWriteAheadBuffer = (*WriteAheadBuffer)(nil)

// ROWriteAheadBuffer is the interface of the read-only write-ahead buffer.
type ROWriteAheadBuffer interface {
	// ReadFromExclusiveTimeTick reads messages from the buffer from the exclusive time tick.
	// Return a reader if the timetick can be consumed from the write-ahead buffer, otherwise return error.
	ReadFromExclusiveTimeTick(ctx context.Context, timetick uint64) (*WriteAheadBufferReader, error)
}

// NewWriteAheadBuffer creates a new WriteAheadBuffer.
func NewWriteAheadBuffer(
	pchannel string,
	logger *mlog.Logger,
	capacity int,
	_ time.Duration, // Retained for compatibility; consumption now controls retention.
	lastConfirmedTimeTickMessage message.ImmutableMessage,
) *WriteAheadBuffer {
	return &WriteAheadBuffer{
		logger:              logger,
		cond:                syncutil.NewContextCond(&sync.Mutex{}),
		pendingMessages:     newPendingQueue(lastConfirmedTimeTickMessage),
		lastTimeTickMessage: lastConfirmedTimeTickMessage,
		metrics:             metricsutil.NewWriteAheadBufferMetrics(pchannel, capacity),
	}
}

// WriteAheadBuffer is a buffer that stores messages in order of time tick.
type WriteAheadBuffer struct {
	logger          *mlog.Logger
	cond            *syncutil.ContextCond
	closed          bool
	pendingMessages *pendingQueue // The pending message is always sorted by timetick in monotonic ascending order.
	// Ordinary volatile TimeTicks may coalesce; scoped MVCCBarriers are queued.
	lastTimeTickMessage message.ImmutableMessage
	metrics             *metricsutil.WriteAheadBufferMetrics
	pressureMu          sync.Mutex // Serializes notifications without holding the queue lock.
	pressureRegistry    *ratelimit.MuxRateLimitObserverRegistryImpl
	pressureWatcher     config.EventHandler
	pressureState       ratelimit.RateLimitState
}

// Append appends a message to the buffer.
func (w *WriteAheadBuffer) Append(msgs []message.ImmutableMessage, tsMsg message.ImmutableMessage) {
	w.cond.LockAndBroadcast()
	defer func() {
		w.cond.L.Unlock()
		w.refreshPressure()
	}()
	if w.closed {
		return
	}

	if tsMsg.MessageType() != message.MessageTypeTimeTick && tsMsg.MessageType() != message.MessageTypeMVCCBarrier {
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
		w.pendingMessages.Push(msgs)
	}
	if tsMsg.IsPersisted() || tsMsg.MessageType() == message.MessageTypeMVCCBarrier {
		// Scoped progress must reach RecoveryStorage after its complete data prefix.
		w.pendingMessages.Push([]message.ImmutableMessage{tsMsg})
	}

	// Durable catchup can observe a confirmation before its WAB publication.
	// Do not retain a late-published batch that RecoveryStorage already consumed.
	w.pendingMessages.ReleaseThrough(w.pendingMessages.readableAfter)
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
		lastTimeTick:  timetick,
		snapshot:      snapshot,
		underlyingBuf: w,
	}, nil
}

// createSnapshotFromOffset creates a snapshot of the buffer from the given offset.
func (w *WriteAheadBuffer) createSnapshotFromOffset(ctx context.Context, offset int, timeTick uint64) ([]messageWithOffset, error) {
	w.cond.L.Lock()
	for {
		if w.closed {
			w.cond.L.Unlock()
			return nil, ErrClosed
		}
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
	for {
		if w.closed {
			w.cond.L.Unlock()
			return nil, 0, ErrClosed
		}
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

// ReleaseThrough is called only after RecoveryStorage has observed the complete
// ordered prefix, including scoped Summary progress and VChannel resource events.
// It is not a persistence checkpoint or a reader's prefetch position.
func (w *WriteAheadBuffer) ReleaseThrough(tick uint64) {
	w.cond.L.Lock()
	if !w.closed {
		w.pendingMessages.ReleaseThrough(tick)
		w.metrics.Observe(w.pendingMessages.Len(), w.pendingMessages.Size(),
			w.pendingMessages.EarliestTimeTick(), w.lastTimeTickMessage.TimeTick())
	}
	w.cond.L.Unlock()
	w.refreshPressure()
}

func (w *WriteAheadBuffer) Close() {
	w.cond.LockAndBroadcast()
	watcher := w.pressureWatcher
	w.pressureWatcher = nil
	if !w.closed {
		w.closed = true
		w.pendingMessages.buf = nil
		w.pendingMessages.size = 0
		w.lastTimeTickMessage = nil
		w.metrics.Close()
	}
	w.cond.L.Unlock()
	if watcher != nil {
		for _, key := range pressureConfigKeys() {
			paramtable.Get().Unwatch(key, watcher)
		}
	}
	w.refreshPressure()
}

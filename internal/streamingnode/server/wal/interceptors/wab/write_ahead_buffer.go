package wab

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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
	maintenanceManager *MaintenanceManager,
	pchannel string,
	logger *mlog.Logger,
	capacity int,
	keepalive time.Duration,
	lastConfirmedTimeTickMessage message.ImmutableMessage,
) *WriteAheadBuffer {
	buffer := &WriteAheadBuffer{
		logger:             logger,
		cond:               syncutil.NewContextCond(&sync.Mutex{}),
		pendingMessages:    newPendingQueue(capacity, keepalive, lastConfirmedTimeTickMessage),
		metrics:            metricsutil.NewWriteAheadBufferMetrics(pchannel, capacity),
		maintenanceManager: maintenanceManager,
	}
	maintenanceManager.register(buffer)
	return buffer
}

// WriteAheadBuffer is a buffer that stores messages in order of time tick.
type WriteAheadBuffer struct {
	logger             *mlog.Logger
	cond               *syncutil.ContextCond
	closed             bool
	pendingMessages    *pendingQueue // The pending message is always sorted by timetick in monotonic ascending order.
	metrics            *metricsutil.WriteAheadBufferMetrics
	maintenanceManager *MaintenanceManager
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
	if tsMsg.TimeTick() <= w.pendingMessages.LastTimeTick() {
		panic("the time tick of the message is less or equal than the last time tick message")
	}

	if len(msgs) > 0 {
		if msgs[0].TimeTick() <= w.pendingMessages.LastTimeTick() {
			panic("the time tick of the message is less than or equal to the last time tick message")
		}
		if msgs[len(msgs)-1].TimeTick() > tsMsg.TimeTick() {
			panic("the time tick of the message is greater than the time tick message")
		}
		w.pendingMessages.Push(msgs)
	}
	w.pendingMessages.Push([]message.ImmutableMessage{tsMsg})
	w.pendingMessages.Evict()

	w.observeMetricsLocked()
}

func (w *WriteAheadBuffer) evictExpiredMessages() {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	if w.closed || !w.pendingMessages.Evict() {
		return
	}
	w.observeMetricsLocked()
}

func (w *WriteAheadBuffer) observeMetricsLocked() {
	w.metrics.Observe(
		w.pendingMessages.Len(),
		w.pendingMessages.Size(),
		w.pendingMessages.EarliestTimeTick(),
		w.pendingMessages.LastTimeTick(),
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
func (w *WriteAheadBuffer) createSnapshotFromOffset(ctx context.Context, offset int) ([]messageWithOffset, error) {
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

		// The requested timetick is exactly the latest persisted message, so the
		// reader can start from the next offset and wait for future appends.
		if w.pendingMessages.LastTimeTick() == timeTick {
			offset := w.pendingMessages.CurrentOffset() + 1
			w.cond.L.Unlock()
			return nil, offset, nil
		}
		if err := w.cond.Wait(ctx); err != nil {
			return nil, 0, err
		}
	}
}

func (w *WriteAheadBuffer) Close() {
	w.cond.L.Lock()
	if w.closed {
		w.cond.L.Unlock()
		return
	}
	w.metrics.Close()
	w.closed = true
	w.cond.L.Unlock()
	w.maintenanceManager.unregister(w)
}

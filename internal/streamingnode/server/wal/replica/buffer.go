// Package replica manages the shared WAL observations of a node-bound replica.
package replica

import (
	"context"
	"io"
	"slices"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Buffer holds transaction-assembled messages above the last published checkpoint.
// Only the scanner that owns this instance may publish batches. A batch ends at
// a confirmation and contains all effects through that confirmation.
type Buffer struct {
	// The last durable cross-backend link survives suffix truncation. A future
	// writer needs its native anchor even if the query buffer is already empty.
	switchTick                 uint64
	switchStart                message.MessageID
	mu                         sync.Mutex
	floor, through, checkpoint uint64
	tail                       []message.ImmutableMessage
	readers                    map[*Reader]struct{}
	closed                     bool
	changed                    chan struct{}
	cursors                    map[*RecoveryCursor]struct{}
}

func NewBuffer(recoveredThrough uint64) *Buffer {
	return &Buffer{changed: make(chan struct{}), cursors: make(map[*RecoveryCursor]struct{}), floor: recoveredThrough, through: recoveredThrough, checkpoint: recoveredThrough, readers: make(map[*Reader]struct{})}
}

// Observe accepts an ordered batch from the scanner. The caller transfers the
// slice and may release its own references to the immutable messages.
func (b *Buffer) Observe(batch []message.ImmutableMessage, through uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || through <= b.through {
		return
	}
	// A physical restart anchor can precede the logical checkpoint.
	for len(batch) > 0 && batch[0].TimeTick() <= b.through {
		batch = batch[1:]
	}
	b.through = through
	b.notifyLocked()
	b.tail = append(b.tail, batch...)
	for r := range b.readers {
		select {
		case r.live <- batch:
		default:
			r.closeLocked(merr.WrapErrServiceUnavailable("WAL replica reader fell behind"))
		}
	}
	b.truncateLocked()
}

func (b *Buffer) Truncate(published uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.checkpoint = max(b.checkpoint, published)
	b.truncateLocked()
}

func (b *Buffer) truncateLocked() {
	floor := min(b.through, b.checkpoint)
	for c := range b.cursors {
		floor = min(floor, c.through)
	}
	if floor <= b.floor {
		return
	}
	b.floor = floor
	n := 0
	for n < len(b.tail) && b.tail[n].TimeTick() <= floor {
		n++
	}
	clear(b.tail[:n])
	b.tail = b.tail[n:]
	if len(b.tail) == 0 {
		b.tail = nil
	}
}

// Capture pins the suffix and registers for later batches at the same point.
// Returned messages are ordinary Go references, independent of the writer's
// durable acknowledgement tracker. Truncation can never acknowledge a write.
func (b *Buffer) Capture(queueCapacity int) (floor, through uint64, suffix []message.ImmutableMessage, reader *Reader, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, 0, nil, nil, io.EOF
	}
	r := &Reader{buffer: b, live: make(chan []message.ImmutableMessage, queueCapacity), done: make(chan struct{})}
	b.readers[r] = struct{}{}
	return b.floor, b.through, slices.Clone(b.tail), r, nil
}

func (b *Buffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for c := range b.cursors {
		c.barrier, c.txns = nil, nil
	}
	clear(b.cursors)
	b.notifyLocked()
	b.tail = nil
	for r := range b.readers {
		r.closeLocked(io.EOF)
	}
}

type Reader struct {
	buffer *Buffer
	live   chan []message.ImmutableMessage
	done   chan struct{}
	err    error
}

func (r *Reader) Next(ctx context.Context) ([]message.ImmutableMessage, error) {
	select {
	case <-r.done:
		return nil, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case batch := <-r.live:
		return batch, nil
	}
}

func (r *Reader) Close() {
	r.buffer.mu.Lock()
	defer r.buffer.mu.Unlock()
	r.closeLocked(io.EOF)
}

func (r *Reader) closeLocked(err error) {
	if r.err != nil {
		return
	}
	delete(r.buffer.readers, r)
	r.err = err
	close(r.done)
	// Release queued references even when the loading thread is blocked on I/O.
	for len(r.live) > 0 {
		select {
		case <-r.live:
		default:
			return
		}
	}
}

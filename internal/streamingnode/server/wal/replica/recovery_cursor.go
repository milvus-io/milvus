package replica

import (
	"context"
	"io"
	"slices"
	"sort"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// RecoveryCursor joins writer recovery to the existing replica scanner.
// The published checkpoint can pass the cursor while metadata is loading;
// keep the outstanding tail pinned until the recovery reader takes it.
type RecoveryCursor struct {
	buffer         *Buffer
	through        uint64
	barrier        message.ImmutableMessage
	txns           *utility.TxnBuffer
	captureBarrier bool
}

// Window returns the pinned interval through a query dispatch frontier.
func (c *RecoveryCursor) Window(through uint64) ([]message.ImmutableMessage, error) {
	b := c.buffer
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.cursors[c]; !ok || through < c.through || through > b.through {
		return nil, io.ErrUnexpectedEOF
	}
	start := sort.Search(len(b.tail), func(i int) bool { return b.tail[i].TimeTick() > c.through })
	end := sort.Search(len(b.tail), func(i int) bool { return b.tail[i].TimeTick() > through })
	return slices.Clone(b.tail[start:end]), nil
}

// Pin holds the buffered tail while a consumer reads its durable baseline.
// A writer also needs unfinished transactions when its recovery barrier arrives.
func (b *Buffer) Pin(writer bool) (*RecoveryCursor, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, 0, io.EOF
	}
	c := &RecoveryCursor{buffer: b, through: b.floor, captureBarrier: writer}
	b.cursors[c] = struct{}{}
	return c, b.floor, nil
}

// ObserveRecoveryBarrier runs on the scanner goroutine, before it publishes
// the batch. Only a recovering writer needs a copy of unfinished transactions.
func (b *Buffer) ObserveRecoveryBarrier(barrier message.ImmutableMessage, txns *utility.TxnBuffer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for c := range b.cursors {
		if c.captureBarrier {
			c.txns = txns.Snapshot()
			c.barrier = barrier
		}
	}
}

func (c *RecoveryCursor) TakeTransactions(barrier message.ImmutableMessage) (*utility.TxnBuffer, bool) {
	b := c.buffer
	b.mu.Lock()
	defer b.mu.Unlock()
	if c.barrier == nil || c.barrier.TimeTick() != barrier.TimeTick() || c.barrier.WALName() != barrier.WALName() || !c.barrier.MessageID().EQ(barrier.MessageID()) {
		return nil, false
	}
	txns := c.txns
	c.barrier, c.txns, c.captureBarrier = nil, nil, false
	return txns, true
}

// TakeAvailable transfers the confirmed suffix to a staged RecoveryStorage.
// Messages observed later are delivered by Next after the new writer opens.
func (c *RecoveryCursor) TakeAvailable() ([]message.ImmutableMessage, error) {
	b := c.buffer
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.cursors[c]; !ok || b.closed {
		return nil, io.EOF
	}
	return c.takeLocked(), nil
}

func (c *RecoveryCursor) takeLocked() []message.ImmutableMessage {
	b := c.buffer
	i := sort.Search(len(b.tail), func(i int) bool { return b.tail[i].TimeTick() > c.through })
	batch := slices.Clone(b.tail[i:])
	c.through = b.through
	b.truncateLocked()
	return batch
}

func (c *RecoveryCursor) Next(ctx context.Context) ([]message.ImmutableMessage, error) {
	b := c.buffer
	for {
		b.mu.Lock()
		if _, ok := b.cursors[c]; !ok || b.closed {
			b.mu.Unlock()
			return nil, io.EOF
		}
		if b.through > c.through {
			batch := c.takeLocked()
			b.mu.Unlock()
			return batch, nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (c *RecoveryCursor) Close() {
	b := c.buffer
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.cursors, c)
	c.barrier, c.txns = nil, nil
	b.truncateLocked()
	b.notifyLocked()
}

func (b *Buffer) WaitThrough(ctx context.Context, through uint64) error {
	for {
		b.mu.Lock()
		closed, ready, changed := b.closed, b.through >= through, b.changed
		b.mu.Unlock()
		if closed {
			return io.EOF
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (b *Buffer) notifyLocked() { close(b.changed); b.changed = make(chan struct{}) }

// ObserveSwitch records the physical route in scanner order, including links
// replayed from a LastConfirmed position before the logical checkpoint.
func (b *Buffer) ObserveSwitch(tick uint64, start message.MessageID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if tick > b.switchTick {
		b.switchTick, b.switchStart = tick, start
		b.notifyLocked()
	}
}

func (b *Buffer) WriterPosition() (uint64, message.MessageID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.switchTick, b.switchStart
}

// WaitWriterBarrier waits for this writer's own probe on the logical stream.
// A newer link wins even on A -> B -> A, where an obsolete probe may become
// physically readable again. The caller then fences and probes the linked leg.
func (c *RecoveryCursor) WaitWriterBarrier(ctx context.Context, tick uint64, probe message.ImmutableMessage) (uint64, message.MessageID, error) {
	b := c.buffer
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return 0, nil, io.EOF
		}
		if b.switchTick > tick {
			next, start := b.switchTick, b.switchStart
			b.mu.Unlock()
			return next, start, nil
		}
		observed, changed := c.barrier, b.changed
		b.mu.Unlock()
		if observed != nil && observed.TimeTick() == probe.TimeTick() && observed.WALName() == probe.WALName() && observed.MessageID().EQ(probe.MessageID()) {
			return tick, nil, nil
		}
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-changed:
		}
	}
}

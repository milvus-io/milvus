package block

import (
	"context"
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/util"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

var (
	_ Block                   = (*MutableBlock)(nil)
	_ walcache.MessageScanner = (*mutableBlockScannerImpl)(nil)
)

// NewMutableBlock creates a new mutable block.
func NewMutableBlock(capacity int, firstMessage message.ImmutableMessage, evictCallback walcache.EvictCallback) *MutableBlock {
	data := make([]message.ImmutableMessage, 0, 16)
	data = append(data, firstMessage)
	return &MutableBlock{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
		// TODO: preallocate.
		data:          data,
		size:          len(firstMessage.Payload()),
		sealed:        false,
		capacity:      capacity,
		evictCallback: evictCallback,
	}
}

// MutableBlock is a block of messages that can be modified.
// A block of message should always be continuous and ordered by asc.
// A MutableBlock is always setted in memory.
type MutableBlock struct {
	cond          *syncutil.ContextCond
	data          []message.ImmutableMessage
	size          int  // the current size of the block, only size of payload message is counted.
	capacity      int  // the capacity of the block, the size can only grow once beyond the capacity.
	sealed        bool // the block is sealed or not, no more message can be appended if sealed.
	evictCallback walcache.EvictCallback
}

// Count returns the message count in the block.
func (b *MutableBlock) Count() (int, bool) {
	return len(b.data), false
}

// Size returns the size of the block.
// A mutable block cannot estimate the size for now.
func (b *MutableBlock) Size() (int, bool) {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()
	return b.size, false
}

// Read returns a scanner that scans messages in the block.
func (b *MutableBlock) Read(started message.MessageID) (walcache.MessageScanner, error) {
	if !b.Range().In(started) {
		return nil, walcache.ErrNotFound
	}
	return newMutableBlockScannerImpl(b, started), nil
}

// Range returns the range of message id in the block.
func (b *MutableBlock) Range() util.MessageIDRange {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()
	// b.data is always not empty.
	if b.sealed {
		// the block is sealed, return the true range of message id.
		return util.MessageIDRange{
			Begin: b.data[0].MessageID(),
			End:   b.data[len(b.data)-1].MessageID(),
		}
	}
	// the block is not sealed, return a half-open range.
	return util.MessageIDRange{
		Begin: b.data[0].MessageID(),
		End:   nil,
	}
}

// Append appends a message to the block.
// Return the count of the message array that has been appended.
// !!! user should ensure the message is continuous with existed message
// and ordered by the asc. so the block should be used in `SPMC` model.
func (b *MutableBlock) Append(msgs []message.ImmutableMessage) int {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	if b.sealed {
		panic("unreachable: the block is sealed, shouldn't receive any more new messages")
	}

	for offset, msg := range msgs {
		// only use payload size to estimate the size of the block.
		newSize := b.size + len(msg.Payload())
		if newSize > b.capacity {
			return offset
		}
		b.size = newSize
		b.data = append(b.data, msg)
		b.cond.UnsafeBroadcast()
	}
	return len(msgs)
}

// IntoImmutable return a immutable block that can't be modified.
// If the block is empty, return nil and seal operation is ignored.
func (b *MutableBlock) IntoImmutable() *ImmutableBlock {
	b.cond.LockAndBroadcast()
	immutableBlock := newImmutableBlock(b.data, b.size, b.evictCallback)
	b.sealed = true
	b.cond.L.Unlock()
	return immutableBlock
}

// blockUntilFindIndex finds the index of the message that is greater than or equal to the given message id.
func (b *MutableBlock) blockUntilFindIndex(ctx context.Context, started message.MessageID) (int, error) {
	if err := b.blockUntilMessageIDReady(ctx, started); err != nil {
		return 0, err
	}
	b.cond.L.Lock()
	data := b.data
	b.cond.L.Unlock()

	// perform a lowerbound search here.
	k := lowerboundOfMessageList(data, started)
	// if the message id is not found, the mutable block is sealed without the started message id,
	// return ErrNotReach to indicate the the message can not reach, but may be read in future.
	if k >= len(data) {
		return 0, walcache.ErrNotReach
	}
	return k, nil
}

// blockUntilMessageIDReady blocks until the message id is ready to read or block is sealed.
func (b *MutableBlock) blockUntilMessageIDReady(ctx context.Context, started message.MessageID) error {
	b.cond.L.Lock()
	for {
		// block until any following conditions are met:
		// 1. block's last message id is less or equal to the started message id.
		// 2. block is sealed.
		if (len(b.data) != 0 && started.LTE(b.data[len(b.data)-1].MessageID())) || b.sealed {
			break
		}
		if err := b.cond.Wait(ctx); err != nil {
			return err
		}
	}
	b.cond.L.Unlock()
	return nil
}

// blockUntilIndexReady blocks until the message at the given index is ready or sealed.
func (b *MutableBlock) blockUntilIndexReady(ctx context.Context, idx int) error {
	b.cond.L.Lock()
	for {
		if idx < len(b.data) {
			b.cond.L.Unlock()
			return nil
		}
		if b.sealed {
			b.cond.L.Unlock()
			// block is sealed and the index is out of range, return EOF to stop scanning.
			return io.EOF
		}
		if err := b.cond.Wait(ctx); err != nil {
			return err
		}
	}
}

// mustGetMessageAtIndex returns the message at the given index.
func (b *MutableBlock) mustGetMessageAtIndex(idx int) message.ImmutableMessage {
	b.cond.L.Lock()
	msg := b.data[idx]
	b.cond.L.Unlock()
	return msg
}

// newMutableBlockScannerImpl creates a new mutable block scanner.
func newMutableBlockScannerImpl(block *MutableBlock, started message.MessageID) *mutableBlockScannerImpl {
	return &mutableBlockScannerImpl{
		b:       block,
		started: started,
		offset:  -1,
	}
}

// mutableBlockScannerImpl is a scanner that scans messages in a mutable block.
type mutableBlockScannerImpl struct {
	b       *MutableBlock
	started message.MessageID
	offset  int
}

// Scan scans the next message.
func (s *mutableBlockScannerImpl) Scan(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.started != nil {
		// find the index of the message's messageID is equal to started
		var err error
		s.offset, err = s.b.blockUntilFindIndex(ctx, s.started)
		if err != nil {
			return err
		}
		s.started = nil
		// offset found and ready to scan, return nil directyly.
		return nil
	}

	// block until the next message is ready and update offset.
	if err := s.b.blockUntilIndexReady(ctx, s.offset+1); err != nil {
		return err
	}
	s.offset += 1
	return nil
}

// Message returns the current message.
func (s *mutableBlockScannerImpl) Message() message.ImmutableMessage {
	return s.b.mustGetMessageAtIndex(s.offset)
}

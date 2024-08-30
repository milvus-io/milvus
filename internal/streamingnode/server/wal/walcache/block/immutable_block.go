package block

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/util"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	_           Block                   = (*ImmutableBlock)(nil)
	_           walcache.MessageScanner = (*immutableBlockScanner)(nil)
	idAllocator                         = typeutil.NewIDAllocator()
)

// newImmutableBlock creates a new immutable block.
func newImmutableBlock(data []message.ImmutableMessage, size int, evictCallback walcache.EvictCallback) *ImmutableBlock {
	if len(data) == 0 {
		panic("unreachable: immutable block should never be empty")
	}
	r := util.MessageIDRange{
		Begin: data[0].MessageID(),
		End:   data[len(data)-1].MessageID(),
	}
	b := &ImmutableBlock{
		cond:          sync.NewCond(&sync.Mutex{}),
		blockID:       idAllocator.Allocate(),
		state:         blockStateUnregistered,
		data:          data,
		cnt:           len(data),
		size:          size,
		messageRange:  r,
		evictCallback: evictCallback,
		offsets:       nil,
		diskFile:      "",
	}
	// register the immutable block into resource manager.
	// trigger the evicting after that.
	b.asyncRegister()
	return b
}

// ImmutableBlock is a block of messages that cannot be modified.
// A block of message should always be continuous and ordered by asc.
// An ImmutableBlock can be setted in memory or disk.
type ImmutableBlock struct {
	cond          *sync.Cond
	blockID       int64
	state         blockState
	data          []message.ImmutableMessage
	cnt           int
	size          int
	messageRange  util.MessageIDRange
	evictCallback walcache.EvictCallback

	// for disk cache
	offsets  []int
	diskFile string
}

// Count returns the message count in the block.
func (b *ImmutableBlock) Count() (int, bool) {
	return len(b.data), true
}

// Size returns the size of the block.
func (b *ImmutableBlock) Size() (int, bool) {
	return b.size, true
}

// Read creates a scanner to read messages from block.
func (b *ImmutableBlock) Read(started message.MessageID) (walcache.MessageScanner, error) {
	if !b.Range().In(started) {
		return nil, walcache.ErrNotFound
	}
	// We are always requesting a swapin operation before reading.
	// For memory-only mode, the swapin operation is just return the data slice.
	// For disk mode, the swapin operation will try to read the data from disk if needed.
	// !!! We assumed that the scaning on a immutable block is verify fast,
	// so we don't set swap-in lock at scanner, it will slightly cause the over-memory usage,
	// But the resource manager can evict the block as soon as possible to avoid blocking other scanners.
	data, err := b.swapIn()
	if err != nil {
		return nil, walcache.ErrEvicted
	}

	// find the lowerbound of the message list by started message.
	k := lowerboundOfMessageList(data, started)
	if k >= len(data) {
		panic(fmt.Sprintf(
			"unreachable: the message id should be in the range of the block, %s in %s", started.String(), b.Range().String()))
	}
	if !data[k].MessageID().EQ(started) {
		panic(fmt.Sprintf(
			"unreachable: the message id in block should be countinous, expected: %s, found: %s", started.String(), b.data[k].MessageID().String()),
		)
	}
	return &immutableBlockScanner{
		data:      data[k:],
		firstScan: true,
	}, nil
}

// Range returns the range of message id in the block.
func (b *ImmutableBlock) Range() util.MessageIDRange {
	return b.messageRange
}

// immutableBlockScanner is a scanner that scans messages in a block.
// Implement to fast drop element to gc.
type immutableBlockScanner struct {
	data      []message.ImmutableMessage
	firstScan bool
}

// Scan scans the next message.
func (s *immutableBlockScanner) Scan(ctx context.Context) error {
	if len(s.data) == 0 {
		return io.EOF
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.firstScan {
		s.firstScan = false
		return nil
	}
	s.data = s.data[1:]
	if len(s.data) == 0 {
		return io.EOF
	}
	return nil
}

// Message returns the current message.
func (s *immutableBlockScanner) Message() message.ImmutableMessage {
	return s.data[0]
}

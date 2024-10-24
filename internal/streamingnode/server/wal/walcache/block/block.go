package block

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/util"
)

// A Block is a block of messages in cache.
// There's least one message in a block, a empty block is illegal.
// This constraint means that we can always check if a message in a block,
// for easier implementation of `Read` interface.
type Block interface {
	walcache.MessageReader

	// Count returns the message count in the block.
	// Return (-, false) if the block cannot estimate the final count for now.
	Count() (int, bool)

	// Size returns the size of the block.
	// Return (-, false) if the block cannot estimate the final size for now.
	Size() (int, bool)

	// Range returns the range of message id in the block.
	// The range is inclusive, i.e., both the begin and end are included.
	// The end may be nil if the range is not closed, [Begin, +∞).
	Range() util.MessageIDRange
}

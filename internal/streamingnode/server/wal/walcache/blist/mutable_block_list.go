package blist

import (
	"context"
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/block"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/util"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// NewMutableContinousBlockList creates a new MutableContinousBlockList.
func NewMutableContinousBlockList(rotationCapacity int, firstMessage message.ImmutableMessage, evictCallback walcache.EvictCallback) *MutableCountinousBlockList {
	return &MutableCountinousBlockList{
		cond:             syncutil.NewContextCond(&sync.Mutex{}),
		tail:             block.NewMutableBlock(rotationCapacity, firstMessage, evictCallback),
		sealed:           false,
		rotationCapacity: rotationCapacity,
		blocksOffset:     0,
		immutableBlocks:  []*block.ImmutableBlock{},
		evictCallback:    evictCallback,
	}
}

// MutableCountinousBlockList is a ContinousBlockList that can do appending.
type MutableCountinousBlockList struct {
	cond             *syncutil.ContextCond
	tail             *block.MutableBlock
	sealed           bool
	rotationCapacity int                     // should be modifiable when running
	blocksOffset     int                     // the offset of the blocks array after evict operation.
	immutableBlocks  []*block.ImmutableBlock // should never be empty.
	evictCallback    walcache.EvictCallback
}

// Append appends a message to MutableBlockList.
func (mbl *MutableCountinousBlockList) Append(msgs []message.ImmutableMessage) {
	mbl.cond.L.Lock()
	defer mbl.cond.L.Unlock()

	if mbl.sealed {
		panic("unreachable: the block list is sealed, shouldn't receive any more new messages")
	}
	for len(msgs) != 0 {
		msgs = mbl.appendInternal(msgs)
	}
}

// appendInternal appends messages to the tail block.
func (mbl *MutableCountinousBlockList) appendInternal(msgs []message.ImmutableMessage) []message.ImmutableMessage {
	if len(msgs) == 0 {
		return nil
	}
	n := mbl.tail.Append(msgs)
	msgs = msgs[n:]
	if len(msgs) > 0 {
		// If there'are still messages left, seal the tail and create a new one, aka. rotate the block.
		mbl.sealTail()
		mbl.tail = block.NewMutableBlock(mbl.rotationCapacity, msgs[0], mbl.evictCallback)
		msgs = msgs[1:]
	}
	return msgs
}

// sealTail seals the tail block and append it to the block list if needed.
func (mbl *MutableCountinousBlockList) sealTail() {
	// the tail or sealed list changed, notify all the waiters.
	mbl.cond.UnsafeBroadcast()
	sealed := mbl.tail.IntoImmutable()
	mbl.tail = nil
	mbl.immutableBlocks = append(mbl.immutableBlocks, sealed)
}

// Evict evicts the block with the given blockID.
func (mbl *MutableCountinousBlockList) Evict(blockID int64) ([]*ImmutableContinousBlockList, bool) {
	mbl.cond.L.Lock()
	defer mbl.cond.L.Unlock()
	if len(mbl.immutableBlocks) == 0 {
		return nil, false
	}

	immutableBlockLists, idx, ok := splitCountinousImmutableBlocks(mbl.immutableBlocks, blockID)
	if !ok {
		return nil, false
	}

	// check if the last block is coutinuous with the tail.
	if idx == len(mbl.immutableBlocks)-1 ||
		len(immutableBlockLists) == 0 { // redundant condition, but kept to make the logic more clear.
		// the last block is not coutinous with the tail, remove all the mbl.blocks and return.
		mbl.blocksOffset += len(mbl.immutableBlocks)
		mbl.immutableBlocks = make([]*block.ImmutableBlock, 0)
		return immutableBlockLists, true
	}

	// the last block is coutinous with the tail, keep the last block list and return the others.
	lastBlockList := immutableBlockLists[len(immutableBlockLists)-1].blocks
	mbl.blocksOffset += (len(mbl.immutableBlocks) - len(lastBlockList))
	mbl.immutableBlocks = lastBlockList
	return immutableBlockLists[0 : len(immutableBlockLists)-1], true
}

// ReadFrom return a scanner that starts from the given message id.
func (mbl *MutableCountinousBlockList) Read(started message.MessageID) (walcache.MessageScanner, error) {
	mbl.cond.L.Lock()
	defer mbl.cond.L.Unlock()

	if !mbl.rangeInternal().In(started) {
		return nil, walcache.ErrNotFound
	}

	// use growing tail as default.
	offset := len(mbl.immutableBlocks)
	// check if the message is in the sealed blocks.
	for i, b := range mbl.immutableBlocks {
		if b.Range().In(started) {
			offset = i
			break
		}
	}

	blockScanner := newMutableContinousBlockListScanner(mbl, offset+mbl.blocksOffset)
	return newBlockListScanner(blockScanner, started), nil
}

// Range returns the range of the block list.
func (mbl *MutableCountinousBlockList) Range() util.MessageIDRange {
	mbl.cond.L.Lock()
	defer mbl.cond.L.Unlock()

	return mbl.rangeInternal()
}

// RangeOver ranges over the blocks in the immutableContinousBLockList.
func (mbl *MutableCountinousBlockList) RangeOverImmutable(f func(idx int, b *block.ImmutableBlock) bool) {
	var immutablBlocks []*block.ImmutableBlock
	mbl.cond.L.Lock()
	immutablBlocks = mbl.immutableBlocks
	mbl.cond.L.Unlock()

	for idx, b := range immutablBlocks {
		if !f(idx, b) {
			return
		}
	}
}

// rangeInternal returns the range of the block list.
func (mbl *MutableCountinousBlockList) rangeInternal() util.MessageIDRange {
	if mbl.sealed {
		// return the determined range.
		return util.MessageIDRange{
			Begin: mbl.immutableBlocks[0].Range().Begin,
			End:   mbl.immutableBlocks[len(mbl.immutableBlocks)-1].Range().End,
		}
	}
	// return the half-determined range.
	if len(mbl.immutableBlocks) != 0 {
		return util.MessageIDRange{
			Begin: mbl.immutableBlocks[0].Range().Begin,
			End:   nil,
		}
	}
	return mbl.tail.Range()
}

// IntoImmutable converts MutableBlockList to ImmutableBLockList.
// After calling this function, the MutableBlockList should not be used anymore,
// but the existed scanner can still be used.
func (mbl *MutableCountinousBlockList) IntoImmutable() *ImmutableContinousBlockList {
	mbl.cond.L.Lock()
	defer mbl.cond.L.Unlock()

	// A MutableBlockList that have messages always have a tail.
	// if the tail is nil, we cannot seal it.
	if mbl.tail == nil {
		panic("unreachable: the block list is empty, should not be converted to immutable")
	}
	mbl.sealTail()
	mbl.sealed = true
	return newImmutableContinousBlockList(mbl.immutableBlocks)
}

// blockUntilIndexReady blocks until the block at the given index is ready.
func (mbl *MutableCountinousBlockList) blockUntilIndexReady(ctx context.Context, idx int) (block.Block, error) {
	mbl.cond.L.Lock()
	for {
		if b, err := mbl.indexBlockAtInternal(idx); err != nil || b != nil {
			mbl.cond.L.Unlock()
			return b, err
		}
		if err := mbl.cond.Wait(ctx); err != nil {
			return nil, err
		}
	}
}

// indexBlockAt returns the block at the given index.
func (mbl *MutableCountinousBlockList) indexBlockAtInternal(idx int) (block.Block, error) {
	// recover the offset before access the index.
	idx -= mbl.blocksOffset
	if idx < 0 {
		// the idx is evicted from current mutable countinous block list.
		return nil, walcache.ErrEvicted
	}

	// Index out of range after sealed.
	if mbl.sealed && idx >= len(mbl.immutableBlocks) {
		return nil, io.EOF
	}

	if idx < len(mbl.immutableBlocks) {
		return mbl.immutableBlocks[idx], nil
	}
	if idx == len(mbl.immutableBlocks) {
		return mbl.tail, nil
	}
	// Access a index at future.
	return nil, nil
}

// newMutableContinousBlockListScanner creates a new mutableContinousBlockListScanner.
func newMutableContinousBlockListScanner(mbl *MutableCountinousBlockList, idx int) *mutableContinousBlockListScanner {
	return &mutableContinousBlockListScanner{
		mbl: mbl,
		idx: idx - 1,
	}
}

// mutableContinousBlockListScanner is a scanner that scans blocks in a mutableContinousBLockList.
type mutableContinousBlockListScanner struct {
	mbl          *MutableCountinousBlockList
	idx          int
	currentBlock block.Block
}

// Scan scans the next block.
func (s *mutableContinousBlockListScanner) Scan(ctx context.Context) error {
	var err error
	s.idx++
	s.currentBlock, err = s.mbl.blockUntilIndexReady(ctx, s.idx)
	return err
}

// Block returns the current block.
func (s *mutableContinousBlockListScanner) Block() block.Block {
	return s.currentBlock
}

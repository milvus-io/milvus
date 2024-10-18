package blist

import (
	"context"
	"io"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/block"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/util"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var _ blockScanner = (*immutableContinousBlockListBlockScanner)(nil)

// newImmutableContinousBlockList creates a new immutableContinousBLockList.
func newImmutableContinousBlockList(blocks []*block.ImmutableBlock) *ImmutableContinousBlockList {
	if len(blocks) == 0 {
		panic("unreachable: immutable block list should never be empty")
	}
	return &ImmutableContinousBlockList{
		blocks: blocks,
	}
}

// ImmutableContinousBlockList is a continous sequence of ImmutableBlock.
type ImmutableContinousBlockList struct {
	blocks []*block.ImmutableBlock // should never be empty.
}

// BlockCount returns the block count of the immutableContinousBLockList.
func (mbl *ImmutableContinousBlockList) BlockCount() int {
	return len(mbl.blocks)
}

// RangeOver ranges over the blocks in the immutableContinousBLockList.
func (mbl *ImmutableContinousBlockList) RangeOver(f func(idx int, b *block.ImmutableBlock) bool) {
	for idx, b := range mbl.blocks {
		if !f(idx, b) {
			return
		}
	}
}

// Evict evicts the block with the given blockID.
func (mbl *ImmutableContinousBlockList) Evict(blockID int64) ([]*ImmutableContinousBlockList, bool) {
	list, _, ok := splitCountinousImmutableBlocks(mbl.blocks, blockID)
	return list, ok
}

// ReadFrom implements the ReadFrom interface.
func (mbl *ImmutableContinousBlockList) Read(started message.MessageID) (walcache.MessageScanner, error) {
	if !mbl.Range().In(started) {
		return nil, walcache.ErrNotFound
	}
	offset := -1
	for i, b := range mbl.blocks {
		if b.Range().In(started) {
			offset = i
			break
		}
	}
	if offset == -1 {
		panic("unreachable: message id is in cache, but no block found")
	}
	bs := newImmutableContinousBlockListBlockScanner(mbl.blocks[offset:])
	// the scanners of immutable block list will never return ErrNotReach,
	// so we can ignore the error.
	return newBlockListScanner(bs, started), nil
}

// Range returns the message id range of the immutableContinousBLockList.
func (mbl *ImmutableContinousBlockList) Range() util.MessageIDRange {
	return util.MessageIDRange{
		Begin: mbl.blocks[0].Range().Begin,
		End:   mbl.blocks[len(mbl.blocks)-1].Range().End,
	}
}

// newImmutableContinousBlockListBlockScanner creates a new immutableContinousBlockListBlockScanner.
func newImmutableContinousBlockListBlockScanner(blocks []*block.ImmutableBlock) *immutableContinousBlockListBlockScanner {
	return &immutableContinousBlockListBlockScanner{
		blocks:    blocks,
		firstScan: true,
	}
}

// immutableContinousBlockListBlockScanner is a scanner that scans blocks in a immutableContinousBLockList.
type immutableContinousBlockListBlockScanner struct {
	blocks    []*block.ImmutableBlock
	firstScan bool
}

// Scan scans the next block.
func (bs *immutableContinousBlockListBlockScanner) Scan(ctx context.Context) error {
	if len(bs.blocks) == 0 {
		return io.EOF
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if bs.firstScan {
		bs.firstScan = false
		return nil
	}
	bs.blocks = bs.blocks[1:]
	if len(bs.blocks) == 0 {
		return io.EOF
	}
	return nil
}

// Block returns the current block.
func (bs *immutableContinousBlockListBlockScanner) Block() block.Block {
	return bs.blocks[0]
}

// splitCountinousImmutableBlocks splits the blocks into multiple immutableContinousBLockList, and return the blocks at splits idxes.
// Return the split result, split offset and true if splited, false otherwise.
func splitCountinousImmutableBlocks(blocks []*block.ImmutableBlock, blockID int64) ([]*ImmutableContinousBlockList, int, bool) {
	idx := -1
	for i, b := range blocks {
		if b.BlockID() == blockID {
			idx = i
		}
	}
	if idx == -1 {
		// blockID not found, keep the blocks unchanged.
		return []*ImmutableContinousBlockList{newImmutableContinousBlockList(blocks)}, idx, false
	}
	if len(blocks) == 1 {
		// blockID found, and only one block in the list, return empty list.
		return []*ImmutableContinousBlockList{}, idx, true
	}

	// only one block list kept if the tail or the head block is evicted.
	if idx == 0 {
		return []*ImmutableContinousBlockList{newImmutableContinousBlockList(blocks[1:])}, idx, true
	}
	if idx == len(blocks)-1 {
		return []*ImmutableContinousBlockList{
			newImmutableContinousBlockList(blocks[:idx]),
		}, idx, true
	}

	// split the blocks into two lists.
	return []*ImmutableContinousBlockList{
		newImmutableContinousBlockList(blocks[:idx]),
		newImmutableContinousBlockList(blocks[idx+1:]),
	}, idx, true
}

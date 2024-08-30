package block

import (
	"errors"
	"io"
	"os"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/rm"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// The common state transition of a block:
// memory only:
// BlockStateUnregistered -> BlockStateMemoryOnly -> BlockStateEvicted
//
// memory + disk:
//
// BlockStateUnregistered -> BlockStateOnFlusing -> BlockStateMemoryAndDisk -> BlockStateDiskOnly ->  \
// BlockStateMemoryAndDisk -> BlockStateEvicted
type blockState int32

const (
	blockStateUnregistered blockState = iota
	blockStateMemoryOnly
	blockStateOnFlushing
	blockStateDiskOnly

	blockStateMemoryAndDisk
	blockStateEvicted
)

// IsStableState returns whether the state is stable.
func (s blockState) IsStableState() bool {
	return s == blockStateEvicted || s == blockStateMemoryAndDisk || s == blockStateDiskOnly || s == blockStateMemoryOnly
}

// BlockID returns the id of the block.
func (b *ImmutableBlock) BlockID() int64 {
	return b.blockID
}

// Bytes returns the size of the block.
func (b *ImmutableBlock) Bytes() int64 {
	return int64(b.size)
}

// asyncRegister registers the block.
func (b *ImmutableBlock) asyncRegister() {
	// !!! async register the block will cause the memory usage slightly higher than the limit,
	// but it can avoid the block of Appending operation of MutableBlock.
	go b.register()
}

// register notify that the block is registered on manager.
func (b *ImmutableBlock) register() {
	b.cond.L.Lock()
	if b.state != blockStateUnregistered {
		b.cond.L.Unlock()
		panic("block state is not unregistered")
	}
	state := blockStateMemoryOnly
	if rm.Disk != nil {
		// Try to flush the block into disk if disk mode is enabled.
		b.setStateAndUnlockWithBroadcast(blockStateOnFlushing)
		if err := b.onFlushing(b.data); err == nil {
			state = blockStateMemoryAndDisk
		}
		b.cond.L.Lock()
	}

	// we should allocate disk first then allocating memory, to avoid deadlock.
	rm.Memory.BlockAndAllocate(b)
	b.setStateAndUnlockWithBroadcast(state)
}

// onFlushing flushes the block.
func (b *ImmutableBlock) onFlushing(data []message.ImmutableMessage) error {
	// block until there's enough disk space to flush.
	rm.Disk.BlockAndAllocate(b)
	path := rm.Disk.GetPath(b.BlockID())

	// flush the block to disk.
	if err := b.flush(path, data); err != nil {
		// open disk file failure.
		// free the disk quota and return.
		os.Remove(path)
		rm.Disk.Free(b.Bytes())
		return err
	}
	return nil
}

// flush flushes the block to writer.
func (b *ImmutableBlock) flush(path string, data []message.ImmutableMessage) (err error) {
	w, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		err = w.Close()
	}()

	offsets := make([]int, 0, len(data)+1)
	offsets = append(offsets, 0)
	written := 0

	// most of the time, the payload is not small, so we don't use bufio.Writer here.
	for _, msg := range data {
		n, err := w.Write(msg.Payload())
		if err != nil {
			return err
		}
		written += n
		offsets = append(offsets, written)
	}
	// flush success, mark the block as memory and disk.
	b.cond.L.Lock()
	b.diskFile = path
	b.offsets = offsets
	b.cond.L.Unlock()
	return nil
}

// AsyncEvictMemory evicts the block from memory.
func (b *ImmutableBlock) AsyncEvictMemory() {
	go b.evictMemory()
}

// evictMemory evicts the block from memory.
func (b *ImmutableBlock) evictMemory() {
	b.cond.L.Lock()
	for !b.state.IsStableState() {
		b.cond.Wait()
	}

	switch b.state {
	case blockStateMemoryOnly:
		b.data = nil
		b.setStateAndUnlockWithBroadcast(blockStateEvicted)
		rm.Memory.Free(b.Bytes())
	case blockStateMemoryAndDisk:
		data := make([]message.ImmutableMessage, 0, len(b.data))
		for _, item := range b.data {
			data = append(data, item.EvictPayload())
		}
		b.data = data
		b.setStateAndUnlockWithBroadcast(blockStateDiskOnly)
		rm.Memory.Free(b.Bytes())
	default:
		b.cond.L.Unlock()
	}
}

// AsyncEvictDisk evicts the block from disk.
func (b *ImmutableBlock) AsyncEvictDisk() {
	go b.evictDisk()
}

// evictDisk evict the disk
func (b *ImmutableBlock) evictDisk() {
	b.cond.L.Lock()
	for !b.state.IsStableState() {
		b.cond.Wait()
	}

	switch b.state {
	case blockStateMemoryOnly:
		b.data = nil
		b.setStateAndUnlockWithBroadcast(blockStateEvicted)
		rm.Memory.Free(b.Bytes())
	case blockStateMemoryAndDisk:
		b.data = nil
		os.Remove(b.diskFile)
		b.diskFile = ""
		b.setStateAndUnlockWithBroadcast(blockStateEvicted)
		rm.Memory.Free(b.Bytes())
		rm.Disk.Free(b.Bytes())
	case blockStateDiskOnly:
		os.Remove(b.diskFile)
		b.setStateAndUnlockWithBroadcast(blockStateEvicted)
		rm.Disk.Free(b.Bytes())
	default:
		b.cond.L.Unlock()
	}
}

// swapIn swaps the block from disk into memory.
func (b *ImmutableBlock) swapIn() ([]message.ImmutableMessage, error) {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	if b.state == blockStateMemoryAndDisk ||
		b.state == blockStateMemoryOnly ||
		b.state == blockStateOnFlushing ||
		b.state == blockStateUnregistered {
		return b.data, nil
	}

	if b.state == blockStateEvicted {
		return nil, errors.New("block state is not disk only")
	}

	if b.diskFile == "" || b.state != blockStateDiskOnly {
		panic("disk file is empty when disk only status")
	}

	r, err := os.Open(b.diskFile)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	rm.Memory.BlockAndAllocate(b)

	// TODO: add crc32 check
	payloads, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(b.offsets) != len(b.data)+1 {
		panic("offsets length is not equal to data length+1")
	}
	data := make([]message.ImmutableMessage, 0, len(b.data))
	for i, msg := range b.data {
		newMsg := message.NewImmutableMesasge(msg.MessageID(), payloads[b.offsets[i]:b.offsets[i+1]], msg.Properties().ToRawMap())
		data = append(data, newMsg)
	}
	b.data = data
	b.state = blockStateMemoryAndDisk
	return data, nil
}

// setStateAndUnlockWithBroadcast sets the state and broadcast the condition.
func (b *ImmutableBlock) setStateAndUnlockWithBroadcast(state blockState) {
	b.state = state
	b.cond.Broadcast()
	b.cond.L.Unlock()
	if state == blockStateEvicted && b.evictCallback != nil {
		go b.evictCallback.OnEvict(b.blockID)
	}
}

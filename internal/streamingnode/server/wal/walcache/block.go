package walcache

// BlockOperator is the interface of block operator.
type BlockOperator interface {
	// BlockID returns the id of the block.
	BlockID() int64

	// Bytes returns the bytes of blocks.
	Bytes() int64

	// AsyncEvictMemory evicts the block from memory async.
	AsyncEvictMemory()

	// AsyncEvictDisk evicts the block from disk async.
	AsyncEvictDisk()
}

// EvictCallback is the callback when block is evicted.
type EvictCallback interface {
	// OnEvict is called when block is evicted.
	OnEvict(blockID int64)
}

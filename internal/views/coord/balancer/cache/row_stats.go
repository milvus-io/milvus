package cache

// ShardSizeStats maps each QueryNode to the row-count load contributed by one shard.
type ShardSizeStats map[int64]NodeSizeStats

// NodeSizeStats splits a node's row-count load by placement state.
type NodeSizeStats struct {
	UpLogicalSize      int64
	PendingLogicalSize int64
}

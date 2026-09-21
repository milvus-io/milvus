package cache

// ShardRowStats maps each QueryNode to the row-count load contributed by one shard.
type ShardRowStats map[int64]NodeRowStats

// NodeRowStats splits a node's row-count load by placement state.
type NodeRowStats struct {
	UpRowCount      int64
	PendingRowCount int64
}

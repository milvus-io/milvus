package qviews

import "time"

const (
	ShardStatsInterval   = 5 * time.Second
	ShardStatsMaxEntries = 1024
)

// ShardStats counts planning attempts within one reporting window.
type ShardStats struct {
	ShardID             ShardID
	RequestCount        uint64
	LastAccessUnixNanos int64
}

// ShardStatsWindow transfers ownership of the detached samples to its consumer.
// Windows are best effort: they are never replayed or persisted.
type ShardStatsWindow struct {
	StartUnixNanos int64
	EndUnixNanos   int64
	Shards         map[ShardID]ShardStats
}

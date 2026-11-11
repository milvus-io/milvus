package qviews

// ShardStats describes planning access, independently of QueryView versions.
// RequestCount belongs to one SN handler lifetime and can reset after handoff.
type ShardStats struct {
	ShardID             ShardID
	RequestCount        uint64
	LastAccessUnixNanos int64
}

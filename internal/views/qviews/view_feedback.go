package qviews

import "time"

const (
	ViewFeedbackInterval    = 5 * time.Second
	ViewFeedbackMinInterval = time.Second
	ViewFeedbackMaxEntries  = 1024
)

// ViewFeedback counts planning attempts within one reporting window.
type ViewFeedback struct {
	ShardID             ShardID
	RequestCount        uint64
	LastAccessUnixNanos int64
}

// ViewFeedbackWindow transfers ownership of the detached samples to its consumer.
// Windows are best effort: they are never replayed or persisted.
type ViewFeedbackWindow struct {
	StartUnixNanos int64
	EndUnixNanos   int64
	Shards         map[ShardID]ViewFeedback
}

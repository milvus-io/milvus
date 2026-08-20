package streamingnode

const (
	MetaPrefix = "streamingnode-meta"

	DirectoryWAL           = "wal"
	DirectorySegmentAssign = "segment-assign"
	DirectoryVChannel      = "vchannel"
	DirectorySchema        = "schema"
	DirectorySummaryStore  = "summary-store"
	// Spelled out rather than shortened to "vchannels": a key is usually read on
	// its own (an etcd dump, a birdwatcher listing, a log line), where a bare
	// "vchannels" says nothing about what it holds and sits one letter away from
	// the unrelated "vchannel" directory holding vchannel schemas.

	KeyConsumeCheckpoint   = "consume-checkpoint"
	KeySalvageCheckpoint   = "salvage-checkpoint"
	KeyPChannelSummaryMeta = "pchannel-summary-meta"
)

package streamingnode

const (
	MetaPrefix = "streamingnode-meta"

	DirectoryWAL             = "wal"
	DirectorySegmentAssign   = "segment-assign"
	DirectoryVChannel        = "vchannel"
	DirectorySchema          = "schema"
	DirectorySummaryStore    = "summary-store"
	DirectorySummaryVChannel = "vchannels"

	KeyConsumeCheckpoint   = "consume-checkpoint"
	KeySalvageCheckpoint   = "salvage-checkpoint"
	KeyPChannelSummaryMeta = "pchannel-meta"
)

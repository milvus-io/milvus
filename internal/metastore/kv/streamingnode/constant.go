package streamingnode

const (
	MetaPrefix = "streamingnode-meta"

	DirectoryWAL           = "wal"
	DirectorySegmentAssign = "segment-assign"
	DirectoryQueryView     = "query-view"
	DirectoryVChannel      = "vchannel"
	DirectorySchema        = "schema"

	KeyConsumeCheckpoint = "consume-checkpoint"
	KeySalvageCheckpoint = "salvage-checkpoint"
)

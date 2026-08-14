package streamingnode

const (
	MetaPrefix = "streamingnode-meta"

	DirectoryWAL           = "wal"
	DirectorySegmentAssign = "segment-assign"
	DirectoryTransformLog  = "tl"
	DirectoryVChannel      = "vchannel"
	DirectorySchema        = "schema"

	KeyConsumeCheckpoint = "consume-checkpoint"
	KeySalvageCheckpoint = "salvage-checkpoint"
)

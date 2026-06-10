package streamingnode

const (
	MetaPrefix = "streamingnode-meta"

	DirectoryWAL           = "wal"
	DirectorySegmentAssign = "segment-assign"

	DirectoryQueryView = "qv"

	DirectoryTransformLog = "transform-log"

	DirectoryVChannel = "vchannel"
	DirectorySchema   = "schema"

	KeyConsumeCheckpoint = "consume-checkpoint"
	KeySalvageCheckpoint = "salvage-checkpoint"
)

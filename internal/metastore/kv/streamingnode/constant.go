package streamingnode

const (
	MetaPrefix = "streamingnode-meta"

	DirectoryWAL            = "wal"
	DirectorySegmentAssign  = "segment-assign"
	DirectoryVChannel       = "vchannel"
	DirectorySchema         = "schema"
	DirectoryWindowStore    = "window-store"
	DirectoryWindowVChannel = "vchannels"

	KeyConsumeCheckpoint  = "consume-checkpoint"
	KeySalvageCheckpoint  = "salvage-checkpoint"
	KeyPChannelWindowMeta = "pchannel-meta"
)

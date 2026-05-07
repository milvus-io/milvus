package streamingcoord

const (
	MetaPrefix          = "streamingcoord-meta/"
	PChannelMetaPrefix  = MetaPrefix + "pchannel/"
	BroadcastTaskPrefix = MetaPrefix + "broadcast-task/"
	VersionKey          = MetaPrefix + "version"
	CChannelMetaKey     = MetaPrefix + "cchannel"

	// Replicate
	ReplicatePChannelMetaPrefix = MetaPrefix + "replicating-pchannel/"
	ReplicateConfigurationKey   = MetaPrefix + "replicate-configuration"
)

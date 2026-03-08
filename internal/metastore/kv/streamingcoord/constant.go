package streamingcoord

const (
	MetaPrefix          = "streamingcoord-meta/"
	PChannelMetaPrefix  = MetaPrefix + "pchannel/"
	BroadcastTaskPrefix = MetaPrefix + "broadcast-task/"
	VersionPrefix       = MetaPrefix + "version/"
	CChannelMetaPrefix  = MetaPrefix + "cchannel/"

	// Replicate
	ReplicatePChannelMetaPrefix = MetaPrefix + "replicating-pchannel/"
	ReplicateConfigurationKey   = MetaPrefix + "replicate-configuration"
)

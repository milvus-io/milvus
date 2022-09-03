package rootcoord

const (
	// TODO: better to make them configurable, use default value if no config was set since we never explode these before.
	snapshotsSep              = "_ts"
	snapshotPrefix            = "snapshots"
	globalIDAllocatorKey      = "idTimestamp"
	globalIDAllocatorSubPath  = "gid"
	globalTSOAllocatorKey     = "timestamp"
	globalTSOAllocatorSubPath = "tso"
)

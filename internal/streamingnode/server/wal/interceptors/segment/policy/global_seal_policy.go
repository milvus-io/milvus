package policy

import "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"

func GetGlobalAsyncSealPolicy() []GlobalAsyncSealPolicy {
	// TODO: dynamic policy can be applied here in future.
	return []GlobalAsyncSealPolicy{}
}

// GlobalAsyncSealPolicy is the policy to check if a global segment should be sealed or not.
type GlobalAsyncSealPolicy interface {
	// ShouldSealed checks if the segment should be sealed, and return the reason string.
	ShouldSealed(m stats.StatsManager)
}

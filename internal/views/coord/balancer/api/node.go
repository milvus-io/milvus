package api

// NodeInfo carries the QueryNode state provided by the coordinator-facing
// node view. Actual per-shard load is maintained by the cache.
type NodeInfo struct {
	NodeID        int64
	Alive         bool
	Stopping      bool
	ResourceGroup string
}

package api

import "github.com/milvus-io/milvus/internal/views/qviews"

// TriggerScope describes the external event scope that dirtied the Balancer.
//
// Empty Trigger() means a full scan. NodeChanged without DirtyNodes also means
// a full scan, because the caller did not provide enough information to narrow
// the affected shards.
type TriggerScope struct {
	NodeChanged      bool
	DirtyNodes       []int64
	DirtyShards      []qviews.ShardID
	DirtyCollections []int64
}

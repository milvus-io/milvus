package shardview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// Builder is the builder of the query view of shard at coord.
type QueryViewAtCoordBuilder struct {
	inner *viewpb.QueryViewOfShard
}

// ShardID returns the shard id of the query view.
func (qb *QueryViewAtCoordBuilder) ShardID() qviews.ShardID {
	return qviews.NewShardIDFromQVMeta(qb.inner.Meta)
}

// DataVersion returns the data version of the query view.
func (qb *QueryViewAtCoordBuilder) DataVersion() int64 {
	return qb.inner.Meta.Version.DataVersion
}

// WithQueryVersion sets the query version of the query view.
func (qb *QueryViewAtCoordBuilder) withQueryVersion(version int64) *QueryViewAtCoordBuilder {
	qb.inner.Meta.Version.QueryVersion = version
	return qb
}

// Build creates a new query view of shard at coord.
func (qb *QueryViewAtCoordBuilder) Build() *queryView {
	qb.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStatePreparing)
	c := qb.inner
	qb.inner = nil
	return newPreparingQueryView(c)
}

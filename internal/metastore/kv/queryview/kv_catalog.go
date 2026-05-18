package queryview

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

const queryViewKeyPrefix = "qv/"

// QueryViewCatalog provides ETCD persistence for query views.
// Used by both Coord and StreamingNode with different key prefixes.
type QueryViewCatalog interface {
	// ListQueryViews loads all persisted query views.
	ListQueryViews(ctx context.Context) ([]*viewpb.QueryViewOfShard, error)

	// SaveQueryViews atomically persists a batch of query views.
	// - State == Dropped: deletes the key from ETCD.
	// - Otherwise: puts the key with serialized value (ready_segment_ids cleared).
	SaveQueryViews(ctx context.Context, views []*viewpb.QueryViewOfShard) error
}

type queryViewCatalog struct {
	metaKV kv.MetaKv
	prefix string // full prefix: {rootPrefix}{queryViewKeyPrefix}
}

// NewQueryViewCatalog creates a new QueryViewCatalog backed by ETCD.
// role identifies the component (e.g. "coord" or "streamingnode"), used as key prefix.
// The provided MetaKv is wrapped with ReliableWriteMetaKv to ensure
// write operations only fail on context cancellation.
func NewQueryViewCatalog(metaKV kv.MetaKv, role string) QueryViewCatalog {
	return &queryViewCatalog{
		metaKV: kv.NewReliableWriteMetaKv(metaKV),
		prefix: role + "/" + queryViewKeyPrefix,
	}
}

func (c *queryViewCatalog) ListQueryViews(ctx context.Context) ([]*viewpb.QueryViewOfShard, error) {
	_, values, err := c.metaKV.LoadWithPrefix(ctx, c.prefix)
	if err != nil {
		return nil, err
	}
	views := make([]*viewpb.QueryViewOfShard, 0, len(values))
	for _, val := range values {
		view := &viewpb.QueryViewOfShard{}
		if err := proto.Unmarshal([]byte(val), view); err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, nil
}

func (c *queryViewCatalog) SaveQueryViews(ctx context.Context, views []*viewpb.QueryViewOfShard) error {
	if len(views) == 0 {
		return nil
	}

	saves := make(map[string]string, len(views))
	removals := make([]string, 0)

	for _, view := range views {
		key := c.buildKey(view.Meta)
		if view.Meta.State == viewpb.QueryViewState_QueryViewStateDropped {
			removals = append(removals, key)
		} else {
			data, err := marshalForPersistence(view)
			if err != nil {
				return err
			}
			saves[key] = string(data)
		}
	}

	return c.metaKV.MultiSaveAndRemove(ctx, saves, removals)
}

// buildKey constructs the ETCD key for a query view.
// Format: {prefix}{collection_id}/{replica_id}/{vchannel}/{sv}/{cv}/{qv}
func (c *queryViewCatalog) buildKey(meta *viewpb.QueryViewMeta) string {
	dv := meta.Version.DataVersion
	return fmt.Sprintf("%s%d/%d/%s/%d/%d/%d",
		c.prefix,
		meta.CollectionId,
		meta.ReplicaId,
		meta.Vchannel,
		dv.StreamingVersion,
		dv.CompactVersion,
		meta.Version.QueryVersion,
	)
}

// marshalForPersistence serializes a QueryViewOfShard for ETCD storage.
// It clears ready_segment_ids (runtime-only state rebuilt from node responses).
func marshalForPersistence(view *viewpb.QueryViewOfShard) ([]byte, error) {
	clone := proto.Clone(view).(*viewpb.QueryViewOfShard)
	for _, qn := range clone.QueryNode {
		for _, p := range qn.Partitions {
			p.ReadySegmentIds = nil
		}
	}
	return proto.Marshal(clone)
}

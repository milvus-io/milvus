package snview

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (h *SNQueryViewHandler) AcquireUpView(ctx context.Context, shardID qviews.ShardID, version qviews.QueryViewVersion) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	h.mu.Lock()
	shard := h.shards[shardID]
	h.mu.Unlock()
	if shard == nil {
		return nil, viewerror.NewViewNotFound("query view %s is not found", shardID.String())
	}
	return shard.acquireUpView(ctx, version)
}

func (s *snShardView) acquireUpView(ctx context.Context, version qviews.QueryViewVersion) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return nil, viewerror.NewViewNotFound("query view %s is not found", version.String())
	}
	if entry.sm.State() != qviews.QueryViewStateUp {
		return nil, viewerror.NewViewInvalidated("query view %s is not up, current state is %s", version.String(), entry.sm.State().String())
	}
	entry.queryRefs++
	view := proto.Clone(entry.View.IntoProto()).(*viewpb.QueryViewOfShard)
	var once sync.Once
	return &QueryViewLease{
		Version: version,
		Meta:    proto.Clone(view.GetMeta()).(*viewpb.QueryViewMeta),
		View:    view,
		Release: func() { once.Do(func() { s.releaseQueryViewLease(version) }) },
	}, nil
}

package datacoord

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// queryViewBalanceProvider translates the recovery DataView manager's immutable
// references into the balancer's scoped snapshots. Row counts come from the
// same published version as membership, rather than a later SegmentMeta read.
type queryViewBalanceProvider struct {
	server *Server
}

func (s *Server) DataViewProvider() balancer.DataViewProvider {
	return &queryViewBalanceProvider{server: s}
}

func (p *queryViewBalanceProvider) DataViewSnapshot(ctx context.Context) *balancer.DataViewSnapshot {
	ids := make(map[int64]struct{})
	for _, collection := range p.server.meta.GetCollections() {
		ids[collection.ID] = struct{}{}
	}
	return p.DataViewSnapshotForCollections(ctx, ids)
}

func (p *queryViewBalanceProvider) DataViewSnapshotForCollections(ctx context.Context, ids map[int64]struct{}) *balancer.DataViewSnapshot {
	if ids == nil {
		return p.DataViewSnapshot(ctx)
	}
	collections := make([]*viewpb.DataViewOfCollection, 0, len(ids))
	segments := queryViewBalanceSegments{}
	for id := range ids {
		ref, err := p.server.dataViewManager.Latest(ctx, id)
		if err != nil {
			mlog.Warn(ctx, "cannot read DataView for balancing", mlog.Int64("collectionID", id), mlog.Err(err))
			continue
		}
		if ref == nil {
			continue
		}
		view := proto.Clone(ref.DataView()).(*viewpb.DataViewOfCollection)
		for _, shard := range view.GetShards() {
			for _, partition := range shard.GetPartitions() {
				for _, segmentID := range partition.GetSegmentIds() {
					if stats, ok := ref.Stats(segmentID); ok {
						segments[segmentID] = &balancer.SegmentInfo{SegmentID: segmentID, PartitionID: partition.GetPartitionId(), RowNum: stats.RowNum}
					}
				}
			}
		}
		ref.Deref()
		collections = append(collections, view)
	}
	return balancer.NewDataViewSnapshot(p.server.queryViewBalanceVersion.Add(1), collections, segments)
}

// Placement-only segments may no longer belong to the latest DataView. Their
// metadata is used only to account for resources still held by older views.
func (p *queryViewBalanceProvider) SegmentSnapshot(ctx context.Context, ids []int64) balancer.SegmentSnapshot {
	segments := queryViewBalanceSegments{}
	for _, id := range ids {
		if segment := p.server.meta.GetSegment(ctx, id); segment != nil {
			segments[id] = &balancer.SegmentInfo{SegmentID: id, PartitionID: segment.GetPartitionID(), RowNum: segment.GetNumOfRows()}
		}
	}
	return segments
}

type queryViewBalanceSegments map[int64]*balancer.SegmentInfo

func (s queryViewBalanceSegments) Get(id int64) (*balancer.SegmentInfo, bool) {
	info, ok := s[id]
	return info, ok
}

var _ balancer.DataViewProvider = (*queryViewBalanceProvider)(nil)

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// DataViewProvider exposes committed, immutable entries and exact-version references.
func (s *Server) DataViewProvider() balancer.DataViewProvider {
	if s.dataViewManager != nil && paramtable.Get().QueryViewCfg.PublishCollectionSize.GetAsBool() {
		return collectionSizeDataViews{DataViewProvider: s.dataViewManager, server: s}
	}
	return s.dataViewManager
}

type collectionSizeDataViews struct {
	balancer.DataViewProvider
	server *Server
}

// CollectionSizeBytes reads one collection from resident DataCoord metadata.
func (p collectionSizeDataViews) CollectionSizeBytes(ctx context.Context, collectionID int64) (int64, error) {
	collection, err := p.server.meta.GetCollectionOrNotFound(collectionID)
	if err != nil {
		return 0, err
	}
	if !collection.IsCreated() {
		return 0, merr.WrapErrCollectionNotFound(collectionID)
	}
	partitions := typeutil.NewSet(collection.Partitions...)
	segments := p.server.meta.SelectSegments(ctx, meta.WithCollection(collectionID), meta.SegmentFilterFunc(func(seg *meta.SegmentInfo) bool {
		return meta.IsSegmentHealthy(seg) && !seg.GetIsImporting() && !seg.GetIsInvisible() && !seg.GetIsFake() && !meta.IsEmptySegmentChangeOutput(seg) &&
			(seg.GetPartitionID() == common.AllPartitionsID || partitions.Contain(seg.GetPartitionID()))
	}))
	var size int64
	ids := make([]int64, 0, len(segments))
	for _, seg := range segments {
		ids = append(ids, seg.GetID())
		size += seg.GetSegmentSize()
		for _, text := range seg.GetTextStatsLogs() {
			size += text.GetLogSize()
		}
		for _, jsonKeys := range seg.GetJsonKeyStats() {
			size += jsonKeys.GetLogSize()
		}
	}
	size += p.server.indexMeta.FinishedIndexBytes(collectionID, ids)
	return size, nil
}

// RefreshDataView synchronously publishes committed SegmentMeta mutations and
// returns the exact version that a serving QueryView must cover.
func (s *Server) RefreshDataView(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return nil, err
	}
	version, err := s.dataViewManager.RecomputeNow(ctx, collectionID, s.meta.LoadableProjection)
	if err != nil {
		return nil, err
	}
	if version == nil {
		// A dropped collection has no publication obligation. Consult RootCoord
		// rather than the DataView manager's process-local drop markers so this
		// also works when a durable callback is replayed after recovery.
		dropped, err := s.collectionDropped(ctx, collectionID)
		if err != nil {
			return nil, err
		}
		if dropped {
			return nil, merr.WrapErrCollectionNotFound(collectionID)
		}
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d DataView is not published", collectionID)
	}
	return version, nil
}

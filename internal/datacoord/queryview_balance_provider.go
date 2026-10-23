package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// DataViewProvider exposes committed, immutable entries and exact-version references.
func (s *Server) DataViewProvider() balancer.DataViewProvider {
	return s.dataViewManager
}

// RefreshDataView synchronously publishes committed SegmentMeta mutations and
// returns the exact version that a serving QueryView must cover.
func (s *Server) RefreshDataView(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return nil, err
	}
	version, err := s.dataViewManager.RecomputeNow(ctx, collectionID, s.meta.loadableProjection)
	if err != nil {
		return nil, err
	}
	if version == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d DataView is not published", collectionID)
	}
	return version, nil
}

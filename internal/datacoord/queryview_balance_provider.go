package datacoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// DataViewProvider exposes committed, immutable entries and exact-version references.
func (s *Server) DataViewProvider() balancer.DataViewProvider {
	return s.dataViewManager
}

// RefreshDataView commits the physical projection and returns the version
// the caller must wait for a certified serving QueryView to cover.
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

// A bounded, owner-routed sample of the local WAL domain. The physical
// candidate is frozen by the caller before this call begins.
func sampleViewReadAdmission(ctx context.Context, channel string) (*viewpb.QueryPlanMVCC, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	wal := streaming.WAL()
	if wal == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("WAL client is unavailable for %s", channel)
	}
	response, err := wal.QueryViewClient().GetMVCCTimestamp(ctx, qviews.ShardID{VChannel: channel}, &viewpb.GetMVCCTimestampRequest{Vchannel: channel})
	if err != nil {
		return nil, err
	}
	return response.GetMvcc(), nil
}

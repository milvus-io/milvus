package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetQueryViewSegmentLoadInfo returns current snapshots in a batch. A failed
// segment cannot terminate metadata acquisition for other segments or Views.
func (s *Server) GetQueryViewSegmentLoadInfo(ctx context.Context, req *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
	resp := &querypb.GetQueryViewSegmentLoadInfoResponse{Status: merr.Success()}
	if err := merr.CheckHealthy(s.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if s.mixCoord == nil {
		resp.Status = merr.Status(merr.WrapErrServiceNotReadyMsg("segment metadata provider is not initialized"))
		return resp, nil
	}
	infos, indexes, err := s.mixCoord.GetQueryViewSegmentLoadInfos(ctx, req.GetCollectionID(), req.GetSegmentIDs())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	resp.Results, resp.IndexInfoList = infos, indexes
	return resp, nil
}

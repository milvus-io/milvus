package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetQueryViewLoadInfo exposes the collection load configuration shared by SN and QN.
func (s *Server) GetQueryViewLoadInfo(ctx context.Context, req *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
	resp := &querypb.GetQueryViewLoadInfoResponse{
		Status:       merr.Success(),
		CollectionID: req.GetCollectionID(),
	}
	if err := merr.CheckHealthy(s.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if req.GetCollectionID() == 0 {
		resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg("collection id is zero"))
		return resp, nil
	}
	if s.qviewsRuntime == nil || s.qviewsRuntime.loadConfigStore == nil || s.qviewsRuntime.loadInfos == nil {
		resp.Status = merr.Status(merr.WrapErrServiceInternalMsg("query view load metadata is not initialized"))
		return resp, nil
	}
	version := req.GetVersion()
	if version == 0 {
		cfg, current := s.qviewsRuntime.loadConfigStore.GetConfigWithVersion(req.GetCollectionID())
		if cfg == nil {
			resp.Status = merr.Status(merr.WrapErrCollectionNotLoaded(req.GetCollectionID()))
			return resp, nil
		}
		version = current
	}
	info, err := s.qviewsRuntime.loadInfos.Get(ctx, req.GetCollectionID(), version)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	return info, nil
}

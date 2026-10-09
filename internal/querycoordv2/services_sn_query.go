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
	if s.qviewsRuntime == nil || s.qviewsRuntime.loadConfigStore == nil {
		resp.Status = merr.Status(merr.WrapErrServiceInternalMsg("query view runtime is nil"))
		return resp, nil
	}
	cfg, version := s.qviewsRuntime.loadConfigStore.GetConfigWithVersion(req.GetCollectionID())
	if cfg == nil {
		resp.Status = merr.Status(merr.WrapErrCollectionNotLoaded(req.GetCollectionID()))
		return resp, nil
	}
	resp.Version = version
	resp.PartitionIDs = append([]int64(nil), cfg.PartitionIDs...)
	resp.LoadFields = cloneLoadFields(cfg.LoadFields)
	return resp, nil
}

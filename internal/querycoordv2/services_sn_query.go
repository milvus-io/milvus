package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetQueryViewLoadInfo resolves current load state for version zero and the exact
// retained snapshot for a nonzero View load-info version.
// Snapshot publication belongs to the view producer, before it publishes a View.
func (s *Server) GetQueryViewLoadInfo(ctx context.Context, req *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
	resp := &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: req.GetCollectionID()}
	if err := merr.CheckHealthy(s.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if req.GetCollectionID() == 0 {
		resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg("collection id is zero"))
		return resp, nil
	}
	if req.GetVersion() == 0 {
		collection := s.meta.GetCollection(ctx, req.GetCollectionID())
		if collection == nil {
			resp.Status = merr.Status(merr.WrapErrCollectionNotLoaded(req.GetCollectionID()))
			return resp, nil
		}
		resp.PartitionIDs = s.meta.GetPartitionIDsByCollection(ctx, req.GetCollectionID())
		for _, fieldID := range collection.GetLoadFields() {
			resp.LoadFields = append(resp.LoadFields, &viewpb.LoadFieldConfig{FieldId: fieldID})
		}
		return resp, nil
	}
	if s.loadInfos == nil {
		resp.Status = merr.Status(merr.WrapErrServiceNotReadyMsg("query view load history is not initialized"))
		return resp, nil
	}
	info, err := s.loadInfos.GetLoadInfo(ctx, req.GetCollectionID(), req.GetVersion())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	return info, nil
}

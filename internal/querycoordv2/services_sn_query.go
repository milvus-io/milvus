package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetQueryViewLoadInfo exposes the currently loaded collection metadata to SN.
// The version is advisory until the versioned Coord load store is extracted.
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
	collection := s.meta.GetCollection(ctx, req.GetCollectionID())
	if collection == nil {
		resp.Status = merr.Status(merr.WrapErrCollectionNotLoaded(req.GetCollectionID()))
		return resp, nil
	}
	resp.PartitionIDs = s.meta.GetPartitionIDsByCollection(ctx, req.GetCollectionID())
	for _, field := range collection.GetLoadFields() {
		resp.LoadFields = append(resp.LoadFields, &messagespb.LoadFieldConfig{FieldId: field})
	}
	return resp, nil
}

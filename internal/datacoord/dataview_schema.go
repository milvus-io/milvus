package datacoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PrepareDataViewSchema runs inside the original ALTER callback, after WAL's
// consuming-side flush fence. Read schema publication is independent of load
// state: a later Load must see the same certified data as an already loaded one.
func (s *Server) PrepareDataViewSchema(ctx context.Context, id int64, published, target *schemapb.CollectionSchema) error {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return err
	}
	var project dataview.Projector
	if resource.SchemaRequiresMaterialization(published, target) {
		project = s.meta.LoadableProjection
	}
	_, err := s.dataViewManager.AdvanceSchema(ctx, id, target.GetVersion(), project)
	return err
}

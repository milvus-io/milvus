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
		project = s.meta.loadableProjection
	}
	_, err := s.dataViewManager.AdvanceSchema(ctx, id, target.GetVersion(), project)
	return err
}

// checkCompactionSchemaPublication prevents any physical replacement (sort,
// merge, clustering or schema-bump) from removing columns before the direct
// DDL has published its logical read schema. The task retries its metadata
// commit; direct DDL never waits for that task. Derived-output materialization
// is deliberately allowed before publication, because DDL waits for its result.
func (m *meta) checkCompactionSchemaPublication(ctx context.Context, collectionID int64, target *schemapb.CollectionSchema) error {
	if m.dataViewManager == nil {
		return nil
	}
	ref, err := m.dataViewManager.Latest(ctx, collectionID)
	if err != nil {
		return err
	}
	if ref == nil {
		return merr.WrapErrServiceNotReadyMsg("collection %d DataView is not initialized", collectionID)
	}
	defer ref.Deref()
	if ref.DataView().GetSchemaVersion() >= target.GetVersion() {
		return nil
	}
	published := m.GetCollection(collectionID)
	if published != nil && resource.SchemaRequiresMaterialization(published.Schema, target) {
		return nil
	}
	return merr.WrapErrServiceNotReadyMsg("collection %d must publish DataView schema %d before committing physical cleanup", collectionID, target.GetVersion())
}

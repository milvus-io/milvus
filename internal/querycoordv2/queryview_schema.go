package querycoordv2

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// updateQueryViewSchema runs inside the AlterCollection acknowledgement, after
// RootCoord has published metadata. It must not acquire a DDL broadcast lock.
func (s *Server) updateQueryViewSchema(ctx context.Context, req *querypb.UpdateLoadConfigRequest) error {
	for _, collectionID := range req.GetCollectionIDs() {
		collection, err := s.broker.DescribeCollection(ctx, collectionID)
		if err != nil {
			return err
		}
		if collection.GetSchema().GetVersion() < req.GetSchemaVersion() {
			return merr.WrapErrServiceNotReadyMsg("collection %d schema %d is not published", collectionID, req.GetSchemaVersion())
		}
		if !s.qviewsRuntime.loadConfigStore.Contains(collectionID) {
			continue
		}
		if err := s.qviewsRuntime.loadConfigStore.AddFunctionFields(ctx, collectionID, collection.GetSchema(), req.GetAddedLoadFields()); err != nil {
			return err
		}
		// This is a wake-up, not a persisted schema requirement. Reconciliation
		// always reads the latest published metadata, also after restart.
		s.qviewsRuntime.balancer.Trigger(balancer.TriggerScope{DirtyCollections: []int64{collectionID}})
		err = s.waitCollectionReadyAtSchemaVersion(ctx, &querypb.EnsureCollectionReadyRequest{
			CollectionID: collectionID, ExpectedVchannels: collection.GetVirtualChannelNames(),
		}, req.GetSchemaVersion())
		// Release removes the serving obligation. A subsequent load reads the
		// already published schema, and cannot revive an old schema requirement.
		if err != nil && !errors.Is(err, merr.ErrCollectionNotLoaded) {
			return err
		}
	}
	return nil
}

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type ExternalRefreshSchemaManager interface {
	PrepareExternalRefreshSchema(context.Context, int64, string, string) (*schemapb.CollectionSchema, error)
	PublishExternalRefreshSchema(context.Context, message.BroadcastResultRefreshExternalCollectionMessageV2) error
}

func (s *Server) SetExternalRefreshSchemaManager(manager ExternalRefreshSchemaManager) {
	s.externalRefreshSchemaManager = manager
}

func refreshOwnsDataView(ctx context.Context, id int64) (bool, error) {
	target, err := broadcast.GetPendingSchemaTarget(id)
	if err != nil {
		return true, nil
	}
	return target != nil && target.RefreshJobID != 0, nil
}

func (s *Server) publishExternalRefresh(ctx context.Context, result message.BroadcastResultRefreshExternalCollectionMessageV2) error {
	id := result.Message.Header().GetCollectionId()
	// One immutable snapshot contains the new membership and its schema. Ordinary
	// recompute is fenced by the pending broadcast, including during recovery.
	if _, err := s.dataViewManager.PublishRefresh(ctx, id, result.Message.MustBody().GetSchema().GetVersion(), s.meta.LoadableProjection); err != nil {
		return err
	}
	if err := s.externalRefreshSchemaManager.PublishExternalRefreshSchema(ctx, result); err != nil {
		return err
	}
	meta.NotifyCollectionIndexBuild(s.notifyIndexChan, id)
	return resource.Resource().Refresh(ctx, id)
}

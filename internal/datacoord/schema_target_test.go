package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPreparationSchemaOverlay(t *testing.T) {
	mockey.PatchConvey("pending target drives jobs without mutating published cache", t, func() {
		m := newEmptyTestMeta()
		m.preparationSchema = coordinatorPreparationSchema
		for _, id := range []int64{1, 2, 3} {
			m.metaStore.PutCollection(&collectionInfo{ID: id, Schema: &schemapb.CollectionSchema{Version: 1}, DatabaseName: "db"})
		}
		mockey.Mock(broadcast.GetPendingSchemaTarget).To(func(id int64) (*broadcaster.SchemaTarget, error) {
			switch id {
			case 1:
				return &broadcaster.SchemaTarget{Schema: &schemapb.CollectionSchema{Version: 2, Properties: []*commonpb.KeyValuePair{{Key: "collection.ttl.seconds", Value: "60"}}}}, nil
			case 2:
				return nil, merr.WrapErrServiceNotReadyMsg("recovering")
			default:
				return nil, nil
			}
		}).Build()
		ctx := context.Background()
		ready := m.collectionsForPreparation(ctx)
		require.Len(t, ready, 2, "one recovering DDL must not block other collections")
		byID := make(map[int64]*collectionInfo)
		for _, collection := range ready {
			byID[collection.ID] = collection
		}
		require.EqualValues(t, 2, byID[1].Schema.Version)
		require.Equal(t, "60", byID[1].Properties["collection.ttl.seconds"])
		require.Empty(t, m.GetCollection(1).Properties)
		require.Equal(t, "db", byID[1].DatabaseName)
		require.EqualValues(t, 1, byID[3].Schema.Version)
		require.EqualValues(t, 1, m.GetCollection(1).Schema.Version)
		require.Len(t, m.GetCollections(), 3, "committed metadata enumeration remains complete")
		_, err := m.collectionForPreparation(ctx, m.GetCollection(2))
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		handler := &ServerHandler{s: &Server{meta: m}}
		got, err := handler.GetCollection(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 2, got.Schema.Version)
		_, err = handler.GetCollection(ctx, 2)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
	})
}

package querynodev2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/qvresource"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestNewQueryViewSegmentManager(t *testing.T) {
	ctx := context.Background()
	t.Run("nil node", func(t *testing.T) {
		var node *QueryNode
		manager, err := node.NewQueryViewSegmentManager(ctx, nil, nil, nil)
		require.Nil(t, manager)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, "missing QueryNode")
	})
	t.Run("missing factory", func(t *testing.T) {
		node := &QueryNode{
			loadResourceBudget: &segments.LoadResourceBudget{},
			chunkManager:       struct{ storage.ChunkManager }{},
		}
		manager, err := node.NewQueryViewSegmentManager(ctx,
			struct {
				qnview.QueryViewLoadMetadataProvider
			}{},
			struct{ wal.TransformLogStreamManager }{}, nil)
		require.Nil(t, manager)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, "missing SegmentLoadInfoStreamFactory")
	})
	t.Run("forward dependencies and result", func(t *testing.T) {
		node := &QueryNode{loadResourceBudget: &segments.LoadResourceBudget{}, chunkManager: struct{ storage.ChunkManager }{}}
		meta := &struct {
			qnview.QueryViewLoadMetadataProvider
		}{}
		streams := &struct{ wal.TransformLogStreamManager }{}
		factory := &struct {
			qnview.SegmentLoadInfoStreamFactory
		}{}
		want := &struct{ qnview.SegmentManager }{}
		patch := mockey.Mock(qvresource.NewQueryViewSegmentManager).To(func(gotCtx context.Context, budget *segments.LoadResourceBudget, cm storage.ChunkManager, gotMeta qnview.QueryViewLoadMetadataProvider, gotStreams wal.TransformLogStreamManager, gotFactory qnview.SegmentLoadInfoStreamFactory) (qnview.SegmentManager, error) {
			require.Equal(t, ctx, gotCtx)
			require.Same(t, node.loadResourceBudget, budget)
			require.Equal(t, node.chunkManager, cm)
			require.Same(t, meta, gotMeta)
			require.Same(t, streams, gotStreams)
			require.Same(t, factory, gotFactory)
			return want, nil
		}).Build()
		defer patch.UnPatch()
		manager, err := node.NewQueryViewSegmentManager(ctx, meta, streams, factory)
		require.NoError(t, err)
		require.Same(t, want, manager)
	})
}

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Schema-only DDL fixtures have no loaded views but still publish the public
// read schema through a fully initialized registry.
func newSchemaViewRegistry(t *testing.T) *coordview.ShardViewRegistry {
	t.Helper()
	backend := &emptySchemaViews{}
	registry, err := coordview.RecoverShardViewRegistry(t.Context(), backend, backend)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	return registry
}

type emptySchemaViews struct{}

func (*emptySchemaViews) ListQueryViews(context.Context) ([]*viewpb.QueryViewOfShard, error) {
	return nil, nil
}

func (*emptySchemaViews) SaveQueryViews(context.Context, []*viewpb.QueryViewOfShard) error {
	return nil
}

func (*emptySchemaViews) SyncViews(context.Context, syncer.SyncGroup) error { return nil }

func (*emptySchemaViews) Close() error { return nil }

package querycoordv2

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv"
)

func walkAll(t *testing.T, store kv.MetaKv, prefix string) map[string]string {
	t.Helper()
	got := make(map[string]string)
	require.NoError(t, store.WalkWithPrefix(context.Background(), prefix, 10, func(key, value []byte) error {
		got[string(key)] = string(value)
		return nil
	}))
	return got
}

// failingWalkKV fails every walk.
type failingWalkKV struct{ kv.MetaKv }

func (failingWalkKV) WalkWithPrefix(context.Context, string, int, func([]byte, []byte) error) error {
	return errors.New("walk failed")
}

func TestMetaPrefetchKV(t *testing.T) {
	ctx := context.Background()
	collections := queryview.VersionedLoadInfoPrefix + "/"
	newBase := func(t *testing.T) kv.MetaKv {
		client, _ := kvfactory.GetEtcdAndPath()
		base := etcdkv.NewEtcdKV(client, "meta-prefetch-"+uuid.New().String())
		t.Cleanup(func() { _ = base.RemoveWithPrefix(context.Background(), "") })
		require.NoError(t, base.Save(ctx, collections+"1", "v1"))
		require.NoError(t, base.Save(ctx, collections+"2", "v2"))
		require.NoError(t, base.Save(ctx, "other/1", "o1"))
		return base
	}

	t.Run("serves walks of prefetched prefixes from the prefetch", func(t *testing.T) {
		base := newBase(t)
		prefetch := newMetaPrefetchKV(ctx, base)
		want := walkAll(t, base, collections)
		require.Len(t, want, 2)
		require.Equal(t, want, walkAll(t, prefetch, collections))
		// A change made behind the prefetch is not seen: the walk is replayed.
		require.NoError(t, base.Save(ctx, collections+"3", "v3"))
		require.Equal(t, want, walkAll(t, prefetch, collections))
		// Other prefixes pass through.
		require.Equal(t, walkAll(t, base, "other/"), walkAll(t, prefetch, "other/"))
		require.Len(t, walkAll(t, prefetch, "other/"), 1)
	})

	t.Run("a write through it drops the prefetch", func(t *testing.T) {
		prefetch := newMetaPrefetchKV(ctx, newBase(t))
		walkAll(t, prefetch, collections)
		require.NoError(t, prefetch.Save(ctx, collections+"3", "v3"))
		require.Len(t, walkAll(t, prefetch, collections), 3)
	})

	t.Run("release makes it a pass-through", func(t *testing.T) {
		base := newBase(t)
		prefetch := newMetaPrefetchKV(ctx, base)
		walkAll(t, prefetch, collections)
		prefetch.release()
		require.NoError(t, base.Save(ctx, collections+"3", "v3"))
		require.Len(t, walkAll(t, prefetch, collections), 3)
	})

	t.Run("a failed prefetch walks again", func(t *testing.T) {
		base := newBase(t)
		prefetch := newMetaPrefetchKV(ctx, failingWalkKV{base})
		prefetch.MetaKv = base
		require.Len(t, walkAll(t, prefetch, collections), 2)
	})

	t.Run("a callback error stops the replay", func(t *testing.T) {
		prefetch := newMetaPrefetchKV(ctx, newBase(t))
		injected := errors.New("injected")
		calls := 0
		err := prefetch.WalkWithPrefix(ctx, collections, 10, func([]byte, []byte) error {
			calls++
			return injected
		})
		require.ErrorIs(t, err, injected)
		require.Equal(t, 1, calls)
	})
}

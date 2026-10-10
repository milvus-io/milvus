package loadmgr

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSchemaRequirementPersistsAndNeverRollsBack(t *testing.T) {
	var persisted *querypb.CollectionLoadInfo
	var saveErr error
	patch := mockey.Mock((*querycoord.Catalog).SaveCollection).To(func(_ *querycoord.Catalog, _ context.Context, c *querypb.CollectionLoadInfo, _ ...*querypb.PartitionLoadInfo) error {
		if saveErr != nil {
			return saveErr
		}
		persisted = proto.Clone(c).(*querypb.CollectionLoadInfo)
		return nil
	}).Build()
	defer patch.UnPatch()
	store := &LoadConfigStore{catalog: querycoord.NewCatalog(nil), collectionLocks: lock.NewKeyLock[int64](), configs: make(map[int64]*LoadConfig), versions: make(map[int64]uint64)}
	loaded, err := store.UpdateSchema(t.Context(), 1, 2)
	require.NoError(t, err)
	require.False(t, loaded, "alter must not load a released collection")
	require.NoError(t, store.Put(t.Context(), &LoadConfig{CollectionID: 1}))
	old := store.Get(1)
	loaded, err = store.UpdateSchema(t.Context(), 1, 2)
	require.NoError(t, err)
	require.True(t, loaded)
	current := store.Get(1)
	require.NotEqual(t, old.ConfigVersion, current.ConfigVersion)
	require.Equal(t, int32(0), old.Config.SchemaVersion, "published snapshots stay immutable")
	recovered := buildFromPersisted(persisted, nil, nil)
	require.Equal(t, int32(2), recovered.SchemaVersion)
	require.Equal(t, current.ConfigVersion, loadInfoVersion(recovered))
	require.NoError(t, store.Put(t.Context(), &LoadConfig{CollectionID: 1, SchemaVersion: 1}))
	require.Equal(t, current.ConfigVersion, store.GetConfigVersion(1))
	saveErr = merr.WrapErrServiceUnavailableMsg("test catalog unavailable")
	_, err = store.UpdateSchema(t.Context(), 1, 3)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, int32(2), store.GetConfig(1).SchemaVersion)
}

package queryview

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCollectionLoadInfoExactRead(t *testing.T) {
	_, storage := newTestCatalog(t)
	ctx := t.Context()
	info := &viewpb.CollectionLoadConfig{
		CollectionID: 1, Version: 10,
		PartitionIds: []int64{100}, RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 101, IndexIds: []int64{201, 202}}},
	}
	encoded, err := proto.Marshal(info)
	require.NoError(t, err)
	key := func(v uint64) string { return fmt.Sprintf("%s/1/%d", VersionedLoadInfoPrefix, v) }
	require.NoError(t, storage.Save(ctx, key(10), string(encoded)))
	got, err := GetCollectionLoadInfo(ctx, storage, 1, 10)
	require.NoError(t, err)
	require.True(t, proto.Equal(info, got))
	missing, err := GetCollectionLoadInfo(ctx, storage, 1, 9)
	require.NoError(t, err)
	require.Nil(t, missing, "never substitute the latest record")
	require.NoError(t, storage.Save(ctx, key(11), string(encoded)))
	_, err = GetCollectionLoadInfo(ctx, storage, 1, 11)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, storage.Save(ctx, key(12), "invalid protobuf"))
	_, err = GetCollectionLoadInfo(ctx, storage, 1, 12)
	require.Error(t, err)
	require.NoError(t, storage.Remove(ctx, key(10)))
	missing, err = GetCollectionLoadInfo(ctx, storage, 1, 10)
	require.NoError(t, err)
	require.Nil(t, missing, "released records must not survive in a reader cache")
}

package queryview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadInfoCatalogPersistsHistoricalDescriptors(t *testing.T) {
	_, storage := newTestCatalog(t)
	ctx := context.Background()
	t.Cleanup(func() { require.NoError(t, storage.RemoveWithPrefix(context.Background(), "coord/qv-load-info/")) })
	catalog := NewLoadInfoCatalog(storage)
	missing, err := catalog.GetLoadInfo(ctx, 1, 10)
	require.NoError(t, err)
	require.Nil(t, missing)
	info := &querypb.GetQueryViewLoadInfoResponse{CollectionID: 1, Version: 10, Status: merr.Success(), PartitionIDs: []int64{100}, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 101, IndexId: 201}}, IndexInfoList: []*indexpb.IndexInfo{{FieldID: 101, IndexID: 201, IndexName: "retained"}}}
	require.NoError(t, catalog.SaveLoadInfo(ctx, info))
	recovered, err := NewLoadInfoCatalog(storage).GetLoadInfo(ctx, 1, 10)
	require.NoError(t, err)
	require.True(t, proto.Equal(info, recovered))
	bytes, err := proto.Marshal(info)
	require.NoError(t, err)
	require.NoError(t, storage.Save(ctx, loadInfoKey(1, 11), string(bytes)))
	_, err = catalog.GetLoadInfo(ctx, 1, 11)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, storage.Save(ctx, loadInfoKey(1, 12), "invalid protobuf"))
	_, err = catalog.GetLoadInfo(ctx, 1, 12)
	require.Error(t, err)
}

package loadmgr

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestAddFunctionFieldsPreservesPartialLoadsAndRelease(t *testing.T) {
	mockey.PatchConvey("schema delta is atomic and idempotent", t, func() {
		catalog := &mocks.QueryCoordCatalog{}
		save := mockey.Mock((*mocks.QueryCoordCatalog).SaveCollection).Return(nil).Build()
		store := &LoadConfigStore{
			catalog: catalog, collectionLocks: lock.NewKeyLock[int64](),
			configs: map[int64]*LoadConfig{1: {CollectionID: 1, RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 7}}}}, versions: map[int64]uint64{1: 1},
		}
		schema := &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{
			{InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
			{InputFieldIds: []int64{200}, OutputFieldIds: []int64{201}},
		}}
		fields := []*messagespb.LoadFieldConfig{{FieldId: 101, IndexId: 8}, {FieldId: 201, IndexId: 9}, {FieldId: 300}}
		require.NoError(t, store.AddFunctionFields(t.Context(), 1, schema, fields))
		requireFieldConfigsEqual(t, []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 7}, {FieldId: 101, IndexId: 8}}, store.GetConfig(1).RequiredFields)
		version := store.GetConfigVersion(1)
		require.NotEqual(t, uint64(1), version)
		require.NoError(t, store.AddFunctionFields(t.Context(), 1, schema, fields))
		require.Equal(t, version, store.GetConfigVersion(1))
		require.Equal(t, 1, save.Times())
		require.NoError(t, store.AddFunctionFields(t.Context(), 2, schema, fields))
		require.Nil(t, store.GetConfig(2))
		// A failed persistence must not publish the next field/version.
		save.UnPatch()
		mockey.Mock((*mocks.QueryCoordCatalog).SaveCollection).Return(merr.WrapErrServiceInternalMsg("storage unavailable")).Build()
		schema.Functions = append(schema.Functions, &schemapb.FunctionSchema{InputFieldIds: []int64{100}, OutputFieldIds: []int64{102}})
		require.Error(t, store.AddFunctionFields(t.Context(), 1, schema, []*messagespb.LoadFieldConfig{{FieldId: 102}}))
		require.Equal(t, version, store.GetConfigVersion(1))
		persisted := store.GetConfig(1).toCollectionLoadInfoProto()
		recovered := buildFromPersisted(persisted, []*querypb.PartitionLoadInfo{}, nil)
		requireFieldConfigsEqual(t, store.GetConfig(1).RequiredFields, recovered.RequiredFields)
	})
}

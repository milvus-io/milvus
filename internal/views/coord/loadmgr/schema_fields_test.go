package loadmgr

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestAddFunctionFieldsPreservesPartialLoadsAndRelease(t *testing.T) {
	mockey.PatchConvey("schema delta is atomic and idempotent", t, func() {
		store, hooks := newTestStore(t)
		require.NoError(t, store.Put(t.Context(), &LoadConfig{CollectionID: 1, RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 100}}}))
		schema := &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{
			{InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
			{InputFieldIds: []int64{200}, OutputFieldIds: []int64{201}},
		}}
		fields := []*viewpb.LoadFieldConfig{{FieldId: 101, IndexIds: []int64{8}}, {FieldId: 201, IndexIds: []int64{9}}, {FieldId: 300}}
		require.NoError(t, store.AddFunctionFields(t.Context(), 1, schema, fields))
		requireFieldConfigsEqual(t, []*viewpb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}}, store.GetConfig(1).RequiredFields)
		version := store.GetConfigVersion(1)
		require.NotEqual(t, uint64(1), version)
		require.NoError(t, store.AddFunctionFields(t.Context(), 1, schema, fields))
		require.Equal(t, version, store.GetConfigVersion(1))
		require.NoError(t, store.AddFunctionFields(t.Context(), 2, schema, fields))
		require.Nil(t, store.GetConfig(2))
		// A failed persistence must not publish the next field/version.
		hooks.beforeSave = func(*viewpb.CollectionLoadConfig) error {
			return merr.WrapErrServiceInternalMsg("storage unavailable")
		}
		schema.Functions = append(schema.Functions, &schemapb.FunctionSchema{InputFieldIds: []int64{100}, OutputFieldIds: []int64{102}})
		require.Error(t, store.AddFunctionFields(t.Context(), 1, schema, []*viewpb.LoadFieldConfig{{FieldId: 102}}))
		require.Equal(t, version, store.GetConfigVersion(1))
		persisted := store.GetConfig(1).toCollectionLoadConfigProto()
		recovered := buildFromPersisted(persisted)
		requireFieldConfigsEqual(t, store.GetConfig(1).RequiredFields, recovered.RequiredFields)
	})
}

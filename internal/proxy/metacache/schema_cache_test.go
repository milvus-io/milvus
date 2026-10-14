package metacache

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSchemaCacheProjectsWALSystemFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test", Version: 3, EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 102, Name: "items", Fields: []*schemapb.FieldSchema{
			{FieldID: 103, Name: "value", DataType: schemapb.DataType_Int64},
		}}},
	}
	original := proto.Clone(schema)
	patch := mockey.Mock(fetchCollectionSchema).Return(&streamingpb.StreamingNodeGetCollectionSchemaResponse{Schema: schema}, nil).Build()
	defer patch.UnPatch()
	for _, version := range []*int32{nil, &schema.Version} {
		cache, err := NewMetaCache(nil)
		require.NoError(t, err)
		body, err := cache.resolveCollectionSchema(t.Context(), 1, []string{"p_1v0"}, version)
		require.NoError(t, err)
		require.Len(t, body.GetFields(), 2)
		for i, field := range body.GetFields() {
			require.True(t, proto.Equal(schema.Fields[i+2], field))
		}
		require.Len(t, body.GetStructArrayFields(), 1)
		require.True(t, proto.Equal(schema.StructArrayFields[0], body.GetStructArrayFields()[0]))
		require.EqualValues(t, 3, body.GetVersion())
		require.True(t, body.GetEnableDynamicField())
		require.True(t, proto.Equal(original, schema), "projecting the client schema must not mutate WAL history")
	}
}

func TestSchemaCacheReadWriteSelectors(t *testing.T) {
	cache, err := NewMetaCache(nil)
	require.NoError(t, err)
	currentWrite := int32(1)
	var exactReads, writeReads int
	patch := mockey.Mock(fetchCollectionSchema).To(func(_ context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
		require.EqualValues(t, 1, req.GetCollectionId())
		require.Equal(t, "p_1v0", req.GetVchannel())
		version := req.GetSchemaVersion()
		if req.GetCurrentWrite() {
			version = currentWrite
			writeReads++
		} else {
			exactReads++
		}
		return &streamingpb.StreamingNodeGetCollectionSchemaResponse{Schema: &schemapb.CollectionSchema{
			Name: "test", Version: version, Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
		}}, nil
	}).Build()
	defer patch.UnPatch()
	zero := int32(0)
	published, err := cache.resolveCollectionSchema(t.Context(), 1, []string{"p_1v0"}, &zero)
	require.NoError(t, err)
	cache.collections[1] = newCollectionInfo(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1, DbName: "default", VirtualChannelNames: []string{"p_1v0"},
	}, published, false, "", false, false)
	write, err := cache.GetCollectionWriteInfo(t.Context(), "default", "", 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, write.Schema.GetVersion())
	read, err := cache.GetCollectionInfo(t.Context(), "default", "", 1)
	require.NoError(t, err)
	require.EqualValues(t, 0, read.Schema.GetVersion(), "write preparation cannot expose a pending read schema")
	one := int32(1)
	body, err := cache.resolveCollectionSchema(t.Context(), 1, []string{"p_1v0"}, &one)
	require.NoError(t, err)
	require.Same(t, body, write.Schema, "selectors share immutable schema bodies")
	require.Equal(t, 1, exactReads)
	currentWrite = 2
	cached, err := cache.GetCollectionWriteInfo(t.Context(), "default", "", 1)
	require.NoError(t, err)
	require.Same(t, write.Schema, cached.Schema)
	require.Equal(t, 1, writeReads)
	cache.RemoveCollectionsByID(t.Context(), 1)
	cache.collections[1] = newCollectionInfo(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1, DbName: "default", VirtualChannelNames: []string{"p_1v0"},
	}, body, false, "", false, false)
	refreshed, err := cache.GetCollectionWriteInfo(t.Context(), "default", "", 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, refreshed.Schema.GetVersion())
	require.Equal(t, 2, writeReads, "invalidation must expire the write selector even while public metadata stays old")
	require.EqualValues(t, 0, read.Schema.GetVersion(), "an in-flight read retains its old schema")
}

func TestSchemaCacheRejectsWrongVersion(t *testing.T) {
	patch := mockey.Mock(fetchCollectionSchema).Return(&streamingpb.StreamingNodeGetCollectionSchemaResponse{Schema: &schemapb.CollectionSchema{Version: 2}}, nil).Build()
	defer patch.UnPatch()
	cache, err := NewMetaCache(nil)
	require.NoError(t, err)
	version := int32(1)
	_, err = cache.resolveCollectionSchema(t.Context(), 1, []string{"p_1v0"}, &version)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, cached := cache.schemaBodies.Load(schemaIdentity{1, 2})
	require.False(t, cached)
}

func TestSchemaSelectorsProjectMetadataWithoutChangingHistory(t *testing.T) {
	body, err := NewSchemaInfo(&schemapb.CollectionSchema{
		Name: "original", Version: 3,
		Properties: []*commonpb.KeyValuePair{{Key: common.CollectionTTLConfigKey, Value: "30"}},
		Fields:     []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
	})
	require.NoError(t, err)
	original := proto.Clone(body.CollectionSchema)
	metadata := &schemapb.CollectionSchema{Name: "renamed", Description: "new description", ExternalSource: "s3://new", ExternalSpec: "{}"}
	properties := []*commonpb.KeyValuePair{
		{Key: common.TimezoneKey, Value: "Asia/Shanghai"},
		{Key: common.AllowInsertAutoIDKey, Value: "true"},
		{Key: common.CollectionTTLConfigKey, Value: "60"},
	}
	selected, err := body.withCollectionMetadata(metadata, properties)
	require.NoError(t, err)
	require.Equal(t, "renamed", selected.GetName())
	require.Equal(t, "renamed", selected.Name)
	require.Equal(t, "new description", selected.GetDescription())
	require.Equal(t, "s3://new", selected.GetExternalSource())
	require.Equal(t, "{}", selected.GetExternalSpec())
	require.Equal(t, "Asia/Shanghai", selected.SchemaHelper.GetTimezone())
	allow, _ := common.IsAllowInsertAutoID(selected.GetProperties()...)
	require.True(t, allow)
	require.EqualValues(t, 30*time.Second, GetCollectionTTL(selected.GetProperties()), "published metadata cannot override the write target's TTL")
	require.EqualValues(t, 3, selected.GetVersion())
	require.True(t, proto.Equal(original, body.CollectionSchema))
	properties = nil // Deleting metadata must also remove its previous value.
	cleared, err := selected.withCollectionMetadata(metadata, properties)
	require.NoError(t, err)
	require.Equal(t, common.DefaultTimezone, cleared.SchemaHelper.GetTimezone())
	allow, _ = common.IsAllowInsertAutoID(cleared.GetProperties()...)
	require.False(t, allow)
	require.EqualValues(t, 30*time.Second, GetCollectionTTL(cleared.GetProperties()))
}

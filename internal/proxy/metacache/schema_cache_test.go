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
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type (
	schemaTestWAL    struct{ streaming.WALAccesser }
	schemaTestClient struct{ streaming.SchemaClient }
)

func TestFetchSchemaUsesExplicitSelectorIncludingZero(t *testing.T) {
	client := &schemaTestClient{}
	wal := &schemaTestWAL{}
	patch := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer patch.UnPatch()
	patch = mockey.Mock((*schemaTestWAL).SchemaClient).Return(client).Build()
	defer patch.UnPatch()
	var exactRequests []streaming.GetSchemaVersionRequest
	patch = mockey.Mock((*schemaTestClient).GetSchemaVersion).To(func(_ *schemaTestClient, _ context.Context, req streaming.GetSchemaVersionRequest) (*streaming.SchemaSnapshot, error) {
		exactRequests = append(exactRequests, req)
		return &streaming.SchemaSnapshot{Schema: &schemapb.CollectionSchema{Version: req.Version}}, nil
	}).Build()
	defer patch.UnPatch()
	var writeRequests []streaming.GetCurrentWriteSchemaRequest
	patch = mockey.Mock((*schemaTestClient).GetCurrentWriteSchema).To(func(_ *schemaTestClient, _ context.Context, req streaming.GetCurrentWriteSchemaRequest) (*streaming.SchemaSnapshot, error) {
		writeRequests = append(writeRequests, req)
		return &streaming.SchemaSnapshot{Schema: &schemapb.CollectionSchema{Version: 3}}, nil
	}).Build()
	defer patch.UnPatch()
	zero := int32(0)
	read, err := fetchCollectionSchema(t.Context(), "p_1v0", &zero)
	require.NoError(t, err)
	require.Zero(t, read.Schema.GetVersion())
	write, err := fetchCollectionSchema(t.Context(), "p_1v0", nil)
	require.NoError(t, err)
	require.EqualValues(t, 3, write.Schema.GetVersion())
	require.Equal(t, []streaming.GetSchemaVersionRequest{{VChannel: "p_1v0", Version: 0}}, exactRequests)
	require.Equal(t, []streaming.GetCurrentWriteSchemaRequest{{VChannel: "p_1v0"}}, writeRequests)
}

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
	patch := mockey.Mock(fetchCollectionSchema).Return(&streaming.SchemaSnapshot{Schema: schema}, nil).Build()
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
	patch := mockey.Mock(fetchCollectionSchema).To(func(_ context.Context, vchannel string, selectedVersion *int32) (*streaming.SchemaSnapshot, error) {
		require.Equal(t, "p_1v0", vchannel)
		version := int32(0)
		if selectedVersion == nil {
			version = currentWrite
			writeReads++
		} else {
			version = *selectedVersion
			exactReads++
		}
		return &streaming.SchemaSnapshot{Schema: &schemapb.CollectionSchema{
			Name: "test", Version: version, Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
		}}, nil
	}).Build()
	defer patch.UnPatch()
	zero := int32(0)
	published, err := cache.resolveCollectionSchema(t.Context(), 1, []string{"p_1v0"}, &zero)
	require.NoError(t, err)
	cache.collections[1] = newCollectionInfo(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1, DbName: "default", VirtualChannelNames: []string{"p_1v0"},
	}, published, false, "")
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
	}, body, false, "")
	refreshed, err := cache.GetCollectionWriteInfo(t.Context(), "default", "", 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, refreshed.Schema.GetVersion())
	require.Equal(t, 2, writeReads, "invalidation must expire the write selector even while public metadata stays old")
	require.EqualValues(t, 0, read.Schema.GetVersion(), "an in-flight read retains its old schema")
}

func TestSchemaCacheRejectsWrongVersion(t *testing.T) {
	patch := mockey.Mock(fetchCollectionSchema).Return(&streaming.SchemaSnapshot{Schema: &schemapb.CollectionSchema{Version: 2}}, nil).Build()
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

func TestWriteSchemaFillCannotResurrectInvalidatedSelection(t *testing.T) {
	cache, err := NewMetaCache(nil)
	require.NoError(t, err)
	published, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "test", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}})
	require.NoError(t, err)
	metadata := &milvuspb.DescribeCollectionResponse{CollectionID: 1, DbName: "default", VirtualChannelNames: []string{"p_1v0"}}
	cache.collections[1] = newCollectionInfo(metadata, published, false, "")
	entered, release := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock(fetchCollectionSchema).To(func(_ context.Context, _ string, version *int32) (*streaming.SchemaSnapshot, error) {
		require.Nil(t, version)
		close(entered)
		<-release
		return &streaming.SchemaSnapshot{Schema: &schemapb.CollectionSchema{Name: "test", Version: 1, Fields: published.GetFields()}}, nil
	}).Build()
	defer patch.UnPatch()
	finished := make(chan *CollectionInfo, 1)
	go func() {
		info, err := cache.GetCollectionWriteInfo(t.Context(), "default", "", 1)
		if err == nil {
			finished <- info
		} else {
			finished <- nil
		}
	}()
	<-entered
	cache.RemoveCollectionsByID(t.Context(), 1)
	// Install the post-invalidation metadata entry while the earlier request
	// still owns the old selector. Completing it must not fill the new one.
	cache.collections[1] = newCollectionInfo(metadata, published, false, "")
	close(release)
	oldRequest := <-finished
	require.NotNil(t, oldRequest)
	require.EqualValues(t, 1, oldRequest.Schema.GetVersion())
	require.Nil(t, cache.collections[1].writeSchema.schema)
	require.Same(t, published, cache.collections[1].Schema)
}

func TestSchemaCacheUnavailableHistoryDoesNotFallBackToWrite(t *testing.T) {
	cache, err := NewMetaCache(nil)
	require.NoError(t, err)
	version := int32(0)
	_, err = cache.resolveCollectionSchema(t.Context(), 1, nil, &version)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	patch := mockey.Mock(fetchCollectionSchema).To(func(_ context.Context, _ string, selected *int32) (*streaming.SchemaSnapshot, error) {
		require.NotNil(t, selected)
		require.Zero(t, *selected)
		return nil, merr.WrapErrServiceUnavailableMsg("history not observed")
	}).Build()
	defer patch.UnPatch()
	_, err = cache.resolveCollectionSchema(t.Context(), 1, []string{"p_1v0"}, &version)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	_, cached := cache.schemaBodies.Load(schemaIdentity{1, 0})
	require.False(t, cached)
}

func TestWriteSchemaLookupErrorsStayUncached(t *testing.T) {
	cache, err := NewMetaCache(nil)
	require.NoError(t, err)
	patch := mockey.Mock((*MetaCache).GetCollectionInfo).Return(nil, merr.ErrCollectionNotFound).Build()
	_, err = cache.GetCollectionWriteInfo(t.Context(), "default", "test", 1)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	patch.UnPatch()
	published, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "test"})
	require.NoError(t, err)
	info := newCollectionInfo(&milvuspb.DescribeCollectionResponse{CollectionID: 1, VirtualChannelNames: []string{"p_1v0"}}, published, false, "")
	patch = mockey.Mock((*MetaCache).GetCollectionInfo).Return(info, nil).Build()
	defer patch.UnPatch()
	fetch := mockey.Mock(fetchCollectionSchema).Return(nil, merr.ErrServiceUnavailable).Build()
	_, err = cache.GetCollectionWriteInfo(t.Context(), "default", "test", 1)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, info.writeSchema.schema)
	fetch.UnPatch()
	// A malformed internal snapshot must fail, rather than poisoning history.
	fetch = mockey.Mock(fetchCollectionSchema).Return(&streaming.SchemaSnapshot{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "duplicate"}, {FieldID: 101, Name: "duplicate"},
	}}}, nil).Build()
	defer fetch.UnPatch()
	_, err = cache.GetCollectionWriteInfo(t.Context(), "default", "test", 1)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Nil(t, info.writeSchema.schema)
}

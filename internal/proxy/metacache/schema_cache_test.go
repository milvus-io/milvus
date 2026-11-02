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

func TestSchemaSelectorsProjectMetadataWithoutChangingHistory(t *testing.T) {
	body, err := NewSchemaInfo(&schemapb.CollectionSchema{
		Name: "original", Version: 3,
		Properties: []*commonpb.KeyValuePair{{Key: common.CollectionTTLConfigKey, Value: "30"}},
		Fields:     []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
	})
	require.NoError(t, err)
	original := proto.Clone(body.CollectionSchema)
	metadata := &CollectionInfo{Name: "renamed", Description: "new description", ExternalSource: "s3://new", ExternalSpec: "{}", Properties: []*commonpb.KeyValuePair{
		{Key: common.TimezoneKey, Value: "Asia/Shanghai"}, {Key: common.AllowInsertAutoIDKey, Value: "true"}, {Key: common.CollectionTTLConfigKey, Value: "60"},
	}}
	selected := withCollectionMetadata(body, metadata)
	require.Equal(t, "renamed", selected.Name)
	require.Equal(t, "new description", selected.Description)
	require.Equal(t, "s3://new", selected.ExternalSource)
	require.Equal(t, "{}", selected.ExternalSpec)
	require.Equal(t, "Asia/Shanghai", selected.SchemaHelper.GetTimezone())
	allow, _ := common.IsAllowInsertAutoID(selected.Properties...)
	require.True(t, allow)
	require.EqualValues(t, 30*time.Second, GetCollectionTTL(selected.Properties))
	require.Same(t, body.Fields[0], selected.Fields[0])
	require.Same(t, body.FieldMap, selected.FieldMap)
	require.True(t, proto.Equal(original, body.CollectionSchema))
	metadata.Properties = nil
	cleared := withCollectionMetadata(selected, metadata)
	require.Equal(t, common.DefaultTimezone, cleared.SchemaHelper.GetTimezone())
	allow, _ = common.IsAllowInsertAutoID(cleared.Properties...)
	require.False(t, allow)
	require.EqualValues(t, 30*time.Second, GetCollectionTTL(cleared.Properties))
	require.NotSame(t, cleared.CollectionSchema, withCollectionMetadata(cleared, metadata).CollectionSchema)
}

func TestColdWriteDoesNotResolveReadSchema(t *testing.T) {
	client := &schemaTestClient{}
	wal := &schemaTestWAL{}
	p := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer p.UnPatch()
	p = mockey.Mock((*schemaTestWAL).SchemaClient).Return(client).Build()
	defer p.UnPatch()
	var reads, writes int
	readErr := error(merr.ErrServiceUnavailable)
	var writeErr error
	body, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c", Version: 2, Properties: []*commonpb.KeyValuePair{{Key: common.CollectionTTLConfigKey, Value: "30"}}})
	require.NoError(t, err)
	p = mockey.Mock((*schemaTestClient).GetSchemaVersion).To(func(_ *schemaTestClient, _ context.Context, req streaming.GetSchemaVersionRequest) (*streaming.SchemaSnapshot, error) {
		reads++
		require.Zero(t, req.Version)
		if readErr != nil {
			return nil, readErr
		}
		result, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c", Version: req.Version})
		return &streaming.SchemaSnapshot{Schema: result.CollectionSchema}, err
	}).Build()
	defer p.UnPatch()
	p = mockey.Mock((*schemaTestClient).GetCurrentWriteSchema).To(func(_ *schemaTestClient, _ context.Context, req streaming.GetCurrentWriteSchemaRequest) (*streaming.SchemaSnapshot, error) {
		writes++
		require.Equal(t, "p_1v0", req.VChannel)
		return &streaming.SchemaSnapshot{Schema: body.CollectionSchema}, writeErr
	}).Build()
	defer p.UnPatch()
	cache, err := NewMetaCache(nil)
	require.NoError(t, err)
	p = mockey.Mock((*MetaCache).describeCollection).Return(&milvuspb.DescribeCollectionResponse{CollectionID: 1, CollectionName: "c", DbName: "default", Schema: &schemapb.CollectionSchema{Name: "c", Version: 0}, VirtualChannelNames: []string{"p_1v0"}}, nil).Build()
	defer p.UnPatch()
	write, err := cache.GetCollectionWriteInfo(t.Context(), "default", "c", 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, write.Schema.Version)
	require.Zero(t, reads)
	require.Equal(t, 1, writes)
	require.EqualValues(t, 30*time.Second, write.CollectionTTL)
	require.Nil(t, cache.collections[1].Schema, "a returned request view must not attach a body to the metadata cache")
	write.Schema.DbName = "request-only"
	require.Empty(t, body.DbName, "legacy envelope mutation must not alter the shared SchemaClient body")
	metadata, err := cache.getCollectionMetadata(t.Context(), "default", "c", 0)
	require.NoError(t, err)
	require.Zero(t, metadata.ReadSchemaVersion)
	require.Nil(t, metadata.Schema)
	require.NotSame(t, metadata, write)
	_, err = resolveSchema(t.Context(), metadata, false)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, 1, reads)
	require.Equal(t, 1, writes, "failed exact history must not fall back to current write")
	_, err = cache.GetCollectionInfo(t.Context(), "default", "c", 0)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	readErr = nil
	read, err := cache.GetCollectionInfo(t.Context(), "default", "c", 0)
	require.NoError(t, err)
	require.Zero(t, read.Schema.Version)
	schema, err := cache.GetCollectionSchema(t.Context(), "default", "c")
	require.NoError(t, err)
	require.Zero(t, schema.GetVersion())
	require.Nil(t, cache.collections[1].Schema)
	writeErr = merr.ErrServiceUnavailable
	_, err = cache.GetCollectionWriteInfo(t.Context(), "default", "c", 0)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	failure := mockey.Mock((*MetaCache).getCollectionMetadata).Return(nil, merr.ErrServiceUnavailable).Build()
	_, err = cache.GetCollectionInfo(t.Context(), "default", "c", 0)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	_, err = cache.GetCollectionWriteInfo(t.Context(), "default", "c", 0)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	_, err = cache.GetCollectionSchema(t.Context(), "default", "c")
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	failure.UnPatch()

	_, err = resolveSchema(t.Context(), &CollectionInfo{CollID: 2}, false)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
}

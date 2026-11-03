package metacache

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type (
	schemaTestWAL    struct{ streaming.WALAccesser }
	schemaTestClient struct{ streaming.SchemaClient }
)

func TestCollectionInfoWriteSchema(t *testing.T) {
	client, wal := &schemaTestClient{}, &schemaTestWAL{}
	p := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer p.UnPatch()
	p = mockey.Mock((*schemaTestWAL).SchemaClient).Return(client).Build()
	defer p.UnPatch()
	read, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c", Version: 0})
	require.NoError(t, err)
	write, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c", Version: 2, Fields: []*schemapb.FieldSchema{
		{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
	}})
	require.NoError(t, err)
	p = mockey.Mock((*schemaTestClient).GetCurrentWriteSchema).To(func(_ *schemaTestClient, _ context.Context, req streaming.GetCurrentWriteSchemaRequest) (*SchemaInfo, error) {
		require.Equal(t, "p_1v0", req.VChannel)
		require.False(t, req.BypassCache)
		return write, nil
	}).Build()
	defer p.UnPatch()
	info := &CollectionInfo{CollID: 1, Schema: read, VChannels: []string{"p_1v0"}}
	actual, err := info.GetWriteSchema(t.Context())
	require.NoError(t, err)
	require.Same(t, write, actual)
	require.Len(t, actual.GetFields(), 2, "write schemas retain system fields")
	require.Same(t, read, info.Schema, "write selection must not change the published read schema")
	_, err = (&CollectionInfo{CollID: 2}).GetWriteSchema(t.Context())
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
}

func TestReadSchemaProjectionPreservesCachedBody(t *testing.T) {
	client, wal := &schemaTestClient{}, &schemaTestWAL{}
	p := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer p.UnPatch()
	p = mockey.Mock((*schemaTestWAL).SchemaClient).Return(client).Build()
	defer p.UnPatch()
	full, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c", Version: 0, EnableDynamicField: true, Fields: []*schemapb.FieldSchema{
		{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
	}})
	require.NoError(t, err)
	p = mockey.Mock((*schemaTestClient).GetSchemaVersion).To(func(_ *schemaTestClient, _ context.Context, req streaming.GetSchemaVersionRequest) (*SchemaInfo, error) {
		require.Zero(t, req.Version)
		require.True(t, req.BypassCache)
		return full, nil
	}).Build()
	defer p.UnPatch()
	read, err := loadCollectionSchema(t.Context(), &milvuspb.DescribeCollectionResponse{
		CollectionID: 1, Schema: &schemapb.CollectionSchema{Version: 0}, VirtualChannelNames: []string{"p_1v0"},
	})
	require.NoError(t, err)
	require.NotSame(t, full, read)
	require.NotSame(t, full.CollectionSchema, read.CollectionSchema)
	require.Len(t, full.Fields, 4)
	require.Len(t, read.Fields, 2)
	require.Same(t, full.Fields[2], read.Fields[0])
	require.Same(t, full.Fields[3], read.Fields[1])
	for _, name := range []string{"RowID", "Timestamp"} {
		_, ok := read.MapFieldID(name)
		require.False(t, ok)
		_, err := read.SchemaHelper.GetFieldFromName(name)
		require.Error(t, err)
		field, err := read.SchemaHelper.GetFieldFromNameDefaultJSON(name)
		require.NoError(t, err)
		require.Same(t, full.Fields[3], field)
		_, err = full.SchemaHelper.GetFieldFromName(name)
		require.NoError(t, err)
	}
}

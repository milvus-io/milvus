package queryresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestReadSchemasRetainExactCollectionsAcrossHandoff(t *testing.T) {
	paramtable.Init()
	schema := mock_segcore.GenTestCollectionSchema("read-schemas", schemapb.DataType_Int64, false)
	next := proto.Clone(schema).(*schemapb.CollectionSchema)
	next.Version = 1
	next.Fields = append(next.Fields, &schemapb.FieldSchema{FieldID: 200, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true})
	module, err := NewReadSchemaModuleBuilder().NewRuntime()
	require.NoError(t, err)
	resources := module.(*readSchemas)
	defer resources.Close()
	view := walview.VChannelWALView{CollectionID: 7331, VChannel: "schema_v0", Schema: schema, LoadInfoVersion: 10}
	captures := 0
	view.CaptureQueryView = func(meta *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		captures++
		capture(walview.VChannelWALView{CollectionID: view.CollectionID, VChannel: view.VChannel, Schema: next, LoadInfoVersion: meta.GetLoadInfoVersion()})
		return true
	}
	require.NoError(t, resources.Prepare(t.Context(), view))
	runtime := NewQueryRuntime(resources)
	oldRead, ok := runtime.ReadRuntime(&viewpb.QueryViewMeta{SchemaVersion: 0, LoadInfoVersion: 10})
	require.True(t, ok)
	require.True(t, oldRead.schema.pin()) // A query outlives the view/runtime owner.
	require.NoError(t, resources.PrepareSchema(t.Context(), &viewpb.QueryViewMeta{SchemaVersion: 1, LoadInfoVersion: 11}, nil))
	newRead, ok := runtime.ReadRuntime(&viewpb.QueryViewMeta{SchemaVersion: 1, LoadInfoVersion: 11})
	require.True(t, ok)
	require.NotSame(t, oldRead.schema.collection, newRead.schema.collection)
	require.NotEqual(t, oldRead.schema.functionKey, newRead.schema.functionKey)
	require.True(t, proto.Equal(schema, oldRead.schema.collection.Schema()))
	require.True(t, proto.Equal(next, newRead.schema.collection.Schema()))
	require.NoError(t, resources.PrepareSchema(t.Context(), &viewpb.QueryViewMeta{SchemaVersion: 0, LoadInfoVersion: 10}, nil))
	require.Equal(t, 1, captures, "preparing an older retained view must not fetch latest")
	_, ok = runtime.ReadRuntime(&viewpb.QueryViewMeta{SchemaVersion: 2, LoadInfoVersion: 11})
	require.False(t, ok)
	released := 0
	// Count destruction while retaining the real native release.
	var release func(*segcore.CCollection)
	patch := mockey.Mock((*segcore.CCollection).Release).Origin(&release).To(func(c *segcore.CCollection) { released++; release(c) }).Build()
	defer patch.UnPatch()
	runtime.Close()
	require.Equal(t, 1, released, "in-flight old query retains its native planning schema")
	require.EqualValues(t, 0, oldRead.schema.collection.Schema().GetVersion())
	require.False(t, newRead.schema.pin())
	oldRead.schema.release()
	require.Equal(t, 2, released)
}

func TestReadSchemaPreparationRejectsWrongCapture(t *testing.T) {
	module, err := NewReadSchemaModuleBuilder().NewRuntime()
	require.NoError(t, err)
	resources := module.(*readSchemas)
	defer resources.Close()
	resources.capture = func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		capture(walview.VChannelWALView{Schema: &schemapb.CollectionSchema{Version: 3}})
		return true
	}
	require.ErrorIs(t, resources.PrepareSchema(context.Background(), &viewpb.QueryViewMeta{SchemaVersion: 1}, nil), merr.ErrServiceNotReady)
	require.Empty(t, resources.resources)
}

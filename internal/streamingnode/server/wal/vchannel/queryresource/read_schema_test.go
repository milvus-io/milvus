package queryresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestReadSchemasShareSameBindingAcrossViewsAndReclaimUnusedSchemas(t *testing.T) {
	paramtable.Init()
	schema := mock_segcore.GenTestCollectionSchema("read-schemas", schemapb.DataType_Int64, false)
	pool := &readSchemaPool{resources: make(map[collectionSchemaKey]*readSchema)}
	module := &readSchemas{pool: pool}
	runtime := NewQueryRuntime(module)
	defer runtime.Close()
	view := walview.VChannelWALView{CollectionID: 7331, VChannel: "v1", Schema: schema}
	view.CaptureQueryView = func(meta *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		next := view
		next.Schema = proto.Clone(schema).(*schemapb.CollectionSchema)
		next.Schema.Version = meta.GetSchemaVersion()
		capture(next)
		return true
	}
	require.NoError(t, runtime.Initialize(t.Context(), view))
	bind := func(version int32, load uint64) *viewResources {
		ref := runtime.bindView(&viewpb.QueryViewMeta{CollectionId: 7331, SchemaVersion: version, LoadInfoVersion: load})
		require.NoError(t, ref.prepare(t.Context(), nil))
		return ref
	}
	first, second := bind(1, 10), bind(1, 10)
	old, _ := first.readRuntime()
	shared, _ := second.readRuntime()
	require.Same(t, old.schema, shared.schema, "Views with the same schema and load version share their immutable binding")
	require.True(t, old.schema.pin())
	first.retire()
	require.NoError(t, first.release())
	require.Len(t, pool.resources, 1)
	second.retire()
	require.NoError(t, second.release())
	require.Empty(t, pool.resources)
	require.Equal(t, 1, old.schema.refs, "native handle outlives the View guards")
	replacement := bind(1, 10)
	current, _ := replacement.readRuntime()
	require.NotSame(t, old.schema, current.schema)
	require.NotEqual(t, old.schema.functionKey, current.schema.functionKey)
	old.schema.release()
	require.Equal(t, 0, old.schema.refs)
	require.Equal(t, 1, current.schema.refs)
	for version := int32(2); version < 12; version++ {
		next := bind(version, uint64(version))
		replacement.retire()
		require.NoError(t, replacement.release())
		replacement = next
		require.Len(t, pool.resources, 1, "history must not accumulate while the runtime remains live")
	}
	replacement.retire()
	require.NoError(t, replacement.release())
	require.Empty(t, pool.resources)
}

func TestReadSchemaPreparationRejectsWrongCapture(t *testing.T) {
	module := &readSchemas{pool: &readSchemaPool{resources: make(map[collectionSchemaKey]*readSchema)}, capture: func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		capture(walview.VChannelWALView{Schema: &schemapb.CollectionSchema{Version: 3}})
		return true
	}}
	binding := module.BindView(&viewpb.QueryViewMeta{SchemaVersion: 1})
	require.ErrorIs(t, binding.Prepare(context.Background(), nil), merr.ErrServiceNotReady)
	module.capture = nil
	require.ErrorIs(t, binding.Prepare(context.Background(), nil), merr.ErrServiceNotReady, "an unavailable exact snapshot must not fall back to another schema")
	binding.Release()
	require.Empty(t, module.pool.resources)
}

func TestReadSchemaLoadScopesIsolateIndexMetadata(t *testing.T) {
	paramtable.Init()
	pool := &readSchemaPool{resources: make(map[collectionSchemaKey]*readSchema)}
	params := []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}
	schema := &schemapb.CollectionSchema{Name: "shared-indexes", Version: 1, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "a", DataType: schemapb.DataType_FloatVector, TypeParams: params},
		{FieldID: 102, Name: "b", DataType: schemapb.DataType_FloatVector, TypeParams: params},
	}}
	module := &readSchemas{pool: pool, capture: func(_ *viewpb.QueryViewMeta, fn func(walview.VChannelWALView)) bool {
		fn(walview.VChannelWALView{CollectionID: 7882, Schema: schema})
		return true
	}}
	index := func(id int64) *indexpb.IndexInfo {
		return &indexpb.IndexInfo{CollectionID: 7882, FieldID: id, IndexID: id, TypeParams: params, IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "HNSW"}, {Key: "metric_type", Value: "COSINE"}}}
	}
	provider := fakeLoadInfoProvider{}
	patch := mockey.Mock(fakeLoadInfoProvider.QueryViewLoadInfo).To(func(_ fakeLoadInfoProvider, _ context.Context, _ int64, version uint64) (QueryViewLoadInfo, error) {
		field := int64(version)
		return QueryViewLoadInfo{RequiredFields: loadFields(field), IndexInfos: []*indexpb.IndexInfo{index(field)}}, nil
	}).Build()
	defer patch.UnPatch()
	a := module.BindView(&viewpb.QueryViewMeta{SchemaVersion: 1, LoadInfoVersion: 101}).(*readSchemaBinding)
	b := module.BindView(&viewpb.QueryViewMeta{SchemaVersion: 1, LoadInfoVersion: 102}).(*readSchemaBinding)
	defer a.Release()
	defer b.Release()
	require.NoError(t, a.Prepare(t.Context(), provider))
	require.NoError(t, b.Prepare(t.Context(), provider))
	require.NoError(t, a.Prepare(t.Context(), provider))
	require.NotSame(t, a.schema, b.schema)
	require.Len(t, a.schema.collection.IndexMeta().GetIndexMetas(), 1)
	require.Len(t, b.schema.collection.IndexMeta().GetIndexMetas(), 1)
	holder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 16)}}}})
	require.NoError(t, err)
	for field, binding := range map[int64]*readSchemaBinding{101: a, 102: b} {
		plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{VectorType: planpb.VectorType_FloatVector, FieldId: field, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: 10, MetricType: "COSINE", SearchParams: "{}", RoundDecimal: -1}}}})
		require.NoError(t, err)
		req, err := segcore.NewSearchRequest(binding.schema.collection, &querypb.SearchRequest{Req: &internalpb.SearchRequest{SerializedExprPlan: plan, MetricType: "COSINE"}}, holder)
		require.NoError(t, err)
		req.Delete()
	}
}

func TestReadSchemaFailedPreparationDoesNotRetainOwnership(t *testing.T) {
	paramtable.Init()
	schema := mock_segcore.GenTestCollectionSchema("failed-binding", schemapb.DataType_Int64, false)
	pool := &readSchemaPool{resources: make(map[collectionSchemaKey]*readSchema)}
	module := &readSchemas{pool: pool, capture: func(_ *viewpb.QueryViewMeta, fn func(walview.VChannelWALView)) bool {
		fn(walview.VChannelWALView{CollectionID: 7883, Schema: schema})
		return true
	}}
	b := module.BindView(&viewpb.QueryViewMeta{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, b.Prepare(ctx, nil), context.Canceled)
	require.Empty(t, pool.resources)
	b.Release()
	mockey.PatchConvey("native allocation failure leaves no cached schema", t, func() {
		mockey.Mock(segcore.CreateCCollection).Return(nil, merr.WrapErrServiceNotReadyMsg("native unavailable")).Build()
		require.ErrorIs(t, b.Prepare(t.Context(), nil), merr.ErrServiceNotReady)
		require.Empty(t, pool.resources)
	})
}

// Only Release is exercised; Mockey supplies its behavior at the call boundary.
type schemaTestHandle struct{ snview.GrowingSegmentHandle }

func (*schemaTestHandle) Release() { panic("Release must be patched by Mockey") }

func TestNativeHandleRetainsReadSchemaAfterLastView(t *testing.T) {
	paramtable.Init()
	schema := mock_segcore.GenTestCollectionSchema("native-schema-pin", schemapb.DataType_Int64, false)
	pool := &readSchemaPool{resources: make(map[collectionSchemaKey]*readSchema)}
	module := &readSchemas{pool: pool}
	runtime := NewQueryRuntime(module)
	defer runtime.Close()
	view := walview.VChannelWALView{CollectionID: 7884, Schema: schema}
	view.CaptureQueryView = func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool { capture(view); return true }
	require.NoError(t, runtime.Initialize(t.Context(), view))
	ref := runtime.bindView(&viewpb.QueryViewMeta{})
	require.NoError(t, ref.prepare(t.Context(), nil))
	read, ok := ref.readRuntime()
	require.True(t, ok)
	require.Same(t, schema, read.Schema())
	require.NotNil(t, read.GlobalOptimizer())
	releases := 0
	mockey.PatchConvey("native handle pins schema, including conversion after withdrawal", t, func() {
		mockey.Mock((*schemaTestHandle).Release).To(func(*schemaTestHandle) { releases++ }).Build()
		mockey.Mock((*QueryRuntime).AcquireGrowingSegmentHandles).To(func(*QueryRuntime, context.Context, qviews.DataVersion, []int64) ([]snview.GrowingSegmentHandle, error) {
			return []snview.GrowingSegmentHandle{&schemaTestHandle{}}, nil
		}).Build()
		handles, err := read.AcquireGrowingSegmentHandles(t.Context(), qviews.DataVersion{}, nil)
		require.NoError(t, err)
		ref.retire()
		require.NoError(t, ref.release())
		require.Empty(t, pool.resources)
		require.Equal(t, 1, read.schema.refs)
		require.Same(t, read.schema.collection, handles[0].Collection())
		handles[0].Release()
		handles[0].Release()
		require.Equal(t, 1, releases)
		require.Zero(t, read.schema.refs)
		_, err = read.AcquireGrowingSegmentHandles(t.Context(), qviews.DataVersion{}, nil)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		require.Equal(t, 2, releases, "late acquisition must release its physical handle when schema pin fails")
	})
	mockey.PatchConvey("physical acquisition error is preserved", t, func() {
		mockey.Mock((*QueryRuntime).AcquireGrowingSegmentHandles).Return(nil, context.Canceled).Build()
		_, err := read.AcquireGrowingSegmentHandles(t.Context(), qviews.DataVersion{}, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

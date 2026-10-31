package qvresource

import (
	"context"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQueryViewCollectionRuntimeManager_AcquireRefsCollectionAndReleaseUnrefs(t *testing.T) {
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, &schemapb.CollectionSchema{Name: "coll"})
	collection := &fakeQVCollectionManager{collection: localCollection}
	provider := &fakeQVLoadMetadataProvider{
		partitionIDs: []int64{10, 20},
		loadFields:   []int64{100, 101},
	}
	manager := newTestQueryViewCollectionRuntimeManager(provider, collection, mockQueryViewSchemaClient(t))

	guard, retryable, err := manager.Acquire(context.Background(), qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{
			CollectionId:    1,
			LoadInfoVersion: 7,
		},
		&viewpb.QueryViewOfQueryNode{
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{1}},
				{PartitionId: 20, SegmentIds: []int64{2}},
			},
		},
	).(*qviews.QueryViewAtQueryNode))
	require.NoError(t, err)
	assert.False(t, retryable)
	require.NotNil(t, guard)
	assert.NotSame(t, localCollection, guard.Collection())
	assert.Equal(t, localCollection.ID(), guard.Collection().ID())
	assert.ElementsMatch(t, []int64{10, 20}, guard.Collection().GetPartitions())

	assert.Equal(t, int64(1), guard.CollectionID())
	assert.Equal(t, "db", guard.DatabaseName())
	assert.Equal(t, "coll", guard.Schema().GetName())
	assert.Zero(t, guard.SchemaVersion())
	assert.Equal(t, int64(1), collection.putCollectionID)
	assert.Equal(t, "coll", collection.putSchema.GetName())
	require.NotNil(t, collection.putLoadMeta)
	assert.Equal(t, querypb.LoadType_LoadCollection, collection.putLoadMeta.GetLoadType())
	assert.Equal(t, int64(1), collection.putLoadMeta.GetCollectionID())
	assert.Equal(t, "db", collection.putLoadMeta.GetDbName())
	assert.Zero(t, collection.putLoadMeta.GetSchemaBarrierTs())
	assert.Empty(t, collection.putLoadMeta.GetPartitionIDs())
	assert.Empty(t, collection.putLoadMeta.GetLoadFields())

	guard.Release()
	assert.Equal(t, int64(1), collection.unrefCollection)
	assert.Equal(t, uint32(1), collection.unrefCount)
}

func TestQueryViewCollectionRuntimeManager_AcquireUsesLoadInfoVersion(t *testing.T) {
	collection := &fakeQVCollectionManager{}
	provider := &fakeQVLoadMetadataProvider{
		partitionIDs: []int64{10, 20},
		loadFields:   []int64{100, 101, 102},
	}
	manager := newTestQueryViewCollectionRuntimeManager(provider, collection, mockQueryViewSchemaClient(t))

	var getInfo func(*fakeQVLoadMetadataProvider, context.Context, int64, qnview.QueryViewLoadInfoVersion) (qnview.QueryViewLoadInfo, error)
	patch := mockey.Mock((*fakeQVLoadMetadataProvider).GetQueryViewLoadInfo).Origin(&getInfo).To(
		func(p *fakeQVLoadMetadataProvider, ctx context.Context, collectionID int64, version qnview.QueryViewLoadInfoVersion) (qnview.QueryViewLoadInfo, error) {
			require.EqualValues(t, 7, version)
			return getInfo(p, ctx, collectionID, version)
		}).Build()
	defer patch.UnPatch()

	guard, retryable, err := manager.Acquire(context.Background(), qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{
			CollectionId:    1,
			LoadInfoVersion: 7,
		},
		&viewpb.QueryViewOfQueryNode{
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{1}},
			},
		},
	).(*qviews.QueryViewAtQueryNode))
	require.NoError(t, err)
	assert.False(t, retryable)
	require.NotNil(t, guard)

	assert.Empty(t, collection.putLoadMeta.GetPartitionIDs())
	assert.Empty(t, collection.putLoadMeta.GetLoadFields())
}

func TestQueryViewCollectionRuntimeManager_AcquireClassifiesRetryability(t *testing.T) {
	view := qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{CollectionId: 1, LoadInfoVersion: 7},
		&viewpb.QueryViewOfQueryNode{},
	).(*qviews.QueryViewAtQueryNode)

	t.Run("transient", func(t *testing.T) {
		manager := newTestQueryViewCollectionRuntimeManager(
			&fakeQVLoadMetadataProvider{err: merr.WrapErrNodeNotMatch(1, 2)},
			&fakeQVCollectionManager{},
			mockQueryViewSchemaClient(t),
		)

		guard, retryable, err := manager.Acquire(context.Background(), view)

		assert.Nil(t, guard)
		assert.True(t, retryable)
		require.ErrorIs(t, err, merr.ErrNodeNotMatch)
	})

	t.Run("not found", func(t *testing.T) {
		manager := newTestQueryViewCollectionRuntimeManager(
			&fakeQVLoadMetadataProvider{err: merr.WrapErrCollectionNotFound(1)},
			&fakeQVCollectionManager{},
			mockQueryViewSchemaClient(t),
		)

		guard, retryable, err := manager.Acquire(context.Background(), view)

		assert.Nil(t, guard)
		assert.False(t, retryable)
		require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})
}

// Patch points use mockey so requests are checked at the streaming boundary.
type queryViewSchemaClient struct{ streaming.SchemaClient }

func (*queryViewSchemaClient) GetSchemaVersion(context.Context, streaming.GetSchemaVersionRequest) (*streaming.SchemaSnapshot, error) {
	panic("mock with mockey")
}

func mockQueryViewSchemaClient(t *testing.T) *queryViewSchemaClient {
	t.Helper()
	client := &queryViewSchemaClient{}
	patch := mockey.Mock((*queryViewSchemaClient).GetSchemaVersion).To(
		func(_ *queryViewSchemaClient, _ context.Context, req streaming.GetSchemaVersionRequest) (*streaming.SchemaSnapshot, error) {
			return &streaming.SchemaSnapshot{
				Schema:            &schemapb.CollectionSchema{Name: "coll", DbName: "db", Version: req.Version, Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}},
				EffectiveTimeTick: 9,
			}, nil
		}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return client
}

func TestQueryViewCollectionRuntimeUsesExactStreamingSchema(t *testing.T) {
	// A QueryView can prepare before public metadata is updated and can retain
	// its old schema after publication advances. No DescribeCollection is needed.
	for _, version := range []int32{0, 2, 1} {
		t.Run(strconv.Itoa(int(version)), func(t *testing.T) {
			schemas := mockQueryViewSchemaClient(t)
			manager := newTestQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{}, &fakeQVCollectionManager{}, schemas)
			view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", SchemaVersion: version}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
			guard, retryable, err := manager.Acquire(t.Context(), view)
			require.NoError(t, err)
			require.False(t, retryable)
			require.Equal(t, version, guard.Schema().GetVersion())
			guard.Release()
		})
	}
}

func TestQueryViewCollectionRuntimeClassifiesStreamingErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{"not observed yet", status.NewInner("schema not observed yet"), true},
		{"retired", status.NewUnrecoverableError("schema retired"), false},
		{"shutdown", status.NewOnShutdownError("moving"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock((*queryViewSchemaClient).GetSchemaVersion).Return(nil, tc.err).Build()
			defer patch.UnPatch()
			collection := &fakeQVCollectionManager{}
			manager := newTestQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{}, collection, &queryViewSchemaClient{})
			view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", SchemaVersion: 2}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
			guard, retryable, err := manager.Acquire(t.Context(), view)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.retryable, retryable)
			require.Nil(t, guard)
			require.Zero(t, collection.putCount)
		})
	}
}

func newTestQueryViewCollectionRuntimeManager(meta qnview.QueryViewLoadMetadataProvider, collection qvCollectionManager, schemas streaming.SchemaClient) *queryViewCollectionRuntimeManager {
	manager := newQueryViewCollectionRuntimeManager(meta, schemas)
	manager.newCollections = func() qvCollectionManager { return collection }
	return manager
}

func TestQueryViewCollectionRuntimeIsolatesReadGenerations(t *testing.T) {
	paramtable.Init()
	manager := newQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{}, mockQueryViewSchemaClient(t))
	acquire := func(schema int32, load uint64) qnview.CollectionRuntimeGuard {
		view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{
			CollectionId: 1, Vchannel: "p_1v0", SchemaVersion: schema, LoadInfoVersion: load,
		}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
		guard, retry, err := manager.Acquire(t.Context(), view)
		require.NoError(t, err)
		require.False(t, retry)
		return guard
	}
	old := acquire(0, 10)
	oldAgain := acquire(0, 10)
	target := acquire(1, 11)
	changedLoad := acquire(1, 12)
	require.Same(t, old.CCollection(), oldAgain.CCollection(), "identical read generations share planning resources")
	require.NotSame(t, old.CCollection(), target.CCollection())
	require.NotSame(t, target.CCollection(), changedLoad.CCollection(), "index selections have independent bindings over the shared parsed Schema")
	require.EqualValues(t, 0, old.Collection().Schema().GetVersion())
	require.EqualValues(t, 1, target.Collection().Schema().GetVersion())
	old.Release()
	require.Len(t, manager.collections, 3, "another View keeps its exact metadata binding alive")
	target.Release()
	changedLoad.Release()
	require.Len(t, manager.collections, 1)
	oldAgain.Release()
	require.Empty(t, manager.collections)
}

func TestQueryViewCollectionRuntimeIgnoresPerChannelSchemaTicks(t *testing.T) {
	paramtable.Init()
	patch := mockey.Mock((*queryViewSchemaClient).GetSchemaVersion).To(
		func(_ *queryViewSchemaClient, _ context.Context, req streaming.GetSchemaVersionRequest) (*streaming.SchemaSnapshot, error) {
			tick := uint64(900)
			if req.VChannel == "p_1v1" {
				tick = 1000
			}
			return &streaming.SchemaSnapshot{
				Schema:            &schemapb.CollectionSchema{Name: "coll", Version: req.Version, Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}},
				EffectiveTimeTick: tick,
			}, nil
		}).Build()
	defer patch.UnPatch()
	manager := newQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{}, &queryViewSchemaClient{})
	var first qnview.CollectionRuntimeGuard
	for _, channel := range []string{"p_1v0", "p_1v1", "p_1v0"} {
		view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{CollectionId: 1, Vchannel: channel, SchemaVersion: 2, LoadInfoVersion: 7}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
		guard, retry, err := manager.Acquire(t.Context(), view)
		require.NoError(t, err)
		require.False(t, retry)
		defer guard.Release()
		if first == nil {
			first = guard
		}
		require.Same(t, first.CCollection(), guard.CCollection())
		schema, nativeVersion := guard.Collection().SchemaAndSegcoreVersion()
		require.EqualValues(t, 2, nativeVersion)
		require.EqualValues(t, schema.GetVersion(), guard.SchemaVersion())
		require.EqualValues(t, nativeVersion, guard.SchemaVersion(), "reopen and initial segment load use the same collection-wide identity")
	}
}

func TestQueryViewRequiredFieldsAreIndependentOfHints(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}, {FieldID: 102, IsDynamic: true}}}
	for _, tc := range []struct {
		name  string
		hints []int64
		want  []int64
	}{
		{name: "empty hints follow schema"},
		{name: "skip dynamic normalized by producer", hints: []int64{100, 101}, want: []int64{100, 101}},
		{name: "partial preload", hints: []int64{100}, want: []int64{100}},
		{name: "explicit dynamic preload", hints: []int64{102}, want: []int64{102}},
		{name: "hints filtered by bound schema", hints: []int64{103, 101}, want: []int64{101}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := qnview.QueryViewLoadInfo{RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}, {FieldId: 102}}}
			for _, id := range tc.hints {
				info.LoadFields = append(info.LoadFields, &viewpb.LoadFieldConfig{FieldId: id})
			}
			require.Equal(t, []int64{100, 101, 102}, loadInfoFieldIDs(info), "preload hints must not remove dynamic-field query access")
			require.Equal(t, tc.want, loadInfoHintIDs(info, schema))
			info.RequiredFields = nil
			require.Empty(t, loadInfoFieldIDs(info), "missing requirements must not expand to all fields")
		})
	}
}

func TestCollectionRuntimeRejectsMismatchedLoadIdentity(t *testing.T) {
	for _, info := range []qnview.QueryViewLoadInfo{{CollectionID: 2, Version: 7}, {CollectionID: 1, Version: 8}} {
		t.Run(strconv.FormatInt(info.CollectionID, 10)+"-"+strconv.FormatUint(uint64(info.Version), 10), func(t *testing.T) {
			provider := &fakeQVLoadMetadataProvider{}
			patch := mockey.Mock((*fakeQVLoadMetadataProvider).GetQueryViewLoadInfo).Return(info, nil).Build()
			defer patch.UnPatch()
			manager := newQueryViewCollectionRuntimeManager(provider, nil)
			_, err := manager.loadInfo(context.Background(), &viewpb.QueryViewMeta{CollectionId: 1, LoadInfoVersion: 7})
			require.ErrorIs(t, err, merr.ErrServiceNotReady)
			require.True(t, isRetryableCollectionRuntimeError(err))
		})
	}
}

func TestCollectionRuntimeRejectsMissingRequiredFields(t *testing.T) {
	patch := mockey.Mock((*fakeQVLoadMetadataProvider).GetQueryViewLoadInfo).Return(qnview.QueryViewLoadInfo{
		CollectionID: 1, Version: 7, LoadFields: []*viewpb.LoadFieldConfig{{FieldId: 100}},
	}, nil).Build()
	defer patch.UnPatch()
	collection := &fakeQVCollectionManager{}
	manager := newTestQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{}, collection, mockQueryViewSchemaClient(t))
	view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{CollectionId: 1, LoadInfoVersion: 7}, &viewpb.QueryViewOfQueryNode{}).(*qviews.QueryViewAtQueryNode)
	guard, retryable, err := manager.Acquire(t.Context(), view)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.True(t, retryable)
	require.Nil(t, guard)
	require.Zero(t, collection.putCount)
}

func TestCollectionRuntimePreservesEmptySnapshotPartitions(t *testing.T) {
	collection := &fakeQVCollectionManager{collection: segments.NewCollectionWithoutSegcoreForTest(1, &schemapb.CollectionSchema{})}
	manager := newTestQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{}, collection, mockQueryViewSchemaClient(t))
	view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{CollectionId: 1, LoadInfoVersion: 7}, &viewpb.QueryViewOfQueryNode{
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1}}},
	}).(*qviews.QueryViewAtQueryNode)
	guard, retryable, err := manager.Acquire(t.Context(), view)
	require.NoError(t, err)
	require.False(t, retryable)
	defer guard.Release()
	require.Empty(t, guard.Collection().GetPartitions(), "the snapshot is authoritative; do not infer scope from a node's View")
}

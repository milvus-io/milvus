package qvresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testLoadMetadataProvider struct {
	loadInfo      qnview.QueryViewLoadInfo
	describeError error
	loadError     error
	schema        *schemapb.CollectionSchema
}

func (p testLoadMetadataProvider) DescribeCollection(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) {
	if p.describeError != nil {
		return nil, p.describeError
	}
	schema := p.schema
	if schema == nil {
		schema = &schemapb.CollectionSchema{Name: "test", Version: 1}
	}
	return &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Schema: schema,
		DbName: "default",
	}, nil
}

func (p testLoadMetadataProvider) GetQueryViewLoadInfo(
	context.Context,
	int64,
	qnview.QueryViewLoadInfoVersion,
) (qnview.QueryViewLoadInfo, error) {
	return p.loadInfo, p.loadError
}

type rejectingCollectionManager struct {
	putCalled  bool
	collection *segments.Collection
	loadMeta   *querypb.LoadMetaInfo
	unrefCount uint32
}

func (*rejectingCollectionManager) List() []int64                    { return nil }
func (*rejectingCollectionManager) ListWithName() map[int64]string   { return nil }
func (m *rejectingCollectionManager) Get(int64) *segments.Collection { return m.collection }
func (m *rejectingCollectionManager) PutOrRef(_ int64, _ *schemapb.CollectionSchema, _ *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) error {
	m.putCalled = true
	m.loadMeta = loadMeta
	return nil
}
func (*rejectingCollectionManager) Ref(int64, uint32) bool { return false }
func (m *rejectingCollectionManager) Unref(_ int64, count uint32) bool {
	m.unrefCount += count
	return true
}

func (*rejectingCollectionManager) UpdateSchema(int64, *schemapb.CollectionSchema, uint64) error {
	return nil
}

func TestCollectionRuntimeRequiresExactLoadInfoVersion(t *testing.T) {
	collections := &rejectingCollectionManager{}
	manager := NewQueryViewCollectionRuntimeManager(testLoadMetadataProvider{
		loadInfo: qnview.QueryViewLoadInfo{
			CollectionID: 1,
			Version:      8,
		},
	}, collections)

	guard, retryable, err := manager.Acquire(context.Background(), testQueryViewAtQueryNode(7))
	require.Error(t, err)
	assert.Nil(t, guard)
	assert.True(t, retryable)
	assert.ErrorContains(t, err, "version mismatch")
	assert.False(t, collections.putCalled, "mismatched metadata must not mutate local collection state")
}

func TestCollectionRuntimeRejectsWrongCollectionSnapshot(t *testing.T) {
	collections := &rejectingCollectionManager{}
	manager := NewQueryViewCollectionRuntimeManager(testLoadMetadataProvider{
		loadInfo: qnview.QueryViewLoadInfo{CollectionID: 2, Version: 7},
	}, collections)

	guard, retryable, err := manager.Acquire(context.Background(), testQueryViewAtQueryNode(7))
	require.Error(t, err)
	assert.Nil(t, guard)
	assert.True(t, retryable)
	assert.ErrorContains(t, err, "collection mismatch")
	assert.False(t, collections.putCalled)
}

func TestCollectionRuntimePinsExactSnapshot(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "test", Version: 3}
	collection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	collections := &rejectingCollectionManager{collection: collection}
	manager := NewQueryViewCollectionRuntimeManager(testLoadMetadataProvider{
		schema: schema,
		loadInfo: qnview.QueryViewLoadInfo{
			CollectionID: 1,
			Version:      7,
			PartitionIDs: []int64{10, 20},
			LoadFields: []*messagespb.LoadFieldConfig{
				{FieldId: 100},
				{FieldId: 101},
			},
		},
	}, collections)

	guard, retryable, err := manager.Acquire(context.Background(), testQueryViewAtQueryNode(7))
	require.NoError(t, err)
	assert.False(t, retryable)
	require.NotNil(t, guard)
	assert.Equal(t, int64(1), guard.CollectionID())
	assert.Equal(t, "default", guard.DatabaseName())
	assert.Equal(t, "test", guard.Schema().GetName())
	assert.Equal(t, int64(3), guard.SchemaVersion())
	assert.Nil(t, guard.CCollection())
	assert.Same(t, collection, guard.PinnedCollection())
	assert.Equal(t, []int64{10, 20}, collections.loadMeta.GetPartitionIDs())
	assert.Equal(t, []int64{100, 101}, collections.loadMeta.GetLoadFields())
	require.Error(t, guard.(qnview.CollectionIndexMetaUpdater).UpdateIndexMeta(context.Background(), nil))
	guard.Release()
	assert.Equal(t, uint32(1), collections.unrefCount)
}

func TestLoadInfoFallbackHelpers(t *testing.T) {
	view := &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{
		{PartitionId: 10},
		{PartitionId: 20},
	}}
	assert.Equal(t, []int64{10, 20}, loadInfoPartitionIDs(qnview.QueryViewLoadInfo{}, view))
	assert.Empty(t, loadInfoFieldIDs(qnview.QueryViewLoadInfo{}))
	assert.False(t, isRetryableCollectionRuntimeError(nil))
	assert.False(t, isRetryableCollectionRuntimeError(merr.WrapErrParameterInvalid("expected", "actual")))
	assert.True(t, isRetryableCollectionRuntimeError(merr.WrapErrNodeNotMatch(1, 2)))
}

func TestCollectionRuntimeClassifiesMetadataErrors(t *testing.T) {
	t.Run("transient", func(t *testing.T) {
		manager := NewQueryViewCollectionRuntimeManager(testLoadMetadataProvider{
			describeError: merr.WrapErrNodeNotMatch(1, 2),
		}, &rejectingCollectionManager{})
		guard, retryable, err := manager.Acquire(context.Background(), testQueryViewAtQueryNode(7))
		assert.Nil(t, guard)
		assert.True(t, retryable)
		require.ErrorIs(t, err, merr.ErrNodeNotMatch)
	})

	t.Run("not found", func(t *testing.T) {
		manager := NewQueryViewCollectionRuntimeManager(testLoadMetadataProvider{
			describeError: merr.WrapErrCollectionNotFound(1),
		}, &rejectingCollectionManager{})
		guard, retryable, err := manager.Acquire(context.Background(), testQueryViewAtQueryNode(7))
		assert.Nil(t, guard)
		assert.False(t, retryable)
		require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})

	t.Run("nil view", func(t *testing.T) {
		manager := NewQueryViewCollectionRuntimeManager(testLoadMetadataProvider{}, &rejectingCollectionManager{})
		guard, retryable, err := manager.Acquire(context.Background(), nil)
		assert.Nil(t, guard)
		assert.False(t, retryable)
		require.Error(t, err)
	})
}

func testQueryViewAtQueryNode(loadInfoVersion uint64) *qviews.QueryViewAtQueryNode {
	meta := &viewpb.QueryViewMeta{
		CollectionId:    1,
		ReplicaId:       2,
		Vchannel:        "by-dev-rootcoord-dml_0v0",
		LoadInfoVersion: loadInfoVersion,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			QueryVersion: 1,
		},
	}
	view := &viewpb.QueryViewOfQueryNode{NodeId: 3}
	return qviews.NewQueryViewAtQueryNode(meta, view).(*qviews.QueryViewAtQueryNode)
}

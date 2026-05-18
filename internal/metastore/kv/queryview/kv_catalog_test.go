//go:build test && dynamic

package queryview

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/mocks/mock_kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func newTestCatalog(t *testing.T) (QueryViewCatalog, map[string]string) {
	kv := mock_kv.NewMockMetaKv(t)
	kvStorage := make(map[string]string)

	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, prefix string) ([]string, []string, error) {
			keys := make([]string, 0)
			vals := make([]string, 0)
			for k, v := range kvStorage {
				if strings.HasPrefix(k, prefix) {
					keys = append(keys, k)
					vals = append(vals, v)
				}
			}
			return keys, vals, nil
		}).Maybe()

	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			for k, v := range saves {
				kvStorage[k] = v
			}
			for _, k := range removals {
				delete(kvStorage, k)
			}
			return nil
		}).Maybe()

	catalog := NewQueryViewCatalog(kv, "test")
	return catalog, kvStorage
}

func makeTestView(collectionID, replicaID int64, vchannel string, sv, cv, qv int64, state viewpb.QueryViewState) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: collectionID,
			ReplicaId:    replicaID,
			Vchannel:     vchannel,
			Version: &viewpb.QueryViewVersion{
				DataVersion: &viewpb.DataVersion{
					StreamingVersion: sv,
					CompactVersion:   cv,
				},
				QueryVersion: qv,
			},
			State: state,
			Settings: &viewpb.QueryViewSettings{
				RequiredPartitions: []int64{100, 200},
			},
		},
		QueryNode: []*viewpb.QueryViewOfQueryNode{
			{
				NodeId: 1,
				Partitions: []*viewpb.QueryViewOfPartition{
					{
						PartitionId:     100,
						SegmentIds:      []int64{1001, 1002, 1003},
						ReadySegmentIds: []int64{1001},
					},
				},
			},
			{
				NodeId: 2,
				Partitions: []*viewpb.QueryViewOfPartition{
					{
						PartitionId:     200,
						SegmentIds:      []int64{2001, 2002},
						ReadySegmentIds: []int64{2001, 2002},
					},
				},
			},
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
}

func TestQueryViewCatalog_EmptyList(t *testing.T) {
	catalog, _ := newTestCatalog(t)
	views, err := catalog.ListQueryViews(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, views)
}

func TestQueryViewCatalog_SaveAndList(t *testing.T) {
	catalog, _ := newTestCatalog(t)
	ctx := context.Background()

	view := makeTestView(1, 1, "v1", 1, 0, 1, viewpb.QueryViewState_QueryViewStatePreparing)

	// Save Preparing
	err := catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{view})
	assert.NoError(t, err)

	// List and verify
	views, err := catalog.ListQueryViews(ctx)
	assert.NoError(t, err)
	assert.Len(t, views, 1)

	// Verify ready_segment_ids is cleared in persisted data
	for _, qn := range views[0].QueryNode {
		for _, p := range qn.Partitions {
			assert.Empty(t, p.ReadySegmentIds)
		}
	}

	// Verify immutable fields are preserved
	assert.Equal(t, view.Meta.CollectionId, views[0].Meta.CollectionId)
	assert.Equal(t, view.Meta.ReplicaId, views[0].Meta.ReplicaId)
	assert.Equal(t, view.Meta.Vchannel, views[0].Meta.Vchannel)
	assert.True(t, proto.Equal(view.Meta.Version, views[0].Meta.Version))
	assert.True(t, proto.Equal(view.Meta.Settings, views[0].Meta.Settings))
	assert.Equal(t, view.QueryNode[0].NodeId, views[0].QueryNode[0].NodeId)
	assert.Equal(t, view.QueryNode[0].Partitions[0].SegmentIds, views[0].QueryNode[0].Partitions[0].SegmentIds)
}

func TestQueryViewCatalog_StateTransition(t *testing.T) {
	catalog, _ := newTestCatalog(t)
	ctx := context.Background()

	view := makeTestView(1, 1, "v1", 1, 0, 1, viewpb.QueryViewState_QueryViewStatePreparing)

	// Preparing
	err := catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{view})
	assert.NoError(t, err)

	// Up
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	err = catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{view})
	assert.NoError(t, err)

	views, err := catalog.ListQueryViews(ctx)
	assert.NoError(t, err)
	assert.Len(t, views, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUp, views[0].Meta.State)

	// Down
	view.Meta.State = viewpb.QueryViewState_QueryViewStateDown
	err = catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{view})
	assert.NoError(t, err)

	views, err = catalog.ListQueryViews(ctx)
	assert.NoError(t, err)
	assert.Len(t, views, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDown, views[0].Meta.State)
}

func TestQueryViewCatalog_DroppedDeletesKey(t *testing.T) {
	catalog, _ := newTestCatalog(t)
	ctx := context.Background()

	view := makeTestView(1, 1, "v1", 1, 0, 1, viewpb.QueryViewState_QueryViewStatePreparing)

	// Save then drop
	err := catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{view})
	assert.NoError(t, err)

	view.Meta.State = viewpb.QueryViewState_QueryViewStateDropped
	err = catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{view})
	assert.NoError(t, err)

	views, err := catalog.ListQueryViews(ctx)
	assert.NoError(t, err)
	assert.Empty(t, views)
}

func TestQueryViewCatalog_AtomicDropAndCreate(t *testing.T) {
	catalog, _ := newTestCatalog(t)
	ctx := context.Background()

	oldView := makeTestView(1, 1, "v1", 1, 0, 1, viewpb.QueryViewState_QueryViewStateDown)
	err := catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{oldView})
	assert.NoError(t, err)

	// Atomic: drop old + create new
	oldView.Meta.State = viewpb.QueryViewState_QueryViewStateDropped
	newView := makeTestView(1, 1, "v1", 2, 0, 1, viewpb.QueryViewState_QueryViewStatePreparing)
	err = catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{oldView, newView})
	assert.NoError(t, err)

	views, err := catalog.ListQueryViews(ctx)
	assert.NoError(t, err)
	assert.Len(t, views, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, views[0].Meta.State)
	assert.Equal(t, int64(2), views[0].Meta.Version.DataVersion.StreamingVersion)
}

func TestQueryViewCatalog_AtomicUnrecoverableAndCreate(t *testing.T) {
	catalog, _ := newTestCatalog(t)
	ctx := context.Background()

	oldView := makeTestView(1, 1, "v1", 1, 0, 1, viewpb.QueryViewState_QueryViewStateUp)
	err := catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{oldView})
	assert.NoError(t, err)

	// Atomic: mark old as Unrecoverable + create new Preparing
	oldView.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
	newView := makeTestView(1, 1, "v1", 1, 0, 2, viewpb.QueryViewState_QueryViewStatePreparing)
	err = catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{oldView, newView})
	assert.NoError(t, err)

	views, err := catalog.ListQueryViews(ctx)
	assert.NoError(t, err)
	assert.Len(t, views, 2)
}

func TestQueryViewCatalog_MultipleShards(t *testing.T) {
	catalog, _ := newTestCatalog(t)
	ctx := context.Background()

	v1 := makeTestView(1, 1, "v1", 1, 0, 1, viewpb.QueryViewState_QueryViewStatePreparing)
	v2 := makeTestView(1, 1, "v2", 1, 0, 1, viewpb.QueryViewState_QueryViewStateUp)
	v3 := makeTestView(2, 1, "v1", 3, 0, 1, viewpb.QueryViewState_QueryViewStatePreparing)

	err := catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{v1, v2, v3})
	assert.NoError(t, err)

	views, err := catalog.ListQueryViews(ctx)
	assert.NoError(t, err)
	assert.Len(t, views, 3)
}

func TestQueryViewCatalog_SaveEmptySlice(t *testing.T) {
	catalog, _ := newTestCatalog(t)
	err := catalog.SaveQueryViews(context.Background(), nil)
	assert.NoError(t, err)
	err = catalog.SaveQueryViews(context.Background(), []*viewpb.QueryViewOfShard{})
	assert.NoError(t, err)
}

func TestBuildKey(t *testing.T) {
	meta := &viewpb.QueryViewMeta{
		CollectionId: 100,
		ReplicaId:    1,
		Vchannel:     "by-dev-rootcoord-dml_0_100v0",
		Version: &viewpb.QueryViewVersion{
			DataVersion: &viewpb.DataVersion{
				StreamingVersion: 5,
				CompactVersion:   3,
			},
			QueryVersion: 2,
		},
	}

	c := &queryViewCatalog{prefix: "coord/qv/"}
	assert.Equal(t, "coord/qv/100/1/by-dev-rootcoord-dml_0_100v0/5/3/2", c.buildKey(meta))

	c = &queryViewCatalog{prefix: "streamingnode/qv/"}
	assert.Equal(t, "streamingnode/qv/100/1/by-dev-rootcoord-dml_0_100v0/5/3/2", c.buildKey(meta))
}

func TestMarshalForPersistence_ClearsReadySegmentIds(t *testing.T) {
	view := makeTestView(1, 1, "v1", 1, 0, 1, viewpb.QueryViewState_QueryViewStatePreparing)

	// Verify source has ready_segment_ids set
	assert.NotEmpty(t, view.QueryNode[0].Partitions[0].ReadySegmentIds)

	data, err := marshalForPersistence(view)
	assert.NoError(t, err)

	// Verify original is not modified
	assert.NotEmpty(t, view.QueryNode[0].Partitions[0].ReadySegmentIds)

	// Verify persisted data has ready_segment_ids cleared
	restored := &viewpb.QueryViewOfShard{}
	err = proto.Unmarshal(data, restored)
	assert.NoError(t, err)
	for _, qn := range restored.QueryNode {
		for _, p := range qn.Partitions {
			assert.Empty(t, p.ReadySegmentIds)
		}
	}

	// Verify segment_ids are preserved
	assert.Equal(t, []int64{1001, 1002, 1003}, restored.QueryNode[0].Partitions[0].SegmentIds)
}

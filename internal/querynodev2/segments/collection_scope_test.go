package segments

import (
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCollectionLoadScopesShareNativeSchema(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Name: "scope", Version: 7, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "a", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "b", DataType: schemapb.DataType_Int64},
	}}
	owner, err := NewCollection(1, schema, nil, &querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection})
	require.NoError(t, err)
	defer DeleteCollection(owner)
	left := owner.WithLoadScope([]int64{10}, []int64{100, 101})
	right := owner.WithLoadScope([]int64{20}, []int64{100, 102})
	empty := owner.WithLoadScope(nil, nil)
	require.Empty(t, empty.loadFields.Collect())
	require.Same(t, left.GetCCollection(), right.GetCCollection())
	require.Same(t, owner.GetCCollection(), left.GetCCollection())
	require.ElementsMatch(t, []int64{100, 101}, left.loadFields.Collect())
	require.ElementsMatch(t, []int64{100, 102}, right.loadFields.Collect())
	require.ElementsMatch(t, []int64{100, 101, 102}, owner.loadFields.Collect())
	right.AddPartition(30)
	require.Equal(t, []int64{10}, left.GetPartitions())
	require.Empty(t, owner.GetPartitions())

	var captured []int64
	patch := mockey.Mock(segcore.CreateCSegment).To(func(req *segcore.CreateCSegmentRequest) (segcore.CSegment, error) {
		require.Same(t, owner.GetCCollection(), req.Collection)
		require.True(t, req.ExplicitLoadScope)
		captured = append([]int64(nil), req.LoadFields...)
		return nil, nil
	}).Build()
	defer patch.UnPatch()
	_, err = left.CreateCSegment(&segcore.CreateCSegmentRequest{LoadInfo: &querypb.SegmentLoadInfo{}})
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{100, 101}, captured)
	_, err = right.CreateCSegment(&segcore.CreateCSegmentRequest{LoadInfo: &querypb.SegmentLoadInfo{}})
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{100, 102}, captured)
}

func TestCollectionLoadScopeIndexUpdatesPreserveOtherScopes(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Name: "scope", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "a", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "b", DataType: schemapb.DataType_Int64},
	}}
	owner, err := NewCollection(1, schema, nil, &querypb.LoadMetaInfo{})
	require.NoError(t, err)
	defer DeleteCollection(owner)
	index := func(field, version int64) *segcorepb.CollectionIndexMeta {
		return &segcorepb.CollectionIndexMeta{MaxIndexRowCount: 1024, IndexMetas: []*segcorepb.FieldIndexMeta{{
			FieldID: field, IndexId: version, IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "STL_SORT"}},
		}}}
	}
	left := owner.WithLoadScope(nil, []int64{100, 101})
	right := owner.WithLoadScope(nil, []int64{100, 102})
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for _, view := range []*Collection{left, right} {
		wg.Add(1)
		go func(view *Collection) {
			defer wg.Done()
			field := int64(101)
			if view == right {
				field = 102
			}
			for version := int64(1); version <= 20; version++ {
				if err := view.UpdateIndexMeta(index(field, version)); err != nil {
					errors <- err
					return
				}
			}
		}(view)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	current := owner.GetCCollection().IndexMeta()
	require.Len(t, current.GetIndexMetas(), 2)
	for _, meta := range current.GetIndexMetas() {
		require.EqualValues(t, 20, meta.GetIndexId())
	}
	// Segment metadata can include fields outside the requesting View's scope.
	full := index(101, 21)
	full.IndexMetas = append(full.IndexMetas, index(102, 19).IndexMetas...)
	require.NoError(t, left.UpdateIndexMeta(full))
	current = owner.GetCCollection().IndexMeta()
	require.Len(t, current.GetIndexMetas(), 2)
	for _, meta := range current.GetIndexMetas() {
		if meta.GetFieldID() == 101 {
			require.EqualValues(t, 21, meta.GetIndexId())
		}
		if meta.GetFieldID() == 102 {
			require.EqualValues(t, 20, meta.GetIndexId())
		}
	}
	// Dropping an index in one scope must preserve the other View's metadata.
	require.NoError(t, left.UpdateIndexMeta(&segcorepb.CollectionIndexMeta{MaxIndexRowCount: 1024}))
	current = owner.GetCCollection().IndexMeta()
	require.Len(t, current.GetIndexMetas(), 1)
	require.EqualValues(t, 102, current.GetIndexMetas()[0].GetFieldID())
}

package metacache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestMetaCacheMissRefreshesSchemaBeforePublication(t *testing.T) {
	client, wal := &schemaTestClient{}, &schemaTestWAL{}
	p := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer p.UnPatch()
	p = mockey.Mock((*schemaTestWAL).SchemaClient).Return(client).Build()
	defer p.UnPatch()
	cache, err := NewMetaCache(nil)
	require.NoError(t, err)
	description := "before alter"
	var describes, refreshes int
	p = mockey.Mock((*MetaCache).describeCollection).To(func(*MetaCache, context.Context, string, string, int64) (*milvuspb.DescribeCollectionResponse, error) {
		describes++
		return &milvuspb.DescribeCollectionResponse{
			CollectionID: 1, DbName: "default", VirtualChannelNames: []string{"p_1v0"},
			Schema: &schemapb.CollectionSchema{Name: "c", Description: description, Version: 3},
		}, nil
	}).Build()
	defer p.UnPatch()
	refreshErr := error(merr.ErrServiceUnavailable)
	p = mockey.Mock((*schemaTestClient).GetSchemaVersion).To(func(_ *schemaTestClient, _ context.Context, req streaming.GetSchemaVersionRequest) (*SchemaInfo, error) {
		refreshes++
		require.True(t, req.BypassCache)
		require.Equal(t, "p_1v0", req.VChannel)
		// The cache mutex is not held across SN I/O, and no metadata is visible yet.
		_, cached := cache.getCollection("default", "c", 1)
		require.False(t, cached)
		if refreshErr != nil {
			return nil, refreshErr
		}
		require.EqualValues(t, 3, req.Version)
		return NewSchemaInfo(&schemapb.CollectionSchema{Name: "c", Description: description, Version: req.Version, Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64},
			{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}})
	}).Build()
	defer p.UnPatch()
	_, err = cache.UpdateByName(t.Context(), "default", "c")
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Empty(t, cache.collections)
	refreshErr = nil
	info, err := cache.UpdateByName(t.Context(), "default", "c")
	require.NoError(t, err)
	require.EqualValues(t, 3, info.Schema.GetVersion(), "refresh must not publish the SN write version as a read version")
	require.Equal(t, 2, refreshes)
	hit, err := cache.UpdateByID(t.Context(), "", 1)
	require.NoError(t, err)
	require.Same(t, info, hit)
	hit, err = cache.GetCollectionInfo(t.Context(), "default", "c", 1)
	require.NoError(t, err)
	require.Same(t, info, hit, "metadata getters must not create per-request schema views")
	hit, err = cache.GetCollectionInfo(t.Context(), "default", "c", 1)
	require.NoError(t, err)
	require.Same(t, info, hit)
	require.Equal(t, 2, refreshes)
	require.Equal(t, 2, describes)
	cache.InvalidateCollectionMeta(t.Context(), "default", "c", 1, false)
	description = "after alter"
	info, err = cache.UpdateByID(t.Context(), "", 1)
	require.NoError(t, err)
	require.EqualValues(t, 3, info.Schema.GetVersion())
	require.Equal(t, 3, refreshes, "same-version invalidation must force another refresh")
	require.Equal(t, 3, describes)
	require.Equal(t, "after alter", info.Schema.Description)
	require.Len(t, info.Schema.Fields, 1)
	_, err = info.Schema.SchemaHelper.GetFieldFromName("RowID")
	require.Error(t, err)
	_, err = loadCollectionSchema(t.Context(), &milvuspb.DescribeCollectionResponse{CollectionID: 1})
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
}

func TestMetaCacheInvalidationDrainsSchemaRefresh(t *testing.T) {
	client, wal := &schemaTestClient{}, &schemaTestWAL{}
	p := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer p.UnPatch()
	p = mockey.Mock((*schemaTestWAL).SchemaClient).Return(client).Build()
	defer p.UnPatch()
	cache, err := NewMetaCache(nil)
	require.NoError(t, err)
	p = mockey.Mock((*MetaCache).describeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1, DbName: "default", VirtualChannelNames: []string{"p_1v0"},
		Schema: &schemapb.CollectionSchema{Name: "c"},
	}, nil).Build()
	defer p.UnPatch()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var refreshes atomic.Int32
	p = mockey.Mock((*schemaTestClient).GetSchemaVersion).To(func(_ *schemaTestClient, ctx context.Context, req streaming.GetSchemaVersionRequest) (*SchemaInfo, error) {
		if refreshes.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return NewSchemaInfo(&schemapb.CollectionSchema{Name: "c"})
	}).Build()
	defer p.UnPatch()
	filled := make(chan error, 1)
	go func() {
		_, err := cache.UpdateByName(t.Context(), "default", "c")
		filled <- err
	}()
	<-started
	invalidating, invalidated := make(chan struct{}), make(chan struct{})
	go func() {
		close(invalidating)
		cache.InvalidateCollectionMeta(t.Context(), "default", "c", 1, false)
		close(invalidated)
	}()
	<-invalidating
	select {
	case <-invalidated:
		t.Fatal("invalidation must wait for the old schema refresh")
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	require.NoError(t, <-filled)
	select {
	case <-invalidated:
	case <-time.After(5 * time.Second):
		t.Fatal("invalidation did not finish")
	}
	_, cached := cache.getCollection("default", "c", 1)
	require.False(t, cached)
	_, err = cache.UpdateByName(t.Context(), "default", "c")
	require.NoError(t, err)
	require.EqualValues(t, 2, refreshes.Load())
}

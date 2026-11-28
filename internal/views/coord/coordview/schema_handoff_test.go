package coordview

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSchemaHandoffRetainsPublishedUp(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	mgr := newTestManager(t, catalog, syncer)
	up := func(dataVersion int64, schema int32) qviews.QueryViewVersion {
		require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(dataVersion, 1, 1).SetSchemaVersion(schema)))
		version := testVersion(dataVersion, 1, 1)
		simulateNodeResponse(t, syncer, testQN1, version, qviews.QueryViewStateReady)
		simulateNodeResponse(t, syncer, testSN, version, qviews.QueryViewStateReady)
		simulateNodeResponse(t, syncer, testSN, version, qviews.QueryViewStateUp)
		return version
	}
	old := up(1, 0)
	mgr.PublishSchema(0)
	current := up(2, 1)
	require.Equal(t, qviews.QueryViewStateUp, mgr.views[old].State())
	require.Equal(t, qviews.QueryViewStateUp, mgr.views[current].State())
	require.Len(t, mgr.Stats().UpBySchema, 2)

	// Same-schema replacement still retires its predecessor.
	replacement := up(3, 1)
	require.Equal(t, qviews.QueryViewStateDown, mgr.views[current].State())
	require.Equal(t, qviews.QueryViewStateUp, mgr.views[old].State())
	mgr.PublishSchema(1)
	mgr.waitFlush()
	require.Equal(t, qviews.QueryViewStateDown, mgr.views[old].State())
	require.Equal(t, qviews.QueryViewStateUp, mgr.views[replacement].State())
	mgr.PublishSchema(0) // replay cannot lower the publication fence
	require.EqualValues(t, 1, *mgr.publishedSchema)
	require.Len(t, mgr.Stats().UpBySchema, 1)
}

func TestSchemaHandoffRecoveryAndRelease(t *testing.T) {
	old := buildTestViewWithVersion(1, 1, 1, 1)
	next := buildTestViewWithVersion(1, 2, 1, 1)
	old.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	next.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	old.Meta.SchemaVersion = 0
	next.Meta.SchemaVersion = 1
	mgr := newTestManager(t, newMockCatalog(), newMockSyncer(), old, next)
	require.Len(t, mgr.Stats().UpBySchema, 2, "recovery must retain both until public metadata is recovered")
	require.NoError(t, mgr.RequestRelease(t.Context()))
	require.Empty(t, mgr.Stats().UpBySchema, "release drains every serving generation")
	require.Nil(t, mgr.upView)
}

func TestRegistryRepeatedSchemaPublication(t *testing.T) {
	r := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
	manager := r.Ensure(id)
	count := 0
	r.RegisterStatsObserver(func(qviews.ShardID, *ShardStats) { count++ })
	r.PublishCollectionSchema(1, 2)
	r.PublishCollectionSchema(1, 2)
	r.PublishCollectionSchema(1, 1)
	require.Equal(t, 1, count)
	require.Error(t, manager.AddPreparing(t.Context(), testBuilderForShard(1, id).SetSchemaVersion(1)))
	r.PublishCollectionSchema(1, 3)
	require.EqualValues(t, 3, *r.Ensure(qviews.ShardID{ReplicaID: 20, VChannel: "p_1v1"}).publishedSchema)
}

func TestRegistryCollectionReleaseForgetsPublishedSchema(t *testing.T) {
	r := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	for id := int64(1); id <= 1000; id++ {
		// The first public read can precede the first manager.
		r.PublishCollectionSchema(id, 0)
		require.NoError(t, r.RequestCollectionRelease(t.Context(), id))
		r.PublishCollectionSchemaIf(id, 3, func() bool { return false })
	}
	require.Empty(t, r.publishedSchemas)
	require.Empty(t, r.publishingSchemas)

	id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
	r.PublishCollectionSchema(1, 2)
	m := r.Ensure(id)
	require.EqualValues(t, 2, *m.publishedSchema)
	r.PublishCollectionSchema(2, 4)
	require.NoError(t, r.RequestCollectionRelease(t.Context(), 1))
	require.Nil(t, r.Get(id))
	require.NotContains(t, r.publishedSchemas, int64(1))
	require.EqualValues(t, 4, r.publishedSchemas[2])
	r.PublishCollectionSchema(1, 5)
	require.EqualValues(t, 5, *r.Ensure(id).publishedSchema)
}

func TestRegistrySchemaReleaseDuringPublication(t *testing.T) {
	r := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
	manager := r.Ensure(id)
	manager.mu.Lock()
	var unlock sync.Once
	release := func() { unlock.Do(manager.mu.Unlock) }
	defer release()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r.PublishCollectionSchema(1, 2)
	}()
	var pending chan struct{}
	require.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		pending = r.publishingSchemas[1]
		return pending != nil
	}, time.Second, time.Millisecond)

	waiting, replayed := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(replayed)
		r.PublishCollectionSchemaIf(1, 2, func() bool { close(waiting); return true })
	}()
	<-waiting
	released := make(chan error, 1)
	go func() { released <- r.RequestCollectionRelease(t.Context(), 1) }()
	require.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		_, found := r.publishedSchemas[1]
		return !found
	}, time.Second, time.Millisecond)
	select {
	case <-replayed:
		t.Fatal("duplicate publication returned before manager notification completed")
	default:
	}
	release()
	require.NoError(t, <-released)
	<-replayed
	<-finished
	<-pending // The old publisher still wakes existing waiters after eviction.
	require.Empty(t, r.publishedSchemas)
	require.Empty(t, r.publishingSchemas)
}

package dataview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAdvanceSchemaPreservesPublishedFootprint(t *testing.T) {
	paramtable.Init()
	cost := &paramtable.Get().QueryViewCfg.LogicalSizeSegmentBaseCost
	previous := cost.GetValue()
	require.NoError(t, paramtable.Get().Save(cost.Key, "16"))
	t.Cleanup(func() { paramtable.Get().Save(cost.Key, previous) })
	m, _ := newTestManager()
	_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	_, err = m.RecomputeNow(t.Context(), 1, projectSegments(LoadableSegment{SegmentID: 10, VChannel: "v1", PartitionID: 1, RowNum: 42, SchemaVersion: 1}))
	require.NoError(t, err)
	old, err := m.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer old.Deref()
	before, ok := old.Stats(10)
	require.True(t, ok)
	require.EqualValues(t, 58, before.LogicalSize)
	var publications []*api.CollectionDataView
	unsubscribe := m.RegisterDataViewListener(func(_ int64, view *api.CollectionDataView) { publications = append(publications, view) })
	defer unsubscribe()
	require.NoError(t, paramtable.Get().Save(cost.Key, "32"))
	_, err = m.AdvanceSchema(t.Context(), 1, 2, nil)
	require.NoError(t, err)
	latest, err := m.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer latest.Deref()
	after, ok := latest.Stats(10)
	require.True(t, ok)
	require.EqualValues(t, 74, after.LogicalSize)
	unchanged, ok := old.Stats(10)
	require.True(t, ok)
	require.Equal(t, before, unchanged)
	require.Len(t, publications, 2)
	require.EqualValues(t, 1, publications[0].SchemaVersion)
	require.EqualValues(t, 2, publications[1].SchemaVersion)
	require.Equal(t, old.DataView().Shards, latest.DataView().Shards)
}

func TestAdvanceSchemaDirectAndInheritance(t *testing.T) {
	m, _ := newTestManager()
	ctx := t.Context()
	_, err := m.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	old, err := m.Latest(ctx, 1)
	require.NoError(t, err)
	defer old.Deref()
	v, err := m.AdvanceSchema(ctx, 1, 2, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, v.CompactVersion)
	require.EqualValues(t, 1, old.DataView().SchemaVersion, "old references are immutable")
	for _, target := range []int32{2, 1} {
		replay, err := m.AdvanceSchema(ctx, 1, target, nil)
		require.NoError(t, err)
		require.Equal(t, v, replay, "replay cannot regress schema or burn versions")
	}
	_, err = m.RecomputeNow(ctx, 1, projectSegments(segment(10, "v1", 1)))
	require.NoError(t, err)
	latest, err := m.Latest(ctx, 1)
	require.NoError(t, err)
	defer latest.Deref()
	require.EqualValues(t, 2, latest.DataView().SchemaVersion)
}

func TestAdvanceSchemaFollowsReplacementAndMetadataOnlyBump(t *testing.T) {
	m, _ := newTestManager()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := m.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	var current atomic.Pointer[[]LoadableSegment]
	a := []LoadableSegment{{SegmentID: 10, VChannel: "v1", PartitionID: 1, SchemaVersion: 1}}
	current.Store(&a)
	observed := make(chan struct{}, 8)
	project := func(context.Context, int64, *viewpb.DataViewOfCollection) ([]LoadableSegment, error) {
		select {
		case observed <- struct{}{}:
		default:
		}
		return *current.Load(), nil
	}
	_, err = m.RecomputeNow(ctx, 1, project)
	require.NoError(t, err)
	<-observed
	done := make(chan error, 1)
	go func() { _, err := m.AdvanceSchema(ctx, 1, 2, project); done <- err }()
	select {
	case <-observed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("premature schema publication: %v", err)
	default:
	}
	// Sort replaces A; the original ID must not become an eternal wait target.
	b := []LoadableSegment{{SegmentID: 20, VChannel: "v1", PartitionID: 1, SchemaVersion: 1}}
	current.Store(&b)
	_, err = m.RecomputeNow(ctx, 1, project)
	require.NoError(t, err)
	// Pure schema bump has no membership/Manifest delta, but must wake the waiter.
	bReady := []LoadableSegment{{SegmentID: 20, VChannel: "v1", PartitionID: 1, SchemaVersion: 2}}
	current.Store(&bReady)
	_, err = m.RecomputeNow(ctx, 1, project)
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	latest, err := m.Latest(ctx, 1)
	require.NoError(t, err)
	defer latest.Deref()
	require.EqualValues(t, 2, latest.DataView().SchemaVersion)
	require.Equal(t, []int64{20}, latest.DataView().Shards[0].Partitions[0].SegmentIds)
}

func TestAdvanceSchemaCancellationAndPersistenceRetry(t *testing.T) {
	m, catalog := newTestManager()
	_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = m.AdvanceSchema(ctx, 1, 2, projectSegments(LoadableSegment{SegmentID: 10, VChannel: "v1", PartitionID: 1, SchemaVersion: 1}))
	require.ErrorIs(t, err, context.Canceled)
	catalog.saveErr = context.DeadlineExceeded
	_, err = m.AdvanceSchema(t.Context(), 1, 2, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	old, err := m.Latest(t.Context(), 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, old.DataView().SchemaVersion)
	old.Deref()
	v, err := m.AdvanceSchema(t.Context(), 1, 2, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, v.CompactVersion)
	recovered, err := RecoverManager(t.Context(), catalog, recoverAllCollections, nil, nil, nil)
	require.NoError(t, err)
	latest, err := recovered.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer latest.Deref()
	require.EqualValues(t, 2, latest.DataView().SchemaVersion)
}

func TestAdvanceSchemaWakesWhenRecomputeQueueIsFull(t *testing.T) {
	m := newManager(t.Context(), &fakeDataViewCatalog{}, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := m.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	var version atomic.Int32
	version.Store(1)
	observed := make(chan struct{}, 2)
	m.projector = func(context.Context, int64, *viewpb.DataViewOfCollection) ([]LoadableSegment, error) {
		v := version.Load()
		select {
		case observed <- struct{}{}:
		default:
		}
		return []LoadableSegment{{SegmentID: 10, VChannel: "v1", PartitionID: 1, SchemaVersion: v}}, nil
	}
	// No queue worker: fill it to prove progress is independent of its drain.
	m.queue.ch = make(chan int64, 1)
	m.queue.ch <- 99
	done := make(chan error, 1)
	go func() { _, err := m.AdvanceSchema(ctx, 1, 2, m.projector); done <- err }()
	select {
	case <-observed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	version.Store(2)
	require.ErrorIs(t, m.Recompute(ctx, 1), errRecomputeQueueFull)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestAdvanceSchemaDropWakesWaiter(t *testing.T) {
	m := newManager(t.Context(), &fakeDataViewCatalog{}, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := m.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	observed := make(chan struct{})
	project := func(context.Context, int64, *viewpb.DataViewOfCollection) ([]LoadableSegment, error) {
		close(observed)
		return []LoadableSegment{{SegmentID: 10, VChannel: "v1", PartitionID: 1, SchemaVersion: 1}}, nil
	}
	done := make(chan error, 1)
	go func() { _, err := m.AdvanceSchema(ctx, 1, 2, project); done <- err }()
	select {
	case <-observed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err = m.OnDropCollection(ctx, 1)
	require.NoError(t, err)
	select {
	case err := <-done:
		require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestAdvanceSchemaRetriesProjectionFailure(t *testing.T) {
	m := newManager(t.Context(), &fakeDataViewCatalog{}, nil)
	_, err := m.AdvanceSchema(t.Context(), 1, 2, nil)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	_, err = m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	_, err = m.AdvanceSchema(t.Context(), 1, 2, func(context.Context, int64, *viewpb.DataViewOfCollection) ([]LoadableSegment, error) {
		return nil, context.DeadlineExceeded
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = m.AdvanceSchema(t.Context(), 1, 2, projectSegments(LoadableSegment{SegmentID: 10, VChannel: "v1", PartitionID: 1, SchemaVersion: 2}))
	require.NoError(t, err)
}

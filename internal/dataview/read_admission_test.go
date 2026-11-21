package dataview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReadAdmissionFlushReplacementAndStaleSchemaCapture(t *testing.T) {
	catalog := &fakeDataViewCatalog{}
	m := newManager(t.Context(), catalog, nil)
	ctx := t.Context()
	var publications []*api.CollectionDataView
	stop := m.RegisterDataViewListener(func(_ int64, v *api.CollectionDataView) { publications = append(publications, v) })
	defer stop()
	pair := &viewpb.QueryPlanMVCC{GrowingTimetick: 120, TransformingTimetick: 140}
	m.readAdmissionSample = func(context.Context, string) (*viewpb.QueryPlanMVCC, error) { return pair, nil }
	_, err := m.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v0"}})
	require.NoError(t, err)
	require.Len(t, publications, 1)
	require.NotNil(t, publications[0].Shard("v0").ReadAdmissionFloor)

	event := FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segmentWithRows(10, "v0", 1, 5)}}
	_, _, abort, err := m.PrepareFlush(ctx, event)
	require.NoError(t, err)
	abort()
	require.NoError(t, m.certifyReadAdmission(ctx, 1))
	physical, commit, _, err := m.PrepareFlush(ctx, event)
	require.NoError(t, err)
	require.Nil(t, physical.GetShards()[0].GetReadAdmissionFloor())
	commit()
	require.Len(t, publications, 1, "an unconfirmed G-to-T handoff is invisible")
	ref, err := m.Get(ctx, 1, physical.GetDataVersion())
	require.NoError(t, err)
	require.Nil(t, ref, "direct resource hydration cannot pin an unconfirmed version")
	catalog.saveErr = merr.ErrServiceUnavailable
	require.ErrorIs(t, m.certifyReadAdmission(ctx, 1), merr.ErrServiceUnavailable)
	require.Len(t, publications, 1, "a failed certificate commit cannot publish")
	require.NoError(t, m.certifyReadAdmission(ctx, 1))
	require.Len(t, publications, 2)
	first := publications[1]
	require.True(t, proto.Equal(pair, first.Shard("v0").ReadAdmissionFloor))
	require.Equal(t, int64(5), first.TotalRows)
	require.Equal(t, physical.GetDataVersion().GetStreamingVersion(), first.DataVersion.StreamingVersion)

	// Complete erasure still changes the base. A failed sampler keeps the
	// retained predecessor addressable, including its exact imported rows.
	_, commit, _, err = m.PublishChange(ctx, ChangeGroupDataViewEvent{CollectionID: 1, SupersededSegmentIDs: []int64{10}})
	require.NoError(t, err)
	commit()
	m.readAdmissionSample = func(context.Context, string) (*viewpb.QueryPlanMVCC, error) { return nil, context.DeadlineExceeded }
	require.ErrorIs(t, m.certifyReadAdmission(ctx, 1), context.DeadlineExceeded)
	require.Len(t, publications, 2)
	ref, err = m.Get(ctx, 1, first.DataVersion.IntoProto())
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	require.Equal(t, []int64{10}, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds())

	// The WAL call must run without a mutation lock. Replace its frozen
	// candidate with a schema projection and ensure the old reply cannot win.
	m.readAdmissionSample = func(context.Context, string) (*viewpb.QueryPlanMVCC, error) {
		seg := segmentWithRows(20, "v0", 1, 9)
		seg.SchemaVersion = 1
		_, err := m.AdvanceSchema(ctx, 1, 1, projectSegments(seg))
		return pair, err
	}
	require.NoError(t, m.certifyReadAdmission(ctx, 1))
	require.Len(t, publications, 2)
	m.readAdmissionSample = func(context.Context, string) (*viewpb.QueryPlanMVCC, error) {
		return &viewpb.QueryPlanMVCC{GrowingTimetick: 130, TransformingTimetick: 135}, nil
	}
	require.ErrorIs(t, m.certifyReadAdmission(ctx, 1), merr.ErrServiceNotReady)
	require.Len(t, publications, 2)
	m.readAdmissionSample = func(context.Context, string) (*viewpb.QueryPlanMVCC, error) { return pair, nil }
	require.NoError(t, m.certifyReadAdmission(ctx, 1))
	require.Len(t, publications, 3)
	require.EqualValues(t, 1, publications[2].SchemaVersion)
	require.Equal(t, int64(9), publications[2].TotalRows)
	_, err = m.RecomputeNow(ctx, 1, projectSegments())
	require.NoError(t, err)
	require.Len(t, publications, 3)
	require.NoError(t, m.certifyReadAdmission(ctx, 1))
	require.Len(t, publications, 4)
	require.Empty(t, publications[3].Shard("v0").Partitions, "legacy empty-output recompute is certified before publication")
}

func TestReadAdmissionRestartWithUncertifiedPhysicalCommit(t *testing.T) {
	old := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: version(1, 0), Shards: []*viewpb.DataViewOfShard{{Vchannel: "v0", ReadAdmissionFloor: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 110}}}}
	physical := canonicalDataViewClone(old)
	physical.DataVersion = version(2, 0)
	physical.Shards[0].ReadAdmissionFloor = nil
	catalog := &fakeDataViewCatalog{views: []*viewpb.DataViewOfCollection{physical, old}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	available := make(chan struct{})
	manager, err := RecoverManager(ctx, catalog, recoverAllCollections, nil, nil, nil,
		WithReadAdmissionSampler(func(ctx context.Context, channel string) (*viewpb.QueryPlanMVCC, error) {
			close(started)
			select {
			case <-available:
				return &viewpb.QueryPlanMVCC{GrowingTimetick: 120, TransformingTimetick: 140}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}))
	require.NoError(t, err)
	<-started
	events := make(chan *api.CollectionDataView, 2)
	stop := manager.RegisterDataViewListener(func(_ int64, v *api.CollectionDataView) { events <- v })
	defer stop()
	published := <-events
	require.NotNil(t, published)
	require.Equal(t, int64(1), published.DataVersion.StreamingVersion)
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	oldRef, err := manager.Get(ctx, 1, old.GetDataVersion())
	require.NoError(t, err)
	require.NotNil(t, oldRef)
	oldRef.Deref()
	ref, err := manager.Get(ctx, 1, physical.GetDataVersion())
	require.NoError(t, err)
	require.Nil(t, ref)
	close(available)
	select {
	case next := <-events:
		require.Equal(t, int64(2), next.DataVersion.StreamingVersion)
		require.Equal(t, int64(1), next.DataVersion.CompactVersion)
		require.EqualValues(t, 140, next.Shard("v0").ReadAdmissionFloor.TransformingTimetick)
	case <-time.After(3 * time.Second):
		t.Fatal("recovered certification did not publish on a quiet shard")
	}
}

func TestReadAdmissionSameSegmentManifestAndUnchangedSchema(t *testing.T) {
	ctx := t.Context()
	calls := 0
	m := newManager(ctx, &fakeDataViewCatalog{}, nil, WithReadAdmissionSampler(func(context.Context, string) (*viewpb.QueryPlanMVCC, error) {
		calls++
		return &viewpb.QueryPlanMVCC{GrowingTimetick: 90, TransformingTimetick: uint64(100 + calls)}, nil
	}))
	seg := segmentWithManifestVersion(10, "v", 1, 1)
	_, err := m.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v"}, Segments: []LoadableSegment{seg}})
	require.NoError(t, err)
	require.Nil(t, m.Published(1))
	require.NoError(t, m.certifyReadAdmission(ctx, 1))
	initial := m.Published(1)
	require.EqualValues(t, 101, initial.Shard("v").ReadAdmissionFloor.TransformingTimetick)
	_, err = m.AdvanceSchema(ctx, 1, 2, nil)
	require.NoError(t, err)
	schema := m.Published(1)
	require.EqualValues(t, 2, schema.SchemaVersion)
	require.True(t, proto.Equal(initial.Shard("v").ReadAdmissionFloor, schema.Shard("v").ReadAdmissionFloor))
	require.Equal(t, 1, calls)
	seg.ManifestVersion++
	version, err := m.RecomputeNow(ctx, 1, projectSegments(seg))
	require.NoError(t, err)
	require.Same(t, schema, m.Published(1))
	ref, err := m.Get(ctx, 1, version)
	require.NoError(t, err)
	require.Nil(t, ref)
	require.NoError(t, m.certifyReadAdmission(ctx, 1))
	updated := m.Published(1)
	require.EqualValues(t, 2, updated.Shard("v").Partitions[0].Segments[0].ManifestVersion)
	require.EqualValues(t, 102, updated.Shard("v").ReadAdmissionFloor.TransformingTimetick)
	require.EqualValues(t, 1, initial.Shard("v").Partitions[0].Segments[0].ManifestVersion)
}

func TestReadAdmissionFullRecomputeQueuePublishesQuietCollection(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m := newManager(ctx, &fakeDataViewCatalog{}, nil, WithReadAdmissionSampler(func(context.Context, string) (*viewpb.QueryPlanMVCC, error) {
		return &viewpb.QueryPlanMVCC{GrowingTimetick: 120, TransformingTimetick: 140}, nil
	}))
	m.queue.ch = make(chan int64, 1)
	require.NoError(t, m.queue.Enqueue(1))
	require.ErrorIs(t, m.queue.Enqueue(2), errRecomputeQueueFull)
	_, err := m.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{CollectionID: 2, VChannels: []string{"v2"}})
	require.NoError(t, err)
	m.queue.requireCertification(2)
	require.Len(t, m.queue.overflow, 1)
	published := make(chan *api.CollectionDataView, 1)
	stop := m.RegisterDataViewListener(func(_ int64, view *api.CollectionDataView) { published <- view })
	defer stop()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		m.queue.run(ctx)
	}()
	defer func() { cancel(); <-stopped }()
	select {
	case v := <-published:
		require.EqualValues(t, 2, v.CollectionID)
		require.EqualValues(t, 140, v.Shard("v2").ReadAdmissionFloor.TransformingTimetick)
	case <-time.After(3 * time.Second):
		t.Fatal("a full queue stranded bootstrap certification on a quiet collection")
	}
}

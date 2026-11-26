package adaptor

import (
	"context"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestReplicaFollowerConfirmsDownBeforeRelease(t *testing.T) {
	const (
		preparing = viewpb.QueryViewState_QueryViewStatePreparing
		ready     = viewpb.QueryViewState_QueryViewStateReady
		up        = viewpb.QueryViewState_QueryViewStateUp
		down      = viewpb.QueryViewState_QueryViewStateDown
		dropped   = viewpb.QueryViewState_QueryViewStateDropped
	)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	view := newQueryShutdownTestView("p_1v0", 10, preparing)
	key := viewKey(view)
	client, root := kvfactory.GetEtcdAndPath()
	store := etcdkv.NewEtcdKV(client, path.Join(root, t.Name(), uuid.NewString()))
	t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")) })
	catalog := queryview.NewQueryViewCatalog(store, "coord")
	snCatalog := streamingnode.NewCataLog(store)
	resource.InitForTest(t, resource.OptQueryViewCatalog(catalog), resource.OptStreamingNodeCatalog(snCatalog))
	t.Cleanup(resource.Resource().TimeTickInspector().Close)
	require.NoError(t, catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{view}))

	var releases atomic.Int32
	resources := newQueryShutdownTestResMgr(func(snview.ReleaseResource) { releases.Add(1) })
	readerViews := snview.RecoverPChannelSNQueryViewHandler(ctx, "p", nil, resources, nil)
	defer readerViews.CloseForHandoff()
	follower := newFollower("p", readerViews, nil, nil)
	writer := replica.NewPublisher(nil)
	defer writer.Close()
	topology := &streamingpb.WALReplicaTopology{
		Pchannel: "p", Revision: 1, WriterReplicaId: 1, WriterTerm: 7,
		Members: []*streamingpb.WALReplicaPlacement{{WalReplicaId: 1}, {WalReplicaId: 2}},
	}
	require.NoError(t, streamingcoord.NewReplicaTopologyCatalog(store).Save(ctx, nil, topology))
	require.NoError(t, snCatalog.SaveRecoverySnapshot(ctx, "p", &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{Term: topology.WriterTerm},
	}))
	writer.SetMembers(topology, true)
	writer.PublishView(key, preparing)
	sub, snapshot, err := writer.Subscribe(2)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, follower.apply(ctx, snapshot, true))
	acquired, ok := resources.getAcquired(key)
	require.True(t, ok)
	acquired.OnReady()
	report, err := follower.next(ctx)
	require.NoError(t, err)
	require.Equal(t, ready, report.LocalState)
	sub.Report(report)
	require.NoError(t, writer.WaitReports(ctx, key, ready))

	// The subscription coalesces Up and Down while the follower is still Ready.
	durable := []*viewpb.QueryViewOfShard{newQueryShutdownTestView("p_1v0", 10, up)}
	require.NoError(t, snCatalog.SaveQueryViews(ctx, "p", durable))
	writer.PublishView(key, up)
	local := &roWALAdaptorImpl{readViews: snview.RecoverPChannelSNQueryViewHandler(ctx, "p", nil, newQueryShutdownTestResMgr(nil), durable)}
	defer local.readViews.CloseForHandoff()
	local.follower = newFollower("p", local.readViews, nil, durable)
	control := newWriterViewControl(ctx, local, writer, durable)
	defer control.Stop()
	view.Meta.State = down
	require.NoError(t, catalog.SaveQueryViews(ctx, []*viewpb.QueryViewOfShard{view}))
	coordReport := make(chan qviews.QueryViewState, 1)
	control.ApplyViews([]handler.ApplyView{{
		View:     qviews.NewQueryViewAtWorkNodeFromProto(view),
		OnReport: func(v qviews.QueryViewAtWorkNode) { coordReport <- v.State() },
	}})
	delta, err := sub.Next(ctx)
	require.NoError(t, err)
	require.NoError(t, follower.apply(ctx, delta, false))
	report, err = follower.next(ctx)
	require.NoError(t, err)
	require.Equal(t, down, report.LocalState)
	persisted, err := snCatalog.ListQueryViews(ctx, "p")
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Equal(t, up, persisted[0].GetMeta().GetState())
	select {
	case ack := <-coordReport:
		t.Fatalf("writer reported %s before receiving the reader's Down", ack)
	default:
	}
	sub.Report(report)
	select {
	case ack := <-coordReport:
		require.Equal(t, qviews.QueryViewStateDown, ack)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	persisted, err = snCatalog.ListQueryViews(ctx, "p")
	require.NoError(t, err)
	require.Empty(t, persisted)
	require.Zero(t, releases.Load())

	writer.PublishView(key, dropped)
	delta, err = sub.Next(ctx)
	require.NoError(t, err)
	require.NoError(t, follower.apply(ctx, delta, false))
	// A repeated Down report may precede the asynchronous release callback.
	for {
		report, err = follower.next(ctx)
		require.NoError(t, err)
		if report.LocalState == dropped {
			break
		}
		require.Equal(t, down, report.LocalState)
	}
	require.EqualValues(t, 1, releases.Load())
}

func TestReplicaFollowerRegistersOldestDataVersionAcrossShards(t *testing.T) {
	ctx := t.Context()
	client, root := kvfactory.GetEtcdAndPath()
	store := etcdkv.NewEtcdKV(client, path.Join(root, t.Name(), uuid.NewString()))
	t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")) })
	catalog := queryview.NewQueryViewCatalog(store, "coord")
	resource.InitForTest(t, resource.OptQueryViewCatalog(catalog))
	t.Cleanup(resource.Resource().TimeTickInspector().Close)

	var registrations []int64
	patch := mockey.Mock((*queryShutdownTestResMgr).Acquire).To(func(_ *queryShutdownTestResMgr, req snview.AcquireResource) {
		registrations = append(registrations, req.Key.QueryViewVersion.DataVersion.StreamingVersion)
	}).Build()
	defer patch.UnPatch()
	views := snview.RecoverPChannelSNQueryViewHandler(ctx, "p", nil, newQueryShutdownTestResMgr(nil), nil)
	defer views.CloseForHandoff()
	reader := newFollower("p", views, nil, nil)

	// Interleave data versions across two logical replicas. Applying a full
	// shard at a time cannot produce the required registration order.
	var definitions []*viewpb.QueryViewOfShard
	var state streamingpb.WALReplicaState
	for version := int64(4); version > 0; version-- {
		target := viewpb.QueryViewState_QueryViewStateUp
		if version == 4 {
			target = viewpb.QueryViewState_QueryViewStatePreparing
		}
		view := newQueryShutdownTestView("p_1v0", 10+version%2, target)
		view.Meta.Version.DataVersion.StreamingVersion = version
		definitions = append(definitions, view)
		key := viewKey(view)
		state.QueryViews = append(state.QueryViews, &streamingpb.WALQueryViewSignal{
			View:  &viewpb.QueryViewKey{Shard: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()},
			State: target,
		})
	}
	require.NoError(t, catalog.SaveQueryViews(ctx, definitions))
	require.NoError(t, reader.apply(ctx, &state, true))
	require.Equal(t, []int64{1, 2, 3, 4}, registrations)
}

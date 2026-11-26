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
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestWriterViewReplicaPreparation(t *testing.T) {
	t.Run("reader recovers the same version", func(t *testing.T) {
		f := newReplicaPreparationTest(t)
		// The RW's resource manager uses manual readiness callbacks. The RO
		// runs its scheduler, shared growing runtime and per-view preparation.
		patch := mockey.Mock((*vchannel.ReplicaQueries).PrepareUp).
			When(func(q *vchannel.ReplicaQueries, _ context.Context, _ qviews.QueryViewKey) bool {
				return q == f.control.readSide.readQueries
			}).Return(nil).Build()
		t.Cleanup(func() { patch.UnPatch() })

		var attempts atomic.Int32
		retrying := make(chan struct{})
		resume := make(chan struct{})
		reader, sub := f.startReader(func(ctx context.Context) error {
			switch attempts.Add(1) {
			case 1:
				return merr.WrapErrIoFailedReason("failed to read sealed metadata")
			case 2:
				close(retrying)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-resume:
				}
			}
			return nil
		})

		f.local.OnReady()
		f.subscribe(3)(viewpb.QueryViewState_QueryViewStateReady)
		select {
		case <-retrying:
		case <-f.ctx.Done():
			t.Fatal("reader never retried the failed metadata read")
		}
		f.expectPending()
		f.checkDurableViews(f.v1)
		close(resume)
		for {
			report, err := reader.next(f.ctx)
			require.NoError(t, err)
			sub.Report(report)
			if report.LocalState == viewpb.QueryViewState_QueryViewStateReady {
				break
			}
			require.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, report.LocalState)
		}
		f.expectReport(qviews.QueryViewStateReady)
		f.checkDurableViews(f.v1)
		f.reports = f.push(qviews.QueryViewStateUp)
		f.expectReport(qviews.QueryViewStateUp)
		f.checkDurableViews(f.v1, f.key)

		delta, err := sub.Next(f.ctx)
		require.NoError(t, err)
		require.NoError(t, reader.apply(f.ctx, delta, false))
		for {
			report, err := reader.next(f.ctx)
			require.NoError(t, err)
			if report.LocalState == viewpb.QueryViewState_QueryViewStateUp {
				break
			}
			require.Equal(t, viewpb.QueryViewState_QueryViewStateReady, report.LocalState)
		}
		lease, err := reader.handler.AcquireUpView(f.ctx, f.key.ShardID, f.key.QueryViewVersion)
		require.NoError(t, err)
		lease.Release()
	})

	t.Run("writer fails while readers are preparing", func(t *testing.T) {
		f := newReplicaPreparationTest(t)
		f.subscribe(2)(viewpb.QueryViewState_QueryViewStatePreparing)
		f.local.OnUnrecoverable()
		f.expectReport(qviews.QueryViewStateUnrecoverable)
		f.checkDurableViews(f.v1)
	})

	t.Run("Down supersedes pending preparation", func(t *testing.T) {
		f := newReplicaPreparationTest(t)
		first, second := f.subscribe(2), f.subscribe(3)
		f.local.OnReady()
		first(viewpb.QueryViewState_QueryViewStateReady)
		obsolete := f.reports

		f.reports = f.push(qviews.QueryViewStateDown)
		second(viewpb.QueryViewState_QueryViewStateReady)
		first(viewpb.QueryViewState_QueryViewStateDown)
		f.expectPending()
		second(viewpb.QueryViewState_QueryViewStateDown)
		f.expectReport(qviews.QueryViewStateDown)
		require.Empty(t, f.releases)

		f.reports = f.push(qviews.QueryViewStateDropped)
		f.expectReport(qviews.QueryViewStateDropped)
		select {
		case released := <-f.releases:
			require.Equal(t, f.key, released)
		default:
			t.Fatal("reported Dropped without releasing resources")
		}
		f.control.Stop()
		require.Empty(t, obsolete)
		f.checkDurableViews(f.v1)
	})
}

type replicaPreparationTest struct {
	t        *testing.T
	ctx      context.Context
	control  *writerViewControl
	local    snview.AcquireResource
	views    *snview.SNQueryViewHandler
	catalog  metastore.StreamingNodeCataLog
	reports  <-chan qviews.QueryViewAtWorkNode
	releases <-chan qviews.QueryViewKey
	template *viewpb.QueryViewOfShard
	v1, key  qviews.QueryViewKey
}

func newReplicaPreparationTest(t *testing.T) *replicaPreparationTest {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	client, root := kvfactory.GetEtcdAndPath()
	store := etcdkv.NewEtcdKV(client, path.Join(root, t.Name(), uuid.NewString()))
	t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")) })
	catalog := streamingnode.NewCataLog(store)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptQueryViewCatalog(queryview.NewQueryViewCatalog(store, "coord")))
	t.Cleanup(resource.Resource().TimeTickInspector().Close)

	v1 := newQueryShutdownTestView("p_1v0", 10, viewpb.QueryViewState_QueryViewStateUp)
	require.NoError(t, catalog.SaveQueryViews(ctx, "p", []*viewpb.QueryViewOfShard{v1}))
	durable, err := catalog.ListQueryViews(ctx, "p")
	require.NoError(t, err)
	topology := &streamingpb.WALReplicaTopology{
		Pchannel: "p", Revision: 2, WriterReplicaId: 1, WriterTerm: 8,
		Members: []*streamingpb.WALReplicaPlacement{{WalReplicaId: 1}, {WalReplicaId: 2}, {WalReplicaId: 3}},
	}
	require.NoError(t, streamingcoord.NewReplicaTopologyCatalog(store).Save(ctx, nil, topology))
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p", &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{Term: topology.WriterTerm},
	}))
	view := newQueryShutdownTestView("p_1v0", 10, viewpb.QueryViewState_QueryViewStatePreparing)
	view.Meta.Version.QueryVersion++
	publisher := replica.NewPublisher(nil)
	t.Cleanup(publisher.Close)
	publisher.SetMembers(topology, true)
	releases := make(chan qviews.QueryViewKey, 3)
	resources := newQueryShutdownTestResMgr(func(req snview.ReleaseResource) { releases <- req.Key })
	local := &roWALAdaptorImpl{
		readQueries: vchannel.NewReplicaQueries(ctx, "p", replica.NewBuffer(0), 0),
		readViews:   snview.RecoverPChannelSNQueryViewHandler(ctx, "p", nil, resources, durable),
	}
	t.Cleanup(local.readQueries.Close)
	t.Cleanup(local.readViews.CloseForHandoff)
	local.follower = newFollower("p", local.readViews, local.readQueries, durable)
	recovered, ok := resources.getAcquired(viewKey(v1))
	require.True(t, ok)
	recovered.OnReady()
	control := newWriterViewControl(ctx, local, publisher, durable)
	t.Cleanup(control.Stop)
	f := &replicaPreparationTest{
		t: t, ctx: ctx, control: control, views: local.readViews, catalog: catalog,
		releases: releases, template: view, v1: viewKey(v1), key: viewKey(view),
	}
	f.reports = f.push(qviews.QueryViewStatePreparing)
	f.local, ok = resources.getAcquired(f.key)
	require.True(t, ok)
	return f
}

func (f *replicaPreparationTest) startReader(refresh func(context.Context) error) (*replicaFollower, *replica.Subscription) {
	f.t.Helper()
	// Isolate metadata reads and use an empty growing snapshot. Preserve the
	// normal ReplicaQueries configuration, including its retry policy.
	var original func(queryresource.Config) *queryresource.Manager
	patch := mockey.Mock(queryresource.NewManager).Origin(&original).To(func(cfg queryresource.Config) *queryresource.Manager {
		cfg.Builders = []queryresource.QueryRuntimeModuleBuilder{queryresource.NewGrowingRuntimeModuleBuilder(nil)}
		cfg.CaptureView = func(_ context.Context, meta *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) error {
			install(walview.VChannelWALView{VChannel: meta.Vchannel, RefreshSealedVersions: refresh})
			return nil
		}
		return original(cfg)
	}).Build()
	f.t.Cleanup(func() { patch.UnPatch() })
	queries := vchannel.NewReplicaQueries(f.ctx, "p", replica.NewBuffer(0), 0)
	f.t.Cleanup(queries.Close)
	views := snview.RecoverPChannelSNQueryViewHandler(f.ctx, "p", nil, queries, nil)
	f.t.Cleanup(views.CloseForHandoff)
	reader := newFollower("p", views, queries, nil)
	sub, snapshot, err := f.control.publisher.Subscribe(2)
	require.NoError(f.t, err)
	f.t.Cleanup(sub.Close)
	require.NoError(f.t, reader.apply(f.ctx, snapshot, true))
	return reader, sub
}

func (f *replicaPreparationTest) push(state qviews.QueryViewState) <-chan qviews.QueryViewAtWorkNode {
	view := proto.CloneOf(f.template)
	view.Meta.State = viewpb.QueryViewState(state)
	require.NoError(f.t, resource.Resource().QueryViewCatalog().SaveQueryViews(f.ctx, []*viewpb.QueryViewOfShard{view}))
	reports := make(chan qviews.QueryViewAtWorkNode, 8)
	f.control.ApplyViews([]handler.ApplyView{{
		View: qviews.NewQueryViewAtWorkNodeFromProto(view),
		OnReport: func(v qviews.QueryViewAtWorkNode) {
			if v.State() != qviews.QueryViewStatePreparing {
				reports <- v
			}
		},
	}})
	return reports
}

func (f *replicaPreparationTest) subscribe(id int64) func(viewpb.QueryViewState) {
	f.t.Helper()
	sub, _, err := f.control.publisher.Subscribe(id)
	require.NoError(f.t, err)
	f.t.Cleanup(sub.Close)
	viewID := &viewpb.QueryViewKey{Shard: f.key.ShardID.IntoProto(), Version: f.key.QueryViewVersion.IntoProto()}
	return func(state viewpb.QueryViewState) {
		sub.Report(&streamingpb.WALQueryViewReport{View: viewID, LocalState: state})
	}
}

func (f *replicaPreparationTest) expectReport(expected qviews.QueryViewState) {
	f.t.Helper()
	select {
	case got := <-f.reports:
		require.Equal(f.t, f.key, got.QueryViewKey())
		require.Equal(f.t, expected, got.State())
	case <-f.ctx.Done():
		f.t.Fatalf("waiting for %v on %s: %v", expected, f.key, f.ctx.Err())
	}
}

func (f *replicaPreparationTest) expectPending() {
	f.t.Helper()
	select {
	case got := <-f.reports:
		f.t.Fatalf("reported %s before reader confirmation", got.State())
	case <-time.After(50 * time.Millisecond):
	}
}

func (f *replicaPreparationTest) checkDurableViews(expected ...qviews.QueryViewKey) {
	f.t.Helper()
	views, err := f.catalog.ListQueryViews(f.ctx, "p")
	require.NoError(f.t, err)
	var actual []qviews.QueryViewKey
	for _, v := range views {
		require.Equal(f.t, viewpb.QueryViewState_QueryViewStateUp, v.GetMeta().GetState())
		actual = append(actual, viewKey(v))
	}
	require.ElementsMatch(f.t, expected, actual)
	lease, err := f.views.AcquireUpView(f.ctx, f.v1.ShardID, f.v1.QueryViewVersion)
	require.NoError(f.t, err)
	lease.Release()
}

func TestWriterViewDirectDropRetiresDurableUp(t *testing.T) {
	f := newReplicaPreparationTest(t)
	f.key = f.v1
	f.template = newQueryShutdownTestView("p_1v0", 10, viewpb.QueryViewState_QueryViewStateDropped)
	f.reports = f.push(qviews.QueryViewStateDropped)
	// Readers joining during retirement must still see the Down intent.
	sub, snapshot, err := f.control.publisher.Subscribe(2)
	require.NoError(t, err)
	defer sub.Close()
	id := &viewpb.QueryViewKey{Shard: f.key.ShardID.IntoProto(), Version: f.key.QueryViewVersion.IntoProto()}
	var state viewpb.QueryViewState
	for _, signal := range snapshot.QueryViews {
		if proto.Equal(signal.View, id) {
			state = signal.State
		}
	}
	require.Equal(t, viewpb.QueryViewState_QueryViewStateDown, state)
	two := f.subscribe(3)
	sub.Report(&streamingpb.WALQueryViewReport{View: id, LocalState: viewpb.QueryViewState_QueryViewStateDown})
	f.expectPending()
	durable, err := f.catalog.ListQueryViews(f.ctx, "p")
	require.NoError(t, err)
	require.Len(t, durable, 1)
	require.Equal(t, f.key, viewKey(durable[0]))
	two(viewpb.QueryViewState_QueryViewStateDropped)
	f.expectReport(qviews.QueryViewStateDropped)
	durable, err = f.catalog.ListQueryViews(f.ctx, "p")
	require.NoError(t, err)
	require.Empty(t, durable)
	f.reports = f.push(qviews.QueryViewStateDropped)
	f.expectReport(qviews.QueryViewStateDropped)
}

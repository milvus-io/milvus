package querycoordv2

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type handoverTestBackend struct {
	task  *datapb.SplitShardTask
	calls []string
	fail  string
}

func (b *handoverTestBackend) ListShardSplits() []*datapb.SplitShardTask {
	return []*datapb.SplitShardTask{b.task}
}

func (b *handoverTestBackend) step(name string) error {
	b.calls = append(b.calls, name)
	if b.fail == name {
		return errors.New(name)
	}
	return nil
}

func (b *handoverTestBackend) AdoptShardSplit(context.Context, int64) error {
	if err := b.step("adopt"); err != nil {
		return err
	}
	b.task.State = datapb.SplitShardTaskState_SplitShardTaskAdopted
	return nil
}

func (b *handoverTestBackend) ResumeShardSplitL0(context.Context, int64) error {
	return b.step("resume")
}

func (b *handoverTestBackend) ReleaseShardSplitPins(context.Context, int64) (bool, error) {
	err := b.step("pins")
	return err == nil, err
}

func (b *handoverTestBackend) RetireShardSplit(context.Context, int64) error {
	if err := b.step("retire"); err != nil {
		return err
	}
	b.task.State = datapb.SplitShardTaskState_SplitShardTaskRetired
	return nil
}

type blockingHandoverBackend struct {
	*handoverTestBackend
	started chan context.Context
	done    chan struct{}
}

func (b *blockingHandoverBackend) AdoptShardSplit(ctx context.Context, _ int64) error {
	b.started <- ctx
	<-ctx.Done()
	close(b.done)
	return ctx.Err()
}

func TestSplitHandoverStopCancelsAndJoinsActiveAdoption(t *testing.T) {
	parent, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	b := &blockingHandoverBackend{
		handoverTestBackend: &handoverTestBackend{task: &datapb.SplitShardTask{TaskId: 1, CollectionId: 100, State: datapb.SplitShardTaskState_SplitShardTaskRewriteDone}},
		started:             make(chan context.Context, 1), done: make(chan struct{}),
	}
	r := &qviewsRuntime{splitHandover: &splitHandover{backend: b}}
	r.startSplitHandover(parent)
	defer r.splitHandover.stop()
	select {
	case worker := <-b.started:
		r.splitHandover.stop()
		require.ErrorIs(t, worker.Err(), context.Canceled)
		require.NoError(t, parent.Err())
		select {
		case <-b.done:
		default:
			t.Fatal("stop returned before adoption finished")
		}
	case <-parent.Done():
		t.Fatal("adoption did not start")
	}
}

func TestSplitHandoverRetriesEachPersistentPhaseInOrder(t *testing.T) {
	for _, failure := range []string{"adopt", "publish", "resume", "pins", "retire"} {
		t.Run(failure, func(t *testing.T) {
			b := &handoverTestBackend{task: &datapb.SplitShardTask{TaskId: 1, CollectionId: 100, SourceVchannel: "s", State: datapb.SplitShardTaskState_SplitShardTaskRewriteDone}, fail: failure}
			publish := func(context.Context, *datapb.SplitShardTask) error { return b.step("publish") }
			require.Error(t, runSplitHandover(t.Context(), b, b.task, publish))
			require.NotEqual(t, datapb.SplitShardTaskState_SplitShardTaskRetired, b.task.State)
			b.fail = ""
			b.calls = nil
			require.NoError(t, runSplitHandover(t.Context(), b, b.task, publish))
			require.Equal(t, []string{"adopt", "publish", "resume", "pins", "retire"}, b.calls)
		})
	}
}

func TestSchemaCallbackCanFinishRecoveredAdoptionBeforeWorkerStarts(t *testing.T) {
	// Persisted adoption is present, but the background worker has not started.
	// Use real cache, load store and registry; an unloaded collection has no
	// serving views to preempt and must not wait for an impossible g2 load.
	cfg, err := loadmgr.RecoverLoadConfigStore(t.Context(), loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	transport := syncer.NewReliableSyncer(&fakeRuntimeViewSyncClient{}, nil, nil)
	defer transport.Close()
	registry, err := coordview.RecoverShardViewRegistry(t.Context(), &fakeQueryViewCatalog{}, transport)
	require.NoError(t, err)
	defer registry.Close()
	c := cache.New(balancer.DefaultBalanceConfig())
	c.PublishDataView(100, api.PrepareCollectionDataView(&api.CollectionDataView{CollectionID: 100, ShardSetEpoch: 3, Shards: []*api.ShardDataView{{VChannel: "t1"}, {VChannel: "t2"}}}))
	b := &handoverTestBackend{task: &datapb.SplitShardTask{TaskId: 1, CollectionId: 100, SourceVchannel: "s", State: datapb.SplitShardTaskState_SplitShardTaskAdopted}}
	r := &qviewsRuntime{splitHandover: &splitHandover{backend: b}, loadConfigStore: cfg, balanceCache: c, shardViewRegistry: registry, balancer: &fakeRuntimeBalancer{}, resources: &qvresource.Resources{}}
	release, err := qvresource.AcquireSplitLifecycle(t.Context(), 100)
	require.NoError(t, err)
	defer release()
	require.NoError(t, r.finishSplitAdoptionBeforeSchema(t.Context(), 100))
	require.Empty(t, b.calls, "schema recovery only completes read publication; it does not run adoption or retirement under another lock")
}

type handoverSilentSyncer struct{}

func (*handoverSilentSyncer) SyncViews(context.Context, syncer.SyncGroup) error { return nil }
func (*handoverSilentSyncer) Close() error                                      { return nil }

func TestLoadedPendingAlterCanPlanRecoveredG2BeforePreparingNewSchema(t *testing.T) {
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	cfg := &loadmgr.LoadConfig{CollectionID: 100, PartitionIDs: []int64{10}, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 1, ResourceGroup: "rg1"}}}
	require.NoError(t, store.Put(t.Context(), cfg))
	reg, err := coordview.RecoverShardViewRegistry(t.Context(), &fakeQueryViewCatalog{}, &handoverSilentSyncer{})
	require.NoError(t, err)
	defer reg.Close()
	c := cache.New(balancer.DefaultBalanceConfig())
	c.PublishLoadConfig(100, cfg, store.GetConfigVersion(100))
	native := api.PrepareCollectionDataView(&api.CollectionDataView{CollectionID: 100, SchemaVersion: 1, ShardSetEpoch: 3, DataVersion: qviews.DataVersion{StreamingVersion: 1, CompactVersion: 3}, Shards: []*api.ShardDataView{
		{VChannel: "p1_100v1", GrowingTransformStartAfterTimetick: 100, CompactedTransformStartAfterTimetick: 100}, {VChannel: "p2_100v2", GrowingTransformStartAfterTimetick: 100, CompactedTransformStartAfterTimetick: 100},
	}})
	c.PublishDataView(100, native)
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	detach := reg.RegisterPublicationListener(c.PublishShard)
	defer detach()
	c.MarkReady()
	overrides := &sync.Map{}
	provider := withSplitServingSchema(func(context.Context, int64) (int32, error) { return 2, nil }, overrides)
	controller := balancer.NewDefaultBalancer(c, reg, nil, provider)
	controller.Trigger(balancer.TriggerScope{DirtyCollections: []int64{100}})
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Empty(t, reg.ShardIDs(), "pending S1 gates ordinary planning of adopted DataView S0")
	backend := &handoverTestBackend{task: &datapb.SplitShardTask{TaskId: 1, CollectionId: 100, SourceVchannel: "p0_100v0", State: datapb.SplitShardTaskState_SplitShardTaskAdopted}}
	r := &qviewsRuntime{splitServingSchemas: overrides, splitHandover: &splitHandover{backend: backend}, loadConfigStore: store, balanceCache: c, shardViewRegistry: reg, balancer: controller, resources: &qvresource.Resources{DataViews: newRuntimeDataViewProvider(t)}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	release, err := qvresource.AcquireSplitLifecycle(ctx, 100)
	require.NoError(t, err)
	defer release()
	done := make(chan error, 1)
	go func() { done <- r.finishSplitAdoptionBeforeSchema(ctx, 100) }()
	require.Eventually(t, func() bool { _, ok := overrides.Load(int64(100)); return ok }, time.Second, time.Millisecond)
	controller.Trigger(balancer.TriggerScope{DirtyCollections: []int64{100}})
	require.NoError(t, controller.Reconcile(t.Context()))
	for _, channel := range []string{"p1_100v1", "p2_100v2"} {
		manager := reg.Get(qviews.ShardID{ReplicaID: 1, VChannel: channel})
		require.NotNil(t, manager, "old g2 can now be prepared despite the pending ALTER")
		require.EqualValues(t, 1, manager.Stats().PreparingSchemaVersion)
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	_, ok := overrides.Load(int64(100))
	require.False(t, ok, "normal target-schema gating resumes when recovery ends")
}

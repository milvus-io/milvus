package adaptor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type queryShutdownTestResMgr struct {
	mu        sync.Mutex
	acquired  map[qviews.QueryViewKey]snview.AcquireResource
	onRelease func(snview.ReleaseResource)
}

func newQueryShutdownTestResMgr(onRelease func(snview.ReleaseResource)) *queryShutdownTestResMgr {
	return &queryShutdownTestResMgr{
		acquired:  make(map[qviews.QueryViewKey]snview.AcquireResource),
		onRelease: onRelease,
	}
}

func (m *queryShutdownTestResMgr) Acquire(req snview.AcquireResource) {
	m.mu.Lock()
	m.acquired[req.Key] = req
	m.mu.Unlock()
}

func (m *queryShutdownTestResMgr) Release(req snview.ReleaseResource) {
	if m.onRelease != nil {
		m.onRelease(req)
	}
	go req.OnDropped()
}

func (m *queryShutdownTestResMgr) getAcquired(key qviews.QueryViewKey) (snview.AcquireResource, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.acquired[key]
	return req, ok
}

type failingQueryViewMetaKV struct {
	kv.MetaKv
	entered chan struct{}
	once    sync.Once
}

func (f *failingQueryViewMetaKV) MultiSaveAndRemove(
	ctx context.Context,
	_ map[string]string,
	_ []string,
	_ ...predicates.Predicate,
) error {
	f.once.Do(func() {
		close(f.entered)
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	return merr.WrapErrIoFailedReason("etcd unavailable")
}

func newQueryShutdownTestView(vchannel string, replicaID int64, state viewpb.QueryViewState) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1,
			ReplicaId:    replicaID,
			Vchannel:     vchannel,
			State:        state,
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 1},
				QueryVersion: 1,
			},
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
}

func newQueryShutdownTestWAL(
	t *testing.T,
	catalog metastore.StreamingNodeCataLog,
	resMgr snview.StreamingNodeResourceManager,
	views []*viewpb.QueryViewOfShard,
) *walAdaptorImpl {
	t.Helper()
	paramtable.Init()
	resource.InitForTest(t)

	walImpls := newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return walimplstest.NewTestMessageID(1), nil
	})
	roWAL := adaptImplsToROWAL(walImpls, func() {})
	param, err := buildInterceptorParams(context.Background(), walImpls, nil)
	require.NoError(t, err)
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Close().Return()
	param.ShardManager = shardManager

	w := adaptImplsToRWWAL(roWAL, nil, param)
	w.queryViewHandler = snview.RecoverPChannelSNQueryViewHandler(
		roWAL.availableCtx,
		walImpls.Channel().Name,
		catalog,
		resMgr,
		views,
	)
	return w
}

func TestWALCloseWakesRecoveryWaitersBeforeResourceRelease(t *testing.T) {
	mockey.PatchConvey("close wakes recovery waiters before lifetime drain and resource release", t, func() {
		var acquireExited atomic.Bool
		var released atomic.Int32
		resMgr := newQueryShutdownTestResMgr(func(snview.ReleaseResource) {
			require.True(t, acquireExited.Load(), "query plan waiter must exit before handoff releases view resources")
			released.Add(1)
		})
		recovered := newQueryShutdownTestView("first-time-tick-test_1v0", 1, viewpb.QueryViewState_QueryViewStateUp)
		w := newQueryShutdownTestWAL(t, nil, resMgr, []*viewpb.QueryViewOfShard{recovered})

		entered := make(chan struct{})
		var enterOnce sync.Once
		var orig func(*snview.SNQueryViewHandler, context.Context, qviews.ShardID, int32) (*snview.QueryViewLease, error)
		mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Origin(&orig).To(
			func(h *snview.SNQueryViewHandler, ctx context.Context, shardID qviews.ShardID, schema int32) (*snview.QueryViewLease, error) {
				enterOnce.Do(func() { close(entered) })
				lease, err := orig(h, ctx, shardID, schema)
				acquireExited.Store(true)
				return lease, err
			},
		).Build()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		planErr := make(chan error, 1)
		go func() {
			_, err := w.GetQueryPlan(ctx, &viewpb.GetQueryPlanRequest{
				ShardId: qviews.NewShardIDFromQVMeta(recovered.GetMeta()).IntoProto(),
				Mvcc:    &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 1}},
				Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{CollectionID: 1}},
			})
			planErr <- err
		}()
		waitFenceTestSignal(t, entered)
		// Let AcquireUpViewAtSchema park on the shard recovery channel.
		time.Sleep(20 * time.Millisecond)

		start := time.Now()
		w.Close()
		elapsed := time.Since(start)
		require.Less(t, elapsed, 2*time.Second, "recovery waiter should wake without waiting for the 3s force-cancel timeout")

		err := waitFenceTestError(t, planErr)
		viewErr := viewerror.AsViewError(err)
		require.NotNil(t, viewErr)
		require.True(t, viewErr.IsOnShutdown(), "%v", err)
		require.True(t, viewErr.IsRetryable(), "%v", err)
		require.EqualValues(t, 1, released.Load())
	})
}

func TestWALCloseArmsForceCancelBeforeWaitingForShardLocks(t *testing.T) {
	mockey.PatchConvey("stalled SaveQueryViews under shard lock cannot prevent forced cancellation", t, func() {
		metaKV := &failingQueryViewMetaKV{entered: make(chan struct{})}
		catalog := streamingnode.NewCataLog(metaKV)
		var released atomic.Int32
		resMgr := newQueryShutdownTestResMgr(func(snview.ReleaseResource) {
			released.Add(1)
		})

		recovered := newQueryShutdownTestView("first-time-tick-test_1v1", 1, viewpb.QueryViewState_QueryViewStateUp)
		w := newQueryShutdownTestWAL(t, catalog, resMgr, []*viewpb.QueryViewOfShard{recovered})

		// Prepare a second shard and stall its Up persistence inside ReliableWriteMetaKv
		// while holding that shard's mutex.
		preparingProto := newQueryShutdownTestView("first-time-tick-test_1v0", 1, viewpb.QueryViewState_QueryViewStatePreparing)
		preparingView := qviews.NewFullQueryViewAtStreamingNode(preparingProto.GetMeta(), preparingProto.GetStreamingNode(), nil)
		w.queryViewHandler.ApplyViews([]worknodehandler.ApplyView{{View: preparingView}})
		acquired, ok := resMgr.getAcquired(preparingView.QueryViewKey())
		require.True(t, ok)
		acquired.OnReady()

		applyDone := make(chan struct{})
		go func() {
			defer close(applyDone)
			upProto := newQueryShutdownTestView("first-time-tick-test_1v0", 1, viewpb.QueryViewState_QueryViewStateUp)
			upView := qviews.NewFullQueryViewAtStreamingNode(upProto.GetMeta(), upProto.GetStreamingNode(), nil)
			w.queryViewHandler.ApplyViews([]worknodehandler.ApplyView{{View: upView}})
		}()
		waitFenceTestSignal(t, metaKV.entered)

		entered := make(chan struct{})
		var enterOnce sync.Once
		var orig func(*snview.SNQueryViewHandler, context.Context, qviews.ShardID, int32) (*snview.QueryViewLease, error)
		mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Origin(&orig).To(
			func(h *snview.SNQueryViewHandler, ctx context.Context, shardID qviews.ShardID, schema int32) (*snview.QueryViewLease, error) {
				enterOnce.Do(func() { close(entered) })
				return orig(h, ctx, shardID, schema)
			},
		).Build()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		planErr := make(chan error, 1)
		go func() {
			_, err := w.GetQueryPlan(ctx, &viewpb.GetQueryPlanRequest{
				ShardId: qviews.NewShardIDFromQVMeta(recovered.GetMeta()).IntoProto(),
				Mvcc:    &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 1}},
				Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{CollectionID: 1}},
			})
			planErr <- err
		}()
		waitFenceTestSignal(t, entered)

		closeDone := make(chan struct{})
		go func() {
			defer close(closeDone)
			w.Close()
		}()

		select {
		case <-closeDone:
		case <-time.After(8 * time.Second):
			w.availableCancel()
			t.Fatal("WAL Close hung waiting for shard mutex before arming forced cancellation")
		}
		waitFenceTestSignal(t, applyDone)

		err := waitFenceTestError(t, planErr)
		viewErr := viewerror.AsViewError(err)
		require.NotNil(t, viewErr)
		require.True(t, viewErr.IsOnShutdown(), "%v", err)
		require.EqualValues(t, 2, released.Load())
	})
}

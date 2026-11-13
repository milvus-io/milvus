package vchannel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type prewarmModule struct {
	warmed chan *viewpb.QueryViewMeta
}

func (m *prewarmModule) NewRuntime() (queryresource.QueryRuntimeModule, error)       { return m, nil }
func (*prewarmModule) Prepare(context.Context, walview.VChannelWALView) error        { return nil }
func (*prewarmModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {}
func (*prewarmModule) Advance(qviews.DataVersion)                                    {}
func (*prewarmModule) Close()                                                        {}
func (m *prewarmModule) BindView(meta *viewpb.QueryViewMeta) queryresource.QueryViewResource {
	return &prewarmResource{module: m, meta: meta}
}

type prewarmResource struct {
	module *prewarmModule
	meta   *viewpb.QueryViewMeta
}

func (*prewarmResource) Prepare(context.Context, queryresource.LoadInfoProvider) error { return nil }
func (*prewarmResource) Release()                                                      {}
func (r *prewarmResource) Prewarm(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.module.warmed <- r.meta
	return nil
}

func TestPChannelRecoveryManagerPrewarm(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := queryresource.NewDispatcher(1)
	defer dispatcher.Close()
	warmed := make(chan *viewpb.QueryViewMeta, 1)
	resources := queryresource.NewManager(queryresource.Config{
		Scheduler: scheduler, Dispatcher: dispatcher,
		Builders: []queryresource.QueryRuntimeModuleBuilder{&prewarmModule{warmed: warmed}},
	})
	defer resources.Close()
	manager := &PChannelRecoveryManager{modules: typeutil.NewConcurrentMap[string, *VChannelRecoveryModule]()}
	manager.modules.Insert("v1", &VChannelRecoveryModule{queryResources: resources})
	// This is the concrete manager injected into SNQueryViewHandler in production.
	var warmer interface {
		Prewarm(context.Context, qviews.QueryViewKey) error
	} = manager
	meta := &viewpb.QueryViewMeta{
		Vchannel: "v1", ReplicaId: 1,
		Version: &viewpb.QueryViewVersion{QueryVersion: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}},
	}
	key := qviews.QueryViewKey{ShardID: qviews.NewShardIDFromQVMeta(meta), QueryViewVersion: qviews.FromProtoQueryViewVersion(meta.Version)}
	ready := make(chan struct{})
	resources.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { close(ready) }},
		func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) { return walview.VChannelWALView{}, true })
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("view not ready")
	}
	require.Empty(t, warmed, "preparing a View must not trigger prewarm")
	require.NoError(t, warmer.Prewarm(t.Context(), key))
	require.True(t, proto.Equal(meta, <-warmed))
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, warmer.Prewarm(canceled, key), context.Canceled)
	require.Empty(t, warmed)
	missing := key
	missing.QueryViewVersion.QueryVersion++
	require.True(t, viewerror.AsViewError(warmer.Prewarm(t.Context(), missing)).IsViewInvalidated())
	missing = key
	missing.ShardID.VChannel = "missing"
	require.True(t, viewerror.AsViewError(warmer.Prewarm(t.Context(), missing)).IsViewInvalidated())
}

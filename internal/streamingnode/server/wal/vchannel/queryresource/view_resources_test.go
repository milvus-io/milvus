package queryresource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func awaitResourceSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("resource lifecycle did not complete")
	}
}

func TestManagerViewResourcesDrainBeforeDropAndRejectRetiredTasks(t *testing.T) {
	paramtable.Init()
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	manager := NewManager(Config{Scheduler: scheduler, Dispatcher: dispatcher, Builders: []QueryRuntimeModuleBuilder{readSchemaBuilder{}}})
	defer manager.Close()
	schema := mock_segcore.GenTestCollectionSchema("owned-read-schema", schemapb.DataType_Int64, false)
	view := walview.VChannelWALView{CollectionID: 7881, Schema: schema}
	view.CaptureQueryView = func(meta *viewpb.QueryViewMeta, fn func(walview.VChannelWALView)) bool {
		captured := view
		captured.Schema = proto.Clone(schema).(*schemapb.CollectionSchema)
		captured.Schema.Version = meta.GetSchemaVersion()
		fn(captured)
		return true
	}
	build := func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) { return view, true }
	first, keyA := testManagerQueryViewMetaAndKey(1)
	second, keyB := testManagerQueryViewMetaAndKey(1)
	second.Version.QueryVersion = 2
	keyB.QueryViewVersion.QueryVersion = 2
	first.CollectionId, second.CollectionId = 7881, 7881
	first.SchemaVersion, second.SchemaVersion = 1, 2
	readyA, readyB := make(chan struct{}), make(chan struct{})
	manager.AcquireLocked(snview.AcquireResource{Key: keyA, Meta: first, OnReady: func() { close(readyA) }}, build)
	awaitResourceSignal(t, readyA)
	manager.AcquireLocked(snview.AcquireResource{Key: keyB, Meta: second, OnReady: func() { close(readyB) }}, build)
	awaitResourceSignal(t, readyB)
	old, ok := manager.ReadRuntime(keyA)
	require.True(t, ok)
	live, ok := manager.ReadRuntime(keyB)
	require.True(t, ok)
	_, err := old.GlobalOptimizer().OptimizeSearch(t.Context(), &internalpb.SearchRequest{CollectionID: 7881})
	require.NoError(t, err)
	manager.mu.Lock()
	stale := resourceReadyTask{manager: manager, key: keyA, ref: manager.refs[keyA], runtime: manager.runtime, onReady: func() { t.Error("retired task published readiness") }}
	manager.mu.Unlock()
	workCtx, end, err := old.resources.begin(t.Context())
	require.NoError(t, err)
	end = sync.OnceFunc(end)
	defer end()
	dropped := make(chan struct{})
	manager.Release(snview.ReleaseResource{Key: keyA, OnDropped: func() { close(dropped) }})
	_, err = old.GlobalOptimizer().OptimizeSearch(t.Context(), &internalpb.SearchRequest{CollectionID: 7881})
	require.ErrorIs(t, err, context.Canceled, "retired View must reject new planning borrows")
	awaitResourceSignal(t, workCtx.Done())
	require.ErrorIs(t, workCtx.Err(), context.Canceled)
	require.Never(t, func() bool {
		select {
		case <-dropped:
			return true
		default:
			return false
		}
	}, 20*time.Millisecond, time.Millisecond)
	require.Equal(t, 1, old.schema.refs, "operation keeps native schema alive while unwinding")
	// Re-register the same key while the retired operation still owns its guard.
	replacementReady := make(chan struct{})
	manager.AcquireLocked(snview.AcquireResource{Key: keyA, Meta: first, OnReady: func() { close(replacementReady) }}, build)
	awaitResourceSignal(t, replacementReady)
	require.NoError(t, stale.Execute(t.Context()))
	end()
	awaitResourceSignal(t, dropped)
	require.Equal(t, 1, old.schema.refs, "the new registration shares the schema and owns its own guard")
	replacement, ok := manager.ReadRuntime(keyA)
	require.True(t, ok)
	require.NotSame(t, old.resources, replacement.resources)
	secondDrop := make(chan struct{})
	manager.Release(snview.ReleaseResource{Key: keyA, OnDropped: func() { close(secondDrop) }})
	awaitResourceSignal(t, secondDrop)
	require.Zero(t, old.schema.refs, "A's exclusive schema is reclaimed while B remains ready")
	require.Equal(t, 1, live.schema.refs)
	_, ok = manager.ReadRuntime(keyB)
	require.True(t, ok)
	// Final release must cancel borrowed work before closing the whole runtime.
	finalCtx, finish, err := live.resources.begin(t.Context())
	require.NoError(t, err)
	finish = sync.OnceFunc(finish)
	defer finish()
	finalDrop := make(chan struct{})
	manager.Release(snview.ReleaseResource{Key: keyB, OnDropped: func() { close(finalDrop) }})
	awaitResourceSignal(t, finalCtx.Done())
	require.ErrorIs(t, finalCtx.Err(), context.Canceled)
	require.Never(t, func() bool {
		select {
		case <-finalDrop:
			return true
		default:
			return false
		}
	}, 20*time.Millisecond, time.Millisecond)
	finish()
	awaitResourceSignal(t, finalDrop)
	require.Zero(t, live.schema.refs)
}

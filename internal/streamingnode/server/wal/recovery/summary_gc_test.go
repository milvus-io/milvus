package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestSummaryGCWorkersProgressWithoutPersistenceOrTraffic(t *testing.T) {
	ctx := context.Background()
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := walsummary.NewStore(cm, "gc-worker-pchannel", 1)
	manager := walsummary.NewManager(walsummary.ManagerConfig{
		PChannel: store.PChannel(), Term: store.Term(), Store: store,
		Runtime:       moduleapi.Runtime{Scheduler: immediateTaskScheduler{}},
		FlushMaxBytes: 1, GCInterval: 10 * time.Millisecond, OrphanGCInterval: 20 * time.Millisecond,
	})
	require.NoError(t, manager.Restore(ctx))
	manager.SetDataViewRetention("test-vchannel", 100)
	manager.ObserveMessage(ctx, newRecoveryTestDeleteMessage(t, "test-vchannel", 50))
	require.Len(t, manager.Manifest().Chunks, 1)
	orphan := walsummary.NewStore(cm, store.PChannel(), 0).ChunkKey(0)
	require.NoError(t, cm.Write(ctx, orphan, []byte("unreferenced")))
	rs.summaryManager = manager
	rs.startSummaryWorkers()
	t.Cleanup(func() { rs.backgroundTaskNotifier.Cancel(); rs.summaryWG.Wait(); rs.closeRecoveryResources() })
	require.Eventually(t, func() bool {
		exists, err := cm.Exist(ctx, orphan)
		return err == nil && !exists && len(manager.Manifest().Chunks) == 0
	}, 3*time.Second, time.Millisecond, "both GC policies must progress without a checkpoint or new writes")
	rs.backgroundTaskNotifier.Cancel()
	stopped := make(chan struct{})
	go func() { rs.summaryWG.Wait(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Summary workers did not stop")
	}
}

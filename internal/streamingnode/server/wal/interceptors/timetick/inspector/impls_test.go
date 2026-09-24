package inspector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMaybeForcePersistedSync(t *testing.T) {
	paramtable.Init()
	// A one-minute sync period: the first sync is persisted immediately, a
	// second one within the period is skipped, and one after the period is
	// persisted again.
	require.NoError(t, paramtable.Get().Save("dataNode.segment.syncPeriod", "60"))
	t.Cleanup(func() {
		require.NoError(t, paramtable.Get().Save("dataNode.segment.syncPeriod", "600"))
	})

	i := &timeTickSyncInspectorImpl{
		taskNotifier:      syncutil.NewAsyncTaskNotifier[struct{}](),
		syncNotifier:      newSyncNotifier(),
		operators:         typeutil.NewConcurrentMap[string, TimeTickSyncOperator](),
		lastPersistedSync: typeutil.NewConcurrentMap[string, time.Time](),
	}
	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	i.operators.Insert("test", operator)

	persistedCalled := make(chan bool, 4)
	operator.EXPECT().Sync(mock.Anything, mock.Anything).Run(func(_ context.Context, persisted bool) {
		persistedCalled <- persisted
	}).Times(2)

	now := time.Now()
	// First call: no record yet, so a persisted sync must be triggered.
	i.maybeForcePersistedSync("test", now)
	assert.True(t, <-persistedCalled, "first sync should be persisted")

	// Second call within the interval: no new sync.
	i.maybeForcePersistedSync("test", now.Add(30*time.Second))
	select {
	case <-persistedCalled:
		t.Fatal("unexpected sync within the sync period")
	default:
	}

	// Interval elapsed: trigger again.
	i.maybeForcePersistedSync("test", now.Add(2*time.Minute))
	assert.True(t, <-persistedCalled, "sync after the sync period should be persisted")

	// The recorded time is refreshed only when a sync is triggered.
	last, ok := i.lastPersistedSync.Get("test")
	assert.True(t, ok)
	assert.Equal(t, now.Add(2*time.Minute), last)
}

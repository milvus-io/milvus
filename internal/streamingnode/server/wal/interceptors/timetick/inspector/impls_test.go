package inspector

import (
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

	operator.EXPECT().Sync(mock.Anything, true).Times(2)
	waitForSync := func() {
		t.Helper()
		// A notification from inside Sync can arrive before asyncSync releases
		// the working slot. Wait for that cleanup before requesting another sync.
		require.Eventually(t, func() bool {
			return !i.working.Contain("test")
		}, 10*time.Second, time.Millisecond, "sync should release its working slot")
	}

	now := time.Now()
	// First call: no record yet, so a persisted sync must be triggered.
	require.True(t, i.maybeForcePersistedSync("test", now), "first sync should be launched")
	waitForSync()

	// Second call within the interval: no new sync.
	require.False(t, i.maybeForcePersistedSync("test", now.Add(30*time.Second)), "no sync within the sync period")

	// Interval elapsed: trigger again.
	require.True(t, i.maybeForcePersistedSync("test", now.Add(2*time.Minute)), "sync after the sync period should be launched")
	waitForSync()

	// The recorded time is refreshed only when a sync is triggered.
	last, ok := i.lastPersistedSync.Get("test")
	assert.True(t, ok)
	assert.Equal(t, now.Add(2*time.Minute), last)
}

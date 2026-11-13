package broadcaster

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestBroadcastTasksShareLoggerAndKeepMessageFields(t *testing.T) {
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "info", DisableTimestamp: true})
	logger := mlog.With(mlog.FieldComponent("broadcaster"))
	metrics := newBroadcasterMetrics()
	bm := &broadcastTaskManager{mu: &sync.Mutex{}, tasks: make(map[uint64]*broadcastTask)}
	for _, id := range []uint64{101, 202} {
		task := newBroadcastTaskFromProto(createTruncateBroadcastTask(id, []string{"v1"}), metrics, nil)
		task.SetLogger(logger)
		// Recovery must not retain a separately encoded logger for each task.
		require.Same(t, logger, task.Logger())
		bm.tasks[id] = task
		require.NoError(t, bm.LegacyAck(t.Context(), id, "missing"))
	}
	logger.Info(t.Context(), "shared logger")

	lines := strings.Split(strings.TrimSpace(sink.String()), "\n")
	require.Len(t, lines, 3)
	require.Contains(t, lines[0], "broadcastID=101")
	require.NotContains(t, lines[0], "broadcastID=202")
	require.Contains(t, lines[1], "broadcastID=202")
	require.NotContains(t, lines[1], "broadcastID=101")
	require.NotContains(t, lines[2], "broadcastID")
}

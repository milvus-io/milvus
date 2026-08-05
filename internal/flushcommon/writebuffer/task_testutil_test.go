package writebuffer

import (
	"context"

	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
)

// runSyncTaskInline runs both phases the way the dispatcher does, for tests that
// stub the sync manager and only need the task's end-to-end effect.
func runSyncTaskInline(ctx context.Context, task syncmgr.Task) error {
	if err := task.Prepare(ctx); err != nil {
		return err
	}
	return task.Commit(ctx)
}

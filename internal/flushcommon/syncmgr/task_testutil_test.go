package syncmgr

import "context"

// runTaskForTest runs both phases the way the dispatcher does, for tests that
// only care about the end-to-end effect of a single attempt.
func runTaskForTest(ctx context.Context, task Task) error {
	if err := task.Prepare(ctx); err != nil {
		return err
	}
	return task.Commit(ctx)
}

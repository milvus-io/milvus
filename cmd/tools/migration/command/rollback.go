package command

import (
	"context"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	"github.com/milvus-io/milvus/cmd/tools/migration/console"
	"github.com/milvus-io/milvus/cmd/tools/migration/migration"
)

func Rollback(c *configs.Config) {
	ctx := context.Background()
	runner := migration.NewRunner(ctx, c)
	console.ExitIf(runner.CheckSessions())
	console.ExitIf(runner.RegisterSession())
	fn := func() { runner.Stop() }
	defer fn()
	// double check.
	console.ExitIf(runner.CheckSessions(), console.AddCallbacks(fn))
	console.ExitIf(runner.Rollback(), console.AddCallbacks(fn))
}

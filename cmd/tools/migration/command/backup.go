package command

import (
	"context"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	"github.com/milvus-io/milvus/cmd/tools/migration/console"
	"github.com/milvus-io/milvus/cmd/tools/migration/migration"
)

func Backup(c *configs.Config) {
	ctx := context.Background()
	runner := migration.NewRunner(ctx, c)
	console.ExitIf(runner.CheckSessions())
	console.ExitIf(runner.RegisterSession())
	defer runner.Stop()
	// double check.
	console.ExitIf(runner.CheckSessions())
	console.ExitIf(runner.Backup())
}

package command

import (
	"context"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"

	"github.com/milvus-io/milvus/cmd/tools/migration/console"

	"github.com/milvus-io/milvus/cmd/tools/migration/migration"
)

func Run(c *configs.Config) {
	ctx := context.Background()
	runner := migration.NewRunner(ctx, c)
	console.AbnormalExitIf(runner.CheckSessions(), false)
	console.AbnormalExitIf(runner.RegisterSession(), false)
	defer runner.Stop()
	// double check.
	console.AbnormalExitIf(runner.CheckSessions(), false)
	console.AbnormalExitIf(runner.Validate(), false)
	console.NormalExitIf(runner.CheckCompatible(), "version compatible, no need to migrate")
	if c.RunWithBackup {
		console.AbnormalExitIf(runner.Backup(), false)
	} else {
		console.Warning("run migration without backup!")
	}
	console.AbnormalExitIf(runner.Migrate(), true)
}

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
	fn := func() { runner.Stop() }
	defer fn()
	// double check.
	console.AbnormalExitIf(runner.CheckSessions(), false, console.AddCallbacks(fn))
	console.AbnormalExitIf(runner.Validate(), false, console.AddCallbacks(fn))
	console.NormalExitIf(runner.CheckCompatible(), "version compatible, no need to migrate", console.AddCallbacks(fn))
	if c.RunWithBackup {
		console.AbnormalExitIf(runner.Backup(), false, console.AddCallbacks(fn))
	} else {
		console.Warning("run migration without backup!")
	}
	console.AbnormalExitIf(runner.Migrate(), true, console.AddCallbacks(fn))
}

package command

import (
	"flag"
	"fmt"
	"os"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	"github.com/milvus-io/milvus/cmd/tools/migration/console"
)

func Execute(args []string) {
	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, usageLineV2)
	}

	c := &commandParser{}
	c.format(args, flags)

	console.ErrorExitIf(c.configYaml == "", false, "config not set")

	cfg := configs.NewConfig(c.configYaml)
	switch cfg.Cmd {
	case configs.RunCmd:
		Run(cfg)
	case configs.BackupCmd:
		Backup(cfg)
	case configs.RollbackCmd:
		Rollback(cfg)
	default:
		console.AbnormalExit(false, fmt.Sprintf("cmd not set or not supported: %s", cfg.Cmd))
	}
}

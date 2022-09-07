package main

import (
	"context"
	"flag"

	"github.com/milvus-io/milvus/cmd/tools/migration/migration"
)

type dryRun struct {
	config
}

func (r *dryRun) execute(args []string, flags *flag.FlagSet) {
	r.format(args, flags)
	DryRun(r.sourceYaml, r.targetYaml, r.sourceVersion, r.targetVersion)
}

func DryRun(sourceYaml string, targetYaml string, sourceVersion string, targetVersion string) {
	ctx := context.Background()

	definition, err := migration.ParseToMigrationDef(sourceYaml, targetYaml, sourceVersion, targetVersion)
	if err != nil {
		panic(err)
	}

	runner := migration.NewRunner(ctx, definition)
	runner.DryRun()
}

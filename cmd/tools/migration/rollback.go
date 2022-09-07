package main

import (
	"context"
	"flag"

	"github.com/milvus-io/milvus/cmd/tools/migration/migration"
)

type rollback struct {
	config
}

func (r *rollback) execute(args []string, flags *flag.FlagSet) {
	r.format(args, flags)
	Rollback(r.sourceYaml, r.targetYaml, r.sourceVersion, r.targetVersion)
}

func Rollback(sourceYaml string, targetYaml string, sourceVersion string, targetVersion string) {
	ctx := context.Background()

	definition, err := migration.ParseToMigrationDef(sourceYaml, targetYaml, sourceVersion, targetVersion)
	if err != nil {
		panic(err)
	}

	runner := migration.NewRunner(ctx, definition)

	if err := runner.CheckSessions(); err != nil {
		panic(err)
	}

	if err := runner.RegisterSession(); err != nil {
		panic(err)
	}

	defer runner.Stop()

	// double check.
	if err := runner.CheckSessions(); err != nil {
		panic(err)
	}

	if err := runner.CleanTarget(); err != nil {
		panic(err)
	}

	if err := runner.Rollback(); err != nil {
		panic(err)
	}
}

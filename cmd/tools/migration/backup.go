package main

import (
	"context"
	"flag"

	"github.com/milvus-io/milvus/cmd/tools/migration/migration"
)

type backup struct {
	config
}

func (r *backup) execute(args []string, flags *flag.FlagSet) {
	r.formatSourceYaml(args, flags)
	r.formatSourceVersion(args, flags)
	Backup(r.sourceYaml, r.sourceVersion)
}

func Backup(sourceYaml string, sourceVersion string) {
	ctx := context.Background()

	// ugly here, since we didn't need target.
	definition, err := migration.ParseToMigrationDef(sourceYaml, sourceYaml, sourceVersion, sourceVersion)
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

	if err := runner.Backup(); err != nil {
		panic(err)
	}
}

package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/milvus-io/milvus/cmd/tools/migration/migration"
)

type run struct {
	config
}

func (r *run) execute(args []string, flags *flag.FlagSet) {
	r.format(args, flags)
	Run(r.sourceYaml, r.targetYaml, r.sourceVersion, r.targetVersion)
}

func Run(sourceYaml, targetYaml string, sourceVersion, targetVersion string) {
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

	if err := runner.Validate(); err != nil {
		panic(err)
	}

	if runner.CheckCompatible() {
		fmt.Println("version compatible, no need to migrate")
		return
	}

	if err := runner.Backup(); err != nil {
		panic(err)
	}

	if err := runner.Load(); err != nil {
		fmt.Println(err)
		if err := runner.CleanBackup(); err != nil {
			panic(err)
		}
		return
	}

	if err := runner.Migrate(); err != nil {
		fmt.Println(err)
		if err := runner.CleanTarget(); err != nil {
			panic(err)
		}
		if err := runner.Rollback(); err != nil {
			panic(err)
		}
		if err := runner.CleanBackup(); err != nil {
			panic(err)
		}
		return
	}
}
